// Package main demonstrates conditional routing (cache hit/miss) and late binding via WithStateOverlay.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type routeState struct {
	Query        string
	Answer       string
	AllowedTools []string
	SkipPrepare  bool
	PrepareRuns  int
}

func routeResumeTargetPolicy(
	_ context.Context,
	state routeState,
	currentPtr flowy.ExecutionPointer,
) (routeState, flowy.ResumePlan, error) {
	if slices.Contains(state.AllowedTools, "") {
		return state, flowy.ResumeCurrent(), errors.New("empty tool name")
	}
	if len(state.AllowedTools) > 0 {
		state.AllowedTools = append([]string(nil), state.AllowedTools...)
	}
	if currentPtr == "prepare" && state.SkipPrepare {
		return state, flowy.ResumeTo("check_cache"), nil
	}
	return state, flowy.ResumeCurrent(), nil
}

func main() {
	cache := map[string]string{
		"what is flowy": "Flowy is a directive-based agent runtime for Go.",
	}

	cp := testutil.NewMemoryCheckpointer[routeState, flowy.NoEffect]()
	cacheGraph, err := buildCacheRouter(cache).Compile()
	if err != nil {
		log.Fatal(err)
	}
	cacheRunner := cacheGraph.NewRunner(cp)

	miss, err := cacheRunner.Start(context.Background(), "route-miss", routeState{Query: "explain supervisors"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("miss status=%s answer=%q\n", miss.Status, miss.State.Answer)

	hit, err := cacheRunner.Start(context.Background(), "route-hit", routeState{Query: "what is flowy"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("hit status=%s answer=%q\n", hit.Status, hit.State.Answer)

	lateGraph, err := buildLateBindingRouter(cache).Compile()
	if err != nil {
		log.Fatal(err)
	}
	lateRunner := lateGraph.NewRunner(cp)
	pending, err := lateRunner.Start(context.Background(), "route-late", routeState{Query: "what is flowy"})
	if err != nil {
		log.Fatal(err)
	}
	if pending.Status != flowy.RunStatusSuspended {
		log.Fatalf("expected suspended before late binding, got %s", pending.Status)
	}

	bound, err := lateRunner.Resume(
		context.Background(),
		pending.ResumeToken,
		flowy.WithStateOverlay[routeState, flowy.NoEffect](
			routeState{AllowedTools: []string{"search", "calculator"}},
			func(base, overlay routeState) routeState {
				base.AllowedTools = overlay.AllowedTools
				return base
			},
		),
		flowy.WithResumeTargetPolicy[routeState, flowy.NoEffect](routeResumeTargetPolicy),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("late-bind status=%s answer=%q tools=%v\n", bound.Status, bound.State.Answer, bound.State.AllowedTools)

	rewindRunner := lateGraph.NewRunner(cp)
	pendingRewind, err := rewindRunner.Start(
		context.Background(),
		"route-rewind",
		routeState{Query: "explain supervisors"},
	)
	if err != nil {
		log.Fatal(err)
	}
	if pendingRewind.Status != flowy.RunStatusSuspended {
		log.Fatalf("expected suspended before rewind, got %s", pendingRewind.Status)
	}

	rewound, err := rewindRunner.Resume(
		context.Background(),
		pendingRewind.ResumeToken,
		flowy.WithStateOverlay[routeState, flowy.NoEffect](
			routeState{AllowedTools: []string{"search"}, SkipPrepare: true},
			func(base, overlay routeState) routeState {
				base.AllowedTools = overlay.AllowedTools
				base.SkipPrepare = overlay.SkipPrepare
				return base
			},
		),
		flowy.WithResumeTargetPolicy[routeState, flowy.NoEffect](routeResumeTargetPolicy),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rewind status=%s prepare_runs=%d answer=%q\n",
		rewound.Status, rewound.State.PrepareRuns, rewound.State.Answer)
}

func buildCacheRouter(cache map[string]string) *flowy.GraphBuilder[routeState, flowy.NoEffect] {
	return flowy.NewGraph[routeState, flowy.NoEffect](func(_ routeState, u routeState) routeState { return u }).
		AddNode("check_cache", checkCacheNode(cache)).
		AddNode("heavy_llm", heavyLLMNode).
		AddNode("output", outputNode).
		AddConditionalEdge("check_cache", func(_ context.Context, s routeState) (string, error) {
			if _, ok := cache[s.Query]; ok {
				return "output", nil
			}
			return "heavy_llm", nil
		}, "output", "heavy_llm").
		AddEdge("heavy_llm", "output").
		AllowNoOutgoingRoute("output").
		SetEntryPoint("check_cache")
}

func buildLateBindingRouter(cache map[string]string) *flowy.GraphBuilder[routeState, flowy.NoEffect] {
	return flowy.NewGraph[routeState, flowy.NoEffect](func(_ routeState, u routeState) routeState { return u }).
		AddNode("prepare", func(_ context.Context, s routeState) (routeState, flowy.Directive, error) {
			s.PrepareRuns++
			if len(s.AllowedTools) == 0 {
				return s, flowy.Suspend("await_runtime_config"), nil
			}
			return s, flowy.Completed(), nil
		}).
		AddNode("check_cache", checkCacheNode(cache)).
		AddNode("heavy_llm", heavyLLMNode).
		AddNode("output", outputNode).
		AddEdge("prepare", "check_cache").
		AddConditionalEdge("check_cache", func(_ context.Context, s routeState) (string, error) {
			if _, ok := cache[s.Query]; ok {
				return "output", nil
			}
			return "heavy_llm", nil
		}, "output", "heavy_llm").
		AddEdge("heavy_llm", "output").
		AllowNoOutgoingRoute("output").
		SetEntryPoint("prepare")
}

func checkCacheNode(cache map[string]string) flowy.Node[routeState, flowy.NoEffect] {
	return func(_ context.Context, s routeState) (routeState, flowy.Directive, error) {
		if answer, ok := cache[s.Query]; ok {
			s.Answer = answer
		}
		return s, flowy.Completed(), nil
	}
}

func heavyLLMNode(_ context.Context, s routeState) (routeState, flowy.Directive, error) {
	s.Answer = "generated: " + s.Query
	return s, flowy.Completed(), nil
}

func outputNode(_ context.Context, s routeState) (routeState, flowy.Directive, error) {
	if len(s.AllowedTools) > 0 {
		s.Answer += fmt.Sprintf(" [tools=%v]", s.AllowedTools)
	}
	return s, flowy.End(), nil
}
