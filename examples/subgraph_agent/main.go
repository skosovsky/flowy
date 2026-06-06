// Package main demonstrates suspend/resume when parent executes a subgraph node.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type childState struct {
	Approved bool
	Steps    []string
}

type parentState struct {
	Child childState
	Log   []string
}

func main() {
	cp := testutil.NewMemoryCheckpointer[parentState, flowy.NoEffect]()

	subgraph, err := flowy.NewGraph[childState, flowy.NoEffect](
		func(_ childState, u childState) childState { return u },
	).
		AddNode("gate", func(_ context.Context, s childState) (childState, flowy.Directive, error) {
			s.Steps = append(s.Steps, "gate")
			if !s.Approved {
				return s, flowy.Suspend("waiting_subgraph_approval"), nil
			}
			return s, flowy.Completed(), nil
		}).
		AddNode("done", func(_ context.Context, s childState) (childState, flowy.Directive, error) {
			s.Steps = append(s.Steps, "done")
			return s, flowy.End(), nil
		}).
		AddEdge("gate", "done").
		AllowNoOutgoingRoute("done").
		SetEntryPoint("gate").
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	parent, err := flowy.NewGraph[parentState, flowy.NoEffect](
		func(_ parentState, u parentState) parentState { return u },
	).
		AddNode("start", func(_ context.Context, s parentState) (parentState, flowy.Directive, error) {
			s.Log = append(s.Log, "parent_start")
			return s, flowy.Completed(), nil
		}).
		AddNode("subgraph", flowy.SubgraphNode(
			subgraph,
			func(s parentState) childState { return s.Child },
			func(s parentState, child childState) parentState {
				s.Child = child
				return s
			},
		)).
		AddNode("finalize", func(_ context.Context, s parentState) (parentState, flowy.Directive, error) {
			s.Log = append(s.Log, "parent_finalize")
			return s, flowy.End(), nil
		}).
		AddEdge("start", "subgraph").
		AddEdge("subgraph", "finalize").
		AllowNoOutgoingRoute("finalize").
		SetEntryPoint("start").
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := parent.NewRunner(cp)
	first, err := runner.Start(context.Background(), "subgraph-thread", parentState{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"phase1 status=%s reason=%q child_steps=%v log=%v\n",
		first.Status,
		first.Reason,
		first.State.Child.Steps,
		first.State.Log,
	)
	if first.Status != flowy.RunStatusSuspended {
		log.Fatalf("expected suspended status, got %s", first.Status)
	}

	second, err := runner.Resume(
		context.Background(),
		first.ResumeToken,
		flowy.WithStateOverlay[parentState, flowy.NoEffect](parentState{
			Child: childState{Approved: true},
			Log:   []string{"parent_resume"},
		}, func(base, overlay parentState) parentState {
			base.Child.Approved = overlay.Child.Approved
			base.Log = append(base.Log, overlay.Log...)
			return base
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("phase2 status=%s child_steps=%v log=%v\n", second.Status, second.State.Child.Steps, second.State.Log)
}
