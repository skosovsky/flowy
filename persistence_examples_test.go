package flowy_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

//nolint:gocognit // Example shows end-to-end client-side persistence
func ExampleGraph_statefulClientWithCheckpoint() {
	type state struct {
		Text     string `json:"text"`
		Approved bool   `json:"approved"`
	}

	approved := false
	cp := testutil.NewMemoryCheckpointer()
	ser := checkpoint.JSONSerializer[state]{}

	b := flowy.NewGraph[state](func(_, update state) state { return update })
	b.AddNode("process", func(_ context.Context, s state) (state, error) {
		s.Text += "_process"
		return s, nil
	})
	b.AddNode("approve", func(_ context.Context, s state) (state, error) {
		if !approved {
			return s, flowy.ErrSuspend
		}
		s.Text += "_approve"
		s.Approved = true
		return s, nil
	})
	b.AddNode("finish", func(_ context.Context, s state) (state, error) {
		s.Text += "_finish"
		return s, nil
	})
	b.AddEdge("process", "approve")
	b.AddEdge("approve", "finish")
	b.SetEntryPoint("process")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	const threadID = "session-1"
	runID, err := checkpoint.NewSortableID()
	if err != nil {
		panic(err)
	}

	persist := func(step flowy.Step[state]) error {
		raw, marshalErr := ser.Marshal(step.State)
		if marshalErr != nil {
			return marshalErr
		}
		env, encErr := checkpoint.EncodeStateData(raw)
		if encErr != nil {
			return encErr
		}
		id, idErr := checkpoint.NewSortableID()
		if idErr != nil {
			return idErr
		}
		return cp.Save(ctx, checkpoint.Checkpoint{
			ID:        id,
			ThreadID:  threadID,
			RunID:     runID,
			Node:      step.NodeName,
			Next:      step.NextNode,
			StateData: env,
			CreatedAt: time.Now().UTC(),
		})
	}

	var current state
	for step, streamErr := range graph.Stream(ctx, "", state{Text: "init"}) {
		if streamErr != nil {
			if errors.Is(streamErr, flowy.ErrSuspend) {
				if perr := persist(step); perr != nil {
					panic(perr)
				}
				break
			}
			panic(streamErr)
		}
		if perr := persist(step); perr != nil {
			panic(perr)
		}
		current = step.State
	}

	loaded, err := cp.LoadLatest(ctx, threadID)
	if err != nil {
		panic(err)
	}
	raw, decodeErr := checkpoint.DecodeStateData(loaded.StateData)
	if decodeErr != nil {
		panic(decodeErr)
	}
	var st state
	if unmarshalErr := ser.Unmarshal(raw, &st); unmarshalErr != nil {
		panic(unmarshalErr)
	}

	approved = true
	for step, streamErr2 := range graph.Stream(ctx, loaded.Next, st) {
		if streamErr2 != nil {
			panic(streamErr2)
		}
		if perr := persist(step); perr != nil {
			panic(perr)
		}
		current = step.State
	}

	fmt.Println(current.Text)
	// Output:
	// init_process_approve_finish
}

func ExampleExecutionChain_persistenceRecipe() {
	persistCheckpoint := func(_ context.Context, _ string, _ string) error { return nil }

	memoryMw := func(ctx context.Context, state string, chain *flowy.ExecutionChain[string]) (string, error) {
		out, err := chain.Next(ctx, state)
		if err != nil {
			return out, err
		}

		if !chain.CanResolveNext {
			return out, nil
		}

		postState := chain.ApplyUpdate(state, out)
		nextNode, err := chain.ResolveNext(ctx, postState)
		if err != nil {
			return out, err
		}

		if err := persistCheckpoint(ctx, postState, nextNode); err != nil {
			return out, err
		}
		return out, nil
	}

	b := flowy.NewGraph[string](func(_, update string) string { return update })
	b.Use(memoryMw)
	b.AddNode("finish", func(_ context.Context, s string) (string, error) { return s + "_done", nil })
	b.SetEntryPoint("finish")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	if err != nil {
		panic(err)
	}
	out, err := graph.Invoke(context.Background(), "init")
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output:
	// init_done
}
