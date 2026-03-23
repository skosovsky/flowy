// Package main demonstrates Human-in-the-Loop: the graph suspends; the client loads state,
// applies approval, and resumes via Graph.Stream with the checkpointed next node.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type state struct {
	Text     string
	Approved bool
}

func persistStep(
	ctx context.Context,
	cp *testutil.MemoryCheckpointer,
	ser checkpoint.JSONSerializer[state],
	threadID, runID string,
	step flowy.Step[state],
) error {
	raw, err := ser.Marshal(step.State)
	if err != nil {
		return err
	}
	env, err := checkpoint.EncodeStateData(raw)
	if err != nil {
		return err
	}
	id, err := checkpoint.NewSortableID()
	if err != nil {
		return err
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

func resumeAfterSuspend(
	ctx context.Context,
	graph *flowy.Graph[state],
	cp *testutil.MemoryCheckpointer,
	ser checkpoint.JSONSerializer[state],
	threadID, runID string,
	loaded checkpoint.Checkpoint,
) (state, error) {
	raw, err := checkpoint.DecodeStateData(loaded.StateData)
	if err != nil {
		return state{}, err
	}
	var st state
	if err := ser.Unmarshal(raw, &st); err != nil {
		return state{}, err
	}
	st.Approved = true
	st.Text += "[human-approved]"

	var out state
	for step, streamErr := range graph.Stream(ctx, loaded.Next, st) {
		if streamErr != nil {
			return state{}, streamErr
		}
		if err := persistStep(ctx, cp, ser, threadID, runID, step); err != nil {
			return state{}, err
		}
		out = step.State
	}
	return out, nil
}

//nolint:gocognit // CLI example: linear suspend/resume flow
func run(ctx context.Context) error {
	cp := testutil.NewMemoryCheckpointer()
	ser := checkpoint.JSONSerializer[state]{}

	b := flowy.NewGraph[state](func(_, update state) state { return update })
	b.AddNode("process", func(_ context.Context, s state) (state, error) {
		s.Text += "[process]"
		return s, nil
	})
	b.AddNode("approve", func(_ context.Context, s state) (state, error) {
		if !s.Approved {
			return s, flowy.ErrSuspend
		}
		s.Text += "[approve]"
		return s, nil
	})
	b.AddNode("finish", func(_ context.Context, s state) (state, error) {
		s.Text += "[finish]"
		return s, nil
	})
	b.AddEdge("process", "approve")
	b.AddEdge("approve", "finish")
	b.SetEntryPoint("process")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	if err != nil {
		return err
	}

	const threadID = "session_1"
	runID, err := checkpoint.NewSortableID()
	if err != nil {
		return err
	}

	var cur state
	for step, streamErr := range graph.Stream(ctx, "", state{Text: "init"}) {
		if streamErr != nil {
			if !errors.Is(streamErr, flowy.ErrSuspend) {
				return streamErr
			}
			if perr := persistStep(ctx, cp, ser, threadID, runID, step); perr != nil {
				return perr
			}
			fmt.Println("Suspended before approve, state:", step.State.Text)

			loaded, loadErr := cp.LoadLatest(ctx, threadID)
			if loadErr != nil {
				return loadErr
			}
			final, resumeErr := resumeAfterSuspend(ctx, graph, cp, ser, threadID, runID, loaded)
			if resumeErr != nil {
				return resumeErr
			}
			fmt.Println("After resume, final:", final.Text)
			return nil
		}
		if perr := persistStep(ctx, cp, ser, threadID, runID, step); perr != nil {
			return perr
		}
		cur = step.State
	}

	fmt.Println("Final (no suspend):", cur.Text)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
