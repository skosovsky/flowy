// Package main demonstrates SubgraphNodeWithSlot suspend/resume continuity.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type childState struct {
	Step int
}

type parentState struct {
	Child childState
	Slot  flowy.SubgraphSlot[childState, flowy.NoEffect]
}

func main() {
	cp := testutil.NewMemoryCheckpointer[parentState, flowy.NoEffect]()

	sub := flowy.NewGraph[childState, flowy.NoEffect](func(_ childState, u childState) childState { return u })
	sub.AddNode("work", func(_ context.Context, s childState) (childState, flowy.Directive, error) {
		s.Step++
		if s.Step < 2 {
			return s, flowy.Suspend("child-wait"), nil
		}
		return s, flowy.End(), nil
	})
	sub.SetEntryPoint("work")
	sub.AllowNoOutgoingRoute("work")
	subGraph, err := sub.Compile()
	if err != nil {
		log.Fatal(err)
	}

	parent := flowy.NewGraph[parentState, flowy.NoEffect](func(_ parentState, u parentState) parentState { return u })
	parent.AddNode("sub", flowy.SubgraphNodeWithSlot(
		subGraph,
		func(p parentState) childState { return p.Child },
		func(p parentState) (flowy.SubgraphSlot[childState, flowy.NoEffect], bool) {
			if p.Slot.ExecutionPointer == "" {
				return flowy.SubgraphSlot[childState, flowy.NoEffect]{}, false
			}
			return p.Slot, true
		},
		func(p parentState, slot flowy.SubgraphSlot[childState, flowy.NoEffect]) parentState {
			p.Slot = slot
			return p
		},
		func(p parentState, c childState) parentState {
			p.Child = c
			return p
		},
	))
	parent.AddNode("done", func(_ context.Context, p parentState) (parentState, flowy.Directive, error) {
		return p, flowy.End(), nil
	})
	parent.AddEdge("sub", "done")
	parent.AllowNoOutgoingRoute("done")
	parent.SetEntryPoint("sub")
	parentGraph, err := parent.Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := parentGraph.NewRunner(cp)
	first, err := runner.Start(context.Background(), "slot-thread", parentState{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(
		"first status=%s slot=%q child=%d\n",
		first.Status,
		first.State.Slot.ExecutionPointer,
		first.State.Child.Step,
	)

	second, err := runner.Resume(context.Background(), "slot-thread")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("second status=%s child=%d\n", second.Status, second.State.Child.Step)
}
