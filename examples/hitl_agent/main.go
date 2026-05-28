// Package main demonstrates Human-in-the-Loop: Suspend for approval, framework Save, Resume with WithStateOverlay.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type orderState struct {
	Items    []string
	Approved bool
}

func main() {
	cp := testutil.NewMemoryCheckpointer[orderState, flowy.NoEffect]()
	threadID := "order-thread-1"

	graph, err := flowy.NewGraph[orderState, flowy.NoEffect](func(_ orderState, u orderState) orderState { return u }).
		AddNode("collect", func(_ context.Context, s orderState) (orderState, flowy.Directive, error) {
			s.Items = append(s.Items, "widget", "shipping")
			return s, flowy.Completed(), nil
		}).
		AddNode("payment", func(_ context.Context, s orderState) (orderState, flowy.Directive, error) {
			if !s.Approved {
				return s, flowy.Suspend("waiting_for_user_approval"), nil
			}
			return s, flowy.Completed(), nil
		}).
		AddNode("confirm", func(_ context.Context, s orderState) (orderState, flowy.Directive, error) {
			s.Items = append(s.Items, "payment_confirmed")
			return s, flowy.End(), nil
		}).
		AddEdge("collect", "payment").
		AddConditionalEdge("payment", func(_ context.Context, s orderState) (string, error) {
			if s.Approved {
				return "confirm", nil
			}
			return flowy.EndNode, nil
		}, "confirm", flowy.EndNode).
		SetEntryPoint("collect").
		AllowNoOutgoingRoute("confirm").
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := graph.NewRunner(cp)
	result, err := runner.Start(context.Background(), threadID, orderState{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("phase1 status=%s reason=%q items=%v\n", result.Status, result.Reason, result.State.Items)

	if result.Status != flowy.RunStatusSuspended {
		log.Fatal("expected suspended run before human approval")
	}

	resumed, err := runner.Resume(
		context.Background(),
		threadID,
		flowy.WithStateOverlay[orderState, flowy.NoEffect](
			orderState{Approved: true},
			func(base, overlay orderState) orderState {
				base.Approved = overlay.Approved
				return base
			},
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("phase2 status=%s items=%v\n", resumed.Status, resumed.State.Items)
}
