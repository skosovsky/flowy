// Package main demonstrates opt-in durable APIs with host-owned types.
// Memory storage illustrates contracts, not persistence across process loss.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

type state struct {
	Value   int
	Handles []string
}

const (
	workNode       = "work"
	workCounter    = "compute"
	hostOwner      = "host"
	correction     = 10
	legacyRevision = 7
)

func descriptor(label string) flowy.ExecutionDescriptor {
	return flowy.ExecutionDescriptor{GraphID: "durable-example", GraphRevision: label,
		StateCodec: "host-json", ExecutionContract: "sync-aggregate",
		ReplayPolicy: flowy.StepReplayPolicy{Label: "host-pure-steps", Mode: flowy.StepReplaySafe}}
}

func bind(store flowy.ExecutionStore, label string, node flowy.Node[state, flowy.NoEffect],
	options flowy.DurableOptions,
) (*flowy.DurableRunner[state, flowy.NoEffect], error) {
	b := flowy.NewGraph[state, flowy.NoEffect](func(_, update state) state { return update })
	b.AddNode(workNode, node).AllowNoOutgoingRoute(workNode).SetEntryPoint(workNode)
	graph, err := b.Compile()
	if err != nil {
		return nil, err
	}
	return flowy.NewDurableRunner(graph, store, descriptor(label), checkpoint.JSONSerializer[state]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{}, options)
}

func options() flowy.DurableOptions {
	return flowy.DurableOptions{Owner: "example-worker", LeaseTTL: time.Minute}
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	for _, demo := range []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "activity", run: activityDemo},
		{name: "migration/import", run: migrationDemo},
		{name: "children", run: childrenDemo},
		{name: "child recovery", run: childRecoveryDemo},
		{name: "wait", run: waitDemo},
		{name: "fake fork", run: forkDemo},
	} {
		if err := demo.run(ctx); err != nil {
			return fmt.Errorf("%s: %w", demo.name, err)
		}
		fmt.Printf("%s: verified\n", demo.name)
	}
	return nil
}
