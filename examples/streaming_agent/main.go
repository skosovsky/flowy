package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type streamState struct {
	Step int
}

func main() {
	graph, err := flowy.NewGraph(func(_ streamState, update streamState) streamState {
		return update
	}).
		AddNode("start", func(_ context.Context, s streamState) (streamState, flowy.Directive, error) {
			s.Step++
			return s, flowy.Effect(flowy.Suspend("await_input"), map[string]any{"step": s.Step}), nil
		}).
		SetEntryPoint("start").
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := graph.NewRunner(testutil.NewMemoryCheckpointer[streamState]())
	stream, err := runner.Stream(context.Background(), "stream-thread", streamState{})
	if err != nil {
		log.Fatal(err)
	}
	for event := range stream.Events() {
		fmt.Printf("event=%s node=%s effect=%v err=%v\n", event.Type, event.NodeID, event.Effect, event.Error)
	}
	if err := stream.Done(); err != nil {
		log.Fatal(err)
	}
}
