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

type streamEffect struct {
	Step int `json:"step"`
}

func main() {
	graph, err := flowy.NewGraph[streamState, streamEffect](func(_ streamState, update streamState) streamState {
		return update
	}).
		AddNode("start", func(_ context.Context, s streamState) (streamState, flowy.Directive, error) {
			s.Step++
			return s, flowy.Effect[streamEffect](flowy.Suspend("await_input"), streamEffect(s)), nil
		}).
		SetEntryPoint("start").
		AllowNoOutgoingRoute("start").
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := graph.NewRunner(testutil.NewMemoryCheckpointer[streamState, streamEffect]())
	stream, err := runner.Stream(context.Background(), "stream-thread", streamState{})
	if err != nil {
		log.Fatal(err)
	}
	for event := range stream.Events() {
		fmt.Printf(
			"event=%s node=%s has_effect=%v effect=%v err=%v\n",
			event.Type,
			event.NodeID,
			event.HasEffect,
			event.Effect,
			event.Error,
		)
	}
	if err := stream.Done(); err != nil {
		log.Fatal(err)
	}
}
