// Package main demonstrates global middleware: logging and panic recovery.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type agentState struct {
	Trace []string
}

func loggingMiddleware[T any](next flowy.Node[T]) flowy.Node[T] {
	return func(ctx context.Context, state T) (T, flowy.Directive, error) {
		start := time.Now()
		out, directive, err := next(ctx, state)
		log.Printf("node executed in %s err=%v", time.Since(start), err)
		return out, directive, err
	}
}

func main() {
	graph, err := flowy.NewGraph(func(_ agentState, u agentState) agentState { return u }).
		Use(loggingMiddleware[agentState], flowy.RecoverMiddleware[agentState]()).
		AddNode("stable", func(_ context.Context, s agentState) (agentState, flowy.Directive, error) {
			s.Trace = append(s.Trace, "stable_ok")
			return s, flowy.Completed(), nil
		}).
		AddNode("unstable", func(_ context.Context, _ agentState) (agentState, flowy.Directive, error) {
			panic("simulated node failure")
		}).
		AddEdge("stable", "unstable").
		SetEntryPoint("stable").
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := graph.NewRunner(testutil.NewMemoryCheckpointer[agentState]())
	_, err = runner.Start(context.Background(), "mw-thread", agentState{})
	if err == nil {
		log.Fatal("expected error from recovered panic in unstable node")
	}
	fmt.Printf("recovered error: %v\n", err)

	stream, err := runner.Stream(context.Background(), "mw-stream", agentState{})
	if err != nil {
		log.Fatal(err)
	}
	for event := range stream.Events() {
		fmt.Printf("stream event=%s node=%s err=%v\n", event.Type, event.NodeID, event.Error)
	}
	_ = stream.Done()
}
