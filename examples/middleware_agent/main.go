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

func loggingMiddleware(next flowy.Node[agentState, flowy.NoEffect]) flowy.Node[agentState, flowy.NoEffect] {
	return func(ctx context.Context, state agentState) (agentState, flowy.Directive, error) {
		start := time.Now()
		out, directive, err := next(ctx, state)
		log.Printf("node executed in %s err=%v", time.Since(start), err)
		return out, directive, err
	}
}

func main() {
	graph, err := flowy.NewGraph[agentState, flowy.NoEffect](func(_ agentState, u agentState) agentState { return u }).
		Use(loggingMiddleware, flowy.RecoverMiddleware[agentState, flowy.NoEffect]()).
		AddNode("stable", func(_ context.Context, s agentState) (agentState, flowy.Directive, error) {
			s.Trace = append(s.Trace, "stable_ok")
			return s, flowy.Completed(), nil
		}).
		AddNode("unstable", func(_ context.Context, _ agentState) (agentState, flowy.Directive, error) {
			panic("simulated node failure")
		}).
		AddEdge("stable", "unstable").
		AllowNoOutgoingRoute("unstable").
		SetEntryPoint("stable").
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := graph.NewRunner(testutil.NewMemoryCheckpointer[agentState, flowy.NoEffect]())
	_, err = runner.Start(context.Background(), "mw-thread", agentState{})
	if err == nil {
		log.Fatal("expected error from recovered panic in unstable node")
	}
	fmt.Printf("recovered error: %v\n", err)

	stream, err := runner.Stream(context.Background(), "mw-stream", agentState{})
	if err != nil {
		log.Fatal(err)
	}
	events, waitErr := flowy.CollectEventsAndWait(context.Background(), stream)
	for _, event := range events {
		fmt.Printf("stream event=%s node=%s err=%v\n", event.Type, event.ExecutionPointer, event.Error)
	}
	if waitErr != nil {
		fmt.Printf("stream wait error: %v\n", waitErr)
	}
}
