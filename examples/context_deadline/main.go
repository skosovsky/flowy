// Package main demonstrates context cancellation: runner saves snapshot before exit.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

const (
	tickInterval = 200 * time.Millisecond
	runTimeout   = 5 * time.Second
)

type workState struct {
	Ticks int
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cp := testutil.NewMemoryCheckpointer[workState]()
	threadID := "deadline-thread"

	graph, err := flowy.NewGraph(func(_ workState, u workState) workState { return u }).
		AddNode("slow", slowNode).
		SetEntryPoint("slow").
		Compile()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), runTimeout)
	defer cancel()

	runner := graph.NewRunner(cp)
	result, err := runner.Start(ctx, threadID, workState{})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return err
	}
	fmt.Printf("status=%s reason=%q ticks=%d err=%v\n", result.Status, result.Reason, result.State.Ticks, err)
	if result.Status != flowy.RunStatusSuspended || result.Reason != "context_canceled" {
		return fmt.Errorf("unexpected cancel status: %s %q", result.Status, result.Reason)
	}

	snap, loadErr := cp.Load(context.Background(), threadID)
	if loadErr != nil {
		return loadErr
	}
	fmt.Printf("checkpoint saved node=%s ticks=%d revision=%d\n", snap.NodeID, snap.State.Ticks, snap.Revision)
	return nil
}

func slowNode(ctx context.Context, s workState) (workState, flowy.Directive, error) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return s, flowy.Next("slow"), nil
		case <-ticker.C:
			s.Ticks++
		}
	}
}
