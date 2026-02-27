// Package main demonstrates Human-in-the-Loop: interrupt before "approve", then Resume with delta.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func main() {
	ctx := context.Background()
	cp := testutil.NewInMemoryCheckpointer[string]()

	b := flowy.NewGraph[string](func(c, u string) string { return c + u })
	b.AddNode("process", func(_ context.Context, _ string) (string, error) {
		return "[process]", nil
	})
	b.AddNode("approve", func(_ context.Context, _ string) (string, error) {
		return "[approve]", nil
	})
	b.AddEdge("process", "approve")
	b.SetEntryPoint("process")
	b.SetFinishPoint("approve")
	b.InterruptBefore("approve")

	graph, err := b.Compile(
		flowy.WithCheckpointer(cp),
		flowy.WithThreadID[string]("session_1"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// First run: stops at interrupt
	state, err := graph.Invoke(ctx, "init")
	if err != nil && !errors.Is(err, flowy.ErrInterrupt) {
		log.Fatal(err)
	}
	fmt.Println("After interrupt, state:", state)

	// Human "approves" and sends delta (e.g. modified dosage)
	delta := "[human_ok]"
	final, err := graph.Resume(ctx, "session_1", delta)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("After Resume, final:", final)
}
