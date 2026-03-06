// Package main demonstrates Human-in-the-Loop (v2): "approve" node returns ErrSuspend;
// caller persists state and checkpoint (e.g. via testutil.Store), then Resume continues.
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
	store := testutil.NewStore[string]()

	concat := func(c, u string) string { return c + u }
	b := flowy.NewGraph[string](concat)
	b.AddNode("process", func(_ context.Context, s string) (string, error) {
		return s + "[process]", nil
	})
	b.AddNode("approve", func(_ context.Context, _ string) (string, error) {
		return "", flowy.ErrSuspend
	})
	b.AddNode("finish", func(_ context.Context, s string) (string, error) {
		return s + "[finish]", nil
	})
	b.AddEdge("process", "approve")
	b.AddEdge("approve", "finish")
	b.SetEntryPoint("process")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	if err != nil {
		log.Fatal(err)
	}

	// First run: suspend at "approve"
	state, cp, err := graph.Invoke(ctx, "init")
	if err != nil && !errors.Is(err, flowy.ErrSuspend) {
		log.Fatal(err)
	}
	if errors.Is(err, flowy.ErrSuspend) {
		fmt.Println("Suspended at approve, state:", state)
		_ = store.Save(ctx, "session_1", state, cp)
		// In production: persist state and cp (e.g. to DB), then later load and call Resume.
		loaded, cpLoaded, ok := store.Load(ctx, "session_1")
		if !ok {
			log.Fatal("load failed")
		}
		// Build same graph but approve now succeeds (e.g. human approved)
		b2 := flowy.NewGraph[string](concat)
		b2.AddNode("process", func(_ context.Context, s string) (string, error) { return s + "[process]", nil })
		b2.AddNode("approve", func(_ context.Context, s string) (string, error) { return s + "[approve]", nil })
		b2.AddNode("finish", func(_ context.Context, s string) (string, error) { return s + "[finish]", nil })
		b2.AddEdge("process", "approve")
		b2.AddEdge("approve", "finish")
		b2.SetEntryPoint("process")
		b2.SetFinishPoint("finish")
		graph2, _ := b2.Compile()
		final, _, err := graph2.Resume(ctx, loaded, cpLoaded)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("After Resume, final:", final)
		return
	}
	fmt.Println("Final (no suspend):", state)
}
