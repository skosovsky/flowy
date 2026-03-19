// Package main demonstrates Human-in-the-Loop using middleware-based pause/resume.
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

	approved := false
	// Safe pattern: suspend on a normal sequential node, not inside a fan-out branch.
	hitlMw := func(ctx context.Context, state string, meta flowy.MiddlewareContext[string], next flowy.NodeHandler[string]) (string, error) {
		if meta.NodeName == "approve" && !approved {
			if err := store.Save(ctx, "session_1", state, meta.SuspendTarget); err != nil {
				return state, err
			}
			return state, flowy.ErrSuspend
		}
		return next(ctx, state)
	}

	b.Use(hitlMw)
	b.AddNode("process", func(_ context.Context, s string) (string, error) {
		return s + "[process]", nil
	})
	b.AddNode("approve", func(_ context.Context, s string) (string, error) {
		return s + "[approve]", nil
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

	state, err := graph.Invoke(ctx, "init")
	if err != nil && !errors.Is(err, flowy.ErrSuspend) {
		log.Fatal(err)
	}
	if errors.Is(err, flowy.ErrSuspend) {
		fmt.Println("Suspended before approve, state:", state)
		approved = true

		loaded, startNode, ok := store.Load(ctx, "session_1")
		if !ok {
			log.Fatal("load failed")
		}

		final, err := graph.Resume(ctx, loaded, startNode)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("After Resume, final:", final)
		return
	}

	fmt.Println("Final (no suspend):", state)
}
