// Command context_deadline shows macro-level resilience: the caller limits how long
// the whole graph run may take using [context.WithTimeout]. The engine does not add
// its own per-node deadlines.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/skosovsky/flowy"
)

const (
	workSleep = 500 * time.Millisecond
	runBudget = 50 * time.Millisecond
)

func main() {
	reducer := func(_, update string) string { return update }
	b := flowy.NewGraph[string](reducer)
	b.AddNode("work", func(ctx context.Context, s string) (string, error) {
		select {
		case <-time.After(workSleep):
			return s + "done", nil
		case <-ctx.Done():
			return s, ctx.Err()
		}
	})
	b.SetEntryPoint("work")
	b.SetFinishPoint("work")

	graph, err := b.Compile()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), runBudget)
	defer cancel()

	out, err := graph.Invoke(ctx, "")
	if err != nil {
		// Typical outcome here: parent deadline is shorter than the node's sleep.
		fmt.Println("stopped:", err)
		return
	}
	fmt.Println(out)
}
