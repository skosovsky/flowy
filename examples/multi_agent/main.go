// Package main demonstrates supervisor routing to specialized worker nodes.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/patterns"
	"github.com/skosovsky/flowy/testutil"
)

type teamState struct {
	Query  string
	Intent string
	Note   string
}

func main() {
	supervisor := func(_ context.Context, s teamState) (teamState, flowy.Directive, error) {
		switch {
		case strings.Contains(strings.ToLower(s.Query), "billing"):
			s.Intent = "support"
		case strings.Contains(strings.ToLower(s.Query), "buy"):
			s.Intent = "sales"
		default:
			s.Intent = "unknown"
		}
		return s, flowy.Completed(), nil
	}

	workers := map[string]flowy.Node[teamState, flowy.NoEffect]{
		"support_worker": func(_ context.Context, s teamState) (teamState, flowy.Directive, error) {
			s.Note = "support: ticket opened for " + s.Query
			return s, flowy.Completed(), nil
		},
		"sales_worker": func(_ context.Context, s teamState) (teamState, flowy.Directive, error) {
			s.Note = "sales: quote prepared for " + s.Query
			return s, flowy.Completed(), nil
		},
	}

	graph, err := patterns.BuildSupervisor[teamState, flowy.NoEffect](
		supervisor,
		workers,
		func(s teamState) string { return s.Intent },
		patterns.RouteMap{
			"support": "support_worker",
			"sales":   "sales_worker",
		},
	).Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := graph.NewRunner(testutil.NewMemoryCheckpointer[teamState, flowy.NoEffect]())
	result, err := runner.Start(context.Background(), "team-1", teamState{Query: "I want to buy enterprise plan"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status=%s intent=%s note=%s\n", result.Status, result.State.Intent, result.State.Note)
}
