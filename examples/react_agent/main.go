// Package main runs a minimal ReAct-style agent: reason -> tools -> reason -> finish (with cycle).
// The conditional edge from reason exits to finish after 2 steps; WithMaxSteps(25) caps iterations.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
)

const reActMaxSteps = 25

func main() {
	ctx := context.Background()

	type state struct {
		messages []string
		steps    int
	}
	reducer := func(current, update state) state {
		if len(update.messages) > 0 {
			current.messages = append(current.messages, update.messages...)
		}
		if update.steps > 0 {
			current.steps = update.steps
		}
		return current
	}

	b := flowy.NewGraph[state](reducer)

	b.AddNode("reason", func(_ context.Context, s state) (state, error) {
		return state{messages: []string{"reason"}, steps: s.steps + 1}, nil
	})
	b.AddNode("tools", func(_ context.Context, _ state) (state, error) {
		return state{messages: []string{"tools"}}, nil
	})
	b.AddNode("finish", func(_ context.Context, _ state) (state, error) {
		return state{messages: []string{"finish"}}, nil
	})

	b.AddConditionalEdge("reason", func(_ context.Context, s state) (string, error) {
		if s.steps >= 2 {
			return "finish", nil
		}
		return "tools", nil
	})
	b.AddEdge("tools", "reason")
	b.SetEntryPoint("reason")
	b.SetFinishPoint("finish")

	graph, err := b.Compile(flowy.WithMaxSteps(reActMaxSteps))
	if err != nil {
		log.Fatal(err)
	}

	initial := state{messages: []string{}, steps: 0}
	final, err := graph.Invoke(ctx, initial)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("ReAct cycle finished. Messages:", final.messages)
	fmt.Println("Steps:", final.steps)
}
