// Package main shows composition: the seller graph embeds the analyst graph
// as a node via AsNode(), so the inner graph runs with the same state type.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
)

func main() {
	ctx := context.Background()

	// Shared state type so the analyst subgraph can be used as a node via AsNode().
	type state struct {
		Query        string
		ResearchData string
	}
	reducer := func(c, u state) state {
		if u.ResearchData != "" {
			c.ResearchData = u.ResearchData
		}
		if u.Query != "" {
			c.Query = u.Query
		}
		return c
	}

	analystBuilder := flowy.NewGraph[state](reducer)
	analystBuilder.AddNode("research", func(_ context.Context, s state) (state, error) {
		s.ResearchData = "report:" + s.Query
		return s, nil
	})
	analystBuilder.SetEntryPoint("research")
	analystBuilder.SetFinishPoint("research")
	analystGraph, err := analystBuilder.Compile()
	if err != nil {
		log.Fatal(err)
	}

	sellerBuilder := flowy.NewGraph[state](reducer)
	sellerBuilder.AddNode("ask_analyst", analystGraph.AsNode())
	sellerBuilder.SetEntryPoint("ask_analyst")
	sellerBuilder.SetFinishPoint("ask_analyst")

	sellerGraph, err := sellerBuilder.Compile()
	if err != nil {
		log.Fatal(err)
	}

	initial := state{Query: "drug X"}
	final, err := sellerGraph.Invoke(ctx, initial)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Research data:", final.ResearchData)
}
