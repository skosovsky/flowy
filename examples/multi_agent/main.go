// Package main shows composition: seller graph embeds analyst graph as a node via AsNode().
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
)

func main() {
	ctx := context.Background()

	type analystState struct {
		Query  string
		Report string
	}
	analystReducer := func(c, u analystState) analystState {
		if u.Report != "" {
			c.Report = u.Report
		}
		if u.Query != "" {
			c.Query = u.Query
		}
		return c
	}
	analystBuilder := flowy.NewGraph[analystState](analystReducer)
	analystBuilder.AddNode("research", func(_ context.Context, s analystState) (analystState, error) {
		s.Report = "report:" + s.Query
		return s, nil
	})
	analystBuilder.SetEntryPoint("research")
	analystBuilder.SetFinishPoint("research")
	analystGraph, err := analystBuilder.Compile()
	if err != nil {
		log.Fatal(err)
	}

	type sellerState struct {
		PatientQuestion string
		ResearchData    string
	}
	sellerReducer := func(c, u sellerState) sellerState {
		if u.ResearchData != "" {
			c.ResearchData = u.ResearchData
		}
		if u.PatientQuestion != "" {
			c.PatientQuestion = u.PatientQuestion
		}
		return c
	}
	sellerBuilder := flowy.NewGraph[sellerState](sellerReducer)
	sellerBuilder.AddNode("ask_analyst", func(ctx context.Context, state sellerState) (sellerState, error) {
		in := analystState{Query: state.PatientQuestion}
		out, err := analystGraph.Invoke(ctx, in)
		if err != nil {
			return state, err
		}
		return sellerState{ResearchData: out.Report}, nil
	})
	sellerBuilder.SetEntryPoint("ask_analyst")
	sellerBuilder.SetFinishPoint("ask_analyst")

	sellerGraph, err := sellerBuilder.Compile()
	if err != nil {
		log.Fatal(err)
	}

	initial := sellerState{PatientQuestion: "drug X"}
	final, err := sellerGraph.Invoke(ctx, initial)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Research data:", final.ResearchData)
}
