// Package main demonstrates embedding a subgraph via SubgraphNode: parent state has a nested sub-state,
// mapIn/mapOut adapt between parent and sub; the subgraph runs as one node in the parent graph.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
)

func main() {
	ctx := context.Background()

	type SubState struct {
		Count int
		Label string
	}
	type ParentState struct {
		Title string
		Sub   SubState
	}

	subReducer := func(_, update SubState) SubState { return update }
	sub, err := flowy.NewGraph[SubState](subReducer).
		AddNode("inc", func(_ context.Context, s SubState) (SubState, error) {
			return SubState{Count: s.Count + 1, Label: s.Label}, nil
		}).
		AddNode("tag", func(_ context.Context, s SubState) (SubState, error) {
			return SubState{Count: s.Count, Label: s.Label + "_done"}, nil
		}).
		AddEdge("inc", "tag").
		SetEntryPoint("inc").
		SetFinishPoint("tag").
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	mapIn := func(p ParentState) SubState { return p.Sub }
	mapOut := func(p ParentState, s SubState) ParentState {
		p.Sub = s
		return p
	}

	parentReducer := func(_, update ParentState) ParentState { return update }
	b := flowy.NewGraph[ParentState](parentReducer)
	b.AddNode("start", func(_ context.Context, p ParentState) (ParentState, error) {
		p.Title = "run"
		return p, nil
	})
	b.AddNode("sub", flowy.SubgraphNode(sub, mapIn, mapOut))
	b.AddNode("end", func(_ context.Context, p ParentState) (ParentState, error) {
		return p, nil
	})
	b.AddEdge("start", "sub")
	b.AddEdge("sub", "end")
	b.SetEntryPoint("start")
	b.SetFinishPoint("end")

	graph, err := b.Compile()
	if err != nil {
		log.Fatal(err)
	}

	initial := ParentState{Title: "", Sub: SubState{Count: 0, Label: "a"}}
	final, _, err := graph.Invoke(ctx, initial)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Title:", final.Title)
	fmt.Println("Sub.Count:", final.Sub.Count)
	fmt.Println("Sub.Label:", final.Sub.Label)
}
