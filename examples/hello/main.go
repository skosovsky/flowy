// Package main demonstrates a complete graph using host-owned state and effects.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
)

type State struct {
	Name     string
	Greeting string
}
type Effect struct{ Message string }

func run() error {
	builder := flowy.NewGraph[State, Effect](func(_, update State) State { return update })
	builder.AddNode("greet", func(_ context.Context, state State) (State, flowy.Directive, error) {
		state.Greeting = "Hello, " + state.Name
		return state, flowy.Effect(flowy.End(), Effect{Message: state.Greeting}), nil
	}).AllowNoOutgoingRoute("greet").SetEntryPoint("greet")
	graph, err := builder.Compile()
	if err != nil {
		return err
	}
	result, err := graph.NewRunner(nil).Start(context.Background(), "hello", State{Name: "Sergey"})
	if err != nil {
		return err
	}
	fmt.Println(result.State.Greeting)
	for _, effect := range result.Effects {
		fmt.Println(effect.Message)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
