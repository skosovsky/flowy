// Package main demonstrates typed BindingKey injection per invocation.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
)

type agentState struct {
	Greeting string
}

type greeter struct {
	prefix string
}

func (g greeter) Greet(name string) string {
	return g.prefix + name
}

func main() {
	greeterKey := flowy.BindingKey[greeter]{}

	bindings := flowy.NewRunBindings()
	flowy.Bind(bindings, greeterKey, greeter{prefix: "hello, "})

	b := flowy.NewGraph[agentState, flowy.NoEffect](func(_ agentState, u agentState) agentState { return u })
	b.AddNode("greet", func(ctx context.Context, s agentState) (agentState, flowy.Directive, error) {
		g, ok := flowy.BindingFromContext(ctx, greeterKey)
		if !ok {
			return s, flowy.Fail("missing greeter binding"), nil
		}
		s.Greeting = g.Greet("flowy")
		return s, flowy.End(), nil
	})
	b.SetEntryPoint("greet")
	b.AllowNoOutgoingRoute("greet")
	g, err := b.Compile()
	if err != nil {
		log.Fatal(err)
	}

	res, err := g.NewRunner(nil).Start(
		context.Background(),
		"bindings-thread",
		agentState{},
		flowy.WithBindings[agentState, flowy.NoEffect](bindings),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status=%s greeting=%q\n", res.Status, res.State.Greeting)
}
