// Package main demonstrates token streaming: flowy.Stream emits graph state events,
// while LLM token streaming is done via a channel passed through context.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/skosovsky/flowy"
)

type contextKey string

const tokenChanKey contextKey = "tokenChan"

const tokenChanCapacity = 32

func main() {
	ctx := context.Background()
	tokenCh := make(chan string, tokenChanCapacity)
	ctx = context.WithValue(ctx, tokenChanKey, tokenCh)

	reducer := func(_, update string) string { return update }
	b := flowy.NewGraph[string](reducer)

	b.AddNode("call_llm", func(ctx context.Context, s string) (string, error) {
		ch, _ := ctx.Value(tokenChanKey).(chan string)
		if ch != nil {
			go func() {
				for _, word := range []string{"Hello", " ", "world", "!"} {
					ch <- word
				}
				close(ch)
			}()
		}
		return s + "[llm_done]", nil
	})
	b.AddNode("finish", func(_ context.Context, s string) (string, error) {
		return s, nil
	})
	b.AddEdge("call_llm", "finish")
	b.SetEntryPoint("call_llm")
	b.SetFinishPoint("finish")

	graph, err := b.Compile()
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for tok := range tokenCh {
			fmt.Print(tok)
		}
		fmt.Println()
	})

	out, err := graph.Invoke(ctx, "")
	wg.Wait()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("state:", out)
}
