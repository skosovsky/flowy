// Package main demonstrates global and local middlewares: logging, sequential
// step persistence, and a fallback that protects the original state.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/skosovsky/flowy"
)

type memoryStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		mu:   sync.RWMutex{},
		data: make(map[string]string),
	}
}

func (store *memoryStore) save(nodeName, state string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[nodeName] = state
}

func (store *memoryStore) load(nodeName string) string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.data[nodeName]
}

func buildGraph(memory *memoryStore) *flowy.GraphBuilder[string] {
	reducer := func(current, update string) string { return current + update }
	b := flowy.NewGraph[string](reducer)

	loggingMw := func(ctx context.Context, state string, chain *flowy.ExecutionChain[string]) (string, error) {
		start := time.Now()
		out, err := chain.Next(ctx, state)
		log.Printf("node=%s duration=%s err=%v", chain.NodeName, time.Since(start), err)
		return out, err
	}

	memoryMw := func(ctx context.Context, state string, chain *flowy.ExecutionChain[string]) (string, error) {
		out, err := chain.Next(ctx, state)
		if err == nil {
			postState := chain.ApplyUpdate(state, out)
			memory.save(chain.NodeName, postState)
		}
		return out, err
	}

	fallbackNode := func(_ context.Context, _ string) (string, error) {
		return "[fallback]", nil
	}

	fallbackMw := func(ctx context.Context, state string, chain *flowy.ExecutionChain[string]) (string, error) {
		out, err := chain.Next(ctx, state)
		if err == nil {
			return out, nil
		}

		log.Printf("fallback for %s: %v", chain.NodeName, err)

		// Protect the graph from dirty partial output by reusing the original input state.
		return fallbackNode(ctx, state)
	}

	b.Use(loggingMw, memoryMw)
	b.AddNode("start", func(_ context.Context, _ string) (string, error) {
		return "[start]", nil
	})
	b.AddNode("unstable", func(_ context.Context, _ string) (string, error) {
		return "[dirty]", errors.New("primary node failed")
	}, fallbackMw)
	b.AddNode("finish", func(_ context.Context, _ string) (string, error) {
		return "[finish]", nil
	})
	b.AddEdge("start", "unstable")
	b.AddEdge("unstable", "finish")
	b.SetEntryPoint("start")
	b.SetFinishPoint("finish")
	return b
}

func printMemory(memory *memoryStore) {
	fmt.Println("memory[start]:", memory.load("start"))
	fmt.Println("memory[unstable]:", memory.load("unstable"))
	fmt.Println("memory[finish]:", memory.load("finish"))
}

func main() {
	ctx := context.Background()
	memory := newMemoryStore()
	b := buildGraph(memory)

	graph, err := b.Compile()
	if err != nil {
		log.Fatal(err)
	}

	final, err := graph.Invoke(ctx, "init")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("final:", final)
	printMemory(memory)
}
