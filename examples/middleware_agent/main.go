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

func main() {
	ctx := context.Background()

	type memoryStore struct {
		mu   sync.RWMutex
		data map[string]string
	}
	// Demo-only keying by node name. Real persistence should include a thread/session ID.
	save := func(store *memoryStore, nodeName, state string) {
		store.mu.Lock()
		defer store.mu.Unlock()
		store.data[nodeName] = state
	}
	load := func(store *memoryStore, nodeName string) string {
		store.mu.RLock()
		defer store.mu.RUnlock()
		return store.data[nodeName]
	}

	memory := &memoryStore{data: make(map[string]string)}
	reducer := func(current, update string) string { return current + update }
	b := flowy.NewGraph[string](reducer)

	loggingMw := func(ctx context.Context, state string, meta flowy.MiddlewareContext[string], next flowy.NodeHandler[string]) (string, error) {
		start := time.Now()
		out, err := next(ctx, state)
		log.Printf("node=%s duration=%s err=%v", meta.NodeName, time.Since(start), err)
		return out, err
	}

	memoryMw := func(ctx context.Context, state string, meta flowy.MiddlewareContext[string], next flowy.NodeHandler[string]) (string, error) {
		out, err := next(ctx, state)
		if err == nil {
			postState := meta.ApplyUpdate(state, out)
			save(memory, meta.NodeName, postState)
		}
		return out, err
	}

	fallbackNode := func(_ context.Context, _ string) (string, error) {
		return "[fallback]", nil
	}

	fallbackMw := func(ctx context.Context, state string, meta flowy.MiddlewareContext[string], next flowy.NodeHandler[string]) (string, error) {
		out, err := next(ctx, state)
		if err == nil {
			return out, nil
		}

		log.Printf("fallback for %s: %v", meta.NodeName, err)

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

	graph, err := b.Compile()
	if err != nil {
		log.Fatal(err)
	}

	final, err := graph.Invoke(ctx, "init")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("final:", final)
	fmt.Println("memory[start]:", load(memory, "start"))
	fmt.Println("memory[unstable]:", load(memory, "unstable"))
	fmt.Println("memory[finish]:", load(memory, "finish"))
}
