// Package main demonstrates semantic caching with flowy: check cache first, short-circuit
// to format_response on hit, or run full LLM+tools path on miss. Uses typed AgentState
// and a stub SemanticCache (replace with ragy/cache + metry in production).
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/skosovsky/flowy"
)

const (
	semanticSimilarityThreshold = 0.95
	exampleGraphMaxSteps        = 20
)

type ChatMessage struct {
	Role string
	Text string
}

type AgentState struct {
	Query         string
	ChatHistory   []ChatMessage
	CacheHit      bool
	FinalResponse string
	ToolCalls     int
	ToolsUsed     bool
}

type SemanticCache interface {
	Get(ctx context.Context, query string, threshold float64) (response string, hit bool, err error)
	Set(ctx context.Context, query string, response string) error
}

type stubCache struct {
	mu    sync.RWMutex
	store map[string]string
}

func newStubCache() *stubCache {
	return &stubCache{
		mu:    sync.RWMutex{},
		store: make(map[string]string),
	}
}

func (c *stubCache) Get(_ context.Context, query string, _ float64) (string, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	resp, ok := c.store[query]
	return resp, ok, nil
}

func (c *stubCache) Set(_ context.Context, query string, response string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[query] = response
	return nil
}

type Agent struct {
	SemanticCache SemanticCache
}

func (a *Agent) cacheNode(ctx context.Context, state AgentState) (AgentState, flowy.Directive, error) {
	resp, hit, err := a.SemanticCache.Get(ctx, state.Query, semanticSimilarityThreshold)
	if err != nil {
		return state, flowy.Fail("cache lookup failed"), err
	}
	state.CacheHit = hit
	if hit {
		state.FinalResponse = resp
	}
	return state, flowy.Completed(), nil
}

func (a *Agent) llmNode(_ context.Context, state AgentState) (AgentState, flowy.Directive, error) {
	if !state.ToolsUsed {
		return AgentState{ToolCalls: 1, FinalResponse: "[LLM: calling tools]"}, flowy.Completed(), nil
	}
	return AgentState{FinalResponse: "flowy is a Go library for stateful AI agent graphs."}, flowy.Completed(), nil
}

func (a *Agent) toolsNode(_ context.Context, _ AgentState) (AgentState, flowy.Directive, error) {
	return AgentState{
		ToolCalls:   0,
		ToolsUsed:   true,
		ChatHistory: []ChatMessage{{Role: "tool", Text: "result"}},
	}, flowy.Completed(), nil
}

func (a *Agent) saveCacheNode(ctx context.Context, state AgentState) (AgentState, flowy.Directive, error) {
	if state.CacheHit {
		return state, flowy.Completed(), nil
	}
	_ = a.SemanticCache.Set(ctx, state.Query, state.FinalResponse)
	return state, flowy.Completed(), nil
}

func (a *Agent) formatResponseNode(_ context.Context, state AgentState) (AgentState, flowy.Directive, error) {
	return AgentState{FinalResponse: state.FinalResponse}, flowy.End(), nil
}

func (a *Agent) cacheRouter(_ context.Context, state AgentState) (string, error) {
	if state.CacheHit {
		return "format_response_node", nil
	}
	return "llm_node", nil
}

func (a *Agent) llmToToolsRouter(_ context.Context, state AgentState) (string, error) {
	if state.ToolCalls > 0 {
		return "tools_node", nil
	}
	return "save_cache_node", nil
}

func main() {
	ctx := context.Background()
	cache := newStubCache()
	agent := &Agent{SemanticCache: cache}

	reducer := func(current, update AgentState) AgentState {
		if len(update.ChatHistory) > 0 {
			current.ChatHistory = append(current.ChatHistory, update.ChatHistory...)
		}
		if update.FinalResponse != "" {
			current.FinalResponse = update.FinalResponse
		}
		if update.CacheHit {
			current.CacheHit = true
		}
		current.ToolCalls = update.ToolCalls
		if update.ToolsUsed {
			current.ToolsUsed = true
		}
		return current
	}

	b := flowy.NewGraph[AgentState, flowy.NoEffect](reducer)
	b.AddNode("cache_node", agent.cacheNode)
	b.AddNode("llm_node", agent.llmNode)
	b.AddNode("tools_node", agent.toolsNode)
	b.AddNode("save_cache_node", agent.saveCacheNode)
	b.AddNode("format_response_node", agent.formatResponseNode)
	b.AddConditionalEdge("cache_node", agent.cacheRouter, "format_response_node", "llm_node")
	b.AddConditionalEdge("llm_node", agent.llmToToolsRouter, "tools_node", "save_cache_node")
	b.AddEdge("tools_node", "llm_node")
	b.AddEdge("save_cache_node", "format_response_node")
	b.AllowNoOutgoingRoute("format_response_node")
	b.SetEntryPoint("cache_node")

	graph, err := b.Compile(flowy.WithMaxSteps(exampleGraphMaxSteps))
	if err != nil {
		log.Fatal(err)
	}

	run := func(threadID string, initial AgentState) AgentState {
		result, err := graph.NewRunner(nil).Start(ctx, threadID, initial)
		if err != nil {
			log.Fatal(err)
		}
		return result.State
	}

	initial := AgentState{Query: "What is flowy?"}
	final1 := run("semantic-miss", initial)
	fmt.Println("Run 1 (cache miss):", final1.FinalResponse)

	final2 := run("semantic-hit", initial)
	fmt.Println("Run 2 (cache hit):", final2.FinalResponse)
}
