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

// ChatMessage represents a single message in chat history (stub for demo).
type ChatMessage struct {
	Role string
	Text string
}

// AgentState is the graph state. CacheNode sets CacheHit and FinalResponse on hit;
// LLM/tools path populates FinalResponse; routers read CacheHit and ToolCalls.
// ToolsUsed is the explicit phase flag: once tools have run, llmNode produces final answer instead of requesting tools again.
type AgentState struct {
	Query         string
	ChatHistory   []ChatMessage
	CacheHit      bool
	FinalResponse string
	ToolCalls     int  // Used by LLMToToolsRouter: >0 -> tools_node, else -> save_cache_node
	ToolsUsed     bool // True after tools_node has run; llmNode uses this to decide "first call" vs "second call"
}

// SemanticCache is a stub interface for ragy/cache. In production use ragy/cache
// (e.g. pgvector + OpenAI embedder) and pass via Agent struct or closure.
type SemanticCache interface {
	Get(ctx context.Context, query string, threshold float64) (response string, hit bool, err error)
	Set(ctx context.Context, query string, response string) error
}

// stubCache is a thread-safe in-memory cache for the example. Use RWMutex so
// code copied to production does not hit concurrent map writes when scaling.
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

// Agent holds dependencies (e.g. cache) and implements nodes as methods.
type Agent struct {
	SemanticCache SemanticCache
}

func (a *Agent) cacheNode(ctx context.Context, state AgentState) (AgentState, error) {
	// In production with ragy/cache + metry (OpenLLMetry):
	//   resp, hit, err := ragycache.NewVectorCache(pgvectorStore, openaiEmbedder).Get(ctx, query, 0.95)
	//   span := trace.SpanFromContext(ctx)
	//   genai.RecordCacheHit(span, hit, "pgvector_semantic_cache")
	resp, hit, err := a.SemanticCache.Get(ctx, state.Query, semanticSimilarityThreshold)
	if err != nil {
		return state, err
	}
	state.CacheHit = hit
	if hit {
		state.FinalResponse = resp
	}
	return state, nil
}

func (a *Agent) llmNode(_ context.Context, state AgentState) (AgentState, error) {
	// Simulate LLM: first call (ToolsUsed false) requests tools; second call (after tools_node sets ToolsUsed) produces final answer.
	if !state.ToolsUsed {
		return AgentState{ToolCalls: 1, FinalResponse: "[LLM: calling tools]"}, nil
	}
	return AgentState{FinalResponse: "flowy is a Go library for stateful AI agent graphs."}, nil
}

func (a *Agent) toolsNode(_ context.Context, _ AgentState) (AgentState, error) {
	// Simulate tool execution; set ToolsUsed so llmNode knows to produce final answer on next call.
	return AgentState{ToolCalls: 0, ToolsUsed: true, ChatHistory: []ChatMessage{{Role: "tool", Text: "result"}}}, nil
}

func (a *Agent) saveCacheNode(ctx context.Context, state AgentState) (AgentState, error) {
	if state.CacheHit {
		return state, nil
	}
	_ = a.SemanticCache.Set(ctx, state.Query, state.FinalResponse)
	return state, nil
}

func (a *Agent) formatResponseNode(_ context.Context, state AgentState) (AgentState, error) {
	// Optional formatting; for demo we keep FinalResponse as-is.
	return AgentState{FinalResponse: state.FinalResponse}, nil
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

	b := flowy.NewGraph[AgentState](reducer)
	b.AddNode("cache_node", agent.cacheNode)
	b.AddNode("llm_node", agent.llmNode)
	b.AddNode("tools_node", agent.toolsNode)
	b.AddNode("save_cache_node", agent.saveCacheNode)
	b.AddNode("format_response_node", agent.formatResponseNode)

	b.SetEntryPoint("cache_node")
	b.AddChoice("cache_node", agent.cacheRouter)
	b.AddChoice("llm_node", agent.llmToToolsRouter)
	b.AddEdge("tools_node", "llm_node")
	b.AddEdge("save_cache_node", "format_response_node")
	b.SetFinishPoint("format_response_node")

	graph, err := b.Compile(flowy.WithMaxSteps(exampleGraphMaxSteps))
	if err != nil {
		log.Fatal(err)
	}

	// Run 1: cache miss -> full path LLM -> tools -> LLM -> save_cache -> format
	initial := AgentState{Query: "What is flowy?"}
	final1, err := graph.Invoke(ctx, initial)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Run 1 (cache miss):", final1.FinalResponse)

	// Run 2: same query -> cache hit -> short-circuit to format_response
	final2, err := graph.Invoke(ctx, initial)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Run 2 (cache hit):", final2.FinalResponse)

	fmt.Println("\n--- Mermaid ---")
	fmt.Println(graph.ExportMermaid())
}
