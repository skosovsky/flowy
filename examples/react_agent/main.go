package main

import (
	"context"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/patterns"
	"github.com/skosovsky/flowy/testutil"
)

type agentState struct {
	Prompt   string
	Thoughts []string
	Done     bool
}

func main() {
	reason := func(_ context.Context, s agentState) (agentState, flowy.Directive, error) {
		if len(s.Thoughts) == 0 {
			s.Thoughts = append(s.Thoughts, "need_tool")
			return s, flowy.Completed(), nil
		}
		s.Done = true
		s.Thoughts = append(s.Thoughts, "final_answer")
		return s, flowy.Completed(), nil
	}
	action := func(_ context.Context, s agentState) (agentState, flowy.Directive, error) {
		s.Thoughts = append(s.Thoughts, "tool_called")
		return s, flowy.Completed(), nil
	}

	const maxReActSteps = 8
	graph, err := patterns.BuildReAct(
		reason,
		action,
		func(s agentState) bool { return !s.Done },
		maxReActSteps,
	).Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := graph.NewRunner(testutil.NewMemoryCheckpointer[agentState]())
	result, err := runner.Start(context.Background(), "react-thread", agentState{Prompt: "hello"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status=%s thoughts=%v\n", result.Status, result.State.Thoughts)
}
