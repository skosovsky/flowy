package patterns

import (
	"context"
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
)

type memCP[T any] struct{}

func (memCP[T]) Save(context.Context, flowy.Snapshot[T]) error { return nil }
func (memCP[T]) Load(context.Context, string) (flowy.Snapshot[T], error) {
	return flowy.Snapshot[T]{}, flowy.ErrThreadNotFound
}
func (memCP[T]) GetHistory(context.Context, string, int) ([]flowy.Snapshot[T], error) {
	return []flowy.Snapshot[T]{}, nil
}
func (memCP[T]) Prune(context.Context, string, int) error { return nil }

func TestBuildReActMaxSteps(t *testing.T) {
	t.Parallel()
	type state struct{ Pending bool }
	reason := func(_ context.Context, s state) (state, flowy.Directive, error) {
		s.Pending = true
		return s, flowy.Completed(), nil
	}
	action := func(_ context.Context, s state) (state, flowy.Directive, error) {
		s.Pending = true
		return s, flowy.Completed(), nil
	}
	b := BuildReAct(reason, action, func(s state) bool { return s.Pending }, 1)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = g.NewRunner(memCP[state]{}).Start(context.Background(), "r1", state{})
	if !errors.Is(err, flowy.ErrMaxStepsExceeded) {
		t.Fatalf("expected max steps exceeded, got %v", err)
	}
}

func TestBuildSupervisor(t *testing.T) {
	t.Parallel()
	type state struct{ Intent string }
	supervisor := func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Completed(), nil
	}
	workers := map[string]flowy.Node[state]{
		"sales_worker": func(_ context.Context, s state) (state, flowy.Directive, error) {
			s.Intent = "sales_done"
			return s, flowy.Completed(), nil
		},
	}
	b := BuildSupervisor(supervisor, workers, func(s state) string { return s.Intent }, RouteMap{
		"sales": "sales_worker",
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(memCP[state]{}).Start(context.Background(), "s1", state{Intent: "sales"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.Status != flowy.RunStatusCompleted {
		t.Fatalf("expected completed, got %s", res.Status)
	}
	if res.State.Intent != "sales_done" {
		t.Fatalf("worker did not execute: %+v", res.State)
	}
}

func TestBuildEvaluatorOptimizerRetriesToGenerator(t *testing.T) {
	t.Parallel()
	type state struct {
		GenCount int
		Valid    bool
	}
	generator := func(_ context.Context, s state) (state, flowy.Directive, error) {
		s.GenCount++
		if s.GenCount >= 2 {
			s.Valid = true
		}
		return s, flowy.Completed(), nil
	}
	evaluator := func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Completed(), nil
	}
	b := BuildEvaluatorOptimizer(generator, evaluator, func(s state) bool { return s.Valid }, 3)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(memCP[state]{}).Start(context.Background(), "ev-1", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.State.GenCount < 2 {
		t.Fatalf("expected generator rerun, got %+v", res.State)
	}
}
