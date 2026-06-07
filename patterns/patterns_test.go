package patterns

import (
	"context"
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
)

type memCP[T, E any] struct{}

func (memCP[T, E]) Save(context.Context, flowy.Snapshot[T, E]) error { return nil }
func (memCP[T, E]) Load(context.Context, string) (flowy.Snapshot[T, E], error) {
	return flowy.Snapshot[T, E]{}, flowy.ErrThreadNotFound
}
func (memCP[T, E]) GetHistory(context.Context, string, int) ([]flowy.Snapshot[T, E], error) {
	return []flowy.Snapshot[T, E]{}, nil
}
func (memCP[T, E]) Prune(context.Context, string, int) error { return nil }
func (memCP[T, E]) Delete(context.Context, string) error     { return nil }

// DeleteIfIdle is a test stub (no lease store).
func (memCP[T, E]) DeleteIfIdle(context.Context, string) error { return nil }

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
	b := BuildReAct[state, flowy.NoEffect](reason, action, func(s state) bool { return s.Pending }, 1)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(memCP[state, flowy.NoEffect]{}).Start(context.Background(), "r1", state{})
	if !errors.Is(err, flowy.ErrRetryBudgetExceeded) {
		t.Fatalf("expected retry budget exceeded, got %v", err)
	}
	if res == nil || res.Reason != flowy.ErrRetryBudgetExceeded.Error() {
		t.Fatalf("sync reason: want %q, got res=%+v", flowy.ErrRetryBudgetExceeded.Error(), res)
	}
}

func TestBuildSupervisor(t *testing.T) {
	t.Parallel()
	type state struct{ Intent string }
	supervisor := func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Completed(), nil
	}
	workers := map[string]flowy.Node[state, flowy.NoEffect]{
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
	res, err := g.NewRunner(memCP[state, flowy.NoEffect]{}).Start(context.Background(), "s1", state{Intent: "sales"})
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
	b := BuildEvaluatorOptimizer[state, flowy.NoEffect](generator, evaluator, func(s state) bool { return s.Valid }, 3)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := g.NewRunner(memCP[state, flowy.NoEffect]{}).Start(context.Background(), "ev-1", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.State.GenCount < 2 {
		t.Fatalf("expected generator rerun, got %+v", res.State)
	}
}
