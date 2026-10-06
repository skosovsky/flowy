package flowy_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func task23InlineRunner(
	t *testing.T,
	handler flowy.Node[int, string],
) (flowy.Runner[task23Parent, string], *testutil.MemoryCheckpointer[task23Parent, string]) {
	t.Helper()
	b := flowy.NewGraph[int, string](func(_, u int) int { return u })
	b.AddNode("inner", handler).SetEntryPoint("inner").AllowNoOutgoingRoute("inner")
	sub, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := flowy.NewGraph[task23Parent, string](func(_, u task23Parent) task23Parent { return u })
	parent.AddNode("sub", flowy.SubgraphNodeWithSlot(sub, func(s task23Parent) int { return s.Value }, func(s task23Parent) (flowy.SubgraphSlot[int, string], bool) {
		return s.Slot, s.Slot.ExecutionPointer != ""
	}, func(s task23Parent, slot flowy.SubgraphSlot[int, string]) task23Parent { s.Slot = slot; return s }, func(s task23Parent, u int) task23Parent { s.Value = u; return s })).
		SetEntryPoint("sub").
		AddEdge("sub", flowy.EndNode)
	g, err := parent.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := testutil.NewMemoryCheckpointer[task23Parent, string]()
	return g.NewRunner(cp), cp
}

func TestTask23InlineHandoffEffects(t *testing.T) {
	// Arrange.
	r, _ := task23InlineRunner(t, func(_ context.Context, s int) (int, flowy.Directive, error) {
		if s == 0 {
			return 1, flowy.Effect(flowy.Handoff("move"), "handoff"), nil
		}
		return s + 1, flowy.Effect(flowy.End(), "end"), nil
	})
	// Act.
	first, err := r.Start(context.Background(), "handoff", task23Parent{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.Resume(context.Background(), first.ResumeToken)
	// Assert.
	if first.Status != flowy.RunStatusHandoff || first.State.Slot.ExportedEffects != 1 || err != nil ||
		second.Status != flowy.RunStatusCompleted ||
		second.State.Value != 2 ||
		second.State.Slot.ExecutionPointer != "" ||
		!reflect.DeepEqual(second.Effects, []string{"handoff", "end"}) {
		t.Fatalf("first=%+v second=%+v err=%v", first, second, err)
	}
}

func TestTask23InlineFailurePreservesConfirmedSlot(t *testing.T) {
	// Arrange.
	sentinel := errors.New("inner failure")
	fail := true
	r, cp := task23InlineRunner(t, func(_ context.Context, s int) (int, flowy.Directive, error) {
		if s == 0 {
			return 1, flowy.Effect(flowy.Suspend("pause"), "pause"), nil
		}
		if fail {
			return 99, flowy.Effect(flowy.End(), "unconfirmed"), sentinel
		}
		return s + 1, flowy.Effect(flowy.End(), "end"), nil
	})
	first, err := r.Start(context.Background(), "failure", task23Parent{})
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	failed, err := r.Resume(context.Background(), first.ResumeToken)
	snapshot, _, loadErr := cp.Load(context.Background(), "failure")
	// Assert.
	if !errors.Is(err, sentinel) || loadErr != nil || !reflect.DeepEqual(failed.State, first.State) ||
		!reflect.DeepEqual(snapshot.State, first.State) ||
		!reflect.DeepEqual(snapshot.Effects, []string{"pause"}) {
		t.Fatalf("failed=%+v snapshot=%+v err=%v load=%v", failed, snapshot, err, loadErr)
	}
	fail = false
	resumed, err := r.Resume(context.Background(), first.ResumeToken)
	if err != nil || resumed.State.Value != 2 || resumed.State.Slot.ExecutionPointer != "" ||
		!reflect.DeepEqual(resumed.Effects, []string{"pause", "end"}) {
		t.Fatalf("resumed=%+v err=%v", resumed, err)
	}
}

func TestTask23InlineInFlightCancellation(t *testing.T) {
	// Arrange.
	ready := make(chan struct{})
	r, _ := task23InlineRunner(t, func(ctx context.Context, s int) (int, flowy.Directive, error) {
		switch s {
		case 0:
			return 1, flowy.Effect(flowy.Suspend("pause"), "pause"), nil
		case 1:
			close(ready)
			<-ctx.Done()
			return 2, flowy.Effect(flowy.Completed(), "cancel-boundary"), nil
		default:
			return s + 1, flowy.Effect(flowy.End(), "end"), nil
		}
	})
	first, err := r.Start(context.Background(), "cancel", task23Parent{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		result *flowy.RunResult[task23Parent, string]
		err    error
	}
	done := make(chan outcome, 1)
	// Act.
	go func() { result, e := r.Resume(ctx, first.ResumeToken); done <- outcome{result, e} }()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("inner not admitted")
	}
	cancel()
	var canceled outcome
	select {
	case canceled = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not finish")
	}
	// Assert.
	if !errors.Is(canceled.err, context.Canceled) || canceled.result.Status != flowy.RunStatusContextCanceled ||
		canceled.result.State.Value != 2 ||
		canceled.result.State.Slot.ExportedEffects != 2 ||
		!reflect.DeepEqual(canceled.result.Effects, []string{"pause", "cancel-boundary"}) {
		t.Fatalf("cancel=%+v err=%v", canceled.result, canceled.err)
	}
	resumed, err := r.Resume(context.Background(), canceled.result.ResumeToken)
	if err != nil || resumed.State.Value != 3 || resumed.State.Slot.ExecutionPointer != "" ||
		!reflect.DeepEqual(resumed.Effects, []string{"pause", "cancel-boundary", "end"}) {
		t.Fatalf("resumed=%+v err=%v", resumed, err)
	}
}
