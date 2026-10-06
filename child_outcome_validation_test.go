package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type task24ForbiddenCodec[T any] struct{ calls *atomic.Int32 }

func (c task24ForbiddenCodec[T]) Marshal(T) ([]byte, error) {
	c.calls.Add(1)
	return nil, errors.New("unexpected codec")
}
func (c task24ForbiddenCodec[T]) Unmarshal([]byte) (T, error) {
	c.calls.Add(1)
	var value T
	return value, errors.New("unexpected codec")
}

func TestChildOutcomeResolutionRejectsBeforeHostCallbacks(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	base := testutil.NewMemoryExecutionStore(nil)
	var dispatches, callbacks atomic.Int32
	dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
		dispatches.Add(1)
		return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("done")}, nil
	}
	_, err := task24ChildJoinRunner(
		t,
		&faultExecutionStore{ExecutionStore: base, failAt: 5},
		dispatch,
	).Start(ctx, "run", durableTestState{})
	if !errors.Is(err, errInjectedCommit) {
		t.Fatal(err)
	}
	token, decision, _ := task24ChildDecision(t, base)
	b := flowy.NewGraph[durableTestState, flowy.NoEffect](func(_, u durableTestState) durableTestState { return u })
	b.AddNode("node", func(_ context.Context, s durableTestState) (durableTestState, flowy.Directive, error) {
		callbacks.Add(1)
		return s, flowy.End(), nil
	}).SetEntryPoint("node").AllowNoOutgoingRoute("node")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	r, err := flowy.NewDurableRunner(
		g,
		base,
		durableDescriptor("current"),
		task24ForbiddenCodec[durableTestState]{calls: &callbacks},
		task24ForbiddenCodec[[]flowy.NoEffect]{calls: &callbacks},
		flowy.DurableOptions{Owner: "operator", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		change func(*flowy.ChildOutcomeResolution)
		want   error
	}{
		{"reason UTF8", func(d *flowy.ChildOutcomeResolution) { d.Reason = "bad\xff" }, flowy.ErrChildJoinInvalid},
		{"evidence UTF8", func(d *flowy.ChildOutcomeResolution) { d.Evidence = "bad\xff" }, flowy.ErrChildJoinInvalid},
		{"wrong label", func(d *flowy.ChildOutcomeResolution) { d.GroupLabel = "other" }, flowy.ErrChildRevision},
		{"wrong child", func(d *flowy.ChildOutcomeResolution) { d.ExecutionID = "other" }, flowy.ErrChildRevision},
		{"wrong revision", func(d *flowy.ChildOutcomeResolution) { d.ChildRevision++ }, flowy.ErrChildRevision},
		{"wrong node", func(d *flowy.ChildOutcomeResolution) { d.Node = "other" }, flowy.ErrChildRevision},
		{
			"waiting",
			func(d *flowy.ChildOutcomeResolution) { d.Result.State = flowy.ChildWaiting; d.Result.WaitID = "wait" },
			flowy.ErrChildJoinInvalid,
		},
		{
			"completed error",
			func(d *flowy.ChildOutcomeResolution) { d.Result.Error = "error" },
			flowy.ErrChildJoinInvalid,
		},
		{
			"empty failed error",
			func(d *flowy.ChildOutcomeResolution) { d.Result.State = flowy.ChildFailed },
			flowy.ErrChildJoinInvalid,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Act.
			changed := decision
			tc.change(&changed)
			_, resolveErr := r.ResolveChildOutcome(ctx, token, changed)
			// Assert.
			latest, _, group := task24ChildDecision(t, base)
			if !errors.Is(resolveErr, tc.want) || latest != token || callbacks.Load() != 0 || dispatches.Load() != 1 ||
				group.Children[0].OutcomeResolution != nil {
				t.Fatalf(
					"err=%v callbacks=%d dispatches=%d latest=%+v group=%+v",
					resolveErr,
					callbacks.Load(),
					dispatches.Load(),
					latest,
					group,
				)
			}
		})
	}
	// Act/Assert: even successful raw resolution calls none of the poisoned host callbacks.
	resolved, err := r.ResolveChildOutcome(ctx, token, decision)
	if err != nil || callbacks.Load() != 0 || dispatches.Load() != 1 ||
		resolved.SnapshotRevision != token.SnapshotRevision+1 {
		t.Fatalf("resolved=%+v err=%v callbacks=%d", resolved, err, callbacks.Load())
	}
}
