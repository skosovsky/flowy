package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type task24AlteredStore struct {
	flowy.ExecutionStore

	alter func(*flowy.ExecutionEnvelope)
}

func (s task24AlteredStore) LoadExecution(ctx context.Context, id string) (flowy.ExecutionEnvelope, error) {
	e, err := s.ExecutionStore.LoadExecution(ctx, id)
	if err != nil {
		return e, err
	}
	s.alter(&e)
	return flowy.SealExecutionEnvelope(e)
}

func TestChildOutcomeResealedCorruptionAndDescriptorReject(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	dispatch := func(ctx context.Context, _ flowy.ChildInvocation) (flowy.ChildResult, error) {
		calls.Add(1)
		return flowy.ChildResult{State: flowy.ChildUnknown}, ctx.Err()
	}
	runner := task24ChildJoinRunner(t, store, dispatch)
	_, err := runner.Start(ctx, "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatal(err)
	}
	token, decision, _ := task24ChildDecision(t, store)
	resolved, err := runner.ResolveChildOutcome(ctx, token, decision)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		alter func(*flowy.ExecutionEnvelope)
		want  error
	}{
		{"descriptor", func(e *flowy.ExecutionEnvelope) { e.Descriptor.GraphRevision = "incompatible" }, flowy.ErrExecutionIncompatible},
		{"invalid UTF8", func(e *flowy.ExecutionEnvelope) { e.ChildrenPayload = append([]byte{'\xff'}, e.ChildrenPayload...) }, flowy.ErrExecutionCorrupt},
		{"digest", task24CorruptChild(func(c *flowy.ChildRecord) { c.OutcomeResolution.RequestDigest = "forged" }), flowy.ErrExecutionCorrupt},
		{"prior state", task24CorruptChild(func(c *flowy.ChildRecord) { c.OutcomeResolution.PriorState = flowy.ChildCompleted }), flowy.ErrExecutionCorrupt},
		{"prior revision", task24CorruptChild(func(c *flowy.ChildRecord) { c.OutcomeResolution.ChildRevision-- }), flowy.ErrExecutionCorrupt},
		{"future source", task24CorruptChild(func(c *flowy.ChildRecord) { c.OutcomeResolution.SourceRevision = resolved.SnapshotRevision }), flowy.ErrExecutionCorrupt},
		{"fence", task24CorruptChild(func(c *flowy.ChildRecord) { c.OutcomeResolution.Incarnation = c.Incarnation }), flowy.ErrExecutionCorrupt},
		{"payload", task24CorruptChild(func(c *flowy.ChildRecord) { c.Result = []byte("forged") }), flowy.ErrExecutionCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Act.
			altered := task24AlteredStore{ExecutionStore: store, alter: tc.alter}
			_, resolveErr := task24ChildJoinRunner(t, altered, dispatch).ResolveChildOutcome(ctx, resolved, decision)
			// Assert: no new callback/dispatch or authoritative revision.
			latest, _, _ := task24ChildDecision(t, store)
			if !errors.Is(resolveErr, tc.want) || latest != resolved || calls.Load() != 1 {
				t.Fatalf("err=%v latest=%+v calls=%d", resolveErr, latest, calls.Load())
			}
		})
	}
}

func task24CorruptChild(mutate func(*flowy.ChildRecord)) func(*flowy.ExecutionEnvelope) {
	return func(e *flowy.ExecutionEnvelope) {
		var groups map[string]flowy.ChildGroupRecord
		_ = json.Unmarshal(e.ChildrenPayload, &groups)
		for id, g := range groups {
			mutate(&g.Children[0])
			groups[id] = g
		}
		e.ChildrenPayload, _ = json.Marshal(groups)
	}
}
