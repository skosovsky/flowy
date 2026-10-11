package flowy_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

// A nil embedded base makes any non-history call fail the test immediately.
type historicalReadOnlyProbe struct {
	flowy.ExecutionStore

	history flowy.ExecutionHistoryStore
	reads   atomic.Int32
}

func (p *historicalReadOnlyProbe) LoadCheckpoint(
	ctx context.Context,
	id string,
	revision uint64,
) (flowy.ExecutionEnvelope, error) {
	p.reads.Add(1)
	return p.history.LoadCheckpoint(ctx, id, revision)
}

type executionWithoutHistory struct{ flowy.ExecutionStore }

func seedHistoryInspection(t *testing.T) (*testutil.MemoryExecutionStore, flowy.ExecutionEnvelope) {
	t.Helper()
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	lease, err := store.AcquireExecution(ctx, "source", "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
		ExecutionID: "source", Descriptor: durableDescriptor("current"), Activation: 1,
		Progress: flowy.ExecutionProgress{ExecutionPointer: "node", StatePayload: []byte("opaque-host-state")},
	})
	if err != nil {
		t.Fatal(err)
	}
	later := source
	later.Progress.StatePayload = []byte("later-host-state")
	if _, err = store.CommitExecution(ctx, source.Revision, lease, later); err != nil {
		t.Fatal(err)
	}
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	return store, source
}

func TestHistoricalInspectionExactDetachedAndReadOnly(t *testing.T) {
	t.Parallel()
	// Arrange: opaque state cannot be decoded with a guessed JSON codec.
	store, source := seedHistoryInspection(t)
	probe := &historicalReadOnlyProbe{history: store}
	ref := flowy.HistoricalCheckpointReference{
		ExecutionID: source.ExecutionID,
		Revision:    source.Revision,
		Digest:      source.Digest,
	}
	// Act: inspection must use only the exact historical capability.
	inspected, err := flowy.InspectExecutionCheckpoint(context.Background(), probe, ref)
	// Assert.
	if err != nil || inspected.Digest != source.Digest || inspected.Revision != 1 || probe.reads.Load() != 1 ||
		string(inspected.Progress.StatePayload) != "opaque-host-state" {
		t.Fatalf("inspection decoded/dispatched/fell back: %+v err=%v", inspected, err)
	}
	inspected.Progress.StatePayload[0] = 'x'
	retained, retainErr := store.LoadCheckpoint(context.Background(), "source", 1)
	latest, latestErr := store.LoadExecution(context.Background(), "source")
	if retainErr != nil || latestErr != nil || retained.Digest != source.Digest ||
		string(retained.Progress.StatePayload) != "opaque-host-state" || latest.Revision != 2 ||
		string(latest.Progress.StatePayload) != "later-host-state" {
		t.Fatalf(
			"inspection mutated source/history: retained=%+v latest=%+v errors=%v/%v",
			retained,
			latest,
			retainErr,
			latestErr,
		)
	}
}

func TestHistoricalInspectionRejectsUnsupportedMissingAndWrongDigest(t *testing.T) {
	t.Parallel()
	store, source := seedHistoryInspection(t)
	for _, tc := range []struct {
		name     string
		store    flowy.ExecutionStore
		revision uint64
		digest   string
		want     error
	}{
		{name: "unsupported", store: executionWithoutHistory{store}, revision: 1, digest: source.Digest, want: flowy.ErrExecutionHistoryUnsupported},
		{name: "missing", store: store, revision: 99, digest: source.Digest, want: flowy.ErrExecutionCheckpointUnavailable},
		{name: "zero", store: store, revision: 0, digest: source.Digest, want: flowy.ErrExecutionLifecycleInvalid},
		{name: "digest", store: store, revision: 1, digest: strings.Repeat("0", 64), want: flowy.ErrExecutionSourceDigest},
		{name: "empty digest", store: store, revision: 1, digest: "", want: flowy.ErrExecutionSourceDigest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			ref := flowy.HistoricalCheckpointReference{ExecutionID: "source", Revision: tc.revision, Digest: tc.digest}
			// Act.
			got, err := flowy.InspectExecutionCheckpoint(context.Background(), tc.store, ref)
			// Assert: failure returns no checkpoint, especially no latest fallback.
			if !errors.Is(err, tc.want) || got.ExecutionID != "" {
				t.Fatalf("wrong exact-history failure: %+v err=%v", got, err)
			}
		})
	}
}

func TestDurableStartResumeDoNotRequireHistory(t *testing.T) {
	t.Parallel()
	// Arrange: wrapping only the base interface hides optional history entirely.
	store := executionWithoutHistory{testutil.NewMemoryExecutionStore(nil)}
	var nodes atomic.Int32
	b := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	b.AddNode("node", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		nodes.Add(1)
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	runner := compileActivityTestRunner(t, b, store)
	// Act.
	started, err := runner.Start(context.Background(), "ordinary", durableTestState{Value: 7})
	if err != nil {
		t.Fatal(err)
	}
	resumed, resumeErr := runner.Resume(context.Background(), started.ResumeToken)
	// Assert.
	if resumeErr != nil || resumed.State.Value != 7 || nodes.Load() != 1 {
		t.Fatalf("base runtime requires history: %+v err=%v", resumed, resumeErr)
	}
}
