package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type corruptExecutionReadStore struct {
	flowy.ExecutionStore

	mutate func(*flowy.ExecutionEnvelope)
}

func (s corruptExecutionReadStore) LoadExecution(ctx context.Context, id string) (flowy.ExecutionEnvelope, error) {
	envelope, err := s.ExecutionStore.LoadExecution(ctx, id)
	if err == nil {
		s.mutate(&envelope)
	}
	return envelope, err
}

func TestDurableIntegrityRejectsBeforeCodecAndDispatch(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*flowy.ExecutionEnvelope){
		"unsealed": func(e *flowy.ExecutionEnvelope) { e.Digest = "" },
		"sealed null child groups": func(e *flowy.ExecutionEnvelope) {
			e.ChildrenPayload = []byte("null")
			e.Digest, _ = flowy.EnvelopeDigest(*e)
		},
		"state":    func(e *flowy.ExecutionEnvelope) { e.Progress.StatePayload = []byte("tampered") },
		"identity": func(e *flowy.ExecutionEnvelope) { e.ExecutionID = "other" },
		"revision": func(e *flowy.ExecutionEnvelope) { e.Revision++ },
		"sealed null journal": func(e *flowy.ExecutionEnvelope) {
			e.JournalPayload = []byte("null")
			e.Digest, _ = flowy.EnvelopeDigest(*e)
		},
		"sealed malformed journal": func(e *flowy.ExecutionEnvelope) {
			e.JournalPayload = []byte("{")
			e.Digest, _ = flowy.EnvelopeDigest(*e)
		},
		"sealed invalid migration source journal": func(e *flowy.ExecutionEnvelope) {
			e.Descriptor = durableDescriptor("old")
			e.JournalPayload = []byte("null")
			e.Digest, _ = flowy.EnvelopeDigest(*e)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: even a custom adapter bypassing its own validation is untrusted.
			ctx := context.Background()
			base := testutil.NewMemoryExecutionStore(nil)
			lease, err := base.AcquireExecution(ctx, "run", "seed", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			source, err := base.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
				ExecutionID: "run", Descriptor: durableDescriptor("current"),
				Progress: flowy.MigrationState{ExecutionPointer: "node", StatePayload: []byte("opaque")},
			})
			if err != nil {
				t.Fatal(err)
			}
			if releaseErr := base.ReleaseExecution(ctx, lease); releaseErr != nil {
				t.Fatal(releaseErr)
			}
			var probes atomic.Int32
			runner, err := flowy.NewDurableRunner(replayPolicyGraph(t, &probes),
				corruptExecutionReadStore{ExecutionStore: base, mutate: mutate}, durableDescriptor("current"),
				failingDecode{calls: &probes}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
				flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Migrations: []flowy.ExecutionMigration{
					{
						ID:        "old-to-current",
						Source:    durableDescriptor("old"),
						Target:    durableDescriptor("current"),
						Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) { probes.Add(1); return state, nil },
					},
				}})
			if err != nil {
				t.Fatal(err)
			}
			token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}
			// Act.
			_, resumeErr := runner.Resume(ctx, token)
			_, streamErr := runner.ResumeStream(ctx, token)
			_, resolutionErr := runner.ResolveActivity(ctx, token, flowy.ActivityResolution{
				Identity: "activity", InputDigest: "digest", Implementation: "implementation",
				DecisionID: "operator-decision", Action: flowy.ActivityResolveComplete,
				Reason: "verified", Evidence: "host-evidence",
			})
			// Assert.
			assertRuntimeCorruptionErrors(t, resumeErr, streamErr, resolutionErr)
			latest, err := base.LoadExecution(ctx, "run")
			if err != nil || latest.Revision != source.Revision || probes.Load() != 0 {
				t.Fatalf("corruption caused work: revision=%d probes=%d error=%v", latest.Revision, probes.Load(), err)
			}
		})
	}
}

func assertRuntimeCorruptionErrors(t *testing.T, results ...error) {
	t.Helper()
	for _, result := range results {
		if !errors.Is(result, flowy.ErrExecutionCorrupt) {
			t.Fatalf("corruption bypassed: %v", result)
		}
	}
}
