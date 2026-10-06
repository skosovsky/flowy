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

func TestActivityMigrationReferenceCommitFaultRecovery(t *testing.T) {
	t.Parallel()
	for _, failAt := range []int32{1, 2, 3} {
		t.Run(map[int32]string{1: "migration", 2: "reconciled outcome", 3: "terminal"}[failAt], func(t *testing.T) {
			t.Parallel()
			// Arrange: fail one aggregate commit, then recover with a new runner.
			ctx := context.Background()
			base := testutil.NewMemoryExecutionStore(nil)
			var dispatches, reconciles atomic.Int32
			source, request, migration := seedActivityReferenceRecovery(ctx, t, base, &dispatches, &reconciles)
			fault := &faultExecutionStore{ExecutionStore: base, failAt: failAt}
			runner := activityReferenceRunner(
				t,
				fault,
				"new",
				"new-node",
				request,
				[]flowy.ExecutionMigration{migration},
			)
			// Act.
			_, failErr := runner.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision})
			latest, err := base.LoadExecution(ctx, "run")
			if err != nil {
				t.Fatal(err)
			}
			// Assert: failure cannot fabricate terminal completion or a partial journal transition.
			assertActivityReferenceFault(t, source, latest, failAt, failErr, dispatches.Load())
			restarted := activityReferenceRunner(
				t,
				base,
				"new",
				"new-node",
				request,
				[]flowy.ExecutionMigration{migration},
			)
			_, resumeErr := restarted.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: latest.Revision})
			wantReconciles := int32(1)
			if failAt == 2 {
				wantReconciles = 2
			}
			if resumeErr != nil || dispatches.Load() != 1 || reconciles.Load() != wantReconciles {
				t.Fatalf(
					"fault recovery redispatched: err=%v dispatches=%d reconciles=%d",
					resumeErr,
					dispatches.Load(),
					reconciles.Load(),
				)
			}
		})
	}
}

func seedActivityReferenceRecovery(ctx context.Context, t *testing.T, store flowy.ExecutionStore,
	dispatches, reconciles *atomic.Int32) (flowy.ExecutionEnvelope, flowy.ActivityRequest, flowy.ExecutionMigration) {
	t.Helper()
	request := flowy.ActivityRequest{Key: "operation", Implementation: "host", Input: []byte("input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			dispatches.Add(1)
			return nil, errors.New("ambiguous delivery")
		}}
	old := activityReferenceRunner(t, store, "old", "old-node", request, nil)
	if _, err := old.Start(ctx, "run", durableTestState{}); !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	identity := onlyActivityIdentity(t, source)
	request.Reconcile = func(_ context.Context, record flowy.ActivityRecord) ([]byte, error) {
		reconciles.Add(1)
		if record.Identity != identity {
			return nil, errors.New("changed identity")
		}
		return []byte("confirmed"), nil
	}
	migration := flowy.ExecutionMigration{ID: "move", Source: source.Descriptor, Target: durableDescriptor("new"),
		Transform: func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
			state.ExecutionPointer = "new-node"
			state.JournalReferences = map[string]string{"operation": identity}
			return state, nil
		}}
	return source, request, migration
}

func assertActivityReferenceFault(
	t *testing.T,
	source, latest flowy.ExecutionEnvelope,
	failAt int32,
	err error,
	dispatches int32,
) {
	t.Helper()
	if !errors.Is(err, errInjectedCommit) || latest.Terminal != nil || dispatches != 1 ||
		latest.Revision != source.Revision+uint64(failAt-1) {
		t.Fatalf(
			"partial migration/outcome published: %v source=%d latest=%+v dispatches=%d",
			err,
			source.Revision,
			latest,
			dispatches,
		)
	}
	var journal map[string]flowy.ActivityRecord
	if decodeErr := json.Unmarshal(latest.JournalPayload, &journal); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	entry := journal[onlyActivityIdentity(t, source)]
	wantState := flowy.ActivityUnknown
	if failAt == 3 {
		wantState = flowy.ActivityCompleted
	}
	if entry.State != wantState || entry.Node != "old-node" || entry.Attempts[0].State != flowy.ActivityUnknown {
		t.Fatalf("fault rewrote original attempt: %+v", entry)
	}
	if failAt == 1 && latest.Digest != source.Digest {
		t.Fatal("failed migration changed source")
	}
}
