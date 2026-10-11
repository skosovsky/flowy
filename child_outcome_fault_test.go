package flowy_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type childOutcomeLostAckStore struct{ flowy.ExecutionStore }

func (s *childOutcomeLostAckStore) CommitExecution(
	ctx context.Context,
	revision uint64,
	lease flowy.ExecutionLease,
	e flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	result, err := s.ExecutionStore.CommitExecution(ctx, revision, lease, e)
	if err == nil && bytes.Contains(e.ChildrenPayload, []byte(`"outcome_resolution"`)) {
		return flowy.ExecutionEnvelope{}, errInjectedCommit
	}
	return result, err
}

func TestChildOutcomeResolutionCommitFaults(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "outage", true: "lost_ack"}[lostAck], func(t *testing.T) {
			// Arrange: abandon one successful remote outcome at its journal commit.
			ctx := context.Background()
			base := testutil.NewMemoryExecutionStore(nil)
			var calls atomic.Int32
			dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				calls.Add(1)
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
			var failureStore flowy.ExecutionStore = &faultExecutionStore{ExecutionStore: base, failAt: 1}
			if lostAck {
				failureStore = &childOutcomeLostAckStore{ExecutionStore: base}
			}
			// Act: the response cannot establish whether the decision committed.
			_, resolveErr := task24ChildJoinRunner(t, failureStore, dispatch).ResolveChildOutcome(ctx, token, decision)
			latest, _, group := task24ChildDecision(t, base)
			// Assert: only authoritative storage distinguishes commit outage from lost acknowledgement.
			assertTask24ChildFaultOutcome(t, lostAck, resolveErr, latest, token, group, calls.Load())
			recovered, recoveryErr := task24ChildJoinRunner(
				t,
				base,
				dispatch,
			).ResolveChildOutcome(ctx, latest, decision)
			if recoveryErr != nil || calls.Load() != 1 {
				t.Fatalf("recovery=%v calls=%d", recoveryErr, calls.Load())
			}
			result, resumeErr := task24ChildJoinRunner(t, base, dispatch).Resume(ctx, recovered)
			if resumeErr != nil || result.Status != flowy.RunStatusCompleted || calls.Load() != 1 {
				t.Fatalf("join=%+v err=%v calls=%d", result, resumeErr, calls.Load())
			}
		})
	}
}

func assertTask24ChildFaultOutcome(
	t *testing.T,
	lostAck bool,
	resolveErr error,
	latest, token flowy.ResumeToken,
	group flowy.ChildGroupRecord,
	calls int32,
) {
	t.Helper()
	if !errors.Is(resolveErr, errInjectedCommit) || calls != 1 {
		t.Fatalf("err=%v calls=%d", resolveErr, calls)
	}
	if lostAck {
		if latest.SnapshotRevision != token.SnapshotRevision+1 || group.Children[0].OutcomeResolution == nil {
			t.Fatalf("committed decision missing: %+v", group)
		}
	} else if latest != token || group.Children[0].OutcomeResolution != nil {
		t.Fatalf("failed commit visible: %+v", group)
	}
}
