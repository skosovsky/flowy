//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

type postgresLateChildStore struct {
	flowy.ExecutionStore

	late chan error
}

func (s *postgresLateChildStore) CommitExecution(ctx context.Context, revision uint64, lease flowy.ExecutionLease,
	envelope flowy.ExecutionEnvelope) (flowy.ExecutionEnvelope, error) {
	committed, err := s.ExecutionStore.CommitExecution(ctx, revision, lease, envelope)
	var groups map[string]flowy.ChildGroupRecord
	if json.Unmarshal(envelope.ChildrenPayload, &groups) == nil {
		for _, group := range groups {
			for _, child := range group.Children {
				if child.State == flowy.ChildCompleted {
					s.late <- err
				}
			}
		}
	}
	return committed, err
}

func nonCooperativeChildPlan() flowy.ChildGroupPlan {
	return flowy.ChildGroupPlan{Key: "cancel", Label: "isolated", MergeLabel: "ordered", BudgetLabel: "fixed",
		CancelLabel: "confirmed", MaxConcurrency: 1, FailurePolicy: flowy.ChildCollectErrors,
		Children: []flowy.ChildSpec{{ID: "child"}}}
}

func blockedPostgresChildNode(gate <-chan struct{}, calls *atomic.Int32) flowy.Node[intState, flowy.NoEffect] {
	return func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
		started := make(chan struct{})
		finished := make(chan error, 1)
		plan := nonCooperativeChildPlan()
		go func() {
			_, runErr := flowy.RunChildren(
				ctx,
				plan,
				nil,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					calls.Add(1)
					close(started)
					<-gate
					return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("late")}, nil
				},
			)
			finished <- runErr
		}()
		select {
		case <-started:
		case <-ctx.Done():
			return state, flowy.End(), ctx.Err()
		}
		group, err := flowy.PrepareChildren(ctx, plan, nil)
		if err == nil {
			_, err = flowy.CancelChildren(
				ctx,
				group,
				flowy.ChildCancelRequest{ID: "stop", Reason: "host requested"},
				func(context.Context, flowy.ChildCancelNotice) error { return nil },
			)
		}
		if err != nil {
			return state, flowy.End(), err
		}
		select {
		case runErr := <-finished:
			return state, flowy.End(), runErr
		case <-ctx.Done():
			return state, flowy.End(), ctx.Err()
		}
	}
}

func recoveredPostgresChildNode(calls *atomic.Int32) flowy.Node[intState, flowy.NoEffect] {
	return func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
		group, err := flowy.RunChildren(
			ctx,
			nonCooperativeChildPlan(),
			nil,
			func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				calls.Add(100)
				return flowy.ChildResult{State: flowy.ChildCompleted}, nil
			},
		)
		if err == nil {
			_, err = flowy.JoinChildren(
				ctx,
				group,
				func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil },
			)
		}
		return state, flowy.End(), err
	}
}

func TestChildNonCooperativeCancellationPersistentRecoveryFencesLiveOldConnection(t *testing.T) {
	// Arrange: keep the old pool and callback alive while its coordinator releases ownership.
	ctx, pool := racePool(t)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	oldStore := &postgresLateChildStore{ExecutionStore: mustExecutionStore(t, pool), late: make(chan error, 1)}
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	var calls atomic.Int32
	first, err := postgresChildNodeRunner(
		t,
		oldStore,
		blockedPostgresChildNode(gate, &calls),
	).Start(ctx, id, intState{})
	if first == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("coordinator blocked on remote work: %v", err)
	}
	// Act: independent pool owns recovery/confirmation; the old database connection stays live.
	recoveryCtx, recoveryPool := racePool(t)
	recoveryStore := mustExecutionStore(t, recoveryPool)
	runner := postgresChildNodeRunner(t, recoveryStore, recoveredPostgresChildNode(&calls))
	second, err := runner.Resume(recoveryCtx, first.ResumeToken)
	group := postgresStoredChildGroup(recoveryCtx, t, recoveryStore, id)
	if second == nil || !errors.Is(err, flowy.ErrChildrenUnresolved) || group.Children[0].State != flowy.ChildUnknown ||
		group.Children[0].CancelConfirmed {
		t.Fatalf("recovery fabricated outcome: %v group=%+v", err, group)
	}
	assertPersistentChildCancelConfirmation(
		ctx,
		t,
		runner,
		recoveryStore,
		second.ResumeToken,
		group,
		gate,
		oldStore,
		&calls,
	)
}

func assertPersistentChildCancelConfirmation(ctx context.Context, t *testing.T,
	runner *flowy.DurableRunner[intState, flowy.NoEffect], store flowy.ExecutionStore, token flowy.ResumeToken,
	group flowy.ChildGroupRecord, gate chan struct{}, oldStore *postgresLateChildStore, calls *atomic.Int32) {
	t.Helper()
	child := group.Children[0]
	decision := flowy.ChildCancelConfirmation{
		Node:          group.Node,
		Activation:    group.Activation,
		GroupKey:      group.Plan.Key,
		ChildID:       child.Spec.ID,
		ExecutionID:   child.ExecutionID,
		ChildRevision: child.Revision,
		RequestID:     group.CancelRequest.ID,
		DecisionID:    "confirmed",
		Reason:        "worker terminated",
		Evidence:      "host termination evidence",
	}
	token, err := runner.ConfirmChildCancellation(ctx, token, decision)
	if err != nil {
		t.Fatal(err)
	}
	if _, resumeErr := runner.Resume(ctx, token); resumeErr != nil {
		t.Fatal(resumeErr)
	}
	before, err := store.LoadExecution(ctx, token.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	close(gate)
	select {
	case lateErr := <-oldStore.late:
		if !errors.Is(lateErr, flowy.ErrLeaseLost) && !errors.Is(lateErr, flowy.ErrConcurrencyConflict) {
			t.Fatalf("old connection failed without fence/OCC rejection: %v", lateErr)
		}
	case <-ctx.Done():
		t.Fatal("late commit not observed")
	}
	// Assert: database fencing, not pool closure, rejects the old owner.
	after, err := store.LoadExecution(ctx, token.ThreadID)
	if err != nil || before.Digest != after.Digest || after.Terminal == nil || calls.Load() != 1 {
		t.Fatalf("old owner changed durable outcome: %v calls=%d", err, calls.Load())
	}
}
