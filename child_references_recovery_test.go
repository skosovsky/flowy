package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func childMigrationNodeRunner(t *testing.T, store flowy.ExecutionStore, label, node string,
	body flowy.Node[durableTestState, flowy.NoEffect], migrations []flowy.ExecutionMigration,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	b.AddNode(node, body).AllowNoOutgoingRoute(node).SetEntryPoint(node)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor(label),
		checkpoint.JSONSerializer[durableTestState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Migrations: migrations})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func childMigrationIdentity(t *testing.T, source flowy.ExecutionEnvelope) string {
	t.Helper()
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(source.ChildrenPayload, &groups); err != nil || len(groups) != 1 {
		t.Fatalf("group identity: %v/%+v", err, groups)
	}
	for identity := range groups {
		return identity
	}
	t.Fatal("missing group")
	return ""
}

func childMigrationBudgetNode(
	finish *atomic.Bool,
	dispatches, merges *atomic.Int32,
) flowy.Node[durableTestState, flowy.NoEffect] {
	return func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		capacity := map[string]int{"units": 10}
		group, err := flowy.RunChildren(ctx, budgetChildPlan("group", 8), capacity,
			func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				dispatches.Add(1)
				return flowy.ChildResult{State: flowy.ChildCompleted}, nil
			})
		if err != nil {
			return state, flowy.End(), err
		}
		group, err = flowy.ReturnChildBudget(ctx, group, flowy.ChildBudgetReturn{
			ChildID: group.Children[0].Spec.ID, ChildRevision: group.Children[0].Revision,
			DecisionID: "usage", Reason: "completed usage", Evidence: "host meter", Used: map[string]int{"units": 3},
		})
		if err != nil {
			return state, flowy.End(), err
		}
		_, budgetErr := flowy.PrepareChildren(ctx, budgetChildPlan("oversubscribed", 8), capacity)
		if !errors.Is(budgetErr, flowy.ErrBudgetExceeded) {
			return state, flowy.End(), errors.New("migration replenished returned budget twice")
		}
		if !finish.Load() {
			return state, flowy.End(), flowy.ErrChildrenUnresolved
		}
		_, err = flowy.JoinChildren(ctx, group, func(context.Context, []flowy.ChildRecord) ([]byte, error) {
			merges.Add(1)
			return nil, nil
		})
		return state, flowy.End(), err
	}
}

func TestChildMigrationBudgetLedgerDoesNotReplenishOnReplay(t *testing.T) {
	t.Parallel()
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "stream"}[stream], func(t *testing.T) {
			t.Parallel()
			// Arrange: committed allocation eight, consumption three, one return of five.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			var finish atomic.Bool
			var dispatches, merges atomic.Int32
			body := childMigrationBudgetNode(&finish, &dispatches, &merges)
			_, err := childMigrationNodeRunner(
				t,
				store,
				"old",
				"old-node",
				body,
				nil,
			).Start(ctx, "run", durableTestState{})
			if !errors.Is(err, flowy.ErrChildrenUnresolved) {
				t.Fatalf("budget source: %v", err)
			}
			source, err := store.LoadExecution(ctx, "run")
			if err != nil {
				t.Fatal(err)
			}
			migration := childReferenceMigration(source, childMigrationIdentity(t, source))
			target := childMigrationNodeRunner(t, store, "new", "new-node", body, []flowy.ExecutionMigration{migration})
			// Act: replay the old usage claim at a new cursor, then replay it again.
			pending, err := resumeChildReference(ctx, target,
				flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}, stream)
			if !errors.Is(err, flowy.ErrChildrenUnresolved) || pending == nil {
				t.Fatalf("migrated budget: %v", err)
			}
			pending, err = resumeChildReference(ctx, target, pending.ResumeToken, stream)
			head, loadErr := store.LoadExecution(ctx, "run")
			// Assert: only migration adds a revision; the exact allocation/return ledger is retained.
			if !errors.Is(err, flowy.ErrChildrenUnresolved) || pending == nil || loadErr != nil ||
				head.Revision != source.Revision+1 || string(head.ChildrenPayload) != string(source.ChildrenPayload) ||
				dispatches.Load() != 1 || merges.Load() != 0 {
				t.Fatalf("budget replay changed ledger: %v load=%v calls=%d", err, loadErr, dispatches.Load())
			}
			finish.Store(true)
			_, err = resumeChildReference(ctx, target, pending.ResumeToken, stream)
			if err != nil || dispatches.Load() != 1 || merges.Load() != 1 {
				t.Fatalf("migrated budget join: %v calls/merges=%d/%d", err, dispatches.Load(), merges.Load())
			}
		})
	}
}

func childMigrationUnknownNode(
	dispatches, notifications, merges *atomic.Int32,
) flowy.Node[durableTestState, flowy.NoEffect] {
	return func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		capacity := map[string]int{"units": 8}
		group, runErr := flowy.RunChildren(ctx, budgetChildPlan("group", 8), capacity,
			func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				dispatches.Add(1)
				return flowy.ChildResult{State: flowy.ChildUnknown}, nil
			})
		if errors.Is(runErr, flowy.ErrChildrenUnresolved) {
			_, returnErr := flowy.ReturnChildBudget(ctx, group, flowy.ChildBudgetReturn{
				ChildID: group.Children[0].Spec.ID, ChildRevision: group.Children[0].Revision,
				DecisionID: "unused", Reason: "usage claim", Evidence: "host claim", Used: map[string]int{"units": 0},
			})
			_, budgetErr := flowy.PrepareChildren(ctx, budgetChildPlan("second", 1), capacity)
			if !errors.Is(returnErr, flowy.ErrChildrenUnresolved) || !errors.Is(budgetErr, flowy.ErrBudgetExceeded) {
				return state, flowy.End(), errors.New("unknown migration released allocation")
			}
			_, err := flowy.CancelChildren(ctx, group, flowy.ChildCancelRequest{ID: "stop", Reason: "host requested"},
				func(context.Context, flowy.ChildCancelNotice) error { notifications.Add(1); return nil })
			if err != nil {
				return state, flowy.End(), err
			}
			return state, flowy.End(), runErr
		}
		if runErr != nil {
			return state, flowy.End(), runErr
		}
		_, err := flowy.JoinChildren(ctx, group, func(context.Context, []flowy.ChildRecord) ([]byte, error) {
			merges.Add(1)
			return nil, nil
		})
		return state, flowy.End(), err
	}
}

func TestChildMigrationUnknownRetainsBudgetAndOriginalCancellationAddress(t *testing.T) {
	t.Parallel()
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "stream"}[stream], func(t *testing.T) {
			t.Parallel()
			// Arrange: requested unknown external work still occupies the entire capacity.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			var dispatches, notifications, merges atomic.Int32
			body := childMigrationUnknownNode(&dispatches, &notifications, &merges)
			_, err := childMigrationNodeRunner(
				t,
				store,
				"old",
				"old-node",
				body,
				nil,
			).Start(ctx, "run", durableTestState{})
			if !errors.Is(err, flowy.ErrChildrenUnresolved) {
				t.Fatalf("unknown source: %v", err)
			}
			source, err := store.LoadExecution(ctx, "run")
			if err != nil {
				t.Fatal(err)
			}
			migration := childReferenceMigration(source, childMigrationIdentity(t, source))
			target := childMigrationNodeRunner(t, store, "new", "new-node", body, []flowy.ExecutionMigration{migration})
			// Act: migration preserves unknown/request/ledger; only explicit original confirmation settles it.
			pending, err := resumeChildReference(ctx, target,
				flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}, stream)
			head, loadErr := store.LoadExecution(ctx, "run")
			if !errors.Is(err, flowy.ErrChildrenUnresolved) || pending == nil || loadErr != nil ||
				string(
					head.ChildrenPayload,
				) != string(
					source.ChildrenPayload,
				) || dispatches.Load() != 1 || notifications.Load() != 2 {
				t.Fatalf("unknown/request rewritten: %v load=%v calls=%d", err, loadErr, dispatches.Load())
			}
			decision := cancellationDecision(storedChildGroup(t, store, "run"))
			rejectRelabelledChildConfirmation(ctx, t, target, pending.ResumeToken, decision)
			token, err := target.ConfirmChildCancellation(ctx, pending.ResumeToken, decision)
			if err != nil {
				t.Fatal(err)
			}
			_, err = resumeChildReference(ctx, target, token, stream)
			group := storedChildGroup(t, store, "run")
			// Assert: no redispatch/false stop; explicit confirmation and one join preserve prior unknown provenance.
			if err != nil || dispatches.Load() != 1 || merges.Load() != 1 || group.Node != "old-node" ||
				group.Children[0].CancelConfirmation == nil || group.Children[0].CancelConfirmation.PriorState != flowy.ChildUnknown ||
				len(group.BudgetReturns) != 0 {
				t.Fatalf("migrated unknown cancellation: %v group=%+v", err, group)
			}
		})
	}
}

func rejectRelabelledChildConfirmation(ctx context.Context, t *testing.T,
	runner *flowy.DurableRunner[durableTestState, flowy.NoEffect], token flowy.ResumeToken,
	decision flowy.ChildCancelConfirmation,
) {
	t.Helper()
	decision.Node = "new-node"
	_, err := runner.ConfirmChildCancellation(ctx, token, decision)
	if !errors.Is(err, flowy.ErrChildRevision) {
		t.Fatalf("confirmation relabelled original address: %v", err)
	}
}

func TestChildMigrationCommitFaultPreservesGroupAndCanRetry(t *testing.T) {
	t.Parallel()
	// Arrange: the fault is in the migration commit, before target node/child recovery.
	ctx := context.Background()
	base := testutil.NewMemoryExecutionStore(nil)
	var dispatches, merges, oldNodes, newNodes atomic.Int32
	source, plan, identity := childReferenceSource(t, base, &dispatches, &merges, &oldNodes)
	store := &faultExecutionStore{ExecutionStore: base, failAt: 1}
	migration := childReferenceMigration(source, identity)
	target := childReferenceRunner(t, store, "new", "new-node", plan, &dispatches, &merges, &newNodes,
		[]flowy.ExecutionMigration{migration})
	token := flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}
	// Act: failed publication must acknowledge no migrated boundary.
	_, err := target.Resume(ctx, token)
	after, loadErr := base.LoadExecution(ctx, "run")
	// Assert: source remains exact, target node has not run, retry preserves the old group.
	if !errors.Is(err, errInjectedCommit) || loadErr != nil || after.Digest != source.Digest ||
		newNodes.Load() != 0 || dispatches.Load() != 3 {
		t.Fatalf("failed migration exposed target: %v load=%v nodes=%d", err, loadErr, newNodes.Load())
	}
	pending, err := target.Resume(ctx, token)
	after, loadErr = base.LoadExecution(ctx, "run")
	if !errors.Is(err, flowy.ErrChildrenUnresolved) || pending == nil || loadErr != nil ||
		after.Revision != source.Revision+1 || string(after.ChildrenPayload) != string(source.ChildrenPayload) ||
		dispatches.Load() != 3 || newNodes.Load() != 1 {
		t.Fatalf("migration retry duplicated child work: %v load=%v", err, loadErr)
	}
}
