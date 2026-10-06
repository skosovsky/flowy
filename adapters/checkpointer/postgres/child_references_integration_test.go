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
	"github.com/skosovsky/flowy/checkpoint"
)

func postgresChildReferenceRunner(t *testing.T, store flowy.ExecutionStore, label, node string,
	dispatches, merges *atomic.Int32, migrations []flowy.ExecutionMigration,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	plan := flowy.ChildGroupPlan{Key: "waits", Label: "isolated", MergeLabel: "ordered", BudgetLabel: "fixed",
		CancelLabel: "confirmed", MaxConcurrency: 2, FailurePolicy: flowy.ChildCollectErrors,
		Children: []flowy.ChildSpec{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	b := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	b.AddNode(node, func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
		group, err := flowy.RunChildren(ctx, plan, nil,
			func(_ context.Context, invocation flowy.ChildInvocation) (flowy.ChildResult, error) {
				dispatches.Add(1)
				if invocation.ChildID == "a" {
					return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("a")}, nil
				}
				return flowy.ChildResult{State: flowy.ChildWaiting, WaitID: "wait-" + invocation.ChildID}, nil
			})
		if err == nil {
			_, err = flowy.JoinChildren(
				ctx,
				group,
				func(_ context.Context, children []flowy.ChildRecord) ([]byte, error) {
					merges.Add(1)
					return append(append(children[0].Result, children[1].Result...), children[2].Result...), nil
				},
			)
		}
		return state, flowy.End(), err
	}).AllowNoOutgoingRoute(node).SetEntryPoint(node)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, referenceDescriptor(label),
		checkpoint.JSONSerializer[intState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Migrations: migrations})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestChildMigrationPersistentOriginalResolutionAndJoin(t *testing.T) {
	// Arrange: a genuine completed child and two waits survive the original pool.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	store := mustExecutionStore(t, pool)
	var dispatches, merges atomic.Int32
	_, err := postgresChildReferenceRunner(t, store, "old", "old-node", &dispatches, &merges, nil).
		Start(ctx, id, intState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("source fixture: %v", err)
	}
	source, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var groups map[string]flowy.ChildGroupRecord
	if err = json.Unmarshal(source.ChildrenPayload, &groups); err != nil || len(groups) != 1 {
		t.Fatalf("source groups: %v", err)
	}
	identity := ""
	for key := range groups {
		identity = key
	}
	pool.Close()
	// Act: migrate through a new pool, then resolve children with their old addresses.
	migrationCtx, migrationPool := racePool(t)
	migration := flowy.ExecutionMigration{
		ID:     "move-children",
		Source: source.Descriptor,
		Target: referenceDescriptor("new"),
		Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
			state.ExecutionPointer = "new-node"
			state.ChildGroupReferences = map[string]string{"waits": identity}
			return state, nil
		},
	}
	migrationStore := mustExecutionStore(t, migrationPool)
	pending, err := postgresChildReferenceRunner(t, migrationStore, "new", "new-node", &dispatches, &merges,
		[]flowy.ExecutionMigration{migration}).Resume(migrationCtx,
		flowy.ResumeToken{ThreadID: id, SnapshotRevision: source.Revision})
	head, loadErr := migrationStore.LoadExecution(migrationCtx, id)
	if !errors.Is(err, flowy.ErrChildrenUnresolved) || pending == nil || loadErr != nil ||
		string(head.ChildrenPayload) != string(source.ChildrenPayload) || dispatches.Load() != 3 {
		t.Fatalf("migration relaunched or rewrote children: %v load=%v", err, loadErr)
	}
	migrationPool.Close()
	token := resolveMigratedPersistentChild(t, pending.ResumeToken, 1, &dispatches, &merges)
	finishMigratedPersistentChildren(t, token, source, &dispatches, &merges)
}

func resolveMigratedPersistentChild(t *testing.T, token flowy.ResumeToken, index int,
	dispatches, merges *atomic.Int32,
) flowy.ResumeToken {
	t.Helper()
	ctx, pool := racePool(t)
	store := mustExecutionStore(t, pool)
	runner := postgresChildReferenceRunner(t, store, "new", "new-node", dispatches, merges, nil)
	group := postgresStoredChildGroup(ctx, t, store, token.ThreadID)
	next, err := runner.ResolveChildWait(ctx, token, postgresChildWaitDecision(group, index))
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runner.Resume(ctx, next)
	if !errors.Is(err, flowy.ErrChildrenUnresolved) || pending == nil || dispatches.Load() != 3 {
		t.Fatalf("sibling recovery: %v", err)
	}
	pool.Close()
	return pending.ResumeToken
}

func finishMigratedPersistentChildren(t *testing.T, token flowy.ResumeToken, source flowy.ExecutionEnvelope,
	dispatches, merges *atomic.Int32,
) {
	t.Helper()
	ctx, pool := racePool(t)
	store := mustExecutionStore(t, pool)
	runner := postgresChildReferenceRunner(t, store, "new", "new-node", dispatches, merges, nil)
	group := postgresStoredChildGroup(ctx, t, store, token.ThreadID)
	next, err := runner.ResolveChildWait(ctx, token, postgresChildWaitDecision(group, 2))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := runner.ResumeStream(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	for range handle.Events() {
	}
	_, err = handle.WaitResult()
	head, loadErr := store.LoadExecution(ctx, token.ThreadID)
	old, oldErr := store.LoadCheckpoint(ctx, token.ThreadID, source.Revision)
	// Assert: original history remains, siblings are joined once and references clear on advance.
	if err != nil || loadErr != nil || oldErr != nil || old.Digest != source.Digest || head.Terminal == nil ||
		head.Migration == nil || dispatches.Load() != 3 || merges.Load() != 1 || len(head.Progress.ChildGroupReferences) != 0 {
		t.Fatalf(
			"migration restart/join: %v load=%v old=%v dispatch/merge=%d/%d",
			err,
			loadErr,
			oldErr,
			dispatches.Load(),
			merges.Load(),
		)
	}
}
