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

func TestActivityMigrationReferencePersistentManualStreamRecovery(t *testing.T) {
	// Arrange: unresolved source, migrated cursor, operator decision and replay use separate pools.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	var dispatches atomic.Int32
	request := flowy.ActivityRequest{Key: "operation", Implementation: "host", Input: []byte("input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			dispatches.Add(1)
			return nil, errors.New("ambiguous delivery")
		}}
	store := mustExecutionStore(t, pool)
	oldDescriptor := referenceDescriptor("old")
	old := persistentReferenceRunner(t, store, oldDescriptor, "old-node", request, nil)
	if _, err := old.Start(ctx, id, intState{}); !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	entry := persistentReferenceEntry(t, source)
	pool.Close()
	migrateCtx, migratePool := racePool(t)
	targetDescriptor := referenceDescriptor("new")
	migration := flowy.ExecutionMigration{ID: "move", Source: oldDescriptor, Target: targetDescriptor,
		Transform: func(state flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
			state.ExecutionPointer = "new-node"
			state.JournalReferences = map[string]string{"operation": entry.Identity}
			return state, nil
		}}
	migratedStore := mustExecutionStore(t, migratePool)
	target := persistentReferenceRunner(
		t,
		migratedStore,
		targetDescriptor,
		"new-node",
		request,
		[]flowy.ExecutionMigration{migration},
	)
	// Act: migrate but do not invent evidence about the remote outcome.
	_, migrateErr := target.Resume(migrateCtx, flowy.ResumeToken{ThreadID: id, SnapshotRevision: source.Revision})
	if !errors.Is(migrateErr, flowy.ErrActivityUnknown) {
		t.Fatalf("unresolved migration executed: %v", migrateErr)
	}
	migrated, err := migratedStore.LoadExecution(migrateCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	migratePool.Close()
	operatorCtx, operatorPool := racePool(t)
	operator := persistentReferenceRunner(
		t,
		mustExecutionStore(t, operatorPool),
		targetDescriptor,
		"new-node",
		request,
		nil,
	)
	token, err := operator.ResolveActivity(
		operatorCtx,
		flowy.ResumeToken{ThreadID: id, SnapshotRevision: migrated.Revision},
		flowy.ActivityResolution{
			Identity:       entry.Identity,
			InputDigest:    entry.InputDigest,
			Implementation: entry.Implementation,
			DecisionID:     "verified",
			Action:         flowy.ActivityResolveComplete,
			Reason:         "remote confirmed",
			Evidence:       "receipt",
			Outcome:        []byte("confirmed"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	operatorPool.Close()
	replayCtx, replayPool := racePool(t)
	replayedStore := mustExecutionStore(t, replayPool)
	replayed := persistentReferenceRunner(t, replayedStore, targetDescriptor, "new-node", request, nil)
	handle, err := replayed.ResumeStream(replayCtx, token)
	if err != nil {
		t.Fatal(err)
	}
	completedEvents := 0
	for event := range handle.Events() {
		if event.Type == flowy.EventCompleted {
			completedEvents++
		}
	}
	_, err = handle.WaitResult()
	// Assert.
	if err != nil || dispatches.Load() != 1 || completedEvents != 1 {
		t.Fatalf("manual binding replay redispatched: %v count=%d", err, dispatches.Load())
	}
	latest, err := replayedStore.LoadExecution(replayCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	completed := persistentReferenceEntry(t, latest)
	if completed.Identity != entry.Identity || completed.Node != "old-node" ||
		completed.Origin != flowy.ActivityManual ||
		completed.Attempts[0].State != flowy.ActivityUnknown ||
		len(latest.Progress.JournalReferences) != 0 {
		t.Fatalf("migrated manual outcome lost: %+v", completed)
	}
}

func referenceDescriptor(label string) flowy.ExecutionDescriptor {
	return flowy.ExecutionDescriptor{
		GraphID:       "reference-test",
		GraphRevision: label,
		StateCodec:    "json", EffectsCodec: "host-effects-v1",
		ExecutionContract: "sync",
		ReplayPolicy:      flowy.StepReplayPolicy{Label: "safe", Mode: flowy.StepReplaySafe},
	}
}

func persistentReferenceEntry(t *testing.T, envelope flowy.ExecutionEnvelope) flowy.ActivityRecord {
	t.Helper()
	var journal map[string]flowy.ActivityRecord
	if err := json.Unmarshal(envelope.JournalPayload, &journal); err != nil {
		t.Fatal(err)
	}
	if len(journal) != 1 {
		t.Fatalf("journal identity duplicated: %+v", journal)
	}
	for _, entry := range journal {
		return entry
	}
	t.Fatal("missing activity")
	return flowy.ActivityRecord{}
}

func persistentReferenceRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	descriptor flowy.ExecutionDescriptor,
	node string,
	request flowy.ActivityRequest,
	migrations []flowy.ExecutionMigration,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	return persistentReferenceRunnerOptions(t, store, descriptor, node, request,
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Migrations: migrations})
}

func persistentReferenceRunnerOptions(
	t *testing.T,
	store flowy.ExecutionStore,
	descriptor flowy.ExecutionDescriptor,
	node string,
	request flowy.ActivityRequest,
	options flowy.DurableOptions,
) *flowy.DurableRunner[intState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[intState, flowy.NoEffect](func(_, update intState) intState { return update })
	builder.AddNode(node, func(ctx context.Context, state intState) (intState, flowy.Directive, error) {
		_, err := flowy.CallActivity(ctx, request)
		return state, flowy.End(), err
	}).AllowNoOutgoingRoute(node).SetEntryPoint(node)
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, descriptor,
		checkpoint.JSONSerializer[intState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		options)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}
