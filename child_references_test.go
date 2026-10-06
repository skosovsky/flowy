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

func childReferenceRunner(t *testing.T, store flowy.ExecutionStore, label, node string,
	plan flowy.ChildGroupPlan, dispatches, merges, nodes *atomic.Int32, migrations []flowy.ExecutionMigration,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	b := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	b.AddNode(node, func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		nodes.Add(1)
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
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor(label),
		checkpoint.JSONSerializer[durableTestState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Migrations: migrations})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func childReferenceMigration(source flowy.ExecutionEnvelope, binding string) flowy.ExecutionMigration {
	return flowy.ExecutionMigration{ID: "move-children", Source: source.Descriptor, Target: durableDescriptor("new"),
		Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
			state.ExecutionPointer = "new-node"
			if binding != "" {
				state.ChildGroupReferences = map[string]string{"group": binding}
			}
			return state, nil
		}}
}

func childReferenceSource(t *testing.T, store flowy.ExecutionStore,
	dispatches, merges, nodes *atomic.Int32,
) (flowy.ExecutionEnvelope, flowy.ChildGroupPlan, string) {
	t.Helper()
	plan := persistedChildPlan()
	plan.Children = []flowy.ChildSpec{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	_, err := childReferenceRunner(t, store, "old", "old-node", plan, dispatches, merges, nodes, nil).
		Start(context.Background(), "run", durableTestState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) {
		t.Fatalf("child migration fixture: %v", err)
	}
	source, err := store.LoadExecution(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	var groups map[string]flowy.ChildGroupRecord
	if err = json.Unmarshal(source.ChildrenPayload, &groups); err != nil || len(groups) != 1 {
		t.Fatalf("group fixture: %v/%+v", err, groups)
	}
	for identity := range groups {
		return source, plan, identity
	}
	t.Fatal("missing group")
	return flowy.ExecutionEnvelope{}, plan, ""
}

func resumeChildReference(ctx context.Context, runner *flowy.DurableRunner[durableTestState, flowy.NoEffect],
	token flowy.ResumeToken, stream bool,
) (*flowy.RunResult[durableTestState, flowy.NoEffect], error) {
	if !stream {
		return runner.Resume(ctx, token)
	}
	handle, err := runner.ResumeStream(ctx, token)
	if err != nil {
		return nil, err
	}
	for range handle.Events() {
	}
	return handle.WaitResult()
}

func TestChildMigrationBindingPreservesSiblingsAndOriginalResolution(t *testing.T) {
	t.Parallel()
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "stream"}[stream], func(t *testing.T) {
			t.Parallel()
			// Arrange: one completed child and two waiting siblings belong to the old cursor.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			var dispatches, merges, oldNodes, newNodes atomic.Int32
			source, plan, identity := childReferenceSource(t, store, &dispatches, &merges, &oldNodes)
			migration := childReferenceMigration(source, identity)
			target := childReferenceRunner(t, store, "new", "new-node", plan, &dispatches, &merges, &newNodes,
				[]flowy.ExecutionMigration{migration})
			// Act: migrate and recover the existing group, never relaunch its children.
			pending, err := resumeChildReference(ctx, target,
				flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision}, stream)
			head, loadErr := store.LoadExecution(ctx, "run")
			// Assert: only the migration checkpoint was added, with byte-identical group history.
			if !errors.Is(err, flowy.ErrChildrenUnresolved) || pending == nil || loadErr != nil ||
				head.Revision != source.Revision+1 || string(head.ChildrenPayload) != string(source.ChildrenPayload) ||
				head.Progress.ChildGroupReferences[plan.Key] != identity || dispatches.Load() != 3 || merges.Load() != 0 {
				t.Fatalf("migration rewrote/relaunched children: %v load=%v calls=%d", err, loadErr, dispatches.Load())
			}
			group := storedChildGroup(t, store, "run")
			token, err := target.ResolveChildWait(ctx, pending.ResumeToken, childWaitDecision(group, 1))
			if err != nil {
				t.Fatal(err)
			}
			second, err := resumeChildReference(ctx, target, token, stream)
			group = storedChildGroup(t, store, "run")
			if !errors.Is(err, flowy.ErrChildrenUnresolved) || second == nil ||
				group.Children[2].State != flowy.ChildWaiting ||
				group.Node != "old-node" ||
				dispatches.Load() != 3 {
				t.Fatalf("original resolution lost sibling/origin: %+v/%v", group, err)
			}
			token, err = target.ResolveChildWait(ctx, second.ResumeToken, childWaitDecision(group, 2))
			if err != nil {
				t.Fatal(err)
			}
			_, err = resumeChildReference(ctx, target, token, stream)
			finished, loadErr := store.LoadExecution(ctx, "run")
			if err != nil || loadErr != nil || finished.Terminal == nil || dispatches.Load() != 3 ||
				merges.Load() != 1 ||
				len(finished.Progress.ChildGroupReferences) != 0 {
				t.Fatalf(
					"migrated join/advance: %v load=%v calls/merges=%d/%d",
					err,
					loadErr,
					dispatches.Load(),
					merges.Load(),
				)
			}
		})
	}
}

func TestChildMigrationMissingOrForgedBindingRejectsBeforeNode(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"missing", "foreign", "wrong key", "changed plan"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			// Arrange: an unresolved old group cannot be discarded by a new cursor.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			var dispatches, merges, oldNodes, newNodes atomic.Int32
			source, plan, identity := childReferenceSource(t, store, &dispatches, &merges, &oldNodes)
			migration := invalidChildReferenceMigration(source, identity, kind)
			if kind == "changed plan" {
				plan.MergeLabel = "changed"
			}
			target := childReferenceRunner(t, store, "new", "new-node", plan, &dispatches, &merges, &newNodes,
				[]flowy.ExecutionMigration{migration})
			// Act.
			_, err := target.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision})
			// Assert: invalid references fail before commit; plan changes cannot create new children.
			after, loadErr := store.LoadExecution(ctx, "run")
			if kind == "changed plan" {
				if !errors.Is(err, flowy.ErrChildInvalid) || dispatches.Load() != 3 ||
					string(after.ChildrenPayload) != string(source.ChildrenPayload) {
					t.Fatalf("changed plan relaunched children: %v", err)
				}
			} else if !errors.Is(err, flowy.ErrMigrationInvalid) || loadErr != nil || after.Digest != source.Digest || newNodes.Load() != 0 {
				t.Fatalf("invalid binding mutated source: %v load=%v nodes=%d", err, loadErr, newNodes.Load())
			}
		})
	}
}

func invalidChildReferenceMigration(source flowy.ExecutionEnvelope, identity, kind string) flowy.ExecutionMigration {
	switch kind {
	case "missing":
		return childReferenceMigration(source, "")
	case "foreign":
		return childReferenceMigration(source, "foreign-group")
	case "wrong key":
		migration := childReferenceMigration(source, identity)
		migration.Transform = func(state flowy.MigrationState) (flowy.MigrationState, error) {
			state.ExecutionPointer = "new-node"
			state.ChildGroupReferences = map[string]string{"wrong": identity}
			return state, nil
		}
		return migration
	default:
		return childReferenceMigration(source, identity)
	}
}
