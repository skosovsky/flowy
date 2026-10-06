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

func TestActivityMigrationRequiresBindingAndRecoversOriginalIdentity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name            string
		binding, stream bool
	}{
		{name: "sync dropped reference"}, {name: "stream dropped reference", stream: true},
		{name: "sync original attempt", binding: true}, {name: "stream original attempt", binding: true, stream: true},
	} {
		t.Run(
			test.name,
			func(t *testing.T) {
				t.Parallel()
				// Arrange: a dispatched operation has an ambiguous remote outcome.
				ctx := context.Background()
				store := testutil.NewMemoryExecutionStore(nil)
				var dispatches, reconciles atomic.Int32
				request := flowy.ActivityRequest{Key: "operation", Implementation: "host", Input: []byte("input"),
					Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
						dispatches.Add(1)
						return nil, errors.New("ambiguous remote delivery")
					}}
				old := activityReferenceRunner(t, store, "old", "old-node", request, nil)
				if _, err := old.Start(ctx, "run", durableTestState{}); !errors.Is(err, flowy.ErrActivityUnknown) {
					t.Fatalf("seed: %v", err)
				}
				source, err := store.LoadExecution(ctx, "run")
				if err != nil {
					t.Fatal(err)
				}
				identity := onlyActivityIdentity(t, source)
				migration := flowy.ExecutionMigration{
					ID:     "move-cursor",
					Source: source.Descriptor,
					Target: durableDescriptor("new"),
					Transform: func(state flowy.MigrationState) (flowy.MigrationState, error) {
						state.ExecutionPointer = "new-node"
						if test.binding {
							state.JournalReferences = map[string]string{"operation": identity}
						}
						return state, nil
					},
				}
				request.Reconcile = func(_ context.Context, record flowy.ActivityRecord) ([]byte, error) {
					reconciles.Add(1)
					if record.Identity != identity || record.Node != "old-node" {
						return nil, errors.New("address changed")
					}
					return []byte("confirmed"), nil
				}
				target := activityReferenceRunner(
					t,
					store,
					"new",
					"new-node",
					request,
					[]flowy.ExecutionMigration{migration},
				)
				// Act.
				_, resumeErr := resumeMigrated(
					ctx,
					t,
					target,
					flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision},
					test.stream,
				)
				// Assert.
				assertActivityMigrationReference(
					ctx,
					t,
					store,
					source,
					identity,
					test.binding,
					resumeErr,
					dispatches.Load(),
					reconciles.Load(),
				)
			},
		)
	}
}

func onlyActivityIdentity(t *testing.T, source flowy.ExecutionEnvelope) string {
	t.Helper()
	var journal map[string]flowy.ActivityRecord
	if err := json.Unmarshal(source.JournalPayload, &journal); err != nil {
		t.Fatal(err)
	}
	if len(journal) != 1 {
		t.Fatalf("expected one source activity: %+v", journal)
	}
	for identity := range journal {
		return identity
	}
	t.Fatal("missing source activity")
	return ""
}

func activityReferenceRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	label, node string,
	request flowy.ActivityRequest,
	migrations []flowy.ExecutionMigration,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	return activityReferenceRunnerOptions(t, store, label, node, request,
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute, Migrations: migrations})
}

func activityReferenceRunnerOptions(
	t *testing.T,
	store flowy.ExecutionStore,
	label, node string,
	request flowy.ActivityRequest,
	options flowy.DurableOptions,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode(node, func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		_, err := flowy.CallActivity(ctx, request)
		return state, flowy.End(), err
	}).AllowNoOutgoingRoute(node).SetEntryPoint(node)
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor(label),
		checkpoint.JSONSerializer[durableTestState]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		options)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func assertActivityMigrationReference(
	ctx context.Context,
	t *testing.T,
	store flowy.ExecutionStore,
	source flowy.ExecutionEnvelope,
	identity string,
	binding bool,
	resumeErr error,
	dispatches, reconciles int32,
) {
	t.Helper()
	latest, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	if !binding {
		if !errors.Is(resumeErr, flowy.ErrMigrationInvalid) || latest.Revision != source.Revision || dispatches != 1 ||
			reconciles != 0 {
			t.Fatalf(
				"unbound activity escaped: %v revision=%d dispatches=%d reconciles=%d",
				resumeErr,
				latest.Revision,
				dispatches,
				reconciles,
			)
		}
		return
	}
	var journal map[string]flowy.ActivityRecord
	if err := json.Unmarshal(latest.JournalPayload, &journal); err != nil {
		t.Fatal(err)
	}
	entry := journal[identity]
	if resumeErr != nil || dispatches != 1 || reconciles != 1 || entry.State != flowy.ActivityCompleted ||
		entry.Origin != flowy.ActivityReconciled || entry.Node != "old-node" || entry.Attempts[0].State != flowy.ActivityUnknown ||
		len(latest.Progress.JournalReferences) != 0 || len(journal) != 1 {
		t.Fatalf(
			"binding recovery lost identity: %v entry=%+v dispatches=%d reconciles=%d",
			resumeErr,
			entry,
			dispatches,
			reconciles,
		)
	}
}
