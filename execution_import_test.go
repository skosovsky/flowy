package flowy_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func importRunner(
	t *testing.T,
	store flowy.ExecutionStore,
	calls *atomic.Int32,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	return importRunnerOptions(t, store, calls, flowy.DurableOptions{Owner: "import-worker", LeaseTTL: time.Minute})
}

func importRunnerOptions(
	t *testing.T,
	store flowy.ExecutionStore,
	calls *atomic.Int32,
	options flowy.DurableOptions,
) *flowy.DurableRunner[durableTestState, flowy.NoEffect] {
	t.Helper()
	builder := flowy.NewGraph[durableTestState, flowy.NoEffect](
		func(_, update durableTestState) durableTestState { return update },
	)
	builder.AddNode("node", func(_ context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		calls.Add(1)
		state.Value++
		return state, flowy.End(), nil
	}).AllowNoOutgoingRoute("node").SetEntryPoint("node")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := flowy.NewDurableRunner(
		graph,
		store,
		durableDescriptor("target"),
		checkpoint.JSONSerializer[durableTestState]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		options,
	)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func legacySource() flowy.LegacyExecutionSource {
	payload := []byte("explicit legacy artifact")
	sum := sha256.Sum256(payload)
	return flowy.LegacyExecutionSource{
		ID:       "legacy-source",
		Revision: 7,
		Format:   "opaque-host-format",
		Digest:   hex.EncodeToString(sum[:]),
		Payload:  payload,
	}
}

func legacyImporter() flowy.ExecutionImporter {
	return flowy.ExecutionImporter{
		ID:     "host-import",
		Format: "opaque-host-format",
		Transform: func([]byte) (flowy.ImportedExecutionState, error) {
			return flowy.ImportedExecutionState{
				Progress:       flowy.MigrationState{ExecutionPointer: "node", StatePayload: []byte(`{"Value":41}`)},
				EffectsPayload: []byte(`[]`),
			}, nil
		},
	}
}

func TestExplicitImportRetainsSourceAndDoesNotDispatch(t *testing.T) {
	t.Parallel()
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	runner := importRunner(t, store, &calls)
	source := legacySource()
	original := bytes.Clone(source.Payload)
	importer := legacyImporter()
	transform := importer.Transform
	importer.Transform = func(payload []byte) (flowy.ImportedExecutionState, error) {
		payload[0] = 'X'
		return transform(payload)
	}
	// Act.
	token, err := runner.Import(ctx, "new-run", source, importer)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := store.LoadCheckpoint(ctx, "new-run", token.SnapshotRevision)
	// Assert: no old activities are invented and the retained source is detached.
	if err != nil || calls.Load() != 0 || !bytes.Equal(source.Payload, original) || imported.Import == nil ||
		!bytes.Equal(
			imported.Import.Source.Payload,
			original,
		) || imported.Import.ImporterID != importer.ID || len(imported.JournalPayload) != 0 {
		t.Fatalf("invalid import: %+v err=%v calls=%d", imported, err, calls.Load())
	}
	imported.Import.Source.Payload[0] = 'Y'
	resumed, err := runner.Resume(ctx, token)
	if err != nil || resumed.State.Value != 42 || calls.Load() != 1 {
		t.Fatalf("import resume: %+v %v calls=%d", resumed, err, calls.Load())
	}
	retained, err := store.LoadCheckpoint(ctx, "new-run", token.SnapshotRevision)
	if err != nil || !bytes.Equal(retained.Import.Source.Payload, original) {
		t.Fatalf("source retention lost: %+v %v", retained, err)
	}
	if _, importErr := runner.Import(
		ctx,
		"new-run",
		source,
		importer,
	); !errors.Is(
		importErr,
		flowy.ErrConcurrencyConflict,
	) {
		t.Fatalf("existing target overwritten: %v", importErr)
	}
}

func TestExplicitImportFailureLeavesNoTarget(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"digest", "format", "transform", "pointer", "state", "effects", "cancel", "journal-reference", "child-reference", "inline-key", "inline-pointer"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := testutil.NewMemoryExecutionStore(nil)
			var calls atomic.Int32
			runner := importRunner(t, store, &calls)
			source, importer := legacySource(), legacyImporter()
			transform := importer.Transform
			if scenario == "digest" {
				source.Digest = "corrupt"
			}
			if scenario == "format" {
				importer.Format = "different"
			}
			importer.Transform = func(payload []byte) (flowy.ImportedExecutionState, error) {
				state, err := transform(payload)
				if err != nil {
					return state, err
				}
				return invalidImportState(scenario, state, cancel)
			}
			// Act.
			_, importErr := runner.Import(ctx, "new-run", source, importer)
			// Assert.
			assertFailedImport(t, store, &calls, scenario, importErr)
			if scenario == "journal-reference" || scenario == "child-reference" {
				if token, err := runner.Import(
					ctx,
					"new-run",
					legacySource(),
					legacyImporter(),
				); err != nil ||
					token.SnapshotRevision != 1 {
					t.Fatalf("failed import retained target or lease: token=%+v err=%v", token, err)
				}
			}
		})
	}
}

func assertFailedImport(
	t *testing.T,
	store flowy.ExecutionStore,
	calls *atomic.Int32,
	scenario string,
	importErr error,
) {
	t.Helper()
	expected := flowy.ErrExecutionImportInvalid
	if scenario == "cancel" {
		expected = context.Canceled
	}
	if !errors.Is(importErr, expected) {
		t.Fatalf("unexpected import error: %v", importErr)
	}
	_, loadErr := store.LoadExecution(context.Background(), "new-run")
	if !errors.Is(loadErr, flowy.ErrThreadNotFound) || calls.Load() != 0 {
		t.Fatalf("failed import mutated target: %v calls=%d", loadErr, calls.Load())
	}
}

func invalidImportState(
	scenario string,
	state flowy.ImportedExecutionState,
	cancel context.CancelFunc,
) (flowy.ImportedExecutionState, error) {
	switch scenario {
	case "transform":
		return state, errors.New("transform failed")
	case "pointer":
		state.Progress.ExecutionPointer = "absent"
	case "state":
		state.Progress.StatePayload = []byte("invalid")
	case "effects":
		state.EffectsPayload = []byte("invalid")
	case "journal-reference":
		state.Progress.JournalReferences = map[string]string{"activity": "absent"}
	case "child-reference":
		state.Progress.ChildGroupReferences = map[string]string{"group": "absent"}
	case "inline-key":
		state.Progress.ChildCursors = map[string]flowy.ExecutionPointer{string([]byte{0xff}): "inline"}
	case "inline-pointer":
		state.Progress.ChildCursors = map[string]flowy.ExecutionPointer{
			"inline": flowy.ExecutionPointer(string([]byte{0xff})),
		}
	case "cancel":
		cancel()
	}
	return state, nil
}

func TestImportCorruptProvenanceRejectsResume(t *testing.T) {
	t.Parallel()
	// Arrange: damage the retained source artifact in a new raw checkpoint.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	var calls atomic.Int32
	runner := importRunner(t, store, &calls)
	token, err := runner.Import(ctx, "new-run", legacySource(), legacyImporter())
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := store.LoadExecution(ctx, token.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Import.Source.Payload[0] = 'X'
	lease, err := store.AcquireExecution(ctx, token.ThreadID, "seed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	corrupt, err := store.CommitExecution(ctx, token.SnapshotRevision, lease, envelope)
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := store.ReleaseExecution(ctx, lease); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	// Act.
	_, resumeErr := runner.Resume(ctx, flowy.ResumeToken{ThreadID: token.ThreadID, SnapshotRevision: corrupt.Revision})
	// Assert.
	if !errors.Is(resumeErr, flowy.ErrExecutionImportInvalid) || calls.Load() != 0 {
		t.Fatalf("corrupt source dispatched: %v calls=%d", resumeErr, calls.Load())
	}
}

func TestImportLeaseCoversHostTransform(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "renewed", true: "lost"}[lost], func(t *testing.T) {
			// Arrange: block the pure host transform until heartbeat is observed.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			clock := &testExecutionClock{}
			clock.set(time.Now())
			base := testutil.NewMemoryExecutionStore(clock.Now)
			counts := &faultExecutionStore{ExecutionStore: base}
			entered, proceed := make(chan struct{}), make(chan struct{})
			store := &heartbeatProbeStore{
				ExecutionStore: counts,
				renewed:        make(chan context.Context, 1),
				phase:          entered,
			}
			if lost {
				store.failure = flowy.ErrLeaseLost
			}
			var calls atomic.Int32
			runner := importRunnerOptions(
				t,
				store,
				&calls,
				flowy.DurableOptions{Owner: "worker", LeaseTTL: 30 * time.Millisecond},
			)
			importer := legacyImporter()
			transform := importer.Transform
			importer.Transform = func(payload []byte) (flowy.ImportedExecutionState, error) {
				close(entered)
				<-proceed
				return transform(payload)
			}
			var unblock sync.Once
			t.Cleanup(func() { unblock.Do(func() { close(proceed) }) })
			// Act.
			done := make(chan error, 1)
			go func() { _, err := runner.Import(ctx, "run", legacySource(), importer); done <- err }()
			awaitDurableBarrier(ctx, t, entered)
			renewCtx := awaitHeartbeatProbe(ctx, t, store.renewed)
			if lost {
				awaitDurableBarrier(ctx, t, renewCtx.Done())
			}
			unblock.Do(func() { close(proceed) })
			// Assert: losing heartbeat prevents any commit or node call.
			assertImportLeaseResult(ctx, t, done, counts, lost)
			if calls.Load() != 0 {
				t.Fatal("import executed a node")
			}
			assertRetryWorkerReleased(t, base)
		})
	}
}

func assertImportLeaseResult(
	ctx context.Context,
	t *testing.T,
	done <-chan error,
	counts *faultExecutionStore,
	lost bool,
) {
	t.Helper()
	select {
	case err := <-done:
		if lost && (!errors.Is(err, flowy.ErrLeaseLost) || counts.commits.Load() != 0) {
			t.Fatalf("lost import committed: %v commits=%d", err, counts.commits.Load())
		}
		if !lost && (err != nil || counts.commits.Load() != 1) {
			t.Fatalf("renewed import failed: %v commits=%d", err, counts.commits.Load())
		}
	case <-ctx.Done():
		t.Fatal("import did not exit")
	}
}
