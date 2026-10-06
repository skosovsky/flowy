package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

//nolint:gocognit // One adversarial matrix asserts three independent public boundaries against the same before-image.
func TestLifecycleActivityAndChildDependencyMatrix(t *testing.T) {
	for _, name := range []string{"activity prepared", "activity running", "activity unknown", "child planned", "child queued", "child running", "child waiting", "child unknown", "child unjoined", "allocation missing claim", "inline cursor"} {
		t.Run(name, func(t *testing.T) {
			// Arrange: modify a valid committed outcome fixture into one valid unresolved record.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			children := name != "activity prepared" && name != "activity running" && name != "activity unknown" &&
				name != "inline cursor"
			runner := growthRunner(t, store, 1, children)
			completed, err := runner.Start(ctx, "matrix", 0)
			if err != nil {
				t.Fatal(err)
			}
			source, err := store.LoadExecution(ctx, "matrix")
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case children:
				lifecycleMatrixChildren(t, &source, name)
			case name == "inline cursor":
				source.Progress.ChildCursors = map[string]flowy.ExecutionPointer{"inline": "growth"}
			default:
				lifecycleMatrixActivity(t, &source, name)
			}
			lease, err := store.AcquireExecution(ctx, "matrix", "fixture", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			source, err = store.CommitExecution(ctx, completed.ResumeToken.SnapshotRevision, lease, source)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.ReleaseExecution(ctx, lease); err != nil {
				t.Fatal(err)
			}
			var projections atomic.Int32
			request := lifecycleRolloverRequest("next", 1, &projections)
			request.SourceDescriptor = source.Descriptor
			// Act: runner, raw publisher and cleanup must independently reject unresolved dependencies.
			_, runnerErr := runner.Rollover(
				ctx,
				flowy.ResumeToken{ThreadID: "matrix", SnapshotRevision: source.Revision},
				request,
			)
			_, cleanupErr := store.RetainExecution(
				ctx,
				flowy.ExecutionRetentionRequest{
					ExecutionID: "matrix",
					Revision:    source.Revision,
					Policy:      flowy.ExecutionRetentionPolicy{Label: "archive", DeletePayload: true},
				},
			)
			rawLease, err := store.AcquireExecution(ctx, "matrix", "raw", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			_, rawErr := store.CommitRollover(
				ctx,
				rawLease,
				flowy.HistoricalCheckpointReference{
					ExecutionID: "matrix",
					Revision:    source.Revision,
					Digest:      source.Digest,
				},
				flowy.ExecutionEnvelope{ExecutionID: "raw-next"},
			)
			if err = store.ReleaseExecution(ctx, rawLease); err != nil {
				t.Fatal(err)
			}
			after, loadErr := store.LoadExecution(ctx, "matrix")
			_, targetErr := store.LoadExecution(ctx, "next")
			// Assert: no corruption substitute, successful maintenance, source mutation or host projection.
			if !errors.Is(runnerErr, flowy.ErrExecutionLifecycleUnsafe) ||
				!errors.Is(rawErr, flowy.ErrExecutionLifecycleUnsafe) ||
				!errors.Is(cleanupErr, flowy.ErrExecutionLifecycleUnsafe) ||
				loadErr != nil ||
				after.Digest != source.Digest ||
				!errors.Is(targetErr, flowy.ErrThreadNotFound) ||
				projections.Load() != 0 {
				t.Fatalf(
					"runner=%v raw=%v cleanup=%v load=%v target=%v projections=%d",
					runnerErr,
					rawErr,
					cleanupErr,
					loadErr,
					targetErr,
					projections.Load(),
				)
			}
		})
	}
}

func lifecycleMatrixActivity(t *testing.T, source *flowy.ExecutionEnvelope, name string) {
	t.Helper()
	var records map[string]flowy.ActivityRecord
	if err := json.Unmarshal(source.JournalPayload, &records); err != nil {
		t.Fatal(err)
	}
	for id, record := range records {
		source.Activation = record.Activation
		source.Terminal = nil
		record.Outcome = nil
		record.Origin = ""
		record.Classification = ""
		switch name {
		case "activity prepared":
			record.State = flowy.ActivityPrepared
			record.Attempts = nil
		case "activity running":
			record.State = flowy.ActivityRunning
			record.Attempts[0].State = flowy.ActivityRunning
			record.Attempts[0].FinishedAt = time.Time{}
		case "activity unknown":
			record.State = flowy.ActivityUnknown
			record.Classification = flowy.ActivityAmbiguous
			record.Attempts[0].State = flowy.ActivityUnknown
			record.Attempts[0].Classification = flowy.ActivityAmbiguous
		}
		records[id] = record
	}
	payload, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	source.JournalPayload = payload
}

func lifecycleMatrixChildren(t *testing.T, source *flowy.ExecutionEnvelope, name string) {
	t.Helper()
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(source.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	for id, group := range groups {
		child := group.Children[0]
		if name != "allocation missing claim" {
			source.Activation = group.Activation
			source.Terminal = nil
			group.MergedIDs = nil
			group.MergedResult = nil
		}
		switch name {
		case "child planned":
			child.State = flowy.ChildPlanned
			child.Revision = 0
			child.Incarnation = 0
			child.Result = nil
		case "child queued":
			child.State = flowy.ChildQueued
			child.Revision = 1
			child.Incarnation = 0
			child.Result = nil
		case "child running":
			child.State = flowy.ChildRunning
			child.Revision = 2
			child.Result = nil
		case "child waiting":
			child.State = flowy.ChildWaiting
			child.WaitID = "external-wait"
			child.Result = nil
		case "child unknown":
			child.State = flowy.ChildUnknown
			child.Result = nil
		case "allocation missing claim":
			child.Spec.Allocation = map[string]int{"units": 1}
			group.Plan.Children[0].Allocation = map[string]int{"units": 1}
			group.Capacity = map[string]int{"units": 1}
		}
		group.Children[0] = child
		groups[id] = group
	}
	payload, err := json.Marshal(groups)
	if err != nil {
		t.Fatal(err)
	}
	source.ChildrenPayload = payload
}

func TestLifecycleAccountedAllocationAndCountersSurviveTransfer(t *testing.T) {
	// Arrange: a joined child with explicit, revision-bound usage, plus cumulative counters.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	runner := growthRunner(t, store, 1, true)
	completed, err := runner.Start(ctx, "accounted", 0)
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, "accounted")
	if err != nil {
		t.Fatal(err)
	}
	lifecycleMatrixChildren(t, &source, "allocation missing claim")
	var groups map[string]flowy.ChildGroupRecord
	if err = json.Unmarshal(source.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	for id, group := range groups {
		child := group.Children[0]
		group.BudgetReturns = map[string]flowy.ChildBudgetReturnRecord{
			child.Spec.ID: {
				ChildRevision:  child.Revision,
				DecisionID:     "claim",
				Reason:         "used",
				Evidence:       "host receipt",
				Used:           map[string]int{"units": 1},
				Returned:       map[string]int{"units": 0},
				SourceRevision: source.Revision - 1,
				Incarnation:    1,
				At:             time.Now().UTC(),
			},
		}
		groups[id] = group
	}
	source.ChildrenPayload, err = json.Marshal(groups)
	if err != nil {
		t.Fatal(err)
	}
	source.RunMeta.RetryCounts = map[string]int{"growth": 2}
	source.RunMeta.BudgetCounts = map[string]int{"units": 7}
	lease, err := store.AcquireExecution(ctx, "accounted", "fixture", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	source, err = store.CommitExecution(ctx, completed.ResumeToken.SnapshotRevision, lease, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	request := flowy.RolloverRequest{
		SourceDescriptor: source.Descriptor,
		TargetID:         "continued",
		DecisionID:       "continue",
		ProjectionLabel:  "detached-state",
		Policy: flowy.RolloverPolicy{
			Label:             "one",
			MaxRecords:        1,
			MaxAggregateBytes: 131072,
			MaxTargetBytes:    8192,
		},
		Project: func(p flowy.RolloverPayload) (flowy.RolloverPayload, error) {
			p.Progress.StatePayload[0] = '9'
			return p, nil
		},
	}
	// Act: a legitimate accounted allocation can transfer; only segment-local count resets.
	token, rolloverErr := runner.Rollover(
		ctx,
		flowy.ResumeToken{ThreadID: "accounted", SnapshotRevision: source.Revision},
		request,
	)
	target, targetErr := store.LoadExecution(ctx, token.ThreadID)
	historical, historyErr := store.LoadCheckpoint(ctx, "accounted", source.Revision)
	// Assert: no dropped accounting and no source alias from the mutating host projection.
	if rolloverErr != nil || targetErr != nil || historyErr != nil || target.RunMeta.StepCount != 0 ||
		target.RunMeta.RetryCounts["growth"] != 2 ||
		target.RunMeta.BudgetCounts["units"] != 7 ||
		historical.Digest != source.Digest ||
		string(historical.Progress.StatePayload) != "1" ||
		string(target.Progress.StatePayload) != "9" ||
		historical.RunMeta.StepCount != source.RunMeta.StepCount {
		t.Fatalf(
			"rollover=%v target=%v history=%v counters=%+v source=%+v",
			rolloverErr,
			targetErr,
			historyErr,
			target.RunMeta,
			historical.RunMeta,
		)
	}
}

type lifecycleWaitStore struct {
	*testutil.MemoryExecutionStore

	profile       flowy.WaitCapabilityProfile
	registrations atomic.Int32
}

func (s *lifecycleWaitStore) WaitCapabilities() flowy.WaitCapabilityProfile { return s.profile }
func (s *lifecycleWaitStore) RegisterWait(context.Context, flowy.DurableWaitRecord) error {
	s.registrations.Add(1)
	return nil
}

func TestLifecycleArmedWaitRejectsRunnerRawPublicationAndCleanup(t *testing.T) {
	// Arrange: a real persisted armed wait, not an invented malformed record.
	ctx := context.Background()
	store := &lifecycleWaitStore{
		MemoryExecutionStore: testutil.NewMemoryExecutionStore(nil),
		profile:              waitProfileForTest(),
	}
	var calls, projections atomic.Int32
	runner := mustWaitRunner(t, store, &store.profile, &calls)
	armed, err := runner.Start(ctx, "waiting", durableTestState{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, "waiting")
	if err != nil {
		t.Fatal(err)
	}
	request := lifecycleRolloverRequest("next", 1, &projections)
	request.SourceDescriptor = source.Descriptor
	// Act: every maintenance boundary independently refuses the pending continuation.
	_, rolloverErr := runner.Rollover(ctx, armed.ResumeToken, request)
	_, cleanupErr := store.RetainExecution(
		ctx,
		flowy.ExecutionRetentionRequest{
			ExecutionID: "waiting",
			Revision:    source.Revision,
			Policy:      flowy.ExecutionRetentionPolicy{Label: "keep", KeepLast: 1},
		},
	)
	lease, err := store.AcquireExecution(ctx, "waiting", "raw", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, rawErr := store.CommitRollover(
		ctx,
		lease,
		flowy.HistoricalCheckpointReference{ExecutionID: "waiting", Revision: source.Revision, Digest: source.Digest},
		flowy.ExecutionEnvelope{ExecutionID: "raw-next"},
	)
	if err = store.ReleaseExecution(ctx, lease); err != nil {
		t.Fatal(err)
	}
	after, loadErr := store.LoadExecution(ctx, "waiting")
	// Assert: neither registration nor node nor pure projection repeats on rejection.
	if !errors.Is(rolloverErr, flowy.ErrExecutionLifecycleUnsafe) ||
		!errors.Is(cleanupErr, flowy.ErrExecutionLifecycleUnsafe) ||
		!errors.Is(rawErr, flowy.ErrExecutionLifecycleUnsafe) ||
		loadErr != nil ||
		after.Digest != source.Digest ||
		calls.Load() != 1 ||
		projections.Load() != 0 ||
		store.registrations.Load() != 1 {
		t.Fatalf(
			"rollover=%v cleanup=%v raw=%v load=%v calls/projections/registrations=%d/%d/%d",
			rolloverErr,
			cleanupErr,
			rawErr,
			loadErr,
			calls.Load(),
			projections.Load(),
			store.registrations.Load(),
		)
	}
}
