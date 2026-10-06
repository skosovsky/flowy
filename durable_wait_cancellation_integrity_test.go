package flowy

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func canceledWaitEnvelope(t *testing.T) ExecutionEnvelope {
	t.Helper()
	record := waitFixture(t)
	record.State = WaitCanceled
	record.Cancellation = &WaitCancellationRecord{ID: "cancel", Reason: "requested", Evidence: "host-evidence",
		SourceRevision: 2, Incarnation: 1, At: record.Spec.Deadline}
	target, _, _, err := prepareWaitDecision(record, waitEventFixture(record, 2), 3, 2, record.Spec.Deadline, true)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]DurableWaitRecord{
		waitIdentity(record.ExecutionID, record.Node, record.Activation, record.Spec.ID): target,
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := ExecutionEnvelope{ExecutionID: "run", Revision: 4, Activation: 1, WaitsPayload: payload,
		Progress: MigrationState{ExecutionPointer: "waiting"}, Terminal: &ExecutionTerminal{
			Status: RunStatusFailed, Reason: durableWaitCanceledReason,
			Failure: &ExecutionFailure{Message: durableWaitCanceledReason + ": requested"}}}
	if err = validateExecutionCollections(envelope); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestWaitCancellationCorruptionCannotClearTerminalOrForgeProvenance(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*DurableWaitRecord, *ExecutionEnvelope){
		"missing-provenance":  func(r *DurableWaitRecord, _ *ExecutionEnvelope) { r.Cancellation = nil },
		"missing-id":          func(r *DurableWaitRecord, _ *ExecutionEnvelope) { r.Cancellation.ID = "" },
		"missing-evidence":    func(r *DurableWaitRecord, _ *ExecutionEnvelope) { r.Cancellation.Evidence = "" },
		"missing-incarnation": func(r *DurableWaitRecord, _ *ExecutionEnvelope) { r.Cancellation.Incarnation = 0 },
		"missing-time":        func(r *DurableWaitRecord, _ *ExecutionEnvelope) { r.Cancellation.At = time.Time{} },
		"future-source":       func(r *DurableWaitRecord, _ *ExecutionEnvelope) { r.Cancellation.SourceRevision = 4 },
		"before-arm":          func(r *DurableWaitRecord, _ *ExecutionEnvelope) { r.Cancellation.SourceRevision = 1 },
		"source-collision":    func(r *DurableWaitRecord, _ *ExecutionEnvelope) { r.Cancellation.SourceRevision = 3 },
		"clear-terminal":      func(_ *DurableWaitRecord, e *ExecutionEnvelope) { e.Terminal = nil },
		"replace-reason":      func(_ *DurableWaitRecord, e *ExecutionEnvelope) { e.Terminal.Reason = "other" },
		"clear-failure":       func(_ *DurableWaitRecord, e *ExecutionEnvelope) { e.Terminal.Failure = nil },
		"replace-failure":     func(_ *DurableWaitRecord, e *ExecutionEnvelope) { e.Terminal.Failure.Message = "other" },
		"advance-activation":  func(_ *DurableWaitRecord, e *ExecutionEnvelope) { e.Activation++ },
		"advance-pointer":     func(_ *DurableWaitRecord, e *ExecutionEnvelope) { e.Progress.ExecutionPointer = "accepted" },
		"late-unmatched": func(r *DurableWaitRecord, _ *ExecutionEnvelope) {
			d := r.Decisions["event"]
			d.Status = WaitUnmatched
			r.Decisions["event"] = d
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: baseline carries a real-shaped canceled rejection after its cancel commit.
			envelope := canceledWaitEnvelope(t)
			waits, err := executionWaits(envelope)
			if err != nil {
				t.Fatal(err)
			}
			identity := waitIdentity("run", "waiting", 1, "approval")
			record := waits[identity]
			mutate(&record, &envelope)
			waits[identity] = record
			envelope.WaitsPayload, err = json.Marshal(waits)
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			envelope, err = SealExecutionEnvelope(envelope)
			if err != nil {
				t.Fatal(err)
			}
			if integrityErr := ValidateExecutionIntegrity(envelope, "run", 4); integrityErr != nil {
				t.Fatal(integrityErr)
			}
			err = validateExecutionCollections(envelope)
			// Assert: resealing alone cannot make inconsistent semantics valid.
			if !errors.Is(err, ErrExecutionCorrupt) {
				t.Fatalf("cancellation forgery %s accepted: %v", name, err)
			}
		})
	}
}
