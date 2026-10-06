package flowy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func waitFixture(t *testing.T) DurableWaitRecord {
	t.Helper()
	record, err := planDurableWait("run", "waiting", 1, 2, DurableWaitSpec{
		ID: "approval", CorrelationID: "request", Deadline: time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC),
		MatcherLabel: "match", PayloadCodec: "payload", ContinuationLabel: "transition",
		EventPointer: "accepted", TimeoutPointer: "timed-out", WinnerPolicy: WaitFirstCommitted,
	}, WaitCapabilityProfile{Label: "test", JournalOwner: "store", LeaseOwner: "store", TimerOwner: "timer",
		ClockOwner: "clock", RetryOwner: "runtime", RecoveryOwner: "runtime"})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func waitEventFixture(record DurableWaitRecord, revision uint64) WaitDelivery {
	return WaitDelivery{Generation: record.Generation, ID: "event", Kind: WaitEvent,
		CorrelationID: record.Spec.CorrelationID, ExpectedRevision: revision, Payload: []byte("approved")}
}

func TestWaitArbitrationWinnerLoserAndDuplicate(t *testing.T) {
	t.Parallel()
	// Arrange: both deliveries originate from the same committed arm checkpoint.
	record := waitFixture(t)
	event := waitEventFixture(record, 2)
	timer := WaitDelivery{Generation: record.Generation, ID: "timer", Kind: WaitTimer,
		CorrelationID: record.Spec.CorrelationID, ExpectedRevision: 2}
	// Act: event wins revision 3; the competing timer is recorded at revision 4.
	winner, accepted, replay, err := prepareWaitDecision(record, event, 2, 1, record.Spec.Deadline, true)
	if err != nil || replay || accepted.Status != WaitAccepted {
		t.Fatalf("accept: %+v replay=%v err=%v", accepted, replay, err)
	}
	loser, lost, replay, err := prepareWaitDecision(winner, timer, 3, 2, record.Spec.Deadline, true)
	if err != nil || replay || lost.Status != WaitLost {
		t.Fatalf("loser: %+v replay=%v err=%v", lost, replay, err)
	}
	duplicate, cached, replay, err := prepareWaitDecision(loser, event, 4, 3, record.Spec.Deadline, false)
	// Assert: no second winner or mutation, including no matcher-result reinterpretation.
	if err != nil || !replay || cached != accepted || duplicate.WinnerID != "event" ||
		duplicate.State != WaitResolved || len(duplicate.Decisions) != 2 ||
		validateDurableWait(duplicate, 4) != nil || record.State != WaitArmed || len(record.Decisions) != 0 {
		t.Fatalf("duplicate mutated arbitration: %+v cached=%+v replay=%v err=%v", duplicate, cached, replay, err)
	}
	duplicate.Decisions["other"] = cached
	if len(loser.Decisions) != 2 {
		t.Fatal("detached decision target aliases source")
	}
}

func TestWaitTimerWinsWithoutInventingEventAcceptance(t *testing.T) {
	t.Parallel()
	// Arrange.
	record := waitFixture(t)
	timer := waitEventFixture(record, 2)
	timer.ID, timer.Kind, timer.Payload = "timer", WaitTimer, nil
	// Act.
	expired, decision, _, err := prepareWaitDecision(record, timer, 2, 1, record.Spec.Deadline, false)
	if err != nil {
		t.Fatal(err)
	}
	late, rejected, _, err := prepareWaitDecision(
		expired,
		waitEventFixture(record, 2),
		3,
		2,
		record.Spec.Deadline,
		true,
	)
	// Assert.
	if err != nil || decision.Status != WaitAccepted || expired.State != WaitExpired ||
		rejected.Status != WaitLost || late.WinnerID != "timer" || validateDurableWait(late, 4) != nil {
		t.Fatalf("timeout arbitration: %+v err=%v", late, err)
	}
}

func TestWaitTimerDuplicateReplaysDespiteClockAdjustment(t *testing.T) {
	t.Parallel()
	// Arrange: timer acceptance is already durable; the new worker clock differs.
	record := waitFixture(t)
	timer := waitEventFixture(record, 2)
	timer.ID, timer.Kind, timer.Payload = "timer", WaitTimer, nil
	winner, accepted, _, err := prepareWaitDecision(record, timer, 2, 1, record.Spec.Deadline, false)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	_, cached, replay, err := prepareWaitDecision(winner, timer, 3, 2, record.Spec.Deadline.Add(-time.Hour), false)
	// Assert: persisted acceptance, not the new wall clock, determines replay.
	if err != nil || !replay || cached != accepted {
		t.Fatalf("timer replay changed: %+v replay=%v err=%v", cached, replay, err)
	}
}

func TestWaitDuplicateAtRevisionLimitNeedsNoNewCommit(t *testing.T) {
	t.Parallel()
	// Arrange.
	record := waitFixture(t)
	event := waitEventFixture(record, 2)
	winner, accepted, _, err := prepareWaitDecision(record, event, 2, 1, record.Spec.Deadline, true)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	_, cached, replay, duplicateErr := prepareWaitDecision(
		winner,
		event,
		math.MaxUint64,
		2,
		record.Spec.Deadline,
		false,
	)
	event.ID = "new-delivery"
	_, _, _, overflowErr := prepareWaitDecision(winner, event, math.MaxUint64, 2, record.Spec.Deadline, false)
	// Assert: replay is read-only; a new durable decision must not wrap revision.
	if duplicateErr != nil || !replay || cached != accepted || !errors.Is(overflowErr, ErrWaitInvalid) {
		t.Fatalf("revision limit replay=%v duplicate=%v overflow=%v", replay, duplicateErr, overflowErr)
	}
}

func TestWaitCanceledLateEventDoesNotRevive(t *testing.T) {
	t.Parallel()
	// Arrange.
	record := waitFixture(t)
	record.State = WaitCanceled
	record.Cancellation = &WaitCancellationRecord{ID: "cancel", Reason: "requested", Evidence: "host-evidence",
		SourceRevision: 2, Incarnation: 1, At: record.Spec.Deadline}
	// Act.
	target, decision, _, err := prepareWaitDecision(
		record,
		waitEventFixture(record, 2),
		3,
		1,
		record.Spec.Deadline,
		true,
	)
	// Assert: a durable rejection may be recorded, but there is no continuation.
	if err != nil || decision.Status != WaitRejectedCanceled || target.State != WaitCanceled ||
		target.WinnerID != "" || validateDurableWait(target, 4) != nil {
		t.Fatalf("canceled wait revived: %+v err=%v", target, err)
	}
}

func TestWaitPayloadParticipatesInSealAndDetachedClone(t *testing.T) {
	t.Parallel()
	// Arrange.
	record := waitFixture(t)
	payload, err := json.Marshal(map[string]DurableWaitRecord{
		waitIdentity(record.ExecutionID, record.Node, record.Activation, record.Spec.ID): record,
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := SealExecutionEnvelope(ExecutionEnvelope{ExecutionID: "run", Revision: 2, Activation: 1,
		Progress: ExecutionProgress{ExecutionPointer: "waiting"}, WaitsPayload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if collectionsErr := validateExecutionCollections(envelope); collectionsErr != nil {
		t.Fatal(collectionsErr)
	}
	// Act.
	clone := cloneExecutionEnvelope(envelope)
	clone.WaitsPayload[0] = '['
	// Assert.
	if ValidateExecutionIntegrity(envelope, "run", 2) != nil ||
		!errors.Is(ValidateExecutionIntegrity(clone, "run", 2), ErrExecutionCorrupt) {
		t.Fatal("wait payload aliases its source or is missing from the seal")
	}
}

func TestWaitArmedBlocksOrdinarySaveBeforeCodecsOrStore(t *testing.T) {
	t.Parallel()
	// Arrange: intentionally absent codec/store would panic if save crossed the boundary.
	record := waitFixture(t)
	payload, err := json.Marshal(map[string]DurableWaitRecord{
		waitIdentity(record.ExecutionID, record.Node, record.Activation, record.Spec.ID): record,
	})
	if err != nil {
		t.Fatal(err)
	}
	cp := &executionCheckpointer[int, string]{
		lease: ExecutionLease{ExecutionID: "run"}, stepRevision: 2,
		envelope: ExecutionEnvelope{ExecutionID: "run", Revision: 2, Activation: 1,
			Progress: ExecutionProgress{ExecutionPointer: "waiting"}, WaitsPayload: bytes.Clone(payload)},
	}
	// Act.
	_, err = cp.Save(context.Background(), 2, Snapshot[int, string]{ThreadID: "run", State: 42,
		ExecutionPointer: "accepted"})
	// Assert: unresolved wait cannot silently lose pre-wait state or advance the cursor.
	if !errors.Is(err, ErrWaitUnresolved) || cp.envelope.Revision != 2 ||
		cp.envelope.Progress.ExecutionPointer != "waiting" || !bytes.Equal(cp.envelope.WaitsPayload, payload) {
		t.Fatalf("armed boundary crossed: %+v err=%v", cp.envelope, err)
	}
}

func TestWaitCollectionRejectsStrandedArmedWaitAndMalformedPayload(t *testing.T) {
	t.Parallel()
	// Arrange.
	record := waitFixture(t)
	payload, err := json.Marshal(map[string]DurableWaitRecord{
		waitIdentity(record.ExecutionID, record.Node, record.Activation, record.Spec.ID): record,
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, envelope := range map[string]ExecutionEnvelope{
		"advanced": {ExecutionID: "run", Revision: 2, Activation: 1,
			Progress: ExecutionProgress{ExecutionPointer: "accepted"}, WaitsPayload: payload},
		"activation": {ExecutionID: "run", Revision: 2, Activation: 2,
			Progress: ExecutionProgress{ExecutionPointer: "waiting"}, WaitsPayload: payload},
		"terminal": {ExecutionID: "run", Revision: 2, Activation: 1,
			Progress: ExecutionProgress{ExecutionPointer: "waiting"}, WaitsPayload: payload,
			Terminal: &ExecutionTerminal{Status: RunStatusCompleted}},
		"null":          {WaitsPayload: []byte("null")},
		"trailing":      {WaitsPayload: append(bytes.Clone(payload), []byte("{}")...)},
		"unknown-field": {WaitsPayload: []byte(`{"wait":{"unknown":true}}`)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Act.
			_, collectionsErr := executionWaits(envelope)
			// Assert.
			if !errors.Is(collectionsErr, ErrExecutionCorrupt) {
				t.Fatalf("invalid wait collection accepted: %v", collectionsErr)
			}
		})
	}
}

func TestWaitUnmatchedDecisionKeepsWaitArmed(t *testing.T) {
	t.Parallel()
	// Arrange.
	record := waitFixture(t)
	event := waitEventFixture(record, 2)
	// Act.
	target, decision, _, err := prepareWaitDecision(record, event, 2, 1, record.Spec.Deadline, false)
	if err != nil {
		t.Fatal(err)
	}
	event = waitEventFixture(target, 3)
	event.ID = "other"
	winner, _, _, err := prepareWaitDecision(target, event, 3, 1, record.Spec.Deadline, true)
	// Assert.
	if err != nil || decision.Status != WaitUnmatched || target.State != WaitArmed ||
		winner.WinnerID != "other" || validateDurableWait(winner, 4) != nil {
		t.Fatalf("unmatched blocks future event: %+v err=%v", winner, err)
	}
}

func TestWaitRejectsChangedDeliveryAndStaleGeneration(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*WaitDelivery){
		"payload":     func(d *WaitDelivery) { d.Payload = []byte("denied") },
		"correlation": func(d *WaitDelivery) { d.CorrelationID = "different" },
		"kind":        func(d *WaitDelivery) { d.Kind, d.Payload = WaitTimer, nil },
		"generation":  func(d *WaitDelivery) { d.Generation = "old" },
		"future":      func(d *WaitDelivery) { d.ExpectedRevision = 99 },
		"before-arm":  func(d *WaitDelivery) { d.ExpectedRevision = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			record := waitFixture(t)
			event := waitEventFixture(record, 2)
			winner, _, _, err := prepareWaitDecision(record, event, 2, 1, record.Spec.Deadline, true)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&event)
			// Act.
			_, _, _, err = prepareWaitDecision(winner, event, 3, 2, record.Spec.Deadline, true)
			// Assert.
			if err == nil || winner.State != WaitResolved || len(winner.Decisions) != 1 {
				t.Fatalf("changed/stale delivery accepted: %v", err)
			}
			if (name == "payload" || name == "kind" || name == "correlation") && !errors.Is(err, ErrWaitConflict) {
				t.Fatalf("identity conflict lost: %v", err)
			}
		})
	}
}

func TestWaitRejectsEarlyTimerAndStaleArmedRevision(t *testing.T) {
	t.Parallel()
	// Arrange.
	record := waitFixture(t)
	event := waitEventFixture(record, 2)
	timer := event
	timer.Kind, timer.Payload = WaitTimer, nil
	// Act.
	_, _, _, early := prepareWaitDecision(record, timer, 2, 1, record.Spec.Deadline.Add(-time.Nanosecond), true)
	_, _, _, stale := prepareWaitDecision(record, event, 3, 1, record.Spec.Deadline, true)
	// Assert.
	if !errors.Is(early, ErrWaitInvalid) || !errors.Is(stale, ErrWaitStale) {
		t.Fatalf("timer=%v revision=%v", early, stale)
	}
}

func TestWaitCollectionRejectsForgedLedgerBeforeDecode(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*DurableWaitRecord){
		"winner":        func(r *DurableWaitRecord) { r.WinnerID = "absent" },
		"state":         func(r *DurableWaitRecord) { r.State = WaitExpired },
		"generation":    func(r *DurableWaitRecord) { r.Generation = "old" },
		"future-source": func(r *DurableWaitRecord) { d := r.Decisions["event"]; d.SourceRevision = 3; r.Decisions["event"] = d },
		"incarnation":   func(r *DurableWaitRecord) { d := r.Decisions["event"]; d.Incarnation = 0; r.Decisions["event"] = d },
		"digest": func(r *DurableWaitRecord) {
			d := r.Decisions["event"]
			d.PayloadDigest = "not-sha"
			r.Decisions["event"] = d
		},
		"foreign":    func(r *DurableWaitRecord) { r.ExecutionID = "other" },
		"activation": func(r *DurableWaitRecord) { r.Activation = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			record := waitFixture(t)
			winner, _, _, err := prepareWaitDecision(
				record,
				waitEventFixture(record, 2),
				2,
				1,
				record.Spec.Deadline,
				true,
			)
			if err != nil {
				t.Fatal(err)
			}
			identity := waitIdentity(record.ExecutionID, record.Node, record.Activation, record.Spec.ID)
			mutate(&winner)
			payload, err := json.Marshal(map[string]DurableWaitRecord{identity: winner})
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			_, err = executionWaits(ExecutionEnvelope{ExecutionID: "run", Revision: 3, Activation: 1,
				Progress: ExecutionProgress{ExecutionPointer: "waiting"}, WaitsPayload: payload})
			// Assert.
			if !errors.Is(err, ErrExecutionCorrupt) {
				t.Fatalf("forged wait %s accepted: %v", name, err)
			}
		})
	}
}

func TestWaitSpecRequiresExplicitContractsAndUTC(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*DurableWaitSpec){
		"id":              func(s *DurableWaitSpec) { s.ID = "" },
		"correlation":     func(s *DurableWaitSpec) { s.CorrelationID = "" },
		"deadline":        func(s *DurableWaitSpec) { s.Deadline = time.Time{} },
		"timezone":        func(s *DurableWaitSpec) { s.Deadline = s.Deadline.In(time.FixedZone("local", 3600)) },
		"negative year":   func(s *DurableWaitSpec) { s.Deadline = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC) },
		"five digit year": func(s *DurableWaitSpec) { s.Deadline = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"matcher":         func(s *DurableWaitSpec) { s.MatcherLabel = "" },
		"codec":           func(s *DurableWaitSpec) { s.PayloadCodec = "" },
		"continuation":    func(s *DurableWaitSpec) { s.ContinuationLabel = "" },
		"policy":          func(s *DurableWaitSpec) { s.WinnerPolicy = "" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			spec := waitFixture(t).Spec
			mutate(&spec)
			// Act.
			err := spec.Validate()
			// Assert.
			if !errors.Is(err, ErrWaitInvalid) {
				t.Fatalf("incomplete wait contract accepted: %v", err)
			}
		})
	}
}

func TestWaitLateEventCanWinBeforeTimerPublication(t *testing.T) {
	t.Parallel()
	// Arrange: deadline has elapsed, but no timer decision has committed.
	record := waitFixture(t)
	event := waitEventFixture(record, 2)
	// Act.
	target, decision, replay, err := prepareWaitDecision(record, event, 2, 1,
		record.Spec.Deadline.Add(time.Hour), true)
	// Assert: the selected policy orders commits, rather than event wall time.
	if err != nil || replay || decision.Status != WaitAccepted || target.WinnerID != event.ID {
		t.Fatalf("late event: target=%+v decision=%+v replay=%v err=%v", target, decision, replay, err)
	}
}

func TestWaitUnmatchedReplayCannotBecomeAccepted(t *testing.T) {
	t.Parallel()
	// Arrange: the same evidence was durably unmatched.
	record := waitFixture(t)
	event := waitEventFixture(record, 2)
	unmatched, original, _, err := prepareWaitDecision(record, event, 2, 1, record.Spec.Deadline, false)
	if err != nil {
		t.Fatal(err)
	}
	// Act: a later matcher result cannot reinterpret that delivery ID.
	target, cached, replay, err := prepareWaitDecision(unmatched, event, 3, 2, record.Spec.Deadline, true)
	// Assert.
	if err != nil || !replay || cached != original || target.State != WaitArmed || target.WinnerID != "" {
		t.Fatalf("unmatched replay: %+v cached=%+v replay=%v err=%v", target, cached, replay, err)
	}
}

func TestWaitClonePreservesMemoryShapeAndOwnership(t *testing.T) {
	t.Parallel()
	// Arrange: copy is independent of admission and JSON's representable date range.
	source := waitFixture(t)
	source.Spec.Deadline = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	source.Decisions = map[string]WaitDecision{}
	source.Cancellation = &WaitCancellationRecord{Reason: "source"}
	// Act.
	target := cloneDurableWait(source)
	target.Decisions["other"] = WaitDecision{}
	target.Cancellation.Reason = "target"
	nilSource := source
	nilSource.Decisions, nilSource.Cancellation = nil, nil
	nilTarget := cloneDurableWait(nilSource)
	// Assert: no ignored encoding failure, no map/pointer sharing, no nil normalization.
	if target.Spec.Deadline != source.Spec.Deadline || len(source.Decisions) != 0 ||
		source.Cancellation.Reason != "source" || nilTarget.Decisions != nil || nilTarget.Cancellation != nil {
		t.Fatalf("clone changed source/shape: source=%+v target=%+v nil=%+v", source, target, nilTarget)
	}
}
