package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

// Demonstration registration capability: the committed memory aggregate is the
// registration. Production persistence/scheduling belongs to a capable adapter
// and its declared host owner, not to this in-process example.
type demoWaitStore struct {
	*testutil.MemoryExecutionStore

	profile flowy.WaitCapabilityProfile
}

func (s *demoWaitStore) WaitCapabilities() flowy.WaitCapabilityProfile { return s.profile }

func (s *demoWaitStore) RegisterWait(ctx context.Context, expected flowy.DurableWaitRecord) error {
	envelope, err := s.LoadExecution(ctx, expected.ExecutionID)
	if err != nil {
		return err
	}
	records, err := flowy.InspectExecutionWaits(envelope)
	if err != nil {
		return err
	}
	want, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	for _, record := range records {
		actual, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if record.State == flowy.WaitArmed && record.Profile == s.profile && bytes.Equal(actual, want) {
			return nil
		}
	}
	return flowy.ErrWaitRegistration
}

func waitDemo(ctx context.Context) error {
	profile := flowy.WaitCapabilityProfile{Label: "example-host", JournalOwner: "memory", LeaseOwner: "memory",
		TimerOwner: hostOwner, ClockOwner: hostOwner, RetryOwner: "runtime", RecoveryOwner: hostOwner}
	store := &demoWaitStore{MemoryExecutionStore: testutil.NewMemoryExecutionStore(nil), profile: profile}
	opts := options()
	opts.WaitProfile = &profile
	spec := flowy.DurableWaitSpec{
		ID:                "approval",
		CorrelationID:     "host-correlation",
		Deadline:          time.Now().UTC().Add(time.Hour),
		MatcherLabel:      "host-matcher",
		PayloadCodec:      "host-text",
		ContinuationLabel: "host-apply",
		EventPointer:      workNode,
		TimeoutPointer:    workNode,
		WinnerPolicy:      flowy.WaitFirstCommitted,
	}
	runner, err := bind(store, "wait", func(_ context.Context, s state) (state, flowy.Directive, error) {
		if s.Value == 0 {
			s.Value = 1
			return s, flowy.Await(spec), nil
		}
		s.Value++
		return s, flowy.End(), nil
	}, opts)
	if err != nil {
		return err
	}
	if _, startErr := runner.Start(ctx, "wait-demo", state{}); startErr != nil {
		return startErr
	}
	envelope, err := store.LoadExecution(ctx, "wait-demo")
	if err != nil {
		return err
	}
	waits, err := flowy.InspectExecutionWaits(envelope)
	if err != nil || len(waits) != 1 {
		return fmt.Errorf("armed wait absent: %w", err)
	}
	delivery := flowy.WaitDelivery{Generation: waits[0].Generation, ID: "host-event", Kind: flowy.WaitEvent,
		CorrelationID: spec.CorrelationID, ExpectedRevision: envelope.Revision, Payload: []byte("approved")}
	contract := flowy.WaitDeliveryContract[state]{
		MatcherLabel:      spec.MatcherLabel,
		PayloadCodec:      spec.PayloadCodec,
		ContinuationLabel: spec.ContinuationLabel,
		Match:             func(_ context.Context, payload []byte) (bool, error) { return string(payload) == "approved", nil },
		Apply:             func(_ context.Context, s state, _ flowy.WaitDelivery) (state, error) { s.Value = 10; return s, nil },
	}
	accepted, err := runner.DeliverWait(ctx, "wait-demo", delivery, contract)
	if err != nil {
		return err
	}
	duplicate, err := runner.DeliverWait(ctx, "wait-demo", delivery, contract)
	if err != nil || !duplicate.Replay {
		return fmt.Errorf("delivery not replayed: %w", err)
	}
	// A real transport acknowledges here, after committed acceptance, not on discovery.
	result, err := runner.Resume(ctx, accepted.ResumeToken)
	if err != nil || result == nil || result.State.Value != 11 {
		return fmt.Errorf("continuation failed: result=%+v err=%w", result, err)
	}
	return nil
}
