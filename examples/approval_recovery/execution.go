package main

import (
	"context"
	"encoding/json"

	"github.com/skosovsky/flowy"
)

type faultExecutionStore struct {
	flowy.ExecutionStore

	fault hook
}

func (s *faultExecutionStore) WaitCapabilities() flowy.WaitCapabilityProfile { return profile() }
func (s *faultExecutionStore) RegisterWait(ctx context.Context, record flowy.DurableWaitRecord) error {
	head, err := s.LoadExecution(ctx, record.ExecutionID)
	if err != nil {
		return err
	}
	waits, err := flowy.InspectExecutionWaits(head)
	if err != nil {
		return err
	}
	found := false
	for _, w := range waits {
		if w.Generation == record.Generation && w.State == flowy.WaitArmed {
			found = true
		}
	}
	if !found {
		return flowy.ErrWaitRegistration
	}
	if registrar, ok := s.ExecutionStore.(interface {
		RegisterWait(context.Context, flowy.DurableWaitRecord) error
	}); ok {
		if err = registrar.RegisterWait(ctx, record); err != nil {
			return err
		}
	}
	return s.fault.hit("after_arm")
}

//nolint:gocognit,nestif // Explicit fault predicates preserve the crash matrix boundaries.
func (s *faultExecutionStore) CommitExecution(
	ctx context.Context,
	revision uint64,
	lease flowy.ExecutionLease,
	target flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if revision == 0 {
		if err := s.fault.hit("before_intent"); err != nil {
			return flowy.ExecutionEnvelope{}, err
		}
	}
	var waits map[string]flowy.DurableWaitRecord
	if len(target.WaitsPayload) > 0 && json.Unmarshal(target.WaitsPayload, &waits) == nil {
		for _, w := range waits {
			if w.State == flowy.WaitArmed {
				if err := s.fault.hit("before_arm"); err != nil {
					return flowy.ExecutionEnvelope{}, err
				}
			}
		}
	}
	var journal map[string]flowy.ActivityRecord
	if json.Unmarshal(
		target.JournalPayload,
		&journal,
	) == nil {
		for _, a := range journal {
			if a.State == flowy.ActivityCompleted {
				if target.Progress.ExecutionPointer == "effect" {
					if err := s.fault.hit("journal_commit"); err != nil {
						return flowy.ExecutionEnvelope{}, err
					}
				} else if err := s.fault.hit("state_commit"); err != nil {
					return flowy.ExecutionEnvelope{}, err
				}
			}
		}
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, target)
}
