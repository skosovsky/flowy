package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/skosovsky/flowy"
)

// NewWaitExecutionStore explicitly enables durable wait registration/discovery
// on the same aggregate store. It introduces no independently owned scheduler.
func NewWaitExecutionStore(db DB, profile flowy.WaitCapabilityProfile) (*ExecutionStore, error) {
	if db == nil || profile.Validate() != nil {
		return nil, flowy.ErrExecutionCapability
	}
	return &ExecutionStore{db: db, waitProfile: &profile}, nil
}

func (s *ExecutionStore) WaitCapabilities() flowy.WaitCapabilityProfile {
	if s.waitProfile == nil {
		return flowy.WaitCapabilityProfile{}
	}
	return *s.waitProfile
}

// RegisterWait verifies the committed generation. Its aggregate is the durable
// registration; discovery reads that aggregate, so a separate index/write
// cannot acknowledge a timer that was never armed or lose it on worker death.
func (s *ExecutionStore) RegisterWait(ctx context.Context, expected flowy.DurableWaitRecord) error {
	if s.waitProfile == nil || expected.Profile != *s.waitProfile {
		return flowy.ErrExecutionCapability
	}
	envelope, err := s.LoadExecution(ctx, expected.ExecutionID)
	if err != nil {
		return err
	}
	records, err := flowy.InspectExecutionWaits(envelope)
	if err != nil {
		return err
	}
	expectedSpec, err := json.Marshal(expected.Spec)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Generation != expected.Generation {
			continue
		}
		actualSpec, encodeErr := json.Marshal(record.Spec)
		if encodeErr != nil {
			return encodeErr
		}
		if record.State != flowy.WaitArmed || record.ArmRevision != expected.ArmRevision ||
			record.Node != expected.Node || record.Activation != expected.Activation ||
			record.Profile != expected.Profile || !bytes.Equal(actualSpec, expectedSpec) {
			return flowy.ErrWaitConflict
		}
		return nil
	}
	return flowy.ErrWaitNotArmed
}

// DueWait is an observation, not a claimed lease or accepted timer. Delivery
// must acquire the execution and revalidate generation/revision/deadline.
type DueWait struct {
	Wait     flowy.DurableWaitRecord
	Revision uint64
}

// WaitScanPage paginates indexed due work. Reset Cursor after a complete cycle
// so concurrent insertions or reschedules behind it appear in the next cycle.
type WaitScanPage struct {
	Waits       []DueWait
	Cursor      DiscoveryCursor
	More        bool
	Diagnostics []DiscoveryDiagnostic
}

const maxWaitScanHeads = 500

// DiscoverDueWaits observes indexed work and revalidates each authoritative head.
// It performs no dispatch, lease acquisition or timer acceptance.
func (s *ExecutionStore) DiscoverDueWaits(ctx context.Context, now time.Time,
	cursor DiscoveryCursor, scanLimit int,
) (WaitScanPage, error) {
	var empty DiscoveryCursor
	page := WaitScanPage{Waits: make([]DueWait, 0), Cursor: empty, More: false, Diagnostics: nil}
	position, diagnostics, more, err := s.scanDueCandidates(ctx, now, cursor, scanLimit, "wait",
		func(envelope flowy.ExecutionEnvelope, candidate dueCandidate) error {
			waits, inspectErr := s.dueWaitCandidates(envelope, now)
			if inspectErr != nil {
				return inspectErr
			}
			for _, wait := range waits {
				if wait.Wait.Generation == candidate.Identity {
					page.Waits = append(page.Waits, wait)
					return nil
				}
			}
			return flowy.ErrExecutionCorrupt
		})
	if err != nil {
		return WaitScanPage{}, err
	}
	page.Cursor, page.Diagnostics, page.More = position, diagnostics, more
	return page, nil
}

func (s *ExecutionStore) dueWaitCandidates(envelope flowy.ExecutionEnvelope, now time.Time) ([]DueWait, error) {
	if len(envelope.WaitsPayload) == 0 {
		return []DueWait{}, nil
	}
	records, err := flowy.InspectExecutionWaits(envelope)
	if err != nil {
		return nil, err
	}
	result := make([]DueWait, 0)
	for _, record := range records {
		if record.Profile == *s.waitProfile && record.State == flowy.WaitArmed && !now.Before(record.Spec.Deadline) {
			result = append(result, DueWait{Wait: record, Revision: envelope.Revision})
		}
	}
	return result, nil
}
