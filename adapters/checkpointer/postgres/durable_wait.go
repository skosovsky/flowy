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

// WaitScanPage uses bounded keyset pagination over execution heads, including
// non-due heads. Restart scanning from the empty cursor each complete cycle so
// updates/insertions behind the cursor are observed in the next cycle.
type WaitScanPage struct {
	Waits            []DueWait
	AfterExecutionID string
	More             bool
}

const maxWaitScanHeads = 500

// DiscoverDueWaits performs no dispatch, lease acquisition, timer acceptance or
// host decoding. The caller supplies time under the declared clock contract.
func (s *ExecutionStore) DiscoverDueWaits(ctx context.Context, now time.Time,
	afterExecutionID string, scanLimit int,
) (WaitScanPage, error) {
	if s.waitProfile == nil {
		return WaitScanPage{}, flowy.ErrExecutionCapability
	}
	if now.IsZero() || scanLimit <= 0 || scanLimit > maxWaitScanHeads {
		return WaitScanPage{}, flowy.ErrWaitInvalid
	}
	page := WaitScanPage{Waits: make([]DueWait, 0), AfterExecutionID: afterExecutionID, More: false}
	position, err := s.scanExecutionHeads(
		ctx,
		afterExecutionID,
		scanLimit,
		func(envelope flowy.ExecutionEnvelope) error {
			candidates, candidateErr := s.dueWaitCandidates(envelope, now)
			if candidateErr != nil {
				return candidateErr
			}
			page.Waits = append(page.Waits, candidates...)
			return nil
		},
	)
	if err != nil {
		return WaitScanPage{}, err
	}
	page.AfterExecutionID, page.More = position.AfterExecutionID, position.More
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
