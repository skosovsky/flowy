package postgres

import (
	"context"
	"time"

	"github.com/skosovsky/flowy"
)

type ActivityRetryScanPage struct {
	Retries          []flowy.PendingActivityRetry
	AfterExecutionID string
	More             bool
}

// DiscoverDueActivityRetries observes only executions explicitly bound to this
// runtime profile. It does not acquire a lease, Resume, dispatch, classify an
// error, replenish attempts or move a persisted absolute deadline.
func (s *ExecutionStore) DiscoverDueActivityRetries(ctx context.Context, now time.Time,
	afterExecutionID string, scanLimit int,
) (ActivityRetryScanPage, error) {
	if s.waitProfile == nil {
		return ActivityRetryScanPage{}, flowy.ErrExecutionCapability
	}
	if now.IsZero() || scanLimit <= 0 || scanLimit > maxWaitScanHeads {
		return ActivityRetryScanPage{}, flowy.ErrWaitInvalid
	}
	page := ActivityRetryScanPage{Retries: make([]flowy.PendingActivityRetry, 0),
		AfterExecutionID: afterExecutionID, More: false}
	position, err := s.scanExecutionHeads(
		ctx,
		afterExecutionID,
		scanLimit,
		func(envelope flowy.ExecutionEnvelope) error {
			if envelope.RuntimeProfile == nil || *envelope.RuntimeProfile != *s.waitProfile {
				return nil
			}
			retries, inspectErr := flowy.InspectPendingActivityRetries(envelope)
			if inspectErr != nil {
				return inspectErr
			}
			for _, retry := range retries {
				if !now.Before(retry.Deadline) {
					page.Retries = append(page.Retries, retry)
				}
			}
			return nil
		},
	)
	if err != nil {
		return ActivityRetryScanPage{}, err
	}
	page.AfterExecutionID, page.More = position.AfterExecutionID, position.More
	return page, nil
}
