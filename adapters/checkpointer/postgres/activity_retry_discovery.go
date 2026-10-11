package postgres

import (
	"context"
	"time"

	"github.com/skosovsky/flowy"
)

type ActivityRetryScanPage struct {
	Retries     []flowy.PendingActivityRetry
	Cursor      DiscoveryCursor
	More        bool
	Diagnostics []DiscoveryDiagnostic
}

// DiscoverDueActivityRetries observes indexed work bound to this runtime profile.
// It does not acquire a lease, dispatch, reset attempts or move deadlines.
func (s *ExecutionStore) DiscoverDueActivityRetries(ctx context.Context, now time.Time,
	cursor DiscoveryCursor, scanLimit int,
) (ActivityRetryScanPage, error) {
	var empty DiscoveryCursor
	page := ActivityRetryScanPage{
		Retries:     make([]flowy.PendingActivityRetry, 0),
		Cursor:      empty,
		More:        false,
		Diagnostics: nil,
	}
	position, diagnostics, more, err := s.scanDueCandidates(ctx, now, cursor, scanLimit, "retry",
		func(envelope flowy.ExecutionEnvelope, candidate dueCandidate) error {
			retries, inspectErr := flowy.InspectPendingActivityRetries(envelope)
			if inspectErr != nil {
				return inspectErr
			}
			for _, retry := range retries {
				if retry.Identity == candidate.Identity && !now.Before(retry.Deadline) {
					page.Retries = append(page.Retries, retry)
					return nil
				}
			}
			return flowy.ErrExecutionCorrupt
		})
	if err != nil {
		return ActivityRetryScanPage{}, err
	}
	page.Cursor, page.Diagnostics, page.More = position, diagnostics, more
	return page, nil
}
