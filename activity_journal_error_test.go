package flowy

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestActivityJournalErrorPreservesClassification(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{ErrConcurrencyConflict, ErrLeaseLost, ErrExecutionCorrupt,
		ErrInvalidSnapshot, ErrExecutionCapability, ErrExecutionIncompatible, context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			t.Parallel()
			// Arrange.
			original := fmt.Errorf("store: %w", cause)
			// Act.
			classified := activityJournalCommitError(original)
			// Assert: known contract failures are not relabelled as outages.
			if !errors.Is(classified, cause) || errors.Is(classified, ErrActivityJournalUnavailable) {
				t.Fatalf("classification changed: %v", classified)
			}
		})
	}
	// Arrange: an adapter-specific outage is not a core contract sentinel.
	cause := errors.New("connection unavailable")
	// Act.
	classified := activityJournalCommitError(cause)
	// Assert: callers can match both runtime classification and original cause.
	if !errors.Is(classified, ErrActivityJournalUnavailable) || !errors.Is(classified, cause) {
		t.Fatalf("outage classification lost: %v", classified)
	}
}
