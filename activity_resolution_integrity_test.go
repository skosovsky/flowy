package flowy

import (
	"errors"
	"testing"
	"time"
)

func TestActivityDecisionIntegrityRejectsInvalidProvenance(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*ActivityRecord){
		"missing provenance":            func(r *ActivityRecord) { r.Resolutions = nil },
		"decision id":                   func(r *ActivityRecord) { r.Resolutions[0].DecisionID = "" },
		"reason":                        func(r *ActivityRecord) { r.Resolutions[0].Reason = "" },
		"evidence":                      func(r *ActivityRecord) { r.Resolutions[0].Evidence = "" },
		"time":                          func(r *ActivityRecord) { r.Resolutions[0].At = time.Time{} },
		"source revision":               func(r *ActivityRecord) { r.Resolutions[0].SourceRevision = 0 },
		"incarnation":                   func(r *ActivityRecord) { r.Resolutions[0].Incarnation = 0 },
		"attempt zero":                  func(r *ActivityRecord) { r.Resolutions[0].Attempt = 0 },
		"attempt absent":                func(r *ActivityRecord) { r.Resolutions[0].Attempt = 2 },
		"prior state":                   func(r *ActivityRecord) { r.Resolutions[0].PriorState = ActivityCompleted },
		"action":                        func(r *ActivityRecord) { r.Resolutions[0].Action = "invented" },
		"action outcome mismatch":       func(r *ActivityRecord) { r.Resolutions[0].Action = ActivityResolveFail },
		"unsafe retry":                  func(r *ActivityRecord) { r.Resolutions[0].Action = ActivityResolveRetry },
		"duplicate":                     func(r *ActivityRecord) { r.Resolutions = append(r.Resolutions, r.Resolutions[0]) },
		"fabricated successful attempt": func(r *ActivityRecord) { r.Attempts[0].State = ActivityCompleted },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			record := manualActivityForIntegrityTest()
			if err := validateActivityRecord(record); err != nil {
				t.Fatal(err)
			}
			// Act.
			mutate(&record)
			err := validateActivityDecisions(record)
			// Assert.
			if !errors.Is(err, ErrExecutionCorrupt) {
				t.Fatalf("invalid decision accepted: %v", err)
			}
		})
	}
}

func manualActivityForIntegrityTest() ActivityRecord {
	record := validRunningActivityForTest()
	record.Attempts[0].State, record.Attempts[0].Classification = ActivityUnknown, ActivityAmbiguous
	record.State, record.Origin = ActivityCompleted, ActivityManual
	record.Resolutions = []ActivityResolutionRecord{
		{
			DecisionID:     "confirmed",
			Attempt:        1,
			Action:         ActivityResolveComplete,
			PriorState:     ActivityUnknown,
			SourceRevision: 1,
			Incarnation:    2,
			Reason:         "confirmed",
			Evidence:       "receipt",
			At:             time.Now().UTC(),
		},
	}
	return record
}
