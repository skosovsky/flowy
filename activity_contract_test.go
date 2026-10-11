package flowy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestActivityClassificationFailsClosedWithDiagnostic(t *testing.T) {
	// Arrange: a host returns an unsupported classification containing private text.
	request := ActivityRequest{Classify: func(error) ActivityFailureDecision {
		return ActivityFailureDecision{Class: "private-provider-text", NotBefore: time.Now()}
	}}
	// Act.
	decision, invalid := classifyActivityFailure(request, errors.New("private-dispatch-error"))
	// Assert: no hint or unsafe retry is manufactured.
	if !invalid || decision.Class != ActivityAmbiguous || !decision.NotBefore.IsZero() {
		t.Fatalf("decision=%+v diagnostic=%t", decision, invalid)
	}
}

func TestActivityCallbackContextRejectsNestedCall(t *testing.T) {
	// Arrange: callback scope rejects even before required request configuration.
	ctx := context.WithValue(context.Background(), activityCallbackContextKey{}, true)
	// Act.
	_, err := CallActivity(ctx, ActivityRequest{})
	// Assert.
	if !errors.Is(err, ErrExecutionCapability) {
		t.Fatalf("nested call=%v", err)
	}
}

func TestActivityScheduleConstructors(t *testing.T) {
	for _, tc := range []struct {
		name             string
		initial, maximum time.Duration
		multiplier       uint64
		fixed, valid     bool
	}{
		{"zero fixed", 0, 0, 0, true, true},
		{"negative fixed", -1, -1, 0, true, false},
		{"exponential", time.Second, time.Minute, 2, false, true},
		{"zero exponential", 0, time.Minute, 2, false, false},
		{"cap smaller", time.Minute, time.Second, 2, false, false},
		{"unit multiplier", time.Second, time.Minute, 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange/Act: validate the schedule before a policy can dispatch.
			var schedule ActivityRetrySchedule
			var err error
			if tc.fixed {
				schedule, err = NewFixedActivityRetrySchedule(tc.initial)
			} else {
				schedule, err = NewExponentialActivityRetrySchedule(tc.initial, tc.maximum, tc.multiplier)
			}
			// Assert.
			if (err == nil) != tc.valid {
				t.Fatalf("schedule=%+v err=%v", schedule, err)
			}
		})
	}
}
