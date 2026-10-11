package flowy

import (
	"math"
	"testing"
	"time"
)

func TestActivityScheduleBounds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		schedule ActivityRetrySchedule
		attempt  int
		sample   uint64
		want     time.Duration
	}{
		{"fixed", ActivityRetrySchedule{Kind: ActivityRetryFixed, InitialDelay: 10 * time.Second, MaxDelay: 10 * time.Second}, 1, 0, 10 * time.Second},
		{"grow", ActivityRetrySchedule{Kind: ActivityRetryExponential, InitialDelay: time.Second, MaxDelay: 10 * time.Second, Multiplier: 2}, 4, 0, 8 * time.Second},
		{"cap", ActivityRetrySchedule{Kind: ActivityRetryExponential, InitialDelay: time.Second, MaxDelay: 10 * time.Second, Multiplier: 2}, 5, 0, 10 * time.Second},
		{"overflow saturation", ActivityRetrySchedule{Kind: ActivityRetryExponential, InitialDelay: time.Duration(math.MaxInt64 / 2), MaxDelay: time.Duration(math.MaxInt64), Multiplier: math.MaxUint64}, 2, 0, time.Duration(math.MaxInt64)},
		{"large attempt bounded work", ActivityRetrySchedule{Kind: ActivityRetryExponential, InitialDelay: time.Nanosecond, MaxDelay: time.Hour, Multiplier: 2}, math.MaxInt - 1, 0, time.Hour},
		{"jitter lower", ActivityRetrySchedule{Kind: ActivityRetryFixed, InitialDelay: 10 * time.Second, MaxDelay: 10 * time.Second, JitterPermille: 500}, 1, uint64(5 * time.Second), 5 * time.Second},
		{"jitter upper", ActivityRetrySchedule{Kind: ActivityRetryFixed, InitialDelay: 10 * time.Second, MaxDelay: 10 * time.Second, JitterPermille: 500}, 1, 0, 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange.
			policy := ActivityRetryPolicy{
				Label:             "schedule",
				MaxAttempts:       math.MaxInt,
				SafeRetryContract: "safe",
				Schedule:          tc.schedule,
			}
			at := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
			// Act.
			decision := chooseActivityRetrySchedule(
				policy,
				tc.attempt,
				at,
				time.Time{},
				func() uint64 { return tc.sample },
			)
			// Assert.
			if decision.Rejection != "" || decision.SelectedDelay != tc.want ||
				!decision.Deadline.Equal(at.Add(tc.want)) ||
				!validActivityRetryScheduleDecision(policy, &decision) {
				t.Fatalf("decision=%+v expected=%v", decision, tc.want)
			}
		})
	}
}

func TestActivityScheduleHintAndInvalidInput(t *testing.T) {
	at := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		hint      time.Time
		label     string
		want      time.Time
		rejection string
	}{
		{name: "future", hint: at.Add(time.Hour), label: "adapter", want: at.Add(time.Hour)},
		{name: "expired", hint: at.Add(-time.Hour), label: "adapter", want: at.Add(time.Second)},
		{name: "unlabelled", hint: at.Add(time.Hour), rejection: "hint_label"},
		{name: "unrepresentable", hint: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), label: "adapter", rejection: "hint_time"},
		{name: "UTC overflow", hint: time.Date(9999, 12, 31, 23, 0, 0, 0, time.FixedZone("offset", -8*3600)), label: "adapter", rejection: "hint_time"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange.
			policy := ActivityRetryPolicy{
				Label:             "fixed",
				MaxAttempts:       2,
				SafeRetryContract: "safe",
				Schedule: ActivityRetrySchedule{
					Kind:         ActivityRetryFixed,
					InitialDelay: time.Second,
					MaxDelay:     time.Second,
					HintLabel:    tc.label,
				},
			}
			// Act.
			decision := chooseActivityRetrySchedule(policy, 1, at, tc.hint, nil)
			// Assert.
			if decision.Rejection != tc.rejection || !decision.Deadline.Equal(tc.want) ||
				!validActivityRetryScheduleDecision(policy, &decision) {
				t.Fatalf("decision=%+v", decision)
			}
		})
	}
}

func TestActivityScheduleRejectsInvalidConfigAndDeadline(t *testing.T) {
	for _, cfg := range []ActivityRetrySchedule{
		{}, {Kind: ActivityRetryFixed, InitialDelay: -1}, {Kind: ActivityRetryFixed, Multiplier: 2},
		{Kind: ActivityRetryFixed, MaxDelay: time.Second}, {Kind: ActivityRetryFixed, JitterPermille: 1001},
		{Kind: ActivityRetryExponential, InitialDelay: time.Second, MaxDelay: time.Second, Multiplier: 1},
	} {
		// Arrange/Act/Assert.
		if validateActivitySchedule(cfg) == nil {
			t.Fatalf("invalid config accepted: %+v", cfg)
		}
	}
	// Arrange.
	policy := ActivityRetryPolicy{
		Label:             "fixed",
		MaxAttempts:       2,
		SafeRetryContract: "safe",
		Schedule: ActivityRetrySchedule{
			Kind:         ActivityRetryFixed,
			InitialDelay: time.Hour,
			MaxDelay:     time.Hour,
		},
	}
	at := time.Date(9999, 12, 31, 23, 59, 0, 0, time.UTC)
	// Act.
	decision := chooseActivityRetrySchedule(policy, 1, at, time.Time{}, nil)
	// Assert.
	if decision.Rejection != "deadline" || !decision.Deadline.IsZero() ||
		!validActivityRetryScheduleDecision(policy, &decision) {
		t.Fatalf("decision=%+v", decision)
	}
}
