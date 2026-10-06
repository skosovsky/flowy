package flowy

import (
	"errors"
	"math/rand/v2"
	"time"
)

// ActivityRetryScheduleKind selects explicit persisted deadline arithmetic.
type ActivityRetryScheduleKind string

const retryJitterDenominator = 1000

const (
	ActivityRetryFixed       ActivityRetryScheduleKind = "fixed"
	ActivityRetryExponential ActivityRetryScheduleKind = "exponential"
)

// ActivityRetrySchedule is persisted configuration, independent of providers.
// HintLabel names the host translation of failure hints, when enabled.
type ActivityRetrySchedule struct {
	Kind           ActivityRetryScheduleKind `json:"kind"`
	InitialDelay   time.Duration             `json:"initial_delay"`
	MaxDelay       time.Duration             `json:"max_delay"`
	Multiplier     uint64                    `json:"multiplier"`
	JitterPermille uint16                    `json:"jitter_permille"`
	HintLabel      string                    `json:"hint_label"`
}

// ActivityFailureDecision combines host classification with a generic hint.
// A past hint cannot move a retry deadline backwards; unknown never retries.
type ActivityFailureDecision struct {
	Class     ActivityFailureClass
	NotBefore time.Time
}

// ActivityRetryScheduleDecision records one choice, not a live scheduling hook.
// Rejected decisions have no Deadline. Unrepresentable hints are reported by
// Rejection without serializing the invalid time into the execution journal.
type ActivityRetryScheduleDecision struct {
	Attempt       int           `json:"attempt"`
	PreparedAt    time.Time     `json:"prepared_at"`
	BaseDelay     time.Duration `json:"base_delay"`
	SelectedDelay time.Duration `json:"selected_delay"`
	NotBefore     time.Time     `json:"not_before,omitzero"`
	Deadline      time.Time     `json:"deadline,omitzero"`
	Rejection     string        `json:"rejection,omitempty"`
}

var ErrActivityScheduleInvalid = errors.New("flowy: invalid activity retry schedule")

func validateActivitySchedule(schedule ActivityRetrySchedule) error {
	if schedule.InitialDelay < 0 || schedule.MaxDelay < schedule.InitialDelay ||
		schedule.JitterPermille > retryJitterDenominator ||
		!validRuntimeText(schedule.HintLabel) {
		return ErrActivityScheduleInvalid
	}
	switch schedule.Kind {
	case ActivityRetryFixed:
		if schedule.Multiplier != 0 || schedule.MaxDelay != schedule.InitialDelay {
			return ErrActivityScheduleInvalid
		}
	case ActivityRetryExponential:
		if schedule.InitialDelay <= 0 || schedule.Multiplier < 2 {
			return ErrActivityScheduleInvalid
		}
	default:
		return ErrActivityScheduleInvalid
	}
	return nil
}

func activityRetryBaseDelay(schedule ActivityRetrySchedule, attempt int) time.Duration {
	delay := schedule.InitialDelay
	if schedule.Kind == ActivityRetryExponential {
		for number := 1; number < attempt && delay < schedule.MaxDelay; number++ {
			//nolint:gosec // validated positive durations make this quotient nonnegative and bounded by MaxInt64
			if schedule.Multiplier > uint64(schedule.MaxDelay/delay) {
				return schedule.MaxDelay
			}
			//nolint:gosec // preceding division bound proves Multiplier <= MaxDelay/delay <= MaxInt64
			delay *= time.Duration(schedule.Multiplier)
		}
	}
	return delay
}

func activityRetryJitterWidth(delay time.Duration, permille uint16) time.Duration {
	// Each product is bounded by delay; avoid delay*permille overflow.
	return delay/retryJitterDenominator*time.Duration(
		permille,
	) + (delay%retryJitterDenominator)*time.Duration(
		permille,
	)/retryJitterDenominator
}

func activityScheduleTimeValid(value time.Time) bool {
	return !value.IsZero() && value.UTC().Year() >= 1 && value.UTC().Year() <= 9999
}

func chooseActivityRetrySchedule(policy ActivityRetryPolicy, attempt int, at, hint time.Time,
	random func() uint64) ActivityRetryScheduleDecision {
	var decision ActivityRetryScheduleDecision
	decision.Attempt, decision.PreparedAt = attempt, at.UTC()
	if attempt < 1 || attempt >= policy.MaxAttempts || validateActivityRetry(policy) != nil {
		decision.Rejection = "policy"
		return decision
	}
	if !activityScheduleTimeValid(at) {
		decision.Rejection = "clock"
		return decision
	}
	decision.BaseDelay = activityRetryBaseDelay(policy.Schedule, attempt)
	decision.SelectedDelay = decision.BaseDelay
	if !hint.IsZero() {
		if !activityScheduleTimeValid(hint) {
			decision.Rejection = "hint_time"
			return decision
		}
		decision.NotBefore = hint.UTC()
		if policy.Schedule.HintLabel == "" {
			decision.Rejection = "hint_label"
			return decision
		}
	}
	width := activityRetryJitterWidth(decision.BaseDelay, policy.Schedule.JitterPermille)
	if width > 0 {
		if random == nil {
			random = rand.Uint64
		}
		value, ok := activityRetryRandomSample(random)
		if !ok {
			decision.Rejection = "random"
			return decision
		}
		//nolint:gosec // reduction is in [0,width], and positive width is at most MaxInt64
		decision.SelectedDelay -= time.Duration(value % (uint64(width) + 1))
	}
	deadline := at.UTC().Add(decision.SelectedDelay)
	if decision.NotBefore.After(deadline) {
		deadline = decision.NotBefore
	}
	if deadline.Before(at) || !activityScheduleTimeValid(deadline) {
		decision.Rejection = "deadline"
		return decision
	}
	decision.Deadline = deadline
	return decision
}

//nolint:nonamedreturns // deferred panic recovery updates the returned success flag
func activityRetryRandomSample(random func() uint64) (sample uint64, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return random(), true
}

func validActivityRetryScheduleDecision(policy ActivityRetryPolicy, decision *ActivityRetryScheduleDecision) bool {
	if decision == nil {
		return false
	}
	if validateActivityRetry(policy) != nil || decision.Attempt < 1 || decision.Attempt >= policy.MaxAttempts ||
		!activityScheduleTimeValid(decision.PreparedAt) ||
		decision.BaseDelay < 0 ||
		decision.SelectedDelay < 0 ||
		decision.SelectedDelay > decision.BaseDelay {
		return false
	}
	if decision.Rejection != "" {
		return validRejectedActivitySchedule(policy, decision)
	}
	base := activityRetryBaseDelay(policy.Schedule, decision.Attempt)
	if decision.BaseDelay != base ||
		decision.SelectedDelay < base-activityRetryJitterWidth(base, policy.Schedule.JitterPermille) {
		return false
	}
	if !decision.NotBefore.IsZero() &&
		(policy.Schedule.HintLabel == "" || !activityScheduleTimeValid(decision.NotBefore)) {
		return false
	}
	expected := decision.PreparedAt.Add(decision.SelectedDelay)
	if decision.NotBefore.After(expected) {
		expected = decision.NotBefore
	}
	return activityScheduleTimeValid(decision.Deadline) && expected.Equal(decision.Deadline)
}

func validRejectedActivitySchedule(policy ActivityRetryPolicy, d *ActivityRetryScheduleDecision) bool {
	if !d.Deadline.IsZero() || d.BaseDelay != activityRetryBaseDelay(policy.Schedule, d.Attempt) {
		return false
	}
	switch d.Rejection {
	case "hint_time":
		return d.NotBefore.IsZero() && d.SelectedDelay == d.BaseDelay
	case "hint_label":
		return policy.Schedule.HintLabel == "" && activityScheduleTimeValid(d.NotBefore) &&
			d.SelectedDelay == d.BaseDelay
	case "random":
		return activityRetryJitterWidth(d.BaseDelay, policy.Schedule.JitterPermille) > 0 &&
			d.SelectedDelay == d.BaseDelay &&
			validActivityScheduleHint(policy, d.NotBefore)
	case "deadline":
		if !validActivityScheduleHint(policy, d.NotBefore) ||
			d.SelectedDelay < d.BaseDelay-activityRetryJitterWidth(d.BaseDelay, policy.Schedule.JitterPermille) {
			return false
		}
		expected := d.PreparedAt.Add(d.SelectedDelay)
		if d.NotBefore.After(expected) {
			expected = d.NotBefore
		}
		return !activityScheduleTimeValid(expected) || expected.Before(d.PreparedAt)
	default:
		return false
	}
}
func validActivityScheduleHint(policy ActivityRetryPolicy, hint time.Time) bool {
	return hint.IsZero() || (policy.Schedule.HintLabel != "" && activityScheduleTimeValid(hint))
}
