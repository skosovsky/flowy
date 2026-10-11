package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func task24RetryPolicy() flowy.ActivityRetryPolicy {
	return flowy.ActivityRetryPolicy{
		Label:             "exponential",
		MaxAttempts:       3,
		SafeRetryContract: "host-key",
		Schedule: flowy.ActivityRetrySchedule{
			Kind:           flowy.ActivityRetryExponential,
			InitialDelay:   10 * time.Second,
			MaxDelay:       time.Minute,
			Multiplier:     2,
			JitterPermille: 500,
			HintLabel:      "hint-v1",
		},
	}
}

func TestActivityScheduleCompatibilityRejectsEveryChangedField(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	clock := &testExecutionClock{}
	clock.set(time.Now().UTC())
	var dispatches, classifiers atomic.Int32
	policy := task24RetryPolicy()
	decision := flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable}
	first, err := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		decision,
		func() uint64 { return 0 },
		&dispatches,
		&classifiers,
	).Start(ctx, "run", 0)
	if !errors.Is(err, flowy.ErrActivityRetryPending) {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*flowy.ActivityRetryPolicy){
		"kind": func(p *flowy.ActivityRetryPolicy) {
			p.Schedule.Kind = flowy.ActivityRetryFixed
			p.Schedule.Multiplier = 0
			p.Schedule.MaxDelay = p.Schedule.InitialDelay
		},
		"initial":    func(p *flowy.ActivityRetryPolicy) { p.Schedule.InitialDelay++ },
		"max":        func(p *flowy.ActivityRetryPolicy) { p.Schedule.MaxDelay++ },
		"multiplier": func(p *flowy.ActivityRetryPolicy) { p.Schedule.Multiplier++ },
		"jitter":     func(p *flowy.ActivityRetryPolicy) { p.Schedule.JitterPermille++ },
		"hint label": func(p *flowy.ActivityRetryPolicy) { p.Schedule.HintLabel = "hint-v2" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := policy
			mutate(&changed)
			// Act.
			_, resumeErr := task24ScheduleRunner(
				t,
				store,
				clock,
				changed,
				decision,
				func() uint64 { t.Fatal("sample on conflict"); return 0 },
				&dispatches,
				&classifiers,
			).Resume(ctx, first.ResumeToken)
			// Assert.
			if !errors.Is(resumeErr, flowy.ErrActivityConflict) || dispatches.Load() != 1 || classifiers.Load() != 1 {
				t.Fatalf("err=%v calls=%d/%d", resumeErr, dispatches.Load(), classifiers.Load())
			}
		})
	}
}

func TestActivityScheduleRejectsResealedHistoryAndLegacyPolicy(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	clock := &testExecutionClock{}
	clock.set(time.Now().UTC())
	var calls, classifiers atomic.Int32
	first, err := task24ScheduleRunner(
		t,
		store,
		clock,
		task24RetryPolicy(),
		flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable},
		func() uint64 { return 0 },
		&calls,
		&classifiers,
	).Start(ctx, "run", 0)
	if !errors.Is(err, flowy.ErrActivityRetryPending) {
		t.Fatal(err)
	}
	source, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"legacy delay": func(r map[string]any) {
			p := r["retry"].(map[string]any)
			delete(p, "schedule")
			p["delay"] = float64(time.Second)
		},
		"choice deadline": func(r map[string]any) {
			r["next_attempt_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
		},
		"choice delay": func(r map[string]any) {
			a := r["attempts"].([]any)[0].(map[string]any)
			a["retry_schedule"].(map[string]any)["selected_delay"] = float64(-1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var records map[string]any
			if e := json.Unmarshal(source.JournalPayload, &records); e != nil {
				t.Fatal(e)
			}
			for _, raw := range records {
				mutate(raw.(map[string]any))
			}
			e := source
			e.JournalPayload, err = json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			e, err = flowy.SealExecutionEnvelope(e)
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			_, inspectErr := flowy.InspectPendingActivityRetries(e)
			// Assert: a fresh seal does not legalize invalid scheduling history.
			if !errors.Is(inspectErr, flowy.ErrExecutionCorrupt) || calls.Load() != 1 ||
				first.ResumeToken.SnapshotRevision != source.Revision {
				t.Fatalf("err=%v calls=%d", inspectErr, calls.Load())
			}
		})
	}
}

func TestActivityManualSchedulePersistsHintJitterAndOriginalAttempt(t *testing.T) {
	// Arrange: unknown requires host evidence, never ordinary classifier replay.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	clock := &testExecutionClock{}
	at := time.Now().UTC()
	clock.set(at)
	var calls, classifiers, samples atomic.Int32
	policy := task24RetryPolicy()
	random := func() uint64 { samples.Add(1); return uint64(time.Second) }
	runner := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		flowy.ActivityFailureDecision{Class: flowy.ActivityAmbiguous},
		random,
		&calls,
		&classifiers,
	)
	first, err := runner.Start(ctx, "run", 0)
	if !errors.Is(err, flowy.ErrActivityUnknown) {
		t.Fatal(err)
	}
	original := loadOnlyActivity(t, store)
	decision := manualResolution(original, flowy.ActivityResolveRetry)
	decision.NotBefore = at.Add(30 * time.Second)
	decision.SafeRetryContract = policy.SafeRetryContract
	// Act: persist one manual choice then restart with a sampler that must not run.
	token, err := runner.ResolveActivity(ctx, first.ResumeToken, decision)
	if err != nil {
		t.Fatal(err)
	}
	prepared := loadOnlyActivity(t, store)
	restarted := task24ScheduleRunner(
		t,
		store,
		clock,
		policy,
		flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable},
		func() uint64 { t.Fatal("rerolled manual schedule"); return 0 },
		&calls,
		&classifiers,
	)
	_, pendingErr := restarted.Resume(ctx, token)
	clock.set(at.Add(30 * time.Second))
	result, resumeErr := restarted.Resume(ctx, token)
	after := loadOnlyActivity(t, store)
	// Assert: original unknown history/evidence/config retained; choice sampled once.
	choice := prepared.Resolutions[0].RetrySchedule
	if !errors.Is(pendingErr, flowy.ErrActivityRetryPending) || resumeErr != nil ||
		result.Status != flowy.RunStatusCompleted ||
		calls.Load() != 2 ||
		classifiers.Load() != 1 ||
		samples.Load() != 1 ||
		choice == nil ||
		choice.SelectedDelay != 9*time.Second ||
		!choice.Deadline.Equal(decision.NotBefore) ||
		prepared.Attempts[0].State != flowy.ActivityUnknown ||
		after.Attempts[0].State != flowy.ActivityUnknown ||
		after.Retry != policy ||
		len(after.Resolutions) != 1 {
		t.Fatalf(
			"pending=%v resume=%v calls=%d samples=%d prepared=%+v after=%+v",
			pendingErr,
			resumeErr,
			calls.Load(),
			samples.Load(),
			prepared,
			after,
		)
	}
}
