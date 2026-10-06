package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func activityDemo(ctx context.Context) error {
	store := testutil.NewMemoryExecutionStore(nil)
	dispatches, reconciles := 0, 0
	request := flowy.ActivityRequest{Key: "write", Implementation: "host-write", Input: []byte("opaque input"),
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			dispatches++
			return nil, errors.New("receipt lost after remote write")
		}}
	runner, err := bind(store, "activity", func(ctx context.Context, s state) (state, flowy.Directive, error) {
		_, err := flowy.CallActivity(ctx, request)
		return s, flowy.End(), err
	}, options())
	if err != nil {
		return err
	}
	unknown, err := runner.Start(ctx, "activity-demo", state{})
	if !errors.Is(err, flowy.ErrActivityUnknown) || unknown == nil {
		return fmt.Errorf("expected unknown, got %w", err)
	}
	unknown, err = runner.Resume(ctx, unknown.ResumeToken)
	if !errors.Is(err, flowy.ErrActivityUnknown) || dispatches != 1 {
		return fmt.Errorf("blind retry: %w", err)
	}
	// Host evidence confirms the original action; this callback does not dispatch.
	request.Reconcile = func(context.Context, flowy.ActivityRecord) ([]byte, error) {
		reconciles++
		return []byte("host receipt"), nil
	}
	completed, err := runner.Resume(ctx, unknown.ResumeToken)
	if err != nil {
		return err
	}
	_, err = runner.Resume(ctx, completed.ResumeToken)
	if err != nil || dispatches != 1 || reconciles != 1 {
		return fmt.Errorf("cached outcome failed: %w", err)
	}
	return retryDemo(ctx)
}

type demoClock struct{ at time.Time }

func (c *demoClock) Now() time.Time { return c.at }

func retryDemo(ctx context.Context) error {
	clock := &demoClock{at: time.Now().UTC()}
	opts := options()
	opts.Clock = clock
	attempts := 0
	request := flowy.ActivityRequest{
		Key:            "safe-write",
		Implementation: "host-idempotent-write",
		Input:          []byte("input"),
		Retry: flowy.ActivityRetryPolicy{
			Label:             "bounded",
			MaxAttempts:       2,
			Delay:             time.Hour,
			SafeRetryContract: "host-deduplicates",
		},
		Classify: func(error) flowy.ActivityFailureClass { return flowy.ActivityRetryable },
		Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("definitive retryable failure")
			}
			return []byte("receipt"), nil
		},
	}
	runner, err := bind(
		testutil.NewMemoryExecutionStore(nil),
		"retry",
		func(ctx context.Context, s state) (state, flowy.Directive, error) {
			_, err := flowy.CallActivity(ctx, request)
			return s, flowy.End(), err
		},
		opts,
	)
	if err != nil {
		return err
	}
	pending, err := runner.Start(ctx, "retry-demo", state{})
	if !errors.Is(err, flowy.ErrActivityRetryPending) || pending == nil {
		return fmt.Errorf("retry not persisted: %w", err)
	}
	_, err = runner.Resume(ctx, pending.ResumeToken)
	if !errors.Is(err, flowy.ErrActivityRetryPending) || attempts != 1 {
		return fmt.Errorf("early retry dispatched: %w", err)
	}
	clock.at = clock.at.Add(time.Hour)
	completed, err := runner.Resume(ctx, pending.ResumeToken)
	if err != nil || completed == nil || completed.Status != flowy.RunStatusCompleted || attempts != 2 {
		return fmt.Errorf("bounded retry failed: %w", err)
	}
	return nil
}
