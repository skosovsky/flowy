package main

import (
	"context"
	"fmt"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func forkDemo(ctx context.Context) error {
	store := testutil.NewMemoryExecutionStore(nil)
	live, simulated := 0, 0
	node := func(ctx context.Context, s state) (state, flowy.Directive, error) {
		_, err := flowy.CallActivity(
			ctx,
			flowy.ActivityRequest{Key: "write", Implementation: "host-write", Input: []byte("input"),
				Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
					live++
					return []byte("live receipt"), nil
				}},
		)
		if err != nil {
			return s, flowy.Fail("activity"), err
		}
		s.Value++
		return s, flowy.End(), nil
	}
	sourceRunner, err := bind(store, "source", node, options())
	if err != nil {
		return err
	}
	if _, startErr := sourceRunner.Start(
		ctx,
		"fork-source",
		state{Value: 1, Handles: []string{"source-approval"}},
	); startErr != nil {
		return startErr
	}
	source, err := store.LoadExecution(ctx, "fork-source")
	if err != nil {
		return err
	}
	opts := options()
	opts.ForkPolicy = &flowy.ForkExecutionPolicy{Label: "fake-example", Mode: flowy.ForkFake,
		FakeActivity: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
			simulated++
			return []byte("simulated receipt"), nil
		}}
	target, err := bind(store, "fork", node, opts)
	if err != nil {
		return err
	}
	reference := flowy.HistoricalCheckpointReference{
		ExecutionID: source.ExecutionID,
		Revision:    source.Revision,
		Digest:      source.Digest,
	}
	if _, inspectErr := flowy.InspectExecutionCheckpoint(ctx, store, reference); inspectErr != nil {
		return inspectErr
	}
	token, err := target.Fork(
		ctx,
		flowy.ForkRequest{
			Source:      reference,
			TargetID:    "fork-target",
			Mode:        flowy.ForkFake,
			PolicyLabel: opts.ForkPolicy.Label,
			Transform: flowy.ForkTransform{Label: "host-correction", Source: source.Descriptor,
				Transform: func(progress flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
					codec := checkpoint.JSONSerializer[state]{}
					s, decodeErr := codec.Unmarshal(progress.StatePayload)
					if decodeErr != nil {
						return progress, decodeErr
					}
					s.Value, s.Handles = correction, nil
					progress.StatePayload, decodeErr = codec.Marshal(s)
					return progress, decodeErr
				}},
		},
	)
	if err != nil {
		return err
	}
	result, err := target.Resume(ctx, token)
	if err != nil || result == nil || result.State.Value != 11 || len(result.State.Handles) != 0 || live != 1 ||
		simulated != 1 {
		return fmt.Errorf(
			"fake fork leaked live authority: result=%+v live=%d simulated=%d err=%w",
			result,
			live,
			simulated,
			err,
		)
	}
	retained, err := store.LoadExecution(ctx, "fork-source")
	if err != nil || retained.Digest != source.Digest {
		return fmt.Errorf("fork changed source: %w", err)
	}
	return nil
}
