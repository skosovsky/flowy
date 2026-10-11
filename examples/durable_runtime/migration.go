package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func migrationDemo(ctx context.Context) error {
	store := testutil.NewMemoryExecutionStore(nil)
	old, err := bind(store, "old", func(_ context.Context, s state) (state, flowy.Directive, error) {
		return s, flowy.Suspend("ready for migration"), nil
	}, options())
	if err != nil {
		return err
	}
	paused, err := old.Start(ctx, "migration-source", state{Value: 1})
	if err != nil {
		return err
	}
	source, err := store.LoadExecution(ctx, "migration-source")
	if err != nil {
		return err
	}
	opts := options()
	opts.Migrations = []flowy.ExecutionMigration{
		{ID: "host-state-correction", Source: descriptor("old"), Target: descriptor("new"),
			Transform: correctExecutionProgress},
	}
	target, err := bind(store, "new", func(_ context.Context, s state) (state, flowy.Directive, error) {
		s.Value++
		return s, flowy.End(), nil
	}, opts)
	if err != nil {
		return err
	}
	result, err := target.Resume(ctx, paused.ResumeToken)
	if err != nil || result == nil || result.State.Value != 12 {
		return fmt.Errorf("migration failed: result=%+v err=%w", result, err)
	}
	retained, err := store.LoadCheckpoint(ctx, source.ExecutionID, source.Revision)
	if err != nil || retained.Digest != source.Digest {
		return fmt.Errorf("source history changed: %w", err)
	}
	return importDemo(ctx, target)
}

func correctExecutionProgress(progress flowy.ExecutionProgress) (flowy.ExecutionProgress, error) {
	codec := checkpoint.JSONSerializer[state]{}
	value, err := codec.Unmarshal(progress.StatePayload)
	if err != nil {
		return progress, err
	}
	value.Value += correction
	progress.StatePayload, err = codec.Marshal(value)
	return progress, err
}

func importDemo(ctx context.Context, runner *flowy.DurableRunner[state, flowy.NoEffect]) error {
	payload := []byte("host-captured legacy artifact")
	sum := sha256.Sum256(payload)
	source := flowy.LegacyExecutionSource{ID: "legacy-source", Revision: legacyRevision, Format: "host-format",
		Digest: hex.EncodeToString(sum[:]), Payload: payload}
	importer := flowy.ExecutionImporter{ID: "host-converter", Format: source.Format,
		Transform: func([]byte) (flowy.ImportedExecutionState, error) {
			return flowy.ImportedExecutionState{Progress: flowy.ExecutionProgress{
				ExecutionPointer: workNode, StatePayload: []byte(`{"Value":41}`)}, EffectsPayload: []byte("[]")}, nil
		}}
	token, err := runner.Import(ctx, "import-target", source, importer)
	if err != nil {
		return err
	}
	result, err := runner.Resume(ctx, token)
	if err != nil || result == nil || result.State.Value != 42 {
		return fmt.Errorf("import failed: result=%+v err=%w", result, err)
	}
	return nil
}
