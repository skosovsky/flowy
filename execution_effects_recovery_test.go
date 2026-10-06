package flowy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

//nolint:gocognit // combined state/effects codec/transform failure publication matrix
func TestStateAndEffectsMigrationPublishTogether(t *testing.T) {
	for _, failure := range []string{"none", "state codec", "effects codec", "effects transform", "missing transform"} {
		t.Run(failure, func(t *testing.T) {
			// Arrange: raw persisted BYOT source, with an idle initial boundary.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			old := durableDescriptor("old")
			old.EffectsCodec = "effects-v1"
			seed := flowy.ExecutionEnvelope{
				ExecutionID:    "run",
				Descriptor:     old,
				Progress:       flowy.MigrationState{StatePayload: []byte(`{"Value":7}`), ExecutionPointer: "node"},
				EffectsPayload: []byte(`["old"]`),
				Activation:     1,
			}
			lease, err := store.AcquireExecution(ctx, "run", "seed", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			source, err := store.CommitExecution(ctx, 0, lease, seed)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.ReleaseExecution(ctx, lease); err != nil {
				t.Fatal(err)
			}
			target := durableDescriptor("new")
			target.EffectsCodec = "effects-v2"
			migration := flowy.ExecutionMigration{
				ID:     "both-codecs",
				Source: old,
				Target: target,
				Transform: func(s flowy.MigrationState) (flowy.MigrationState, error) {
					s.StatePayload = []byte(`42`)
					if failure == "state codec" {
						s.StatePayload = []byte(`{}`)
					}
					return s, nil
				},
				EffectsTransform: func(payload []byte) ([]byte, error) {
					payload[0] = 'X'
					if failure == "effects transform" {
						return nil, errors.New("host transform rejected")
					}
					if failure == "effects codec" {
						return []byte(`["invalid"]`), nil
					}
					return []byte(`[9]`), nil
				},
			}
			if failure == "missing transform" {
				migration.EffectsTransform = nil
			}
			builder := flowy.NewGraph[int, int](func(_, u int) int { return u })
			builder.AddNode("node", func(_ context.Context, s int) (int, flowy.Directive, error) { return s, flowy.End(), nil }).
				SetEntryPoint("node").
				AllowNoOutgoingRoute("node")
			graph, err := builder.Compile()
			if err != nil {
				t.Fatal(err)
			}
			runner, err := flowy.NewDurableRunner(
				graph,
				store,
				target,
				checkpoint.JSONSerializer[int]{},
				checkpoint.JSONSerializer[[]int]{},
				flowy.DurableOptions{
					Owner:      "new",
					LeaseTTL:   time.Minute,
					Migrations: []flowy.ExecutionMigration{migration},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			result, resumeErr := runner.Resume(
				ctx,
				flowy.ResumeToken{ThreadID: "run", SnapshotRevision: source.Revision},
			)
			head, loadErr := store.LoadExecution(ctx, "run")
			retained, historyErr := store.LoadCheckpoint(ctx, "run", source.Revision)
			// Assert: invalid representation never publishes a half-migrated boundary.
			if loadErr != nil || historyErr != nil || retained.Digest != source.Digest ||
				string(retained.EffectsPayload) != `["old"]` {
				t.Fatalf("load=%v history=%v retained=%+v", loadErr, historyErr, retained)
			}
			if failure != "none" {
				if !errors.Is(resumeErr, flowy.ErrMigrationInvalid) || head.Digest != source.Digest {
					t.Fatalf("err=%v head=%+v", resumeErr, head)
				}
			} else if resumeErr != nil || result.State != 42 || len(result.Effects) != 1 || result.Effects[0] != 9 || head.Descriptor != target || head.Migration == nil {
				t.Fatalf("result=%+v err=%v head=%+v", result, resumeErr, head)
			}
		})
	}
}
