package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func seedMemoryForkAnchor(t *testing.T) (*MemoryExecutionStore, flowy.ExecutionEnvelope, flowy.ExecutionLease) {
	t.Helper()
	ctx := context.Background()
	store := NewMemoryExecutionStore(nil)
	descriptor := flowy.ExecutionDescriptor{GraphID: "g", GraphRevision: "r", StateCodec: "s",
		ExecutionContract: "e", ReplayPolicy: flowy.StepReplayPolicy{Label: "safe", Mode: flowy.StepReplaySafe}}
	lease, err := store.AcquireExecution(ctx, "target", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := store.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
		ExecutionID: "target", Descriptor: descriptor, Progress: flowy.MigrationState{ExecutionPointer: "node"},
		Fork: &flowy.ForkLineage{
			Source: flowy.HistoricalCheckpointReference{ExecutionID: "source", Revision: 1,
				Digest: strings.Repeat("0", 64)},
			SourceDescriptor: descriptor,
			TargetDescriptor: descriptor,
			TargetID:         "target",
			Mode:             flowy.ForkFake,
			PolicyLabel:      "fake",
			TransformLabel:   "copy",
			CreatedAt:        time.Now().UTC(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, envelope, lease
}

func TestMemoryForkAnchorRejectsResealedHistory(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"erase", "mode", "source", "label", "time", "anchor erased"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			// Arrange: a retained first checkpoint and a later head with the same origin.
			ctx := context.Background()
			store, creation, lease := seedMemoryForkAnchor(t)
			head, err := store.CommitExecution(ctx, creation.Revision, lease, creation)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "erase":
				head.Fork = nil
			case "mode":
				head.Fork.Mode, head.Fork.ProjectionLabel = flowy.ForkLive, "invented"
			case "source":
				head.Fork.Source.Digest = strings.Repeat("1", 64)
			case "label":
				head.Fork.TransformLabel = "invented"
			case "time":
				head.Fork.CreatedAt = head.Fork.CreatedAt.Add(time.Hour)
			case "anchor erased":
				delete(store.lineage, "target")
			}
			sealed, err := flowy.SealExecutionEnvelope(head)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(sealed)
			if err != nil {
				t.Fatal(err)
			}
			store.history["target"][1] = payload
			// Act: loading or writing cannot trust a recomputed digest as origin authority.
			_, latestErr := store.LoadExecution(ctx, "target")
			_, exactErr := store.LoadCheckpoint(ctx, "target", 2)
			_, commitErr := store.CommitExecution(ctx, 2, lease, sealed)
			// Assert: no repair or extra revision is published.
			if !errors.Is(latestErr, flowy.ErrExecutionCorrupt) || !errors.Is(exactErr, flowy.ErrExecutionCorrupt) ||
				!errors.Is(commitErr, flowy.ErrExecutionCorrupt) || len(store.history["target"]) != 2 ||
				string(store.history["target"][1]) != string(payload) {
				t.Fatalf("resealed lineage accepted: latest=%v exact=%v commit=%v", latestErr, exactErr, commitErr)
			}
		})
	}
}
