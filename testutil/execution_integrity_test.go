package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestMemoryExecutionStoreRejectsCorruptHistory(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"malformed", "unsealed", "state", "sealed wrong identity", "sealed wrong revision"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			// Arrange: corrupt the actual stored bytes, not the detached load result.
			ctx := context.Background()
			store := NewMemoryExecutionStore(nil)
			lease, err := store.AcquireExecution(ctx, "run", "seed", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := store.CommitExecution(ctx, 0, lease, flowy.ExecutionEnvelope{
				ExecutionID: "run",
				Descriptor: flowy.ExecutionDescriptor{
					GraphID:       "g",
					GraphRevision: "r",
					StateCodec:    "s", EffectsCodec: "host-effects-v1",
					ExecutionContract: "e",
					ReplayPolicy:      flowy.StepReplayPolicy{Label: "safe", Mode: flowy.StepReplaySafe},
				},
				Progress: flowy.ExecutionProgress{ExecutionPointer: "node"},
			})
			if err != nil {
				t.Fatal(err)
			}
			payload := corruptMemoryExecutionPayload(t, envelope, kind)
			store.history["run"][1] = payload
			// Act.
			_, latestErr := store.LoadExecution(ctx, "run")
			_, exactErr := store.LoadCheckpoint(ctx, "run", 1)
			// Assert: neither read can silently recreate an execution or repair history.
			if !errors.Is(latestErr, flowy.ErrExecutionCorrupt) || !errors.Is(exactErr, flowy.ErrExecutionCorrupt) {
				t.Fatalf("corruption accepted: latest=%v exact=%v", latestErr, exactErr)
			}
			if string(store.history["run"][1]) != string(payload) || len(store.history["run"]) != 1 {
				t.Fatal("read rewrote history")
			}
		})
	}
}

func corruptMemoryExecutionPayload(t *testing.T, envelope flowy.ExecutionEnvelope, kind string) []byte {
	t.Helper()
	switch kind {
	case "malformed":
		return []byte("{")
	case "unsealed":
		envelope.Digest = ""
	case "state":
		envelope.Progress.StatePayload = []byte("tampered")
	case "sealed wrong identity":
		envelope.ExecutionID = "other"
	case "sealed wrong revision":
		envelope.Revision++
	}
	if kind == "sealed wrong identity" || kind == "sealed wrong revision" {
		sealed, err := flowy.SealExecutionEnvelope(envelope)
		if err != nil {
			t.Fatal(err)
		}
		envelope = sealed
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
