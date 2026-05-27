package checkpoint

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

type state struct {
	Value string `json:"value"`
}

func TestEncodeDecodeStoredSnapshot(t *testing.T) {
	t.Parallel()
	snapshot := flowy.Snapshot[state]{
		ThreadID: "thread-1",
		Revision: 2,
		NodeID:   "n1",
		State:    state{Value: "ok"},
		RunMeta: flowy.RunMetadata{
			SegmentStartTime: time.Now().UTC(),
			RetryCounts:      map[string]int{"n1": 1},
			StepCount:        3,
		},
		Effects: []any{"metric"},
	}
	serializer := JSONSerializer[state]{}

	stored, err := EncodeStoredSnapshot(snapshot, serializer)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeStoredSnapshot(stored, serializer)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.ThreadID != snapshot.ThreadID || decoded.NodeID != snapshot.NodeID {
		t.Fatalf("decoded snapshot mismatch: %+v", decoded)
	}
	if decoded.Revision != 2 {
		t.Fatalf("expected revision 2, got %d", decoded.Revision)
	}
	if decoded.State.Value != "ok" {
		t.Fatalf("unexpected decoded state: %+v", decoded.State)
	}
}

func TestWithSanitizer(t *testing.T) {
	t.Parallel()
	serializer := WithSanitizer(JSONSerializer[state]{}, func(s *state) {
		s.Value = "sanitized"
	})
	data, err := serializer.Marshal(state{Value: "raw"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoded, err := serializer.Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Value != "sanitized" {
		t.Fatalf("expected sanitized state, got %+v", decoded)
	}
}

func TestDecodeStoredSnapshotLegacyVersionFallback(t *testing.T) {
	t.Parallel()
	legacyPayload := []byte(`{
		"thread_id":"legacy-thread",
		"version":7,
		"node_id":"n1",
		"state_payload":{"value":"ok"},
		"run_meta":{"segment_start_time":"2026-01-01T00:00:00Z","retry_counts":{},"step_count":1},
		"effects":[]
	}`)
	var stored StoredSnapshot
	if err := json.Unmarshal(legacyPayload, &stored); err != nil {
		t.Fatalf("unmarshal legacy stored snapshot: %v", err)
	}
	decoded, err := DecodeStoredSnapshot(stored, JSONSerializer[state]{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Revision != 7 {
		t.Fatalf("expected revision fallback from legacy version, got %d", decoded.Revision)
	}
}
