package checkpoint

import (
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

type state struct {
	Value string `json:"value"`
}

func TestEncodeDecodeStoredSnapshot(t *testing.T) {
	t.Parallel()
	snapshot := flowy.Snapshot[state, string]{
		ThreadID:         "thread-1",
		Revision:         2,
		ExecutionPointer: "n1",
		State:            state{Value: "ok"},
		RunMeta: flowy.RunMetadata{
			SegmentStartTime: time.Now().UTC(),
			RetryCounts:      map[string]int{"n1": 1},
			StepCount:        3,
		},
		Effects: []string{"metric"},
	}
	serializer := JSONSerializer[state]{}

	stored, err := EncodeStoredSnapshot(snapshot, serializer)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeStoredSnapshot[state, string](stored, serializer)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.ThreadID != snapshot.ThreadID || decoded.ExecutionPointer != snapshot.ExecutionPointer {
		t.Fatalf("decoded snapshot mismatch: %+v", decoded)
	}
	if decoded.Revision != 2 {
		t.Fatalf("expected revision 2, got %d", decoded.Revision)
	}
	if decoded.State.Value != "ok" {
		t.Fatalf("unexpected decoded state: %+v", decoded.State)
	}
}

func TestDecodeStoredSnapshotRejectsInvalidEnvelope(t *testing.T) {
	t.Parallel()

	serializer := JSONSerializer[state]{}
	valid, err := EncodeStoredSnapshot(flowy.Snapshot[state, string]{
		ThreadID:         "thread-1",
		Revision:         1,
		ExecutionPointer: "work",
		State:            state{Value: "ok"},
	}, serializer)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*StoredSnapshot)
	}{
		{
			name: "empty thread",
			mutate: func(s *StoredSnapshot) {
				s.ThreadID = ""
			},
		},
		{
			name: "zero revision",
			mutate: func(s *StoredSnapshot) {
				s.Revision = 0
			},
		},
		{
			name: "empty node",
			mutate: func(s *StoredSnapshot) {
				s.NodeID = ""
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			stored := valid
			tc.mutate(&stored)

			_, err := DecodeStoredSnapshot[state, string](stored, serializer)
			if !errors.Is(err, ErrInvalidStoredSnapshot) {
				t.Fatalf("expected ErrInvalidStoredSnapshot, got %v", err)
			}
		})
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
