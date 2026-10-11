package checkpoint_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"unicode/utf8"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func FuzzSnapshotRecordAdmission(f *testing.F) {
	f.Add("run", uint64(1), []byte(`{"value":1}`))
	f.Add("", uint64(0), []byte(`null`))
	f.Add("run", uint64(1), []byte(`{`))
	f.Fuzz(func(t *testing.T, id string, revision uint64, payload []byte) {
		if len(id) > 1024 || len(payload) > 8192 {
			t.Skip()
		}
		// Arrange: JSON BYOT state, independent of runtime descriptor semantics.
		snapshot := flowy.Snapshot[json.RawMessage, flowy.NoEffect]{ThreadID: id, Revision: revision,
			ExecutionPointer: "node", State: bytes.Clone(payload)}
		codec := checkpoint.JSONSerializer[json.RawMessage]{}
		// Act.
		record, err := checkpoint.EncodeRecord(snapshot, codec)
		// Assert: metadata/JSON admission cannot silently normalize invalid input.
		if invalidFuzzSnapshot(id, revision, payload) {
			if err == nil {
				t.Fatal("invalid snapshot admitted")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		assertFuzzSnapshotRecord(t, id, revision, record, codec)
	})
}

func invalidFuzzSnapshot(id string, revision uint64, payload []byte) bool {
	return id == "" || !utf8.ValidString(id) || revision == 0 || !json.Valid(payload)
}

func assertFuzzSnapshotRecord(t *testing.T, id string, revision uint64, record checkpoint.Record,
	codec checkpoint.JSONSerializer[json.RawMessage],
) {
	t.Helper()
	decoded, err := checkpoint.DecodeRecord[json.RawMessage, flowy.NoEffect](record, codec,
		checkpoint.DecodeRecordOptions{ExpectedThreadID: id, ExpectedRevision: revision})
	if err != nil || decoded.ThreadID != id || decoded.Revision != revision ||
		!bytes.Equal(decoded.State, record.StatePayload) {
		t.Fatalf("record roundtrip: %v", err)
	}
	_, err = checkpoint.DecodeRecord[json.RawMessage, flowy.NoEffect](record, codec,
		checkpoint.DecodeRecordOptions{ExpectedThreadID: id + "/other"})
	if !errors.Is(err, flowy.ErrSnapshotEnvelopeInvalid) {
		t.Fatalf("address mismatch: %v", err)
	}
}
