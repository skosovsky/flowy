package flowy

import (
	"bytes"
	"errors"
	"testing"
	"unicode/utf8"
)

func FuzzExecutionEnvelopeIntegrity(f *testing.F) {
	f.Add("run", uint64(1), []byte("opaque\x00payload"))
	f.Add("", uint64(0), []byte{})
	f.Add(string([]byte{0xff}), uint64(1), []byte("invalid identity"))
	f.Fuzz(func(t *testing.T, id string, revision uint64, payload []byte) {
		if len(id) > 1024 || len(payload) > 8192 {
			t.Skip()
		}
		// Arrange: arbitrary identity and opaque bytes at an exact address.
		source := ExecutionEnvelope{ExecutionID: id, Revision: revision,
			Progress: ExecutionProgress{ExecutionPointer: "node", StatePayload: bytes.Clone(payload)}}
		// Act.
		sealed, err := SealExecutionEnvelope(source)
		// Assert: rejected headers never become seals; valid bytes remain lossless.
		if id == "" || !utf8.ValidString(id) || revision == 0 {
			if !errors.Is(err, ErrExecutionCorrupt) {
				t.Fatalf("invalid header sealed: %v", err)
			}
			return
		}
		if err != nil || ValidateExecutionIntegrity(sealed, id, revision) != nil ||
			!bytes.Equal(sealed.Progress.StatePayload, payload) {
			t.Fatalf("seal roundtrip: %v", err)
		}
		tampered := cloneExecutionEnvelope(sealed)
		tampered.Progress.StatePayload = append(tampered.Progress.StatePayload, 0)
		if !errors.Is(ValidateExecutionIntegrity(tampered, id, revision), ErrExecutionCorrupt) {
			t.Fatal("changed payload retained authority under old seal")
		}
		if !errors.Is(ValidateExecutionIntegrity(sealed, id+"/other", revision), ErrExecutionCorrupt) {
			t.Fatal("seal accepted at different address")
		}
	})
}
