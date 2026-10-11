package checkpoint

import (
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
)

type admissionCodec struct{ marshals, unmarshals int }

func (c *admissionCodec) Marshal(_ int) ([]byte, error)   { c.marshals++; return []byte("42"), nil }
func (c *admissionCodec) Unmarshal(_ []byte) (int, error) { c.unmarshals++; return 42, nil }
func TestEncodeRecordRejectsMetadataBeforeCodec(t *testing.T) {
	t.Parallel()
	for name, snapshot := range map[string]flowy.Snapshot[int, flowy.NoEffect]{
		"empty thread":         {ThreadID: "", ExecutionPointer: "node", Revision: 1},
		"empty pointer":        {ThreadID: "run", ExecutionPointer: "", Revision: 1},
		"zero stored revision": {ThreadID: "run", ExecutionPointer: "node", Revision: 0},
		"invalid thread":       {ThreadID: "run\xff", ExecutionPointer: "node", Revision: 1},
		"invalid pointer":      {ThreadID: "run", ExecutionPointer: "node\xff", Revision: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			codec := &admissionCodec{marshals: 0, unmarshals: 0}
			// Act.
			_, err := EncodeRecord(snapshot, codec)
			// Assert.
			if !errors.Is(err, flowy.ErrSnapshotEnvelopeInvalid) || codec.marshals != 0 || codec.unmarshals != 0 {
				t.Fatalf("error=%v marshal=%d unmarshal=%d", err, codec.marshals, codec.unmarshals)
			}
		})
	}
}

type wireAdmissionCodec struct {
	admissionCodec

	payload []byte
}

func (c *wireAdmissionCodec) Marshal(int) ([]byte, error) {
	c.marshals++
	return c.payload, nil
}

func TestEncodeRecordRequiresJSONWireWithoutCallingHostDecoder(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		payload string
		valid   bool
	}{
		{payload: "", valid: false}, {payload: "opaque bytes", valid: false},
		{payload: "{", valid: false}, {payload: "42", valid: true},
		{payload: `{"state":"unicode 确认"}`, valid: true},
	} {
		t.Run(test.payload, func(t *testing.T) {
			t.Parallel()
			// Arrange: the decoder is host code and is not a metadata validator.
			codec := &wireAdmissionCodec{payload: []byte(test.payload)}
			snapshot := flowy.Snapshot[int, flowy.NoEffect]{ThreadID: "run", Revision: 1, ExecutionPointer: "node"}
			// Act.
			_, err := EncodeRecord(snapshot, codec)
			// Assert.
			if (test.valid && err != nil) || (!test.valid && !errors.Is(err, ErrInvalidRecord)) ||
				codec.marshals != 1 || codec.unmarshals != 0 {
				t.Fatalf("valid=%v error=%v marshal=%d unmarshal=%d", test.valid, err, codec.marshals, codec.unmarshals)
			}
		})
	}
}
