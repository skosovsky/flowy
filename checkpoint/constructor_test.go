package checkpoint

import (
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
)

func TestSerializerNilCollaboratorRejectsWithoutPanic(t *testing.T) {
	t.Parallel()
	var typedNil *JSONSerializer[int]
	for _, base := range []flowy.StateSerializer[int]{nil, typedNil} {
		// Arrange: both plain and typed nil implement no usable codec.
		snapshot := flowy.Snapshot[int, flowy.NoEffect]{ThreadID: "run", Revision: 1, ExecutionPointer: "node"}
		record := Record{ThreadID: "run", Revision: 1, NodeID: "node"}
		// Act.
		codec, constructorErr := WithSanitizer(base, nil)
		_, encodeErr := EncodeRecord(snapshot, base)
		_, decodeErr := DecodeRecord[int, flowy.NoEffect](record, base, DecodeRecordOptions{})
		// Assert.
		if codec != nil || !errors.Is(constructorErr, flowy.ErrExecutionCapability) ||
			!errors.Is(encodeErr, flowy.ErrExecutionCapability) ||
			!errors.Is(decodeErr, flowy.ErrExecutionCapability) {
			t.Fatalf("codec=%v constructor=%v encode=%v decode=%v", codec, constructorErr, encodeErr, decodeErr)
		}
	}
}
