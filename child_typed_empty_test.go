package flowy

import (
	"errors"
	"testing"
)

type emptyChildCodec struct{}

func (emptyChildCodec) Marshal(string) ([]byte, error) { return nil, nil }

func (emptyChildCodec) Unmarshal(payload []byte) (string, error) {
	if len(payload) != 0 {
		return "", errors.New("unexpected nonempty payload")
	}
	return "decoded-empty-format", nil
}

func TestTypedCompletedChildDecodesEmptyCodecFormat(t *testing.T) {
	// Arrange: completed is an outcome, even if its host encoding has zero bytes.
	group := confirmedChildFixture(t)
	group.Children[0].State = ChildCompleted
	group.Children[0].CancelConfirmed = false
	group.Children[0].CancelConfirmation = nil
	// Act.
	outcomes, err := DecodeChildOutcomes(group, emptyChildCodec{})
	// Assert: do not replace host decoding with an invented zero result.
	if err != nil || len(outcomes) != 1 || !outcomes[0].HasResult || outcomes[0].Result != "decoded-empty-format" {
		t.Fatalf("empty-format result lost: %v outcomes=%+v", err, outcomes)
	}
}
