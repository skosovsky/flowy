package flowy

import (
	"errors"
	"testing"
)

func TestExecutionIntegrityCoversRawAggregateAndAddress(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*ExecutionEnvelope){
		"missing seal": func(e *ExecutionEnvelope) { e.Digest = "" },
		"identity":     func(e *ExecutionEnvelope) { e.ExecutionID = "other" },
		"revision":     func(e *ExecutionEnvelope) { e.Revision++ },
		"pointer":      func(e *ExecutionEnvelope) { e.Progress.ExecutionPointer = "other" },
		"state":        func(e *ExecutionEnvelope) { e.Progress.StatePayload = []byte("changed") },
		"effects":      func(e *ExecutionEnvelope) { e.EffectsPayload = []byte("changed") },
		"journal":      func(e *ExecutionEnvelope) { e.JournalPayload = []byte("changed") },
		"descriptor":   func(e *ExecutionEnvelope) { e.Descriptor.GraphRevision = "changed" },
		"activation":   func(e *ExecutionEnvelope) { e.Activation++ },
		"terminal":     func(e *ExecutionEnvelope) { e.Terminal = &ExecutionTerminal{Status: RunStatusCompleted} },
		"migration":    func(e *ExecutionEnvelope) { e.Migration = &MigrationProvenance{SourceRevision: 1} },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: revision is part of the seal, assigned before sealing.
			sealed, err := SealExecutionEnvelope(ExecutionEnvelope{
				ExecutionID: "run", Revision: 2, Descriptor: descriptorForTest("current"),
				Progress:       MigrationState{ExecutionPointer: "node", StatePayload: []byte("state")},
				EffectsPayload: []byte("effects"), JournalPayload: []byte("journal"), Activation: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			if integrityErr := ValidateExecutionIntegrity(sealed, "run", 2); integrityErr != nil {
				t.Fatal(integrityErr)
			}
			// Act.
			mutate(&sealed)
			err = ValidateExecutionIntegrity(sealed, "run", 2)
			// Assert.
			if !errors.Is(err, ErrExecutionCorrupt) {
				t.Fatalf("tampering accepted: %v", err)
			}
		})
	}
}

func TestExecutionSealIsStableButDoesNotAuthorizeDifferentAddress(t *testing.T) {
	t.Parallel()
	// Arrange.
	envelope := ExecutionEnvelope{ExecutionID: "run", Revision: 3, Progress: MigrationState{ExecutionPointer: "node"}}
	first, err := SealExecutionEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	// Act: an existing digest is excluded from the hash computation.
	second, err := SealExecutionEnvelope(first)
	// Assert: sealing does not imply descriptor compatibility.
	if err != nil || first.Digest != second.Digest {
		t.Fatalf("unstable seal: %v", err)
	}
	if !errors.Is(first.Descriptor.Validate(), ErrExecutionIncompatible) {
		t.Fatal("seal supplied compatibility")
	}
	for _, address := range []struct {
		id       string
		revision uint64
	}{{"other", 3}, {"run", 2}, {"run", 0}} {
		if !errors.Is(ValidateExecutionIntegrity(first, address.id, address.revision), ErrExecutionCorrupt) {
			t.Fatal("valid seal accepted under wrong address")
		}
	}
}
