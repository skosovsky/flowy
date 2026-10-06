package flowy

import (
	"bytes"
	"errors"
	"testing"
)

func descriptorForTest(revision string) ExecutionDescriptor {
	return ExecutionDescriptor{
		GraphID:           "workflow",
		GraphRevision:     revision,
		StateCodec:        revision,
		ExecutionContract: "sync",
		ReplayPolicy:      StepReplayPolicy{Label: "test-safe-steps", Mode: StepReplaySafe},
	}
}

func TestExecutionMigrationDetachedAndPreservesOutcomes(t *testing.T) {
	// Arrange.
	source := ExecutionEnvelope{
		ExecutionID: "run",
		Revision:    7,
		Descriptor:  descriptorForTest("old"),
		Progress: MigrationState{
			StatePayload:     []byte("old"),
			ExecutionPointer: "before",
			ChildCursors:     map[string]ExecutionPointer{"child": "before"},
		},
		JournalPayload: []byte("completed-outcome"),
		EffectsPayload: []byte("effect"),
	}
	migration := ExecutionMigration{ID: "move", Source: source.Descriptor, Target: descriptorForTest("new"),
		Transform: func(state MigrationState) (MigrationState, error) {
			state.StatePayload[0] = 'N'
			state.ExecutionPointer = "after"
			state.ChildCursors["child"] = "after"
			return state, nil
		}}
	// Act.
	result, err := PrepareExecutionMigration(
		source,
		migration.Target,
		[]ExecutionMigration{migration},
		func(pointer ExecutionPointer) error {
			if pointer != "after" {
				return ErrResumeStartNodeNotFound
			}
			return nil
		},
	)
	// Assert.
	if err != nil {
		t.Fatal(err)
	}
	if string(source.Progress.StatePayload) != "old" || source.Progress.ChildCursors["child"] != "before" {
		t.Fatal("source mutated")
	}
	if !bytes.Equal(result.JournalPayload, source.JournalPayload) || result.Progress.ExecutionPointer != "after" {
		t.Fatal("outcomes or target changed")
	}
	if result.Migration == nil || result.Migration.SourceRevision != 7 || result.Migration.SourceDigest == "" ||
		result.Migration.Digest == "" {
		t.Fatal("missing lineage")
	}
	result.JournalPayload[0] = 'X'
	if string(source.JournalPayload) != "completed-outcome" {
		t.Fatal("journal aliases source")
	}
}

func TestMigrationRejectsMissingAmbiguousAndInvalidTarget(t *testing.T) {
	// Arrange.
	source := ExecutionEnvelope{
		ExecutionID: "run",
		Revision:    1,
		Descriptor:  descriptorForTest("old"),
		Progress:    MigrationState{ExecutionPointer: "node"},
	}
	transform := func(state MigrationState) (MigrationState, error) { return state, nil }
	migration := ExecutionMigration{
		ID:        "a",
		Source:    source.Descriptor,
		Target:    descriptorForTest("new"),
		Transform: transform,
	}
	for _, test := range []struct {
		name        string
		chain       []ExecutionMigration
		targetError error
		want        error
	}{
		{name: "missing", want: ErrMigrationMissing},
		{name: "ambiguous", chain: []ExecutionMigration{migration, migration}, want: ErrMigrationInvalid},
		{name: "invalid pointer", chain: []ExecutionMigration{migration}, targetError: ErrResumeStartNodeNotFound, want: ErrMigrationInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Act.
			_, err := PrepareExecutionMigration(
				source,
				migration.Target,
				test.chain,
				func(ExecutionPointer) error { return test.targetError },
			)
			// Assert.
			if !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}
		})
	}
}

func TestDescriptorRejectsMissingLabelsBeforeDecode(t *testing.T) {
	// Arrange.
	current := descriptorForTest("current")
	// Act.
	err := (ExecutionDescriptor{}).Check(current)
	// Assert.
	if !errors.Is(err, ErrExecutionIncompatible) {
		t.Fatalf("missing descriptor accepted: %v", err)
	}
}
