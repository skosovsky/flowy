package flowy

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestEffectsMigrationRequiresExplicitTransformAndPreservesSource(t *testing.T) {
	// Arrange: state and effects independently change representation.
	source := ExecutionEnvelope{
		ExecutionID:     "run",
		Revision:        7,
		Descriptor:      descriptorForTest("old"),
		Progress:        ExecutionProgress{StatePayload: []byte("old-state"), ExecutionPointer: "node"},
		EffectsPayload:  []byte("old-effect"),
		JournalPayload:  []byte("immutable-activity"),
		ChildrenPayload: []byte("immutable-child"),
	}
	source = sealMigrationFixture(t, source)
	target := descriptorForTest("new")
	target.EffectsCodec = "effects-v2"
	stateCalls, effectCalls := 0, 0
	migration := ExecutionMigration{
		ID:     "both",
		Source: source.Descriptor,
		Target: target,
		Transform: func(s ExecutionProgress) (ExecutionProgress, error) {
			stateCalls++
			s.StatePayload = []byte("new-state")
			return s, nil
		},
	}
	// Act: a missing required path rejects before even the state callback.
	_, missingErr := PrepareExecutionMigration(
		source,
		target,
		[]ExecutionMigration{migration},
		func(ExecutionPointer) error { return nil },
	)
	if !errors.Is(missingErr, ErrMigrationInvalid) || stateCalls != 0 {
		t.Fatalf("err=%v calls=%d", missingErr, stateCalls)
	}
	migration.EffectsTransform = func(payload []byte) ([]byte, error) { effectCalls++; payload[0] = 'N'; return payload, nil }
	migrated, err := PrepareExecutionMigration(
		source,
		target,
		[]ExecutionMigration{migration},
		func(ExecutionPointer) error { return nil },
	)
	// Assert: effects input/output is detached, outcomes remain immutable and source unchanged.
	if err != nil || stateCalls != 1 || effectCalls != 1 || string(migrated.EffectsPayload) != "Nld-effect" ||
		string(source.EffectsPayload) != "old-effect" ||
		!bytes.Equal(migrated.JournalPayload, source.JournalPayload) ||
		!bytes.Equal(migrated.ChildrenPayload, source.ChildrenPayload) ||
		string(source.Progress.StatePayload) != "old-state" {
		t.Fatalf("err=%v source=%+v result=%+v", err, source, migrated)
	}
	migrated.EffectsPayload[0] = 'X'
	if string(source.EffectsPayload) != "old-effect" {
		t.Fatal("effects alias source")
	}
}

func TestEffectsMigrationFailureAndMissingLabelReject(t *testing.T) {
	// Arrange.
	source := ExecutionEnvelope{
		ExecutionID:    "run",
		Revision:       1,
		Descriptor:     descriptorForTest("old"),
		Progress:       ExecutionProgress{ExecutionPointer: "node"},
		EffectsPayload: []byte("old"),
	}
	source = sealMigrationFixture(t, source)
	target := descriptorForTest("new")
	target.EffectsCodec = "effects-v2"
	failure := errors.New("projection rejected")
	migration := ExecutionMigration{
		ID:               "effects",
		Source:           source.Descriptor,
		Target:           target,
		Transform:        func(s ExecutionProgress) (ExecutionProgress, error) { return s, nil },
		EffectsTransform: func([]byte) ([]byte, error) { return nil, failure },
	}
	// Act.
	_, err := PrepareExecutionMigration(
		source,
		target,
		[]ExecutionMigration{migration},
		func(ExecutionPointer) error { return nil },
	)
	unlabeled := source.Descriptor
	unlabeled.EffectsCodec = ""
	// Assert.
	if !errors.Is(err, ErrMigrationInvalid) || !errors.Is(err, failure) || string(source.EffectsPayload) != "old" ||
		!errors.Is(unlabeled.Validate(), ErrExecutionIncompatible) {
		t.Fatalf("err=%v source=%+v", err, source)
	}
}

func TestRolloverCreationSealIsProtectedByIndependentReceipt(t *testing.T) {
	// Arrange: matching immutable lineage does not authorize changing initial bytes.
	source := HistoricalCheckpointReference{
		ExecutionID: "source",
		Revision:    2,
		Digest:      string(bytes.Repeat([]byte("a"), 64)),
	}
	descriptor := descriptorForTest("current")
	policy := RolloverPolicy{Label: "bounded", MaxRecords: 2, MaxAggregateBytes: 65536, MaxTargetBytes: 16384}
	lineage := RolloverLineage{
		Source:           source,
		SourceDescriptor: descriptor,
		TargetDescriptor: descriptor,
		TargetID:         "target",
		DecisionID:       "move",
		Policy:           policy,
		ProjectionLabel:  "projection",
		CreatedAt:        time.Now().UTC(),
	}
	lineage.RequestDigest = rolloverRequestDigest(
		source,
		descriptor,
		descriptor,
		"target",
		"move",
		policy,
		"projection",
	)
	initial, err := SealExecutionEnvelope(
		ExecutionEnvelope{
			ExecutionID: "target",
			Revision:    1,
			Descriptor:  descriptor,
			Progress:    ExecutionProgress{ExecutionPointer: "node", StatePayload: []byte("old")},
			Rollover:    &lineage,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	receipt := RolloverReceipt{
		Lineage:        lineage,
		Target:         HistoricalCheckpointReference{ExecutionID: "target", Revision: 1, Digest: initial.Digest},
		SourceRevision: 3,
	}
	if err = ValidateExecutionLifecycleAnchors(initial, &receipt, nil); err != nil {
		t.Fatal(err)
	}
	// Act.
	initial.Progress.StatePayload = []byte("forged")
	forged, err := SealExecutionEnvelope(initial)
	if err != nil {
		t.Fatal(err)
	}
	// Assert: a recomputed valid envelope seal cannot replace the creation receipt.
	if err = ValidateExecutionLifecycleAnchors(forged, &receipt, nil); !errors.Is(err, ErrExecutionCorrupt) {
		t.Fatalf("forged creation accepted: %v", err)
	}
}
