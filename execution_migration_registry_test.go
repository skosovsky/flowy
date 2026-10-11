package flowy

import (
	"errors"
	"testing"
)

func TestMigrationRegistryRejectsDisconnectedAmbiguityAndReachableCycle(t *testing.T) {
	t.Parallel()
	for _, cycle := range []bool{false, true} {
		t.Run(map[bool]string{false: "disconnected-ambiguous", true: "reachable-cycle"}[cycle], func(t *testing.T) {
			t.Parallel()
			// Arrange: a valid source and transformations that must never run on bad topology.
			source := sealMigrationFixture(t, ExecutionEnvelope{ExecutionID: "run", Revision: 1,
				Descriptor: descriptorForTest("old"), Progress: ExecutionProgress{ExecutionPointer: "node"}})
			target := descriptorForTest("target")
			calls := 0
			transform := func(state ExecutionProgress) (ExecutionProgress, error) { calls++; return state, nil }
			first := ExecutionMigration{ID: "first", Source: source.Descriptor, Target: target, Transform: transform}
			second := ExecutionMigration{ID: "second", Source: descriptorForTest("other"),
				Target: descriptorForTest("branch"), Transform: transform}
			third := ExecutionMigration{ID: "third", Source: second.Source,
				Target: descriptorForTest("other-branch"), Transform: transform}
			if cycle {
				first.Target = second.Source
				second.Target = source.Descriptor
				third.Source = descriptorForTest("unrelated")
			}
			// Act.
			_, err := PrepareExecutionMigration(source, target, []ExecutionMigration{first, second, third},
				func(ExecutionPointer) error { calls++; return nil })
			// Assert: no chosen path or callback can bypass global edge uniqueness or a cycle.
			if !errors.Is(err, ErrMigrationInvalid) || calls != 0 {
				t.Fatalf("bad registry reached callbacks: calls=%d err=%v", calls, err)
			}
		})
	}
}
