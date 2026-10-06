package testutil

import (
	"context"
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
)

func TestMemorySnapshotAdmissionPreservesHealthyHistory(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*flowy.Snapshot[int, flowy.NoEffect]){
		"empty thread":    func(s *flowy.Snapshot[int, flowy.NoEffect]) { s.ThreadID = "" },
		"empty pointer":   func(s *flowy.Snapshot[int, flowy.NoEffect]) { s.ExecutionPointer = "" },
		"invalid thread":  func(s *flowy.Snapshot[int, flowy.NoEffect]) { s.ThreadID = "run\xff" },
		"invalid pointer": func(s *flowy.Snapshot[int, flowy.NoEffect]) { s.ExecutionPointer = "node\xff" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: cloners are host work and must not run for a rejected envelope.
			clones := 0
			cp := NewMemoryCheckpointerWithCloners[int, flowy.NoEffect](
				func(value int) int { clones++; return value },
				nil,
			)
			snapshot := flowy.Snapshot[int, flowy.NoEffect]{ThreadID: "run", ExecutionPointer: "node", State: 42}
			if _, err := cp.Save(context.Background(), 0, snapshot); err != nil {
				t.Fatal(err)
			}
			mutate(&snapshot)
			// Act.
			_, err := cp.Save(context.Background(), 1, snapshot)
			// Assert.
			if !errors.Is(err, flowy.ErrSnapshotEnvelopeInvalid) || clones != 1 {
				t.Fatalf("err=%v clones=%d", err, clones)
			}
			healthy, revision, loadErr := cp.Load(context.Background(), "run")
			history, historyErr := cp.GetHistory(context.Background(), "run", 0)
			if loadErr != nil || historyErr != nil || revision != 1 || healthy.State != 42 || len(history) != 1 {
				t.Fatalf(
					"healthy=%+v revision=%d history=%v load=%v historyErr=%v",
					healthy,
					revision,
					history,
					loadErr,
					historyErr,
				)
			}
		})
	}
}
