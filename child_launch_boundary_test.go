package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildInvalidOutcomeAndPanicCannotRelaunch(t *testing.T) {
	t.Parallel()
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "invalid running outcome", true: "dispatcher panic"}[panics], func(t *testing.T) {
			t.Parallel()
			// Arrange.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			var calls atomic.Int32
			dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				calls.Add(1)
				if panics {
					panic("host worker panic")
				}
				return flowy.ChildResult{State: flowy.ChildRunning}, nil
			}
			runner := childLaunchRunner(t, store, persistedChildPlan(), dispatch)
			// Act.
			_, firstErr := runner.Start(ctx, "run", durableTestState{})
			want := flowy.ErrChildJoinInvalid
			if panics {
				want = flowy.ErrChildrenUnresolved
			}
			if !errors.Is(firstErr, want) {
				t.Fatalf("invalid dispatcher accepted: %v", firstErr)
			}
			latest, err := store.LoadExecution(ctx, "run")
			if err != nil {
				t.Fatal(err)
			}
			_, resumeErr := runner.Resume(ctx, flowy.ResumeToken{ThreadID: "run", SnapshotRevision: latest.Revision})
			// Assert.
			assertChildLaunchFault(ctx, t, store, 5, resumeErr, calls.Load())
		})
	}
}
