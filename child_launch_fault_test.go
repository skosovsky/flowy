package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildLaunchCommitFaultCannotDuplicateDispatch(t *testing.T) {
	t.Parallel()
	for _, failAt := range []int32{2, 3, 4, 5} {
		t.Run(map[int32]string{2: "intent", 3: "queued", 4: "running", 5: "outcome"}[failAt], func(t *testing.T) {
			t.Parallel()
			// Arrange.
			ctx := context.Background()
			base := testutil.NewMemoryExecutionStore(nil)
			store := &faultExecutionStore{ExecutionStore: base, failAt: failAt}
			var calls atomic.Int32
			dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				calls.Add(1)
				return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("done")}, nil
			}
			// Act.
			_, failedErr := childLaunchRunner(
				t,
				store,
				persistedChildPlan(),
				dispatch,
			).Start(ctx, "run", durableTestState{})
			latest, err := base.LoadExecution(ctx, "run")
			if err != nil {
				t.Fatal(err)
			}
			if !errors.Is(failedErr, errInjectedCommit) || latest.Terminal != nil {
				t.Fatalf("fault published terminal: %v %+v", failedErr, latest)
			}
			_, resumeErr := childLaunchRunner(t, base, persistedChildPlan(), dispatch).Resume(ctx,
				flowy.ResumeToken{ThreadID: "run", SnapshotRevision: latest.Revision})
			// Assert: predispatch faults may launch once; an abandoned running child is unknown, never relaunched.
			assertChildLaunchFault(ctx, t, base, failAt, resumeErr, calls.Load())
		})
	}
}

func assertChildLaunchFault(
	ctx context.Context,
	t *testing.T,
	store flowy.ExecutionStore,
	failAt int32,
	resumeErr error,
	calls int32,
) {
	t.Helper()
	if !errors.Is(resumeErr, flowy.ErrChildrenUnresolved) || calls != 1 {
		t.Fatalf("duplicate/absent dispatch: %v calls=%d", resumeErr, calls)
	}
	latest, err := store.LoadExecution(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(latest.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	want := flowy.ChildCompleted
	if failAt == 5 {
		want = flowy.ChildUnknown
	}
	for _, group := range groups {
		if group.Children[0].State != want || group.Children[0].Revision != 3 || latest.Terminal != nil {
			t.Fatalf("fault lost recovery state: %+v", group.Children[0])
		}
	}
}
