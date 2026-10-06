package flowy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestChildConcurrentJoinHasOneCommittedWinner(t *testing.T) {
	// Arrange: two pure merges start against the same aggregate revision.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	runner := childJoinRunner(
		t,
		store,
		func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
			group, err := flowy.RunChildren(
				ctx,
				persistedChildPlan(),
				nil,
				func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
					return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("child")}, nil
				},
			)
			if err != nil {
				return state, flowy.End(), err
			}
			started := make(chan struct{}, 2)
			gate := make(chan struct{})
			finished := make(chan error, 2)
			for range 2 {
				go func() {
					_, joinErr := flowy.JoinChildren(
						ctx,
						group,
						func(context.Context, []flowy.ChildRecord) ([]byte, error) {
							started <- struct{}{}
							<-gate
							return []byte("merged"), nil
						},
					)
					finished <- joinErr
				}()
			}
			<-started
			<-started
			close(gate)
			first, second := <-finished, <-finished
			if first == nil && errors.Is(second, flowy.ErrChildMergeConflict) ||
				second == nil && errors.Is(first, flowy.ErrChildMergeConflict) {
				return state, flowy.End(), nil
			}
			return state, flowy.End(), errors.New("concurrent joins did not produce one winner")
		},
	)
	// Act.
	_, err := runner.Start(ctx, "concurrent", durableTestState{})
	latest, loadErr := store.LoadExecution(ctx, "concurrent")
	// Assert: a single join and terminal publication, with no overwritten outcome.
	if err != nil || loadErr != nil || latest.Terminal == nil || latest.Revision != 7 {
		t.Fatalf("concurrent join: %v load=%v revision=%d", err, loadErr, latest.Revision)
	}
}
