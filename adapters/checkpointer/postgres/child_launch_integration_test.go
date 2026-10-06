//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
)

func TestChildCompletedLaunchPersistentReplayWithoutDispatch(t *testing.T) {
	// Arrange: complete a child but deliberately leave the explicit parent join pending.
	ctx, pool := racePool(t)
	if _, err := pool.Exec(ctx, ExecutionSchemaSQL()); err != nil {
		t.Fatal(err)
	}
	id := testThreadID(t)
	plan := flowy.ChildGroupPlan{
		Key:            "group",
		Label:          "isolated",
		MergeLabel:     "ordered",
		BudgetLabel:    "fixed",
		CancelLabel:    "confirmed",
		MaxConcurrency: 2,
		FailurePolicy:  flowy.ChildCollectErrors,
		Children:       []flowy.ChildSpec{{ID: "child", Input: []byte("input")}},
	}
	var calls atomic.Int32
	dispatch := func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
		calls.Add(1)
		return flowy.ChildResult{State: flowy.ChildCompleted, Payload: []byte("done")}, nil
	}
	store := mustExecutionStore(t, pool)
	first, err := postgresChildLaunchRunner(t, store, plan, dispatch).Start(ctx, id, intState{})
	if !errors.Is(err, flowy.ErrChildrenUnresolved) || first == nil {
		t.Fatalf("parent bypassed join: %v", err)
	}
	source, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	restartCtx, restartPool := racePool(t)
	restartedStore := mustExecutionStore(t, restartPool)
	// Act.
	_, replayErr := postgresChildLaunchRunner(t, restartedStore, plan, dispatch).Resume(restartCtx, first.ResumeToken)
	latest, err := restartedStore.LoadExecution(restartCtx, id)
	// Assert: committed outcome survives a fresh pool with no new dispatch/revision.
	if err != nil || !errors.Is(replayErr, flowy.ErrChildrenUnresolved) || calls.Load() != 1 ||
		latest.Revision != source.Revision ||
		latest.Digest != source.Digest {
		t.Fatalf("child redispatched after restart: %v calls=%d load=%v", replayErr, calls.Load(), err)
	}
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(latest.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups {
		child := group.Children[0]
		if child.State != flowy.ChildCompleted || child.Revision != 3 || string(child.Result) != "done" {
			t.Fatalf("persistent child outcome lost: %+v", child)
		}
	}
}
