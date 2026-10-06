package flowy_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

func TestForkFreezesProjectionRegistrationBeforeTransform(t *testing.T) {
	t.Parallel()
	// Arrange: even an adversarial callback cannot replace the approved registration.
	ctx := context.Background()
	store := testutil.NewMemoryExecutionStore(nil)
	source := seedForkSource(t, store)
	var nodes, live atomic.Int32
	runner := forkRunnerForTest(t, store, nil, &nodes, &live)
	projection := &flowy.ForkProjection{
		Label: "sanitize",
		Project: func(state flowy.MigrationState) (flowy.MigrationState, error) {
			state.StatePayload = []byte(`{"Value":10,"Approved":false}`)
			return state, nil
		},
	}
	request := forkRequestForTest(source, "target")
	request.Projection = projection
	request.Transform.Transform = func(state flowy.MigrationState) (flowy.MigrationState, error) {
		projection.Label = "substituted"
		projection.Project = func(state flowy.MigrationState) (flowy.MigrationState, error) {
			state.StatePayload = []byte(`{"Value":99,"Approved":true}`)
			return state, nil
		}
		return state, nil
	}
	// Act.
	_, err := runner.Fork(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	head, err := store.LoadExecution(ctx, "target")
	if err != nil {
		t.Fatal(err)
	}
	state, err := (checkpoint.JSONSerializer[forkTestState]{}).Unmarshal(head.Progress.StatePayload)
	if err != nil {
		t.Fatal(err)
	}
	// Assert: frozen label and callback agree; external registration changed only itself.
	if head.Fork == nil || head.Fork.ProjectionLabel != "sanitize" || state.Value != 10 || state.Approved ||
		projection.Label != "substituted" || nodes.Load() != 0 || live.Load() != 0 {
		t.Fatalf(
			"projection substituted after validation: state=%+v lineage=%+v external=%s",
			state,
			head.Fork,
			projection.Label,
		)
	}
}
