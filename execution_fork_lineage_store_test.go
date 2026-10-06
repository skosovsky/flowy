package flowy_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

func TestExecutionStoreRejectsForkLineageRewriteUnderValidFence(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*flowy.ExecutionEnvelope){
		"erase": func(e *flowy.ExecutionEnvelope) { e.Fork = nil },
		"live mode": func(e *flowy.ExecutionEnvelope) {
			e.Fork.Mode = flowy.ForkLive
			e.Fork.ProjectionLabel = "invented-projection"
		},
		"source digest": func(e *flowy.ExecutionEnvelope) { e.Fork.Source.Digest = strings.Repeat("0", 64) },
		"transform":     func(e *flowy.ExecutionEnvelope) { e.Fork.TransformLabel = "substituted" },
		"created time":  func(e *flowy.ExecutionEnvelope) { e.Fork.CreatedAt = e.Fork.CreatedAt.Add(time.Hour) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange: writer has a correct live fence/revision, not permission to rewrite origin.
			ctx := context.Background()
			store := testutil.NewMemoryExecutionStore(nil)
			source := seedForkSource(t, store)
			var nodes, live atomic.Int32
			_, err := forkRunnerForTest(t, store, nil, &nodes, &live).Fork(ctx, forkRequestForTest(source, "target"))
			if err != nil {
				t.Fatal(err)
			}
			before, err := store.LoadExecution(ctx, "target")
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := store.LoadExecution(ctx, "target")
			if err != nil {
				t.Fatal(err)
			}
			lease, err := store.AcquireExecution(ctx, "target", "writer", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&candidate)
			// Act: store would normally assign a revision and recompute a valid seal.
			_, commitErr := store.CommitExecution(ctx, before.Revision, lease, candidate)
			after, loadErr := store.LoadExecution(ctx, "target")
			// Assert: immutable creation metadata survives even an otherwise valid write.
			if !errors.Is(commitErr, flowy.ErrExecutionCorrupt) || loadErr != nil || after.Digest != before.Digest ||
				after.Revision != before.Revision {
				t.Fatalf("lineage rewrite accepted: commit=%v after=%+v/%v", commitErr, after, loadErr)
			}
			if err = store.ReleaseExecution(ctx, lease); err != nil {
				t.Fatal(err)
			}
		})
	}
}
