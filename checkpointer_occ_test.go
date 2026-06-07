package flowy

import (
	"context"
	"errors"
	"sync"
	"testing"
)

//nolint:gocognit // concurrent OCC retry loop is the test subject
func TestMemoryCPConcurrentSaveOCC(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	cp := newMemoryCP[state, NoEffect]()
	const workers = 20

	var wg sync.WaitGroup
	successes := make(chan uint64, workers)
	for range workers {
		wg.Go(func() {
			for {
				_, rev, err := cp.Load(context.Background(), "occ-th")
				if err != nil && !errors.Is(err, ErrThreadNotFound) {
					t.Errorf("load: %v", err)
					return
				}
				if errors.Is(err, ErrThreadNotFound) {
					rev = 0
				}
				newRev, err := cp.Save(context.Background(), rev, Snapshot[state, NoEffect]{
					ThreadID:         "occ-th",
					ExecutionPointer: "work",
					State:            state{N: int(rev) + 1},
				})
				if errors.Is(err, ErrConcurrencyConflict) {
					continue
				}
				if err != nil {
					t.Errorf("save: %v", err)
					return
				}
				successes <- newRev
				return
			}
		})
	}
	wg.Wait()
	close(successes)

	var count int
	var maxRev uint64
	for rev := range successes {
		count++
		if rev > maxRev {
			maxRev = rev
		}
	}
	if count != workers {
		t.Fatalf("expected %d successful saves, got %d", workers, count)
	}
	if maxRev != workers {
		t.Fatalf("expected max revision %d, got %d", workers, maxRev)
	}
}

func TestMemoryCPOCCConflictOnStaleExpectedRevision(t *testing.T) {
	t.Parallel()

	type state struct{}
	cp := newMemoryCP[state, NoEffect]()
	if _, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "stale-expected",
		ExecutionPointer: "n",
		State:            state{},
	}); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	_, err := cp.Save(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "stale-expected",
		ExecutionPointer: "n",
		State:            state{},
	})
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
}

func TestResumeOCCWorkerRetryAfterConflict(t *testing.T) {
	t.Parallel()

	type state struct{ N int }
	cp := newMemoryCP[state, NoEffect]()

	b := NewGraph[state, NoEffect](func(_ state, u state) state { return u })
	b.AddNode("step", func(_ context.Context, s state) (state, Directive, error) {
		s.N++
		if s.N < 3 {
			return s, Suspend("more"), nil
		}
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("step")
	b.SetEntryPoint("step")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	runner := g.NewRunner(cp)
	first, err := runner.Start(context.Background(), "retry-th", state{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	staleToken := first.ResumeToken

	if _, resumeErr := runner.Resume(context.Background(), staleToken); resumeErr != nil {
		t.Fatalf("first resume: %v", resumeErr)
	}

	resumed, err := resumeAfterOCCConflict(t, runner, cp, staleToken)
	if err != nil {
		t.Fatalf("resume after reload: %v", err)
	}
	if resumed.Status != RunStatusCompleted {
		t.Fatalf("expected completed, got %s", resumed.Status)
	}
}
