package flowy

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Parallel returns a Node that runs the named target nodes concurrently, then folds
// branch deltas into one state using merge. merge is called sequentially in target
// order on the goroutine that completes the parallel step (not concurrently).
//
// graph must point to the compiled graph before Invoke. Typical pattern:
//
//	var g *Graph[T]
//	b.AddNode("parallel", Parallel(&g, "parallel", merge, "a", "b"))
//	g, err = b.Compile()
//
// nodeName must be the same string used in AddNode for this Parallel node; it is used
// as the suspend resume cursor when a branch returns [ErrSuspend].
func Parallel[T any](graph **Graph[T], nodeName string, merge func(T, T) T, targets ...string) Node[T] {
	return func(ctx context.Context, state T) (T, error) {
		g := *graph
		if g == nil {
			var zero T
			return zero, errors.New("flowy: Parallel used before Compile — assign the result of Compile to *graph")
		}
		if merge == nil {
			var zero T
			return zero, errors.New("flowy: Parallel merge must not be nil")
		}
		return runParallelBranches(ctx, g, nodeName, merge, targets, state)
	}
}

// ParallelDynamic is like [Parallel] but the target node names are chosen at runtime via pick.
func ParallelDynamic[T any](
	graph **Graph[T],
	nodeName string,
	merge func(T, T) T,
	pick func(context.Context, T) ([]string, error),
) Node[T] {
	return func(ctx context.Context, state T) (T, error) {
		g := *graph
		if g == nil {
			var zero T
			return zero, errors.New(
				"flowy: ParallelDynamic used before Compile — assign the result of Compile to *graph",
			)
		}
		if merge == nil {
			var zero T
			return zero, errors.New("flowy: ParallelDynamic merge must not be nil")
		}
		if pick == nil {
			var zero T
			return zero, errors.New("flowy: ParallelDynamic pick must not be nil")
		}
		targets, err := pick(ctx, state)
		if err != nil {
			return state, err
		}
		return runParallelBranches(ctx, g, nodeName, merge, targets, state)
	}
}

// validateParallelTargets returns an error if any target name is not a registered node.
func validateParallelTargets[T any](g *Graph[T], targets []string) error {
	for _, targetName := range targets {
		if _, ok := g.nodes[targetName]; !ok {
			return fmt.Errorf("flowy: parallel target %q is not a registered node", targetName)
		}
	}
	return nil
}

// shouldIgnoreParallelBranchError reports whether a branch error was caused by cancellation
// from another branch (do not record as firstErr).
func shouldIgnoreParallelBranchError(runCtx context.Context, err error) bool {
	cause := context.Cause(runCtx)
	return errors.Is(err, context.Canceled) && cause != nil && !errors.Is(cause, context.Canceled)
}

// tryAcquireParallelSlot acquires a slot in the semaphore, or returns false if runCtx is done.
func tryAcquireParallelSlot(runCtx context.Context, sem chan struct{}) bool {
	if sem == nil {
		return true
	}
	select {
	case sem <- struct{}{}:
		return true
	case <-runCtx.Done():
		return false
	}
}

func releaseParallelSlot(sem chan struct{}) {
	if sem != nil {
		<-sem
	}
}

// returnParallelError maps the first branch error to the parallel node result (suspend snapshot or wrap).
func returnParallelError[T any](firstErr error, state T, parallelNodeName string) (T, error) {
	if errors.Is(firstErr, ErrSuspend) {
		snap, _, _ := unwrapSuspend(firstErr, state, parallelNodeName)
		return snap, firstErr
	}
	return state, fmt.Errorf("flowy: parallel node %q: %w", parallelNodeName, firstErr)
}

func foldParallelResults[T any](acc T, results []T, merge func(T, T) T) T {
	for _, delta := range results {
		acc = merge(acc, delta)
	}
	return acc
}

// runOneParallelBranch executes a single target; branches use parallelNodeName as suspend target.
func runOneParallelBranch[T any](
	runCtx context.Context,
	g *Graph[T],
	parallelNodeName, targetName string,
	state T,
	cfg *runConfig,
	sem chan struct{},
	recordErr func(error),
	slot int,
	results []T,
) {
	if sem != nil {
		if !tryAcquireParallelSlot(runCtx, sem) {
			return
		}
		defer releaseParallelSlot(sem)
	}

	delta, err := g.executeNode(runCtx, state, targetName, parallelNodeName, cfg)
	if err != nil {
		if shouldIgnoreParallelBranchError(runCtx, err) {
			return
		}
		recordErr(err)
		return
	}
	results[slot] = delta
}

// runParallelBranches executes targets in parallel, folds with merge, handles ErrSuspend.
func runParallelBranches[T any](
	ctx context.Context,
	g *Graph[T],
	parallelNodeName string,
	merge func(T, T) T,
	targets []string,
	state T,
) (T, error) {
	if len(targets) == 0 {
		return state, nil
	}
	if err := validateParallelTargets(g, targets); err != nil {
		return state, err
	}

	results := make([]T, len(targets))
	cfg := &g.defaults

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var (
		wg       sync.WaitGroup
		firstErr error
		errMu    sync.Mutex
		sem      chan struct{}
	)
	if cfg.maxConcurrency > 0 {
		sem = make(chan struct{}, cfg.maxConcurrency)
	}

	recordErr := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr != nil {
			return
		}
		firstErr = err
		cancel(err)
	}

	for i, targetName := range targets {
		wg.Go(func() {
			runOneParallelBranch(runCtx, g, parallelNodeName, targetName, state, cfg, sem, recordErr, i, results)
		})
	}

	wg.Wait()
	if firstErr != nil {
		return returnParallelError(firstErr, state, parallelNodeName)
	}

	return foldParallelResults(state, results, merge), nil
}
