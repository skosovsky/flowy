package flowy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Graph is the compiled, immutable graph.
type Graph[T, E any] struct {
	nodes              map[string]nodeDef[T, E]
	edges              map[string]string
	conditionalEdges   map[string]EdgeRouter[T]
	conditionalAllowed map[string]map[string]struct{}
	retryRoutes        map[string]string
	entryPoint         string
	reducer            Reducer[T]
	defaults           runConfig
}

type graphRunner[T, E any] struct {
	graph             *Graph[T, E]
	checkpointer      Checkpointer[T, E]
	interceptors      []StateInterceptor[T]
	leaseManager      LeaseManager
	handoffOutbox     HandoffOutbox
	handoffStaleAfter time.Duration
	logger            *slog.Logger
	sessions          sync.Map // threadID -> *runSession
	durable           *executionCheckpointer[T, E]
}

type eventSink[T, E any] func(ctx context.Context, event RunEvent[T, E]) bool

type streamCloseKey struct{}

const (
	resumeReasonInvalidPointer       = "invalid_pointer"
	resumeReasonInvalidHandoffStatus = "invalid_handoff_status"
)

type streamHandle[T, E any] struct {
	events chan RunEvent[T, E]
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
	err    error
	result *RunResult[T, E]
	onStop func()
}

// NewRunner binds a compiled graph to persistence and returns a lifecycle runner.
func (g *Graph[T, E]) NewRunner(
	checkpointer Checkpointer[T, E],
	interceptors ...StateInterceptor[T],
) Runner[T, E] {
	return g.NewRunnerWithOptions(checkpointer, nil, interceptors...)
}

// NewRunnerWithOptions creates a runner with lease manager and other runner-level options.
func (g *Graph[T, E]) NewRunnerWithOptions(
	checkpointer Checkpointer[T, E],
	runnerOpts []RunnerOption[T, E],
	interceptors ...StateInterceptor[T],
) Runner[T, E] {
	r := &graphRunner[T, E]{ //nolint:exhaustruct_v5 // handoffOutbox and durable default nil until configured
		graph:             g,
		checkpointer:      checkpointer,
		interceptors:      append([]StateInterceptor[T](nil), interceptors...),
		leaseManager:      nil,
		handoffStaleAfter: DefaultHandoffStaleAfter,
		logger:            slog.Default(),
		sessions:          sync.Map{},
	}
	for _, opt := range runnerOpts {
		if opt != nil {
			opt(r)
		}
	}
	if r.leaseManager != nil && r.checkpointer != nil {
		type leaseGuarded interface{ isLeaseGuardCheckpointer() }
		if _, guarded := r.checkpointer.(leaseGuarded); !guarded {
			if _, native := r.checkpointer.(NativeDeleteIfIdleCheckpointer); !native {
				r.checkpointer = NewAdvisoryLeaseGuardCheckpointer(r.checkpointer, r.leaseManager)
			}
		}
	}
	return r
}

func (r *graphRunner[T, E]) Start(
	ctx context.Context,
	threadID string,
	initialState T,
	opts ...RunOption[T, E],
) (*RunResult[T, E], error) {
	if threadID == "" {
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv, optErr := r.resolveRunOptions(opts...)
	if optErr != nil {
		return nil, optErr
	}
	if leaseErr := r.acquireLease(ctx, threadID, &inv); leaseErr != nil {
		return nil, leaseErr
	}

	meta := newRunMetadata()
	mergeRunMetadataInput(&meta, inv.runMetadata)
	runCtx := r.attachInvocation(ctx, inv, &meta)
	result, err := r.execute(
		runCtx,
		threadID,
		r.graph.entryPoint,
		initialState,
		meta,
		nil,
		0,
		nil,
		inv,
	)
	return result, errors.Join(err, r.postRunCleanup(ctx, threadID, inv, result))
}

func (r *graphRunner[T, E]) Resume(
	ctx context.Context,
	token ResumeToken,
	opts ...RunOption[T, E],
) (*RunResult[T, E], error) {
	if r.checkpointer == nil {
		return nil, errors.New("flowy: checkpointer is required for Resume")
	}
	if token.ThreadID == "" {
		emitResumeRejected(ctx, token.ThreadID, "", "empty_token")
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv, optErr := r.resolveRunOptions(opts...)
	if optErr != nil {
		return nil, optErr
	}
	if leaseErr := r.acquireLease(ctx, token.ThreadID, &inv); leaseErr != nil {
		return nil, leaseErr
	}

	decision, err := r.evaluateResume(ctx, token, inv)
	if err != nil {
		return nil, errors.Join(err, r.postRunCleanup(ctx, token.ThreadID, inv, nil))
	}
	runCtx := injectTelemetryContext(r.attachInvocation(ctx, inv, &decision.RunMeta), decision.RunMeta.TelemetryContext)
	result, runErr := r.execute(
		runCtx,
		token.ThreadID,
		string(decision.ExecutionPointer),
		decision.State,
		decision.RunMeta,
		decision.Effects,
		decision.SnapshotRevision,
		nil,
		inv,
	)
	return result, errors.Join(runErr, r.postRunCleanup(ctx, token.ThreadID, inv, result))
}

func (r *graphRunner[T, E]) Stream(
	ctx context.Context,
	threadID string,
	initialState T,
	opts ...RunOption[T, E],
) (StreamHandle[T, E], error) {
	if threadID == "" {
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv, optErr := r.resolveRunOptions(opts...)
	if optErr != nil {
		return nil, optErr
	}
	if leaseErr := r.acquireLease(ctx, threadID, &inv); leaseErr != nil {
		return nil, leaseErr
	}
	meta := newRunMetadata()
	mergeRunMetadataInput(&meta, inv.runMetadata)
	runCtx := r.attachInvocation(ctx, inv, &meta)
	return r.startStream(runCtx, inv, func(
		streamCtx context.Context,
		sink eventSink[T, E],
	) (*RunResult[T, E], error) {
		result, err := r.execute(
			streamCtx,
			threadID,
			r.graph.entryPoint,
			initialState,
			meta,
			nil,
			0,
			sink,
			inv,
		)
		return result, errors.Join(err, r.postRunCleanup(runCtx, threadID, inv, result))
	}), nil
}

func (r *graphRunner[T, E]) ResumeStream(
	ctx context.Context,
	token ResumeToken,
	opts ...RunOption[T, E],
) (StreamHandle[T, E], error) {
	if r.checkpointer == nil {
		return nil, errors.New("flowy: checkpointer is required for ResumeStream")
	}
	if token.ThreadID == "" {
		emitResumeRejected(ctx, token.ThreadID, "", "empty_token")
		return nil, fmt.Errorf("%w: empty thread ID", ErrInvalidResumeToken)
	}
	inv, optErr := r.resolveRunOptions(opts...)
	if optErr != nil {
		return nil, optErr
	}
	if leaseErr := r.acquireLease(ctx, token.ThreadID, &inv); leaseErr != nil {
		return nil, leaseErr
	}
	decision, err := r.evaluateResume(ctx, token, inv)
	if err != nil {
		return nil, errors.Join(err, r.postRunCleanup(ctx, token.ThreadID, inv, nil))
	}
	runCtx := injectTelemetryContext(r.attachInvocation(ctx, inv, &decision.RunMeta), decision.RunMeta.TelemetryContext)
	return r.startStream(runCtx, inv, func(
		streamCtx context.Context,
		sink eventSink[T, E],
	) (*RunResult[T, E], error) {
		result, runErr := r.execute(
			streamCtx,
			token.ThreadID,
			string(decision.ExecutionPointer),
			decision.State,
			decision.RunMeta,
			decision.Effects,
			decision.SnapshotRevision,
			sink,
			inv,
		)
		return result, errors.Join(runErr, r.postRunCleanup(runCtx, token.ThreadID, inv, result))
	}), nil
}

type nodeStepOutcome[T, E any] struct {
	state        T
	meta         RunMetadata
	effects      []E
	base         Directive
	emitCanceled bool
}

type directiveStep[T, E any] struct {
	nextNode string
	result   *RunResult[T, E]
	err      error
	terminal bool
}
