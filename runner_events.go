package flowy

import (
	"context"
	"time"
)

const (
	streamEventBufferSize    = 32
	contextCancelSaveTimeout = 5 * time.Second
	terminalEventEmitTimeout = 250 * time.Millisecond
)

func newRunEventNodeStarted[T any](nodeID string, state T) RunEvent[T] {
	return RunEvent[T]{
		Type:     EventNodeStarted,
		NodeID:   nodeID,
		State:    state,
		Effect:   nil,
		Error:    nil,
		Duration: 0,
		Metrics:  nil,
	}
}

func newRunEventNodeCompleted[T any](nodeID string, state T, duration time.Duration) RunEvent[T] {
	return RunEvent[T]{
		Type:     EventNodeCompleted,
		NodeID:   nodeID,
		State:    state,
		Effect:   nil,
		Error:    nil,
		Duration: duration,
		Metrics:  nil,
	}
}

func newRunEventNodeCompletedWithEffect[T any](
	nodeID string,
	state T,
	effect any,
	duration time.Duration,
	metrics map[string]any,
) RunEvent[T] {
	return RunEvent[T]{
		Type:     EventNodeCompleted,
		NodeID:   nodeID,
		State:    state,
		Effect:   effect,
		Error:    nil,
		Duration: duration,
		Metrics:  metrics,
	}
}

func newRunEventFailed[T any](nodeID string, state T, err error) RunEvent[T] {
	return RunEvent[T]{
		Type:     EventFailed,
		NodeID:   nodeID,
		State:    state,
		Effect:   nil,
		Error:    err,
		Duration: 0,
		Metrics:  nil,
	}
}

func newRunEventSuspended[T any](nodeID string, state T, err error) RunEvent[T] {
	return RunEvent[T]{
		Type:     EventSuspended,
		NodeID:   nodeID,
		State:    state,
		Effect:   nil,
		Error:    err,
		Duration: 0,
		Metrics:  nil,
	}
}

func newRunEventSuspendedNoError[T any](nodeID string, state T) RunEvent[T] {
	return RunEvent[T]{
		Type:     EventSuspended,
		NodeID:   nodeID,
		State:    state,
		Effect:   nil,
		Error:    nil,
		Duration: 0,
		Metrics:  nil,
	}
}

func newRunEventCompleted[T any](nodeID string, state T) RunEvent[T] {
	return RunEvent[T]{
		Type:     EventCompleted,
		NodeID:   nodeID,
		State:    state,
		Effect:   nil,
		Error:    nil,
		Duration: 0,
		Metrics:  nil,
	}
}

func newRunResultCompleted[T any](state T, effects []any, meta RunMetadata, nodeID string) *RunResult[T] {
	return &RunResult[T]{
		State:   state,
		Status:  RunStatusCompleted,
		Effects: append([]any(nil), effects...),
		RunMeta: meta,
		NodeID:  nodeID,
		Reason:  "",
	}
}

func newRunResultContextCanceled[T any](state T, effects []any, meta RunMetadata, nodeID string) *RunResult[T] {
	return &RunResult[T]{
		State:   state,
		Status:  RunStatusSuspended,
		Effects: append([]any(nil), effects...),
		RunMeta: meta,
		NodeID:  nodeID,
		Reason:  "context_canceled",
	}
}

func newRunResultSuspended[T any](state T, effects []any, meta RunMetadata, nodeID, reason string) *RunResult[T] {
	return &RunResult[T]{
		State:   state,
		Status:  RunStatusSuspended,
		Effects: append([]any(nil), effects...),
		RunMeta: meta,
		NodeID:  nodeID,
		Reason:  reason,
	}
}

func emitEvent[T any](ctx context.Context, sink eventSink[T], event RunEvent[T]) bool {
	if sink == nil {
		return true
	}
	return sink(ctx, event)
}

func blankNodeStepOutcome[T any](state T, meta RunMetadata, effects []any) nodeStepOutcome[T] {
	return nodeStepOutcome[T]{
		state:        state,
		meta:         meta,
		effects:      effects,
		base:         directiveWithKind(0),
		emitCanceled: false,
	}
}

func canceledNodeStepOutcome[T any](state T, meta RunMetadata, effects []any) nodeStepOutcome[T] {
	out := blankNodeStepOutcome(state, meta, effects)
	out.emitCanceled = true
	return out
}

func continueDirectiveStep[T any](nextNode string) directiveStep[T] {
	return directiveStep[T]{
		nextNode: nextNode,
		result:   nil,
		err:      nil,
		terminal: false,
	}
}

func terminalDirectiveStep[T any](result *RunResult[T], err error) directiveStep[T] {
	return directiveStep[T]{
		nextNode: "",
		result:   result,
		err:      err,
		terminal: true,
	}
}

func emitTerminalEvent[T any](ctx context.Context, sink eventSink[T], event RunEvent[T]) bool {
	termCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalEventEmitTimeout)
	defer cancel()
	return emitEvent(termCtx, sink, event)
}
