package flowy

import (
	"context"
	"time"
)

const (
	streamEventBufferSize    = 32
	contextCancelSaveTimeout = 5 * time.Second
	terminalEventEmitTimeout = 250 * time.Millisecond
	defaultLeaseTTL          = 30 * time.Second
	leaseHeartbeatDivisor    = 3
)

// leaseHeartbeatInterval returns renew cadence that always fits within lease TTL.
func leaseHeartbeatInterval(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	interval := ttl / leaseHeartbeatDivisor
	maxInterval := ttl / 2
	if interval > maxInterval {
		interval = maxInterval
	}
	if interval <= 0 {
		interval = maxInterval
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	return interval
}

func newRunEventNodeStarted[T, E any](nodeID string, state T) RunEvent[T, E] {
	return RunEvent[T, E]{
		Type:      EventNodeStarted,
		NodeID:    nodeID,
		State:     state,
		HasEffect: false,
	}
}

func newRunEventNodeCompleted[T, E any](nodeID string, state T, duration time.Duration) RunEvent[T, E] {
	return RunEvent[T, E]{
		Type:      EventNodeCompleted,
		NodeID:    nodeID,
		State:     state,
		HasEffect: false,
		Duration:  duration,
	}
}

func newRunEventNodeCompletedWithEffect[T, E any](
	nodeID string,
	state T,
	effect E,
	duration time.Duration,
) RunEvent[T, E] {
	return RunEvent[T, E]{
		Type:      EventNodeCompleted,
		NodeID:    nodeID,
		State:     state,
		Effect:    effect,
		HasEffect: true,
		Duration:  duration,
	}
}

func newRunEventFailed[T, E any](nodeID string, state T, err error) RunEvent[T, E] {
	return RunEvent[T, E]{
		Type:   EventFailed,
		NodeID: nodeID,
		State:  state,
		Error:  err,
	}
}

func newRunEventSuspendedNoError[T, E any](nodeID string, state T, reason string) RunEvent[T, E] {
	return RunEvent[T, E]{
		Type:   EventSuspended,
		NodeID: nodeID,
		State:  state,
		Reason: reason,
	}
}

func newRunEventContextCanceled[T, E any](nodeID string, state T, reason string) RunEvent[T, E] {
	return RunEvent[T, E]{
		Type:   EventContextCanceled,
		NodeID: nodeID,
		State:  state,
		Reason: reason,
	}
}

func newRunEventCompleted[T, E any](nodeID string, state T) RunEvent[T, E] {
	return RunEvent[T, E]{
		Type:   EventCompleted,
		NodeID: nodeID,
		State:  state,
	}
}

func newRunEventHandoff[T, E any](nodeID string, state T, reason string) RunEvent[T, E] {
	return RunEvent[T, E]{
		Type:   EventHandoff,
		NodeID: nodeID,
		State:  state,
		Reason: reason,
	}
}

func newRunResultHandoff[T, E any](state T, effects []E, meta RunMetadata, nodeID, reason string) *RunResult[T, E] {
	return &RunResult[T, E]{
		State:   state,
		Status:  RunStatusHandoff,
		Effects: append([]E(nil), effects...),
		RunMeta: meta,
		NodeID:  nodeID,
		Reason:  reason,
	}
}

func newRunResultCompleted[T, E any](state T, effects []E, meta RunMetadata, nodeID string) *RunResult[T, E] {
	return &RunResult[T, E]{
		State:   state,
		Status:  RunStatusCompleted,
		Effects: append([]E(nil), effects...),
		RunMeta: meta,
		NodeID:  nodeID,
	}
}

func newRunResultContextCanceled[T, E any](state T, effects []E, meta RunMetadata, nodeID string) *RunResult[T, E] {
	return &RunResult[T, E]{
		State:   state,
		Status:  RunStatusContextCanceled,
		Effects: append([]E(nil), effects...),
		RunMeta: meta,
		NodeID:  nodeID,
		Reason:  "context_canceled",
	}
}

func newRunResultSuspended[T, E any](state T, effects []E, meta RunMetadata, nodeID, reason string) *RunResult[T, E] {
	return &RunResult[T, E]{
		State:   state,
		Status:  RunStatusSuspended,
		Effects: append([]E(nil), effects...),
		RunMeta: meta,
		NodeID:  nodeID,
		Reason:  reason,
	}
}

func newRunResultFailed[T, E any](state T, effects []E, meta RunMetadata, nodeID, reason string) *RunResult[T, E] {
	return &RunResult[T, E]{
		State:   state,
		Status:  RunStatusFailed,
		Effects: append([]E(nil), effects...),
		RunMeta: meta,
		NodeID:  nodeID,
		Reason:  reason,
	}
}

func emitEvent[T, E any](ctx context.Context, sink eventSink[T, E], event RunEvent[T, E]) bool {
	if sink == nil {
		return true
	}
	return sink(ctx, event)
}

func blankNodeStepOutcome[T, E any](state T, meta RunMetadata, effects []E) nodeStepOutcome[T, E] {
	return nodeStepOutcome[T, E]{
		state:        state,
		meta:         meta,
		effects:      effects,
		base:         directiveWithKind(0),
		emitCanceled: false,
	}
}

func canceledNodeStepOutcome[T, E any](state T, meta RunMetadata, effects []E) nodeStepOutcome[T, E] {
	out := blankNodeStepOutcome(state, meta, effects)
	out.emitCanceled = true
	return out
}

func continueDirectiveStep[T, E any](nextNode string) directiveStep[T, E] {
	return directiveStep[T, E]{
		nextNode: nextNode,
		result:   nil,
		err:      nil,
		terminal: false,
	}
}

func terminalDirectiveStep[T, E any](result *RunResult[T, E], err error) directiveStep[T, E] {
	return directiveStep[T, E]{
		nextNode: "",
		result:   result,
		err:      err,
		terminal: true,
	}
}

func emitTerminalEvent[T, E any](ctx context.Context, sink eventSink[T, E], event RunEvent[T, E]) bool {
	termCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalEventEmitTimeout)
	defer cancel()
	return emitEvent(termCtx, sink, event)
}
