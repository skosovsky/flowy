package flowy

import (
	"context"
	"errors"
)

// ErrDurableStateUnavailable marks a diagnostic local result when reloading the
// committed state failed. State, effects and metadata in that result have no
// committed authority; inspect storage after correcting the codec/store failure.
var ErrDurableStateUnavailable = errors.New("flowy: committed state unavailable; diagnostic local result")

// interruptionSnapshot retains the committed entry boundary for a durable node
// whose directive has not committed. The handler may have aliased BYOT maps or
// pointers, so the local pre-node value is not a safe rollback copy.
func (r *graphRunner[T, E]) interruptionSnapshot(
	ctx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
) (Snapshot[T, E], error) {
	if r.durable != nil {
		restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), contextCancelSaveTimeout)
		defer cancel()
		snapshot, _, err := r.durable.Load(restoreCtx, threadID)
		return snapshot, err
	}
	return Snapshot[T, E]{
		ThreadID: threadID, ExecutionPointer: ExecutionPointer(current), Revision: 0,
		State: state, RunMeta: meta, Effects: effects,
	}, nil
}

func (r *graphRunner[T, E]) failedDurableStep(
	ctx context.Context,
	threadID, current string,
	state T,
	meta RunMetadata,
	effects []E,
	commitErr error,
) (*RunResult[T, E], error) {
	snapshot, err := r.interruptionSnapshot(ctx, threadID, current, state, meta, effects)
	if err != nil {
		return failedDiagnosticResult(state, effects, meta, current, commitErr, err)
	}
	return failedResultWithReason(snapshot.State, snapshot.Effects, snapshot.RunMeta,
		string(snapshot.ExecutionPointer), commitErr.Error()), commitErr
}

func failedDiagnosticResult[T, E any](state T, effects []E, meta RunMetadata,
	current string, cause, restoreErr error) (*RunResult[T, E], error) {
	err := errors.Join(ErrDurableStateUnavailable, cause, restoreErr)
	return failedResultWithReason(state, effects, meta, current, "diagnostic local state: "+err.Error()), err
}
