package flowy

import (
	"context"
	"errors"
)

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
		snapshot, _, err := r.durable.Load(context.WithoutCancel(ctx), threadID)
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
		return failedResultWithReason(state, effects, meta, current, commitErr.Error()), errors.Join(commitErr, err)
	}
	return failedResultWithReason(snapshot.State, snapshot.Effects, snapshot.RunMeta,
		string(snapshot.ExecutionPointer), commitErr.Error()), commitErr
}
