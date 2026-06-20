package flowy

import "context"

func resumeLoaded[T, E any](
	ctx context.Context,
	runner Runner[T, E],
	cp Checkpointer[T, E],
	threadID string,
	opts ...RunOption[T, E],
) (*RunResult[T, E], error) {
	snap, rev, err := cp.Load(ctx, threadID)
	if err != nil {
		return nil, err
	}
	return runner.Resume(ctx, ResumeToken{ThreadID: snap.ThreadID, SnapshotRevision: rev}, opts...)
}
