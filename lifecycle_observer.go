package flowy

import (
	"context"
	"sync"
)

// LifecycleObserver receives semantic lifecycle events from the runner (metrics/traces).
type LifecycleObserver interface {
	HandoffEnqueued(ctx context.Context, threadID string, pointer ExecutionPointer, status string)
	ResumeRejected(ctx context.Context, threadID string, pointer ExecutionPointer, reason string)
	CheckpointSoftError(ctx context.Context, threadID string, pointer ExecutionPointer)
}

type noopLifecycleObserver struct{}

func (noopLifecycleObserver) HandoffEnqueued(context.Context, string, ExecutionPointer, string) {}
func (noopLifecycleObserver) ResumeRejected(context.Context, string, ExecutionPointer, string)  {}
func (noopLifecycleObserver) CheckpointSoftError(context.Context, string, ExecutionPointer)     {}

var (
	lifecycleObserverMu sync.RWMutex                                //nolint:gochecknoglobals // paired mutex for lifecycleObserver
	lifecycleObserver   LifecycleObserver = noopLifecycleObserver{} //nolint:gochecknoglobals // process-wide observer slot
)

// SetLifecycleObserver installs the process-wide lifecycle observer.
func SetLifecycleObserver(observer LifecycleObserver) {
	lifecycleObserverMu.Lock()
	defer lifecycleObserverMu.Unlock()
	if observer == nil {
		lifecycleObserver = noopLifecycleObserver{}
		return
	}
	lifecycleObserver = observer
}

func emitHandoffEnqueued(ctx context.Context, threadID string, pointer ExecutionPointer, status string) {
	lifecycleObserverMu.RLock()
	obs := lifecycleObserver
	lifecycleObserverMu.RUnlock()
	obs.HandoffEnqueued(ctx, threadID, pointer, status)
}

func emitResumeRejected(ctx context.Context, threadID string, pointer ExecutionPointer, reason string) {
	lifecycleObserverMu.RLock()
	obs := lifecycleObserver
	lifecycleObserverMu.RUnlock()
	obs.ResumeRejected(ctx, threadID, pointer, reason)
}

func emitCheckpointSoftError(ctx context.Context, threadID string, pointer ExecutionPointer) {
	lifecycleObserverMu.RLock()
	obs := lifecycleObserver
	lifecycleObserverMu.RUnlock()
	obs.CheckpointSoftError(ctx, threadID, pointer)
}
