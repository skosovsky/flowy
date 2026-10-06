package flowy

import (
	"context"
	"sync"
)

// LifecycleOperation identifies a runtime-owned boundary, not a provider action.
type LifecycleOperation string

const (
	LifecycleExecution    LifecycleOperation = "execution"
	LifecycleTerminal     LifecycleOperation = "terminal"
	LifecycleCheckpoint   LifecycleOperation = "checkpoint"
	LifecycleHandoff      LifecycleOperation = "handoff"
	LifecycleResume       LifecycleOperation = "resume"
	LifecycleActivity     LifecycleOperation = "activity"
	LifecycleReconcile    LifecycleOperation = "reconcile"
	LifecycleRetry        LifecycleOperation = "retry"
	LifecycleChildLaunch  LifecycleOperation = "child_launch"
	LifecycleChildResolve LifecycleOperation = "child_resolve"
	LifecycleChildJoin    LifecycleOperation = "child_join"
	LifecycleChildCancel  LifecycleOperation = "child_cancel"
	LifecycleWaitArm      LifecycleOperation = "wait_arm"
	LifecycleWaitDelivery LifecycleOperation = "wait_delivery"
	LifecycleWaitCancel   LifecycleOperation = "wait_cancel"
	LifecycleLease        LifecycleOperation = "lease"
	LifecycleMigration    LifecycleOperation = "migration"
	LifecycleImport       LifecycleOperation = "import"
	LifecycleFork         LifecycleOperation = "fork"
	LifecycleRollover     LifecycleOperation = "rollover"
	LifecycleRetention    LifecycleOperation = "retention"
)

const (
	lifecycleOutcomeCompleted = "outcome_completed"
	lifecycleOutcomeFailed    = "outcome_failed"
)

// LifecycleStage separates attempts from authoritative committed outcomes.
type LifecycleStage string

const (
	LifecycleStarted   LifecycleStage = "started"
	LifecycleFailed    LifecycleStage = "failed"
	LifecycleCommitted LifecycleStage = "committed"
	LifecycleReplayed  LifecycleStage = "replayed"
)

// LifecycleObservation contains runtime addresses only. It never carries domain
// payload, business evidence or arbitrary error text. IDs are trace dimensions,
// never default metric dimensions. Revision is acknowledged only after commit.
type LifecycleObservation struct {
	Operation         LifecycleOperation
	Stage             LifecycleStage
	ExecutionID       string
	ParentExecutionID string
	SegmentID         string
	Node              ExecutionPointer
	ActivityID        string
	Attempt           int
	ChildID           string
	ChildExecutionID  string
	WorkID            string
	DecisionID        string
	SourceExecutionID string
	TargetExecutionID string
	TargetRevision    uint64
	LeaseIncarnation  uint64
	SourceRevision    uint64
	Revision          uint64
	Code              string
}

// LifecycleObserver is an optional synchronous observer. Panic is contained;
// blocking callbacks block the caller and must respect cancellation. Core does
// not spawn observer goroutines or queue observations. See runtime-observation-contract.md.
type LifecycleObserver interface {
	ObserveLifecycle(context.Context, LifecycleObservation)
}

var (
	lifecycleObserverMu sync.RWMutex      //nolint:gochecknoglobals // synchronized process-wide installation
	lifecycleObserver   LifecycleObserver //nolint:gochecknoglobals // optional process-wide observer slot
)

// SetLifecycleObserver installs the observer; nil disables all callbacks.
func SetLifecycleObserver(observer LifecycleObserver) {
	lifecycleObserverMu.Lock()
	defer lifecycleObserverMu.Unlock()
	lifecycleObserver = observer
}

func observeLifecycle(ctx context.Context, observation LifecycleObservation) {
	lifecycleObserverMu.RLock()
	observer := lifecycleObserver
	lifecycleObserverMu.RUnlock()
	if observer == nil {
		return
	}
	defer func() { _ = recover() }()
	observer.ObserveLifecycle(ctx, observation)
}

func lifecycleObservation(
	operation LifecycleOperation,
	stage LifecycleStage,
	id string,
	pointer ExecutionPointer,
) LifecycleObservation {
	var event LifecycleObservation
	event.Operation, event.Stage, event.ExecutionID, event.Node = operation, stage, id, pointer
	return event
}

func emitHandoffEnqueued(ctx context.Context, threadID string, pointer ExecutionPointer, status string) {
	stage := LifecycleFailed
	if status == handoffMetricSuccess {
		stage = LifecycleCommitted
	}
	event := lifecycleObservation(LifecycleHandoff, stage, threadID, pointer)
	event.Code = status
	observeLifecycle(ctx, event)
}
func emitResumeRejected(ctx context.Context, threadID string, pointer ExecutionPointer, reason string) {
	event := lifecycleObservation(LifecycleResume, LifecycleFailed, threadID, pointer)
	event.Code = reason
	observeLifecycle(ctx, event)
}
func emitCheckpointSoftError(ctx context.Context, threadID string, pointer ExecutionPointer) {
	event := lifecycleObservation(LifecycleCheckpoint, LifecycleFailed, threadID, pointer)
	event.Code = "soft_error"
	observeLifecycle(ctx, event)
}
