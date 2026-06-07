package flowy

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// Node is the basic execution unit. It returns state update plus a directive.
type Node[T, E any] func(ctx context.Context, state T) (T, Directive, error)

// NodeMiddleware wraps a node handler.
type NodeMiddleware[T, E any] func(next Node[T, E]) Node[T, E]

// Reducer defines how to merge the current state with the update returned by a node.
type Reducer[T any] func(current T, update T) T

type directiveKind int

const (
	directiveCompleted directiveKind = iota + 1
	directiveEnd
	directiveSuspend
	directiveRetry
	directiveEffect
	directiveFail
	directiveHandoff
	// directiveNext is reserved; nodes must not route by node id.
	directiveNext directiveKind = 99
)

// Directive is a platform command returned by nodes.
type Directive struct {
	kind        directiveKind
	reason      string
	maxAttempts int
	base        *Directive
	effect      any
}

func directiveWithKind(kind directiveKind) Directive {
	return Directive{
		kind:        kind,
		reason:      "",
		maxAttempts: 0,
		base:        nil,
		effect:      nil,
	}
}

// Completed marks node success and asks graph router for the next edge.
func Completed() Directive {
	return directiveWithKind(directiveCompleted)
}

// End completes the run.
func End() Directive {
	return directiveWithKind(directiveEnd)
}

// Suspend pauses execution and persists snapshot.
func Suspend(reason string) Directive {
	d := directiveWithKind(directiveSuspend)
	d.reason = reason
	return d
}

// Fail terminates the run with a reason (terminal failure).
func Fail(reason string) Directive {
	d := directiveWithKind(directiveFail)
	d.reason = reason
	return d
}

// Handoff ends the foreground segment and persists state for a background worker to Resume.
func Handoff(reason string) Directive {
	d := directiveWithKind(directiveHandoff)
	d.reason = reason
	return d
}

// Retry reruns using the fallback declared via GraphBuilder.AddRetryRoute.
func Retry(maxAttempts int) Directive {
	d := directiveWithKind(directiveRetry)
	d.maxAttempts = maxAttempts
	return d
}

// Effect attaches a typed side effect payload to a base directive.
func Effect[E any](base Directive, payload E) Directive {
	d := directiveWithKind(directiveEffect)
	d.base = &base
	d.effect = payload
	return d
}

// IsCompleted reports whether directive is Completed.
func (d Directive) IsCompleted() bool {
	return d.kind == directiveCompleted
}

// Type returns directive semantic name for observability.
func (d Directive) Type() string {
	switch d.kind {
	case directiveCompleted:
		return "completed"
	case directiveEnd:
		return "end"
	case directiveSuspend:
		return "suspend"
	case directiveRetry:
		return "retry"
	case directiveEffect:
		return "effect"
	case directiveFail:
		return "fail"
	case directiveHandoff:
		return "handoff"
	case directiveNext:
		return "next"
	default:
		return "unknown"
	}
}

// UnwrapDirective returns base directive and collected typed effects.
func UnwrapDirective[E any](d Directive) (Directive, []E, error) {
	effects := make([]E, 0)
	current := d
	for current.kind == directiveEffect {
		payload, ok := current.effect.(E)
		if !ok {
			return Directive{}, nil, fmt.Errorf("flowy: effect type mismatch: expected %T", *new(E))
		}
		effects = append(effects, payload)
		if current.base == nil {
			return Directive{}, nil, errors.New("flowy: Effect requires base directive")
		}
		current = *current.base
	}
	if current.kind == 0 {
		return Directive{}, nil, errors.New("flowy: zero directive is not allowed")
	}
	slices.Reverse(effects)
	return current, effects, nil
}

// RunStatus represents current run completion status.
type RunStatus string

const (
	RunStatusCompleted       RunStatus = "completed"
	RunStatusSuspended       RunStatus = "suspended"
	RunStatusContextCanceled RunStatus = "context_canceled"
	RunStatusFailed          RunStatus = "failed"
	RunStatusHandoff         RunStatus = "handoff"
)

// Terminal reason suffixes when checkpoint save is skipped or post-save policy fails.
const (
	ReasonSuspendedCheckpointSkipped       = "suspended_checkpoint_skipped"
	ReasonHandoffCheckpointSkipped         = "handoff_checkpoint_skipped"
	ReasonContextCanceledCheckpointSkipped = "context_canceled_checkpoint_skipped"
	ReasonContextCanceledSaveFailed        = "context_canceled_save_failed"
	ReasonHandoffPointerResolveFailed      = "handoff_pointer_resolve_failed"
	ReasonSuspendPointerResolveFailed      = "suspend_pointer_resolve_failed"
	ReasonHandoffSaveFailed                = "handoff_save_failed"
	ReasonHandoffOrphaned                  = "handoff_orphaned"
	ReasonSuspendSaveFailed                = "suspend_save_failed"
)

// SegmentEndReason describes why a compute segment ended.
type SegmentEndReason string

const (
	SegmentEndSuspend         SegmentEndReason = "suspend"
	SegmentEndComplete        SegmentEndReason = "complete"
	SegmentEndFail            SegmentEndReason = "fail"
	SegmentEndHandoff         SegmentEndReason = "handoff"
	SegmentEndContextCanceled SegmentEndReason = "context_canceled"
)

// SegmentInfo tracks the active compute segment lifecycle.
type SegmentInfo struct {
	SegmentID string           `json:"segment_id"`
	StartTime time.Time        `json:"start_time"`
	EndTime   time.Time        `json:"end_time"`
	EndReason SegmentEndReason `json:"end_reason,omitempty"`
}

// ExecutionPointer is the persisted resume point (current graph node id).
type ExecutionPointer string

// ResumeReconciler may adjust state and the resume execution pointer after overlay merge.
// When T is a struct and ReconcileResume mutates state, implement on a pointer receiver;
// the runner reassigns state from reconcileResume (see run_options.go).
type ResumeReconciler interface {
	// ReconcileResume is called after overlay merge and before node execution.
	// Return currentPtr unchanged to keep the snapshot pointer, or a different
	// ExecutionPointer to rewind execution (e.g. after overlay invalidates the saved node).
	ReconcileResume(currentPtr ExecutionPointer) (ExecutionPointer, error)
}

// RunMetadataInput is injectable run metadata for a single Start/Resume/Stream invocation.
type RunMetadataInput struct {
	BudgetCounts     map[string]int
	TelemetryContext map[string]string
}

// HandoffStatus tracks handoff lifecycle in persisted run metadata.
type HandoffStatus string

const (
	HandoffStatusNone     HandoffStatus = ""
	HandoffStatusPending  HandoffStatus = "pending"
	HandoffStatusEnqueued HandoffStatus = "enqueued"
	HandoffStatusOrphaned HandoffStatus = "orphaned"
)

// RunMetadata contains internal runner counters.
type RunMetadata struct {
	Segment          SegmentInfo       `json:"segment"`
	SegmentStartTime time.Time         `json:"segment_start_time"`
	RetryCounts      map[string]int    `json:"retry_counts"`
	BudgetCounts     map[string]int    `json:"budget_counts,omitempty"`
	StepCount        int               `json:"step_count"`
	TelemetryContext map[string]string `json:"telemetry_context,omitempty"`
	HandoffStatus    HandoffStatus     `json:"handoff_status,omitempty"`
	HandoffPendingAt time.Time         `json:"handoff_pending_at,omitzero"`
}

// Snapshot is a persisted run snapshot (bindings are never stored).
type Snapshot[T, E any] struct {
	ThreadID         string
	ExecutionPointer ExecutionPointer
	Revision         uint64
	State            T
	RunMeta          RunMetadata
	Effects          []E
}

// Checkpointer persists and restores snapshots with optimistic concurrency control.
type Checkpointer[T, E any] interface {
	// Save requires expectedRevision (0 for first write). Returns newRevision or ErrConcurrencyConflict.
	Save(ctx context.Context, expectedRevision uint64, snapshot Snapshot[T, E]) (uint64, error)
	// Load returns the latest snapshot and its revision.
	Load(ctx context.Context, threadID string) (Snapshot[T, E], uint64, error)
	GetHistory(ctx context.Context, threadID string, limit int) ([]Snapshot[T, E], error)
	Prune(ctx context.Context, threadID string, retainCount int) error
	// Delete unconditionally removes snapshots. Prefer DeleteIfIdle for runner retention policies.
	Delete(ctx context.Context, threadID string) error
	// DeleteIfIdle removes snapshots only when the thread has no active lease (see ErrThreadLeaseBusy).
	DeleteIfIdle(ctx context.Context, threadID string) error
}

// TransactionalCheckpointer saves a snapshot and runs enqueueFn in one storage transaction.
// When enqueueFn fails the save is rolled back. enqueueFn receives ContextWithOutboxTx(ctx, tx)
// so storage-specific outbox adapters can INSERT in the same transaction (see postgres.PgxTxFromContext).
// Optional — runner falls back to 3-phase FSM when the checkpointer does not implement this interface.
type TransactionalCheckpointer[T, E any] interface {
	Checkpointer[T, E]
	SaveWithOutbox(
		ctx context.Context,
		expectedRevision uint64,
		snapshot Snapshot[T, E],
		enqueueFn func(ctx context.Context) error,
	) (uint64, error)
}

// NativeDeleteIfIdleCheckpointer marks adapters with atomic DeleteIfIdle in shared storage.
// Runner auto-wraps with LeaseGuard only when this marker is absent.
type NativeDeleteIfIdleCheckpointer interface {
	NativeDeleteIfIdle()
}

// NativeLeaseManager marks lease adapters that persist into the same store as native checkpointers.
// Required when using NativeDeleteIfIdleCheckpointer with WithLeaseManager (postgres/redis pairs).
type NativeLeaseManager interface {
	NativeLeaseManager()
}

// StateSerializer converts state to and from bytes.
type StateSerializer[T any] interface {
	Marshal(state T) ([]byte, error)
	Unmarshal(data []byte) (T, error)
}

// StateInterceptor can mutate state before save and after load.
type StateInterceptor[T any] interface {
	BeforeSave(ctx context.Context, state *T) error
	AfterLoad(ctx context.Context, state *T) error
}

// ResumeToken identifies a persisted thread snapshot for optimistic-concurrency resume.
// SnapshotRevision maps to Snapshot.Revision after the terminal save that produced the token.
// Zero SnapshotRevision is invalid — callers must Load before Resume.
type ResumeToken struct {
	ThreadID         string
	SnapshotRevision uint64
}

// ResumeTokenFromSnapshot builds a resume token from a loaded snapshot.
func ResumeTokenFromSnapshot[T, E any](s Snapshot[T, E]) ResumeToken {
	return ResumeToken{ThreadID: s.ThreadID, SnapshotRevision: s.Revision}
}

// SuspendPointerResolver overrides the execution pointer stored on Suspend/Handoff saves.
type SuspendPointerResolver[T any] func(state T, suspendNode ExecutionPointer) (ExecutionPointer, error)

// CheckpointFailurePolicy controls runner behavior when Checkpointer.Save fails.
type CheckpointFailurePolicy string

const (
	CheckpointPolicyHardFail        CheckpointFailurePolicy = "hard_fail"
	CheckpointPolicySkipOnSaveError CheckpointFailurePolicy = "soft_warn" // persisted config value unchanged
)

// RunResult is the final state returned to the application.
type RunResult[T, E any] struct {
	State            T
	Status           RunStatus
	Effects          []E
	RunMeta          RunMetadata
	ExecutionPointer ExecutionPointer
	Reason           string
	ResumeToken      ResumeToken // set after persisted Suspend/Handoff terminal save only
}

type EventType string

const (
	EventNodeStarted      EventType = "node_started"
	EventNodeCompleted    EventType = "node_completed"
	EventCompleted        EventType = "completed"
	EventSuspended        EventType = "suspended"
	EventFailed           EventType = "failed"
	EventHandoff          EventType = "handoff"
	EventContextCanceled  EventType = "context_canceled"
	EventCheckpointFailed EventType = "checkpoint_failed"
)

// RunEvent represents a single lifecycle event with typed effect payload.
type RunEvent[T, E any] struct {
	Type             EventType
	ExecutionPointer ExecutionPointer
	State            T
	Effect           E
	HasEffect        bool
	Error            error
	Duration         time.Duration
	Reason           string // terminal lifecycle reason (suspend, handoff, context cancel)
}

// StreamHandle controls the lifecycle of asynchronous graph streaming.
//
// Events: the channel closes only after the run goroutine fully terminates. Do not block on
// for-range Events on the same goroutine if you plan to call RequestStop or RequestLocalHandoff
// later — use CollectEventsAndWait, ConsumeEventsAndWait, or BeginStreamCollect instead.
//
// RequestStop: closes the event sink and cancels the in-flight run context (cancelSessionForConsumerStop).
// Do not call RequestLocalHandoff after RequestStop; the run has already terminated and the API returns
// [ErrNoActiveExecution]. A terminal event may be dropped after consumer stop; the checkpointer snapshot
// is the source of truth for terminal state and reason (persist-vs-event semantics).
//
// Wait: call exactly once after Events is fully consumed (or use package helpers that do this safely).
// RequestStop after persisted cancel save returns nil; RequestStop with skip-on-save-error policy returns
// [ErrCheckpointSkipped]; parent context cancel returns [context.Canceled]; retention or enqueue
// enqueue failures return their respective errors.
type StreamHandle[T, E any] interface {
	Events() <-chan RunEvent[T, E]
	RequestStop()
	Wait() error
}

// Runner controls lifecycle for start/resume executions.
// Stream and ResumeStream open the event channel immediately; if the thread already has an active
// in-process execution, the duplicate attempt is not rejected at open time — call Wait() on the
// second handle to observe [ErrThreadAlreadyRunning]. Start and Resume reject duplicates synchronously.
type Runner[T, E any] interface {
	Start(ctx context.Context, threadID string, initialState T, opts ...RunOption[T, E]) (*RunResult[T, E], error)
	Resume(ctx context.Context, token ResumeToken, opts ...RunOption[T, E]) (*RunResult[T, E], error)
	Stream(ctx context.Context, threadID string, initialState T, opts ...RunOption[T, E]) (StreamHandle[T, E], error)
	ResumeStream(ctx context.Context, token ResumeToken, opts ...RunOption[T, E]) (StreamHandle[T, E], error)
	// RequestLocalHandoff requests graceful termination of the active foreground execution on this runner instance only.
	// It cancels the in-process run and waits until the handoff terminal path completes, then returns:
	//   - nil when checkpoint was persisted and retention succeeded;
	//   - ErrCheckpointSkipped when skip-on-save-error policy skips persist (no snapshot);
	//   - ErrHandoffEnqueueFailed (and optionally joined retention error) when enqueue fails after persist (snapshot retained);
	//   - wrapped retention or save errors on HardFail paths.
	// A background worker on any instance must call Resume with a new lease; cross-process handoff uses checkpoint + lease, not this API alone.
	RequestLocalHandoff(ctx context.Context, threadID string) error
	// RecoverStaleHandoff re-enqueues orphaned or stale-pending handoff checkpoints.
	RecoverStaleHandoff(ctx context.Context, threadID string, opts ...RecoverStaleHandoffOption) error
}

// Sentinel errors for runner flow and validation.
var (
	ErrMaxStepsExceeded   = errors.New("flowy: max steps exceeded")
	ErrThreadNotFound     = errors.New("flowy: thread snapshot not found")
	ErrLegacyNext         = errors.New("flowy: Next(nodeID) was removed; use Completed() and graph edges")
	ErrLeaseOwnerRequired = errors.New(
		"flowy: WithRunLease owner is required when LeaseManager is configured",
	)
	ErrLeaseLost                        = errors.New("flowy: thread lease lost or expired")
	ErrNoActiveExecution                = errors.New("flowy: no active execution to hand off")
	ErrThreadAlreadyRunning             = errors.New("flowy: thread already has an active in-process execution")
	ErrRetryBudgetExceeded              = errors.New("flowy: per-node retry budget exceeded")
	ErrInvalidSnapshot                  = errors.New("flowy: snapshot has invalid or empty execution pointer")
	ErrResumeReconcileFailed            = errors.New("flowy: resume reconcile failed")
	ErrResumeStartNodeNotFound          = errors.New("flowy: resume start node not found")
	ErrConcurrencyConflict              = errors.New("flowy: concurrency conflict")
	ErrHandoffEnqueueFailed             = errors.New("flowy: handoff enqueue failed")
	ErrHandoffPatchFailed               = errors.New("flowy: handoff status patch failed")
	ErrHandoffPending                   = errors.New("flowy: handoff pending")
	ErrHandoffOrphaned                  = errors.New("flowy: handoff orphaned")
	ErrHandoffAlreadyEnqueued           = errors.New("flowy: handoff already enqueued")
	ErrHandoffNotRecoverable            = errors.New("flowy: handoff status not recoverable")
	ErrHandoffOutboxRequired            = errors.New("flowy: handoff outbox is required for recovery")
	ErrTransactionalOutboxUnsupported   = errors.New("flowy: checkpointer does not support SaveWithOutbox")
	ErrTransactionalHandoffCommitFailed = errors.New("flowy: transactional handoff commit failed")
	ErrInvalidResumeToken               = errors.New("flowy: invalid resume token")
	ErrInvalidCheckpointPolicy          = errors.New("flowy: invalid checkpoint failure policy")
	// ErrCheckpointSkipped is returned from StreamHandle.Wait when consumer RequestStop stops execution
	// with skip-on-save-error policy, and from RequestLocalHandoff when persist is skipped.
	ErrCheckpointSkipped = errors.New("flowy: checkpoint skipped")
)

// EndNode is a terminal graph target for AddEdge/AddConditionalEdge.
const EndNode = "__end__"

type contextKey string

const (
	nodeNameContextKey  contextKey = "flowy.node_name"
	runThreadContextKey contextKey = "flowy.run_thread_id"
)

// NodeNameFromContext returns current node id attached by runner.
func NodeNameFromContext(ctx context.Context) string {
	value, _ := ctx.Value(nodeNameContextKey).(string)
	return value
}

// RunThreadIDFromContext returns the active runner thread id when executing inside a graph run.
func RunThreadIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(runThreadContextKey).(string)
	return value
}

func withNodeName(ctx context.Context, nodeName string) context.Context {
	return context.WithValue(ctx, nodeNameContextKey, nodeName)
}

func withRunThreadID(ctx context.Context, threadID string) context.Context {
	return context.WithValue(ctx, runThreadContextKey, threadID)
}

// TelemetryBridge serializes runtime tracing metadata across suspend/resume boundaries.
type TelemetryBridge interface {
	Extract(ctx context.Context) map[string]string
	Inject(ctx context.Context, metadata map[string]string) context.Context
}

type noopTelemetryBridge struct{}

func (noopTelemetryBridge) Extract(context.Context) map[string]string { return nil }

func (noopTelemetryBridge) Inject(ctx context.Context, _ map[string]string) context.Context {
	return ctx
}

var (
	telemetryBridgeMu sync.RWMutex                            //nolint:gochecknoglobals // paired mutex for telemetryBridge
	telemetryBridge   TelemetryBridge = noopTelemetryBridge{} //nolint:gochecknoglobals // process-wide bridge slot
)

// SetTelemetryBridge installs the process-wide telemetry bridge (stateless Extract/Inject only).
func SetTelemetryBridge(bridge TelemetryBridge) {
	telemetryBridgeMu.Lock()
	defer telemetryBridgeMu.Unlock()
	if bridge == nil {
		telemetryBridge = noopTelemetryBridge{}
		return
	}
	telemetryBridge = bridge
}

func extractTelemetryContext(ctx context.Context) map[string]string {
	telemetryBridgeMu.RLock()
	bridge := telemetryBridge
	telemetryBridgeMu.RUnlock()
	metadata := bridge.Extract(ctx)
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata))
	maps.Copy(out, metadata)
	return out
}

func injectTelemetryContext(ctx context.Context, metadata map[string]string) context.Context {
	if len(metadata) == 0 {
		return ctx
	}
	copyMetadata := make(map[string]string, len(metadata))
	maps.Copy(copyMetadata, metadata)
	telemetryBridgeMu.RLock()
	bridge := telemetryBridge
	telemetryBridgeMu.RUnlock()
	return bridge.Inject(ctx, copyMetadata)
}

func newSegmentInfo() SegmentInfo {
	now := time.Now().UTC()
	return SegmentInfo{
		SegmentID: fmt.Sprintf("seg-%d", now.UnixNano()),
		StartTime: now,
		EndTime:   time.Time{},
		EndReason: "",
	}
}
