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

// ResumableState may reconcile derived fields after overlay merge and before execution.
type ResumableState interface {
	Reconcile() error
}

// RunMetadataInput is injectable run metadata for a single Start/Resume/Stream invocation.
type RunMetadataInput struct {
	BudgetCounts     map[string]int
	TelemetryContext map[string]string
}

// RunMetadata contains internal runner counters.
type RunMetadata struct {
	Segment          SegmentInfo       `json:"segment"`
	SegmentStartTime time.Time         `json:"segment_start_time"`
	RetryCounts      map[string]int    `json:"retry_counts"`
	BudgetCounts     map[string]int    `json:"budget_counts,omitempty"`
	StepCount        int               `json:"step_count"`
	TelemetryContext map[string]string `json:"telemetry_context,omitempty"`
}

// Snapshot is a persisted run snapshot (bindings are never stored).
type Snapshot[T, E any] struct {
	ThreadID         string
	ExecutionPointer ExecutionPointer
	Revision         int
	State            T
	RunMeta          RunMetadata
	Effects          []E
}

// Checkpointer persists and restores snapshots.
type Checkpointer[T, E any] interface {
	Save(ctx context.Context, snapshot Snapshot[T, E]) error
	Load(ctx context.Context, threadID string) (Snapshot[T, E], error)
	GetHistory(ctx context.Context, threadID string, limit int) ([]Snapshot[T, E], error)
	Prune(ctx context.Context, threadID string, retainCount int) error
	// Delete unconditionally removes snapshots. Prefer DeleteIfIdle for runner retention policies.
	Delete(ctx context.Context, threadID string) error
	// DeleteIfIdle removes snapshots only when the thread has no active lease (see ErrThreadBusy).
	DeleteIfIdle(ctx context.Context, threadID string) error
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

// RunResult is the final state returned to the application.
type RunResult[T, E any] struct {
	State            T
	Status           RunStatus
	Effects          []E
	RunMeta          RunMetadata
	ExecutionPointer ExecutionPointer
	Reason           string
}

type EventType string

const (
	EventNodeStarted     EventType = "node_started"
	EventNodeCompleted   EventType = "node_completed"
	EventCompleted       EventType = "completed"
	EventSuspended       EventType = "suspended"
	EventFailed          EventType = "failed"
	EventHandoff         EventType = "handoff"
	EventContextCanceled EventType = "context_canceled"
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
type StreamHandle[T, E any] interface {
	Events() <-chan RunEvent[T, E]
	Close()
	Done() error
}

// Runner controls lifecycle for start/resume executions.
type Runner[T, E any] interface {
	Start(ctx context.Context, threadID string, initialState T, opts ...RunOption[T, E]) (*RunResult[T, E], error)
	Resume(ctx context.Context, threadID string, opts ...RunOption[T, E]) (*RunResult[T, E], error)
	Stream(ctx context.Context, threadID string, initialState T, opts ...RunOption[T, E]) (StreamHandle[T, E], error)
	StreamResume(ctx context.Context, threadID string, opts ...RunOption[T, E]) (StreamHandle[T, E], error)
	// HandoffToBackground requests graceful termination of the active foreground execution on this runner instance only.
	// It cancels the in-process run, waits until the handoff checkpoint is persisted, then returns.
	// A background worker on any instance must call Resume with a new lease; cross-process handoff uses checkpoint + lease, not this API alone.
	HandoffToBackground(ctx context.Context, threadID string) error
}

// Sentinel errors for runner flow and validation.
var (
	ErrMaxStepsExceeded    = errors.New("flowy: max steps exceeded")
	ErrThreadNotFound      = errors.New("flowy: thread snapshot not found")
	ErrLegacyNext          = errors.New("flowy: Next(nodeID) was removed; use Completed() and graph edges")
	ErrLeaseOwnerRequired  = errors.New("flowy: WithRunLease owner is required when LeaseManager is configured")
	ErrLeaseLost           = errors.New("flowy: thread lease lost or expired")
	ErrNoActiveExecution   = errors.New("flowy: no active execution to hand off")
	ErrRetryBudgetExceeded = errors.New("flowy: per-node retry budget exceeded")
	ErrInvalidSnapshot     = errors.New("flowy: snapshot has invalid or empty execution pointer")
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
