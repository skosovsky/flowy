package flowy

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"
)

// Node is the basic execution unit. It returns state update plus a directive.
type Node[T any] func(ctx context.Context, state T) (T, Directive, error)

// NodeMiddleware wraps a node handler.
type NodeMiddleware[T any] func(next Node[T]) Node[T]

// Reducer defines how to merge the current state with the update returned by a node.
type Reducer[T any] func(current T, update T) T

type directiveKind int

const (
	directiveNext directiveKind = iota + 1
	directiveCompleted
	directiveEnd
	directiveSuspend
	directiveRetry
	directiveEffect
)

// Directive is a platform command returned by nodes.
type Directive struct {
	kind         directiveKind
	nextNodeID   string
	reason       string
	maxAttempts  int
	fallbackNode string
	base         *Directive
	effect       any
}

func directiveWithKind(kind directiveKind) Directive {
	return Directive{
		kind:         kind,
		nextNodeID:   "",
		reason:       "",
		maxAttempts:  0,
		fallbackNode: "",
		base:         nil,
		effect:       nil,
	}
}

// Next moves execution to the provided node.
func Next(nodeID string) Directive {
	d := directiveWithKind(directiveNext)
	d.nextNodeID = nodeID
	return d
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

// Retry reruns current node with per-node budget and optional fallback.
func Retry(maxAttempts int, fallbackNode string) Directive {
	d := directiveWithKind(directiveRetry)
	d.maxAttempts = maxAttempts
	d.fallbackNode = fallbackNode
	return d
}

// Effect attaches a side effect payload to a base directive.
func Effect(base Directive, payload any) Directive {
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
	case directiveNext:
		return "next"
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
	default:
		return "unknown"
	}
}

// UnwrapDirective returns base directive and collected effects.
func UnwrapDirective(d Directive) (Directive, []any, error) {
	effects := make([]any, 0)
	current := d
	for current.kind == directiveEffect {
		effects = append(effects, current.effect)
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
	RunStatusCompleted RunStatus = "completed"
	RunStatusSuspended RunStatus = "suspended"
	RunStatusFailed    RunStatus = "failed"
)

// RunMetadata contains internal runner counters.
type RunMetadata struct {
	SegmentStartTime time.Time         `json:"segment_start_time"`
	RetryCounts      map[string]int    `json:"retry_counts"`
	StepCount        int               `json:"step_count"`
	TelemetryContext map[string]string `json:"telemetry_context,omitempty"`
}

// Snapshot is a persisted run snapshot.
type Snapshot[T any] struct {
	ThreadID string
	NodeID   string
	Revision int
	State    T
	RunMeta  RunMetadata
	Effects  []any
}

// Checkpointer persists and restores snapshots.
type Checkpointer[T any] interface {
	Save(ctx context.Context, snapshot Snapshot[T]) error
	Load(ctx context.Context, threadID string) (Snapshot[T], error)
	GetHistory(ctx context.Context, threadID string, limit int) ([]Snapshot[T], error)
	Prune(ctx context.Context, threadID string, retainCount int) error
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

// ResumeOption modifies resume behavior.
type ResumeOption[T any] interface {
	apply(*resumeOptions[T])
}

type resumeOptions[T any] struct {
	patches []func(*T)
}

type resumeOptionFunc[T any] func(*resumeOptions[T])

//nolint:unused // called through ResumeOption.apply in graphRunner.prepareResume
func (f resumeOptionFunc[T]) apply(opts *resumeOptions[T]) {
	f(opts)
}

// Compile-time check that resumeOptionFunc implements ResumeOption.
var _ ResumeOption[struct{}] = resumeOptionFunc[struct{}](nil)

// WithStatePatch mutates loaded state before the first resumed node executes.
func WithStatePatch[T any](patchFn func(state *T)) ResumeOption[T] {
	return resumeOptionFunc[T](func(opts *resumeOptions[T]) {
		if patchFn != nil {
			opts.patches = append(opts.patches, patchFn)
		}
	})
}

// RunResult is the final state returned to the application.
type RunResult[T any] struct {
	State   T
	Status  RunStatus
	Effects []any
	RunMeta RunMetadata
	NodeID  string
	Reason  string
}

type EventType string

const (
	EventNodeStarted   EventType = "node_started"
	EventNodeCompleted EventType = "node_completed"
	EventCompleted     EventType = "completed"
	EventSuspended     EventType = "suspended"
	EventFailed        EventType = "failed"
)

// RunEvent represents a single lifecycle event.
type RunEvent[T any] struct {
	Type     EventType
	NodeID   string
	State    T
	Effect   any
	Error    error
	Duration time.Duration
	Metrics  map[string]any
}

// StreamHandle controls the lifecycle of asynchronous graph streaming.
type StreamHandle[T any] interface {
	Events() <-chan RunEvent[T]
	Close()
	Done() error
}

// Runner controls lifecycle for start/resume executions.
type Runner[T any] interface {
	Start(ctx context.Context, threadID string, initialState T) (*RunResult[T], error)
	Resume(ctx context.Context, threadID string, opts ...ResumeOption[T]) (*RunResult[T], error)
	Stream(ctx context.Context, threadID string, initialState T) (StreamHandle[T], error)
	StreamResume(ctx context.Context, threadID string, opts ...ResumeOption[T]) (StreamHandle[T], error)
}

// Sentinel errors for runner flow and validation.
var (
	ErrMaxStepsExceeded = errors.New("flowy: max steps exceeded")
	ErrThreadNotFound   = errors.New("flowy: thread snapshot not found")
)

// EndNode is a terminal graph target for AddEdge/AddConditionalEdge.
const EndNode = "__end__"

type contextKey string

const nodeNameContextKey contextKey = "flowy.node_name"

// NodeNameFromContext returns current node id attached by runner.
func NodeNameFromContext(ctx context.Context) string {
	value, _ := ctx.Value(nodeNameContextKey).(string)
	return value
}

func withNodeName(ctx context.Context, nodeName string) context.Context {
	return context.WithValue(ctx, nodeNameContextKey, nodeName)
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
