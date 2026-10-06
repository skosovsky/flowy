package flowy

import (
	"context"
	"errors"
	"time"
)

type ForkActivityMode string

const (
	ForkFake          ForkActivityMode = "fake"
	ForkLive          ForkActivityMode = "live"
	defaultForkPolicy                  = "fake"
)

var (
	ErrForkPolicy         = errors.New("flowy: fork execution policy required or incompatible")
	ErrForkTransform      = errors.New("flowy: fork transform or projection failed")
	ErrForkTargetConflict = errors.New("flowy: fork target already exists")
	ErrForkUnresolved     = errors.New("flowy: fork source has unresolved external work")
)

// ForkLineage is immutable creation provenance, not current host authorization.
type ForkLineage struct {
	Source           HistoricalCheckpointReference `json:"source"`
	SourceDescriptor ExecutionDescriptor           `json:"source_descriptor"`
	TargetDescriptor ExecutionDescriptor           `json:"target_descriptor"`
	TargetID         string                        `json:"target_id"`
	Mode             ForkActivityMode              `json:"mode"`
	PolicyLabel      string                        `json:"policy_label"`
	TransformLabel   string                        `json:"transform_label"`
	ProjectionLabel  string                        `json:"projection_label,omitempty"`
	CreatedAt        time.Time                     `json:"created_at"`
}

func (f ForkLineage) Validate() error {
	if f.Source.Validate() != nil || f.SourceDescriptor.Validate() != nil || f.TargetDescriptor.Validate() != nil ||
		!validRuntimeText(f.TargetID, f.Source.ExecutionID, f.PolicyLabel, f.TransformLabel, f.ProjectionLabel) ||
		f.TargetID == "" || f.TargetID == f.Source.ExecutionID || f.PolicyLabel == "" || f.TransformLabel == "" ||
		f.CreatedAt.IsZero() || f.CreatedAt.Location() != time.UTC {
		return ErrExecutionLifecycleInvalid
	}
	if f.Mode != ForkFake && f.Mode != ForkLive {
		return ErrForkPolicy
	}
	if f.Mode == ForkLive && f.ProjectionLabel == "" {
		return ErrForkPolicy
	}
	return nil
}

// ForkExecutionPolicy is supplied on each runner binding; persisted labels do
// not substitute for a live authorization gate or a fake dispatcher.
type ForkExecutionPolicy struct {
	Label        string
	Mode         ForkActivityMode
	Authorize    func(context.Context, ForkLineage) error
	FakeActivity func(context.Context, ActivityInvocation) ([]byte, error)
	FakeChild    ChildDispatcher
}

// ForkTransform is pure and receives detached source bytes under Source.
// Projection is a separate pure host sanitation contract for opaque references.
type ForkTransform struct {
	Label     string
	Source    ExecutionDescriptor
	Transform func(ExecutionProgress) (ExecutionProgress, error)
}

type ForkProjection struct {
	Label   string
	Project func(ExecutionProgress) (ExecutionProgress, error)
}

type ForkRequest struct {
	Source      HistoricalCheckpointReference
	TargetID    string
	Mode        ForkActivityMode
	PolicyLabel string
	Transform   ForkTransform
	Projection  *ForkProjection
}

func (r *DurableRunner[T, E]) checkForkPolicy(ctx context.Context, lineage *ForkLineage) error {
	if lineage == nil {
		return nil
	}
	if err := lineage.Validate(); err != nil {
		return err
	}
	policy := r.options.ForkPolicy
	if policy == nil || policy.Label != lineage.PolicyLabel || policy.Mode != lineage.Mode {
		return ErrForkPolicy
	}
	if lineage.Mode == ForkFake {
		if policy.FakeActivity == nil {
			return ErrForkPolicy
		}
		return nil
	}
	if policy.Authorize == nil {
		return ErrForkPolicy
	}
	if err := policy.Authorize(ctx, *lineage); err != nil {
		return errors.Join(ErrForkPolicy, err)
	}
	return context.Cause(ctx)
}

func forkSourceSettled(source ExecutionEnvelope) error {
	journal, err := executionActivityJournal(source)
	if err != nil {
		return err
	}
	for _, record := range journal {
		if record.State != ActivityCompleted && record.State != ActivityFailed {
			return ErrForkUnresolved
		}
	}
	groups, err := executionChildGroups(source)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if len(group.MergedIDs) != len(group.Children) {
			return ErrForkUnresolved
		}
		for _, child := range group.Children {
			if !settledChild(child) {
				return ErrForkUnresolved
			}
		}
	}
	waits, err := executionWaits(source)
	if err != nil {
		return err
	}
	for _, wait := range waits {
		if wait.State == WaitArmed {
			return ErrForkUnresolved
		}
	}
	return nil
}

func executionForkMode(envelope ExecutionEnvelope) ForkActivityMode {
	if envelope.Fork == nil {
		return ""
	}
	return envelope.Fork.Mode
}

func (c *executionCheckpointer[T, E]) forkActivityRequest(request ActivityRequest) (ActivityRequest, error) {
	if c.forkMode == ForkLive {
		dispatch := request.Dispatch
		request.Dispatch = func(ctx context.Context, invocation ActivityInvocation) ([]byte, error) {
			if err := c.authorizeForkDispatch(ctx); err != nil {
				return nil, err
			}
			return dispatch(ctx, invocation)
		}
		if reconcile := request.Reconcile; reconcile != nil {
			request.Reconcile = func(ctx context.Context, record ActivityRecord) ([]byte, error) {
				if err := c.authorizeForkDispatch(ctx); err != nil {
					return nil, err
				}
				return reconcile(ctx, record)
			}
		}
		return request, nil
	}
	if c.forkMode != ForkFake {
		return request, nil
	}
	if c.forkPolicy == nil || c.forkPolicy.FakeActivity == nil {
		return ActivityRequest{}, ErrForkPolicy
	}
	request.Dispatch, request.Reconcile, request.Classify = c.forkPolicy.FakeActivity, nil, nil
	return request, nil
}

func (c *executionCheckpointer[T, E]) authorizeForkDispatch(ctx context.Context) error {
	if c.forkMode != ForkLive {
		return nil
	}
	c.mu.Lock()
	lineage := cloneExecutionEnvelope(c.envelope).Fork
	c.mu.Unlock()
	policy := c.forkPolicy
	if lineage == nil || policy == nil || policy.Authorize == nil ||
		policy.Mode != lineage.Mode || policy.Label != lineage.PolicyLabel {
		return ErrForkPolicy
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if err := policy.Authorize(ctx, *lineage); err != nil {
		return errors.Join(ErrForkPolicy, err)
	}
	return context.Cause(ctx)
}

func (c *executionCheckpointer[T, E]) activityDispatchOrigin() ActivityOrigin {
	if c.forkMode == ForkFake {
		return ActivitySimulated
	}
	return ActivityLive
}
