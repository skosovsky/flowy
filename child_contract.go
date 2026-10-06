package flowy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"slices"
)

var (
	ErrChildDuplicate     = errors.New("flowy: duplicate child identity")
	ErrChildJoinInvalid   = errors.New("flowy: invalid child group")
	ErrChildRevision      = errors.New("flowy: stale child revision")
	ErrChildrenUnresolved = errors.New("flowy: children unresolved")
	ErrChildMergeConflict = errors.New("flowy: child merge conflict")
)

// ChildState is the persisted structured execution state, independent of domain types.
type ChildState string

const (
	ChildPlanned   ChildState = "planned"
	ChildQueued    ChildState = "queued"
	ChildRunning   ChildState = "running"
	ChildWaiting   ChildState = "waiting"
	ChildCompleted ChildState = "completed"
	ChildFailed    ChildState = "failed"
	ChildCanceled  ChildState = "canceled"
	ChildUnknown   ChildState = "unknown"
)

type ChildFailurePolicy string

const (
	ChildFailFast      ChildFailurePolicy = "fail_fast"
	ChildCollectErrors ChildFailurePolicy = "collect_errors"
)

// ChildSpec contains already projected/encoded isolated host input. Allocation
// counts named units only; it never stands in for a monetary reservation.
type ChildSpec struct {
	ID         string         `json:"id"`
	Input      []byte         `json:"input"`
	Allocation map[string]int `json:"allocation"`
}

// ChildGroupPlan is explicit compatibility-labelled composition, not an inline subgraph.
type ChildGroupPlan struct {
	Key            string             `json:"key"`
	Label          string             `json:"label"`
	MergeLabel     string             `json:"merge_label"`
	BudgetLabel    string             `json:"budget_label"`
	CancelLabel    string             `json:"cancel_label"`
	MaxConcurrency int                `json:"max_concurrency"`
	FailurePolicy  ChildFailurePolicy `json:"failure_policy"`
	Children       []ChildSpec        `json:"children"`
}

// ChildRecord retains an independently revisioned child and its raw outcome.
type ChildRecord struct {
	Spec               ChildSpec                      `json:"spec"`
	ExecutionID        string                         `json:"execution_id"`
	Revision           uint64                         `json:"revision"`
	Incarnation        uint64                         `json:"incarnation"`
	WaitID             string                         `json:"wait_id,omitempty"`
	State              ChildState                     `json:"state"`
	Result             []byte                         `json:"result,omitempty"`
	Error              string                         `json:"error,omitempty"`
	CancelRequested    bool                           `json:"cancel_requested"`
	CancelConfirmed    bool                           `json:"cancel_confirmed"`
	WaitResolution     *ChildWaitResolutionRecord     `json:"wait_resolution,omitempty"`
	CancelConfirmation *ChildCancelConfirmationRecord `json:"cancel_confirmation,omitempty"`
}

type ChildGroupRecord struct {
	ParentID        string                             `json:"parent_id"`
	Node            ExecutionPointer                   `json:"node"`
	Activation      uint64                             `json:"activation"`
	Plan            ChildGroupPlan                     `json:"plan"`
	Children        []ChildRecord                      `json:"children"`
	MergedIDs       []string                           `json:"merged_ids"`
	MergedResult    []byte                             `json:"merged_result,omitempty"`
	CancelRequested bool                               `json:"cancel_requested"`
	CancelRequest   *ChildCancelRequestRecord          `json:"cancel_request,omitempty"`
	Capacity        map[string]int                     `json:"capacity"`
	BudgetReturns   map[string]ChildBudgetReturnRecord `json:"budget_returns,omitempty"`
}

// PlanChildGroup validates and detaches a deterministic plan before persistence
// or dispatch. Revision-zero planned children are not a committed launch.
func PlanChildGroup(parentID string, node ExecutionPointer, activation uint64, plan ChildGroupPlan,
	available map[string]int) (ChildGroupRecord, error) {
	if parentID == "" || node == "" || activation == 0 || plan.Key == "" || plan.Label == "" || plan.MergeLabel == "" ||
		!validRuntimeText(
			parentID,
			string(node),
			plan.Key,
			plan.Label,
			plan.MergeLabel,
			plan.BudgetLabel,
			plan.CancelLabel,
		) ||
		plan.BudgetLabel == "" || plan.CancelLabel == "" || plan.MaxConcurrency <= 0 || len(plan.Children) == 0 ||
		(plan.FailurePolicy != ChildFailFast && plan.FailurePolicy != ChildCollectErrors) {
		return ChildGroupRecord{}, ErrChildJoinInvalid
	}
	remaining := maps.Clone(available)
	if !validChildCapacity(available) {
		return ChildGroupRecord{}, ErrBudgetExceeded
	}
	seen := make(map[string]bool)
	plan.Children = slices.Clone(plan.Children)
	for index, spec := range plan.Children {
		if spec.ID == "" || !validRuntimeText(spec.ID) {
			return ChildGroupRecord{}, ErrChildJoinInvalid
		}
		if seen[spec.ID] {
			return ChildGroupRecord{}, ErrChildDuplicate
		}
		seen[spec.ID] = true
		if err := allocateChildCounters(remaining, spec.Allocation); err != nil {
			return ChildGroupRecord{}, err
		}
		plan.Children[index] = cloneChildSpec(spec)
	}
	slices.SortFunc(plan.Children, func(a, b ChildSpec) int { return compareChildIDs(a.ID, b.ID) })
	group := ChildGroupRecord{
		ParentID:   parentID,
		Node:       node,
		Activation: activation,
		Plan:       plan,
		Children: make(
			[]ChildRecord,
			0,
			len(plan.Children),
		),
		MergedIDs:       nil,
		MergedResult:    nil,
		CancelRequested: false,
		CancelRequest:   nil,
		Capacity:        maps.Clone(available),
		BudgetReturns:   nil,
	}
	for _, spec := range plan.Children {
		group.Children = append(group.Children, ChildRecord{
			Spec:               cloneChildSpec(spec),
			ExecutionID:        childExecutionIdentity(parentID, node, activation, plan.Key, spec.ID),
			Revision:           0,
			Incarnation:        0,
			WaitID:             "",
			WaitResolution:     nil,
			CancelConfirmation: nil,
			State:              ChildPlanned,
			Result:             nil,
			Error:              "",
			CancelRequested:    false,
			CancelConfirmed:    false,
		})
	}
	return group, nil
}

func compareChildIDs(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func childExecutionIdentity(parentID string, node ExecutionPointer, activation uint64, group, child string) string {
	encoded, _ := json.Marshal([]any{"flowy-child", parentID, node, activation, group, child})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func cloneChildSpec(spec ChildSpec) ChildSpec {
	spec.Input, spec.Allocation = bytes.Clone(spec.Input), maps.Clone(spec.Allocation)
	return spec
}

func allocateChildCounters(remaining, allocation map[string]int) error {
	for name, units := range allocation {
		available, exists := remaining[name]
		if name == "" || !validRuntimeText(name) || units < 0 || !exists || available < units {
			return ErrBudgetExceeded
		}
		remaining[name] = available - units
	}
	return nil
}
