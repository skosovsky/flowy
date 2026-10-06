package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/skosovsky/flowy"
)

const (
	parentID       = "approval-agent"
	computeCounter = "compute"
	hostOwner      = "host"
	parentMaxSteps = 8
	discoveryLimit = 8
	cleanupTimeout = 10 * time.Second
)

type parentState struct {
	Plan     []int  `json:"plan"`
	Receipt  string `json:"receipt"`
	Approved bool   `json:"approved"`
	Values   []int  `json:"values"`
}

type effect struct {
	Kind  string `json:"kind"`
	Units int    `json:"units"`
}
type childInput struct {
	Value int `json:"value"`
}
type receipt struct {
	Value     int    `json:"value"`
	Units     int    `json:"units"`
	Reference string `json:"reference"`
}

// These ports and their concrete messages belong to the host, not core.
type model interface {
	Plan(context.Context) ([]int, error)
}
type tool interface {
	Write(context.Context, string, int) (receipt, error)
}

type fakeModel struct{ calls atomic.Int32 }

func (m *fakeModel) Plan(context.Context) ([]int, error) {
	m.calls.Add(1)
	return []int{2, 3}, nil
}

// This service outlives workers. Downstream deduplication is its own guarantee.
type fakeTool struct {
	mu         sync.Mutex
	dispatches map[string]int
	writes     map[string]receipt
}

func newFakeTool() *fakeTool {
	return &fakeTool{dispatches: make(map[string]int), writes: make(map[string]receipt)}
}
func (t *fakeTool) Write(_ context.Context, key string, value int) (receipt, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dispatches[key]++
	if old, ok := t.writes[key]; ok {
		return old, nil
	}
	result := receipt{Value: value * 2, Units: 1, Reference: key}
	t.writes[key] = result
	return result, nil
}

type hostClock struct{ at time.Time }

func (c *hostClock) Now() time.Time { return c.at }

type host struct {
	model      model
	tool       tool
	clock      *hostClock
	deadline   time.Time
	childCalls atomic.Int32
	joins      atomic.Int32
	matches    atomic.Int32
	applies    atomic.Int32
	barrier    *childBarrier
}

// A deterministic finite barrier proves both durable terminals precede the
// injected parent publication fault. It is example synchronization, not core.
type childBarrier struct {
	completed atomic.Int32
	ready     chan struct{}
}

func (b *childBarrier) arrive(ctx context.Context) error {
	if b.completed.Add(1) == 2 {
		close(b.ready)
	}
	select {
	case <-b.ready:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (h *host) callTool(ctx context.Context, value int) (receipt, error) {
	input, err := json.Marshal(value)
	if err != nil {
		return receipt{}, err
	}
	out, err := flowy.CallActivity(ctx, flowy.ActivityRequest{
		Key: "write", Implementation: "host-idempotent-write-v1", Input: input,
		Dispatch: func(ctx context.Context, invocation flowy.ActivityInvocation) ([]byte, error) {
			var request int
			if decodeErr := json.Unmarshal(invocation.Input, &request); decodeErr != nil {
				return nil, decodeErr
			}
			result, writeErr := h.tool.Write(ctx, invocation.Identity, request)
			if writeErr != nil {
				return nil, writeErr
			}
			return json.Marshal(result)
		},
	})
	if err != nil {
		return receipt{}, err
	}
	var result receipt
	err = json.Unmarshal(out.Payload, &result)
	return result, err
}

func descriptor(kind string) flowy.ExecutionDescriptor {
	return flowy.ExecutionDescriptor{GraphID: "blueprint-" + kind, GraphRevision: "v1",
		StateCodec: "host-json-v1", EffectsCodec: "host-effects-json-v1", ExecutionContract: "aggregate-v1",
		ReplayPolicy: flowy.StepReplayPolicy{Label: "host-replay-safe-v1", Mode: flowy.StepReplaySafe}}
}

func waitProfile() flowy.WaitCapabilityProfile {
	return flowy.WaitCapabilityProfile{Label: "blueprint-host-v1", JournalOwner: "postgres", LeaseOwner: "postgres",
		TimerOwner: hostOwner, ClockOwner: hostOwner, RetryOwner: "runtime", RecoveryOwner: hostOwner}
}

func (h *host) options() flowy.DurableOptions {
	profile := waitProfile()
	return flowy.DurableOptions{Owner: "blueprint-worker", LeaseTTL: time.Minute, Clock: h.clock, WaitProfile: &profile}
}

func (h *host) waitSpec() flowy.DurableWaitSpec {
	return flowy.DurableWaitSpec{ID: "approval", CorrelationID: "host-order", Deadline: h.deadline,
		MatcherLabel: "authenticated-approval-v1", PayloadCodec: "host-text-v1", ContinuationLabel: "approve-v1",
		EventPointer: "children", TimeoutPointer: "children", WinnerPolicy: flowy.WaitFirstCommitted}
}

func (h *host) deliveryContract() flowy.WaitDeliveryContract[parentState] {
	return flowy.WaitDeliveryContract[parentState]{MatcherLabel: "authenticated-approval-v1",
		PayloadCodec: "host-text-v1", ContinuationLabel: "approve-v1",
		Match: func(_ context.Context, payload []byte) (bool, error) {
			h.matches.Add(1)
			return string(payload) == "approved", nil
		},
		Apply: func(_ context.Context, s parentState, d flowy.WaitDelivery) (parentState, error) {
			h.applies.Add(1)
			s.Approved = d.Kind == flowy.WaitEvent
			return s, nil
		}}
}

func ordered(outcomes []flowy.TypedChildOutcome[receipt]) ([]int, error) {
	if len(outcomes) != 2 || outcomes[0].ID != "a" || outcomes[1].ID != "b" {
		return nil, fmt.Errorf("unexpected child order: %+v", outcomes)
	}
	return []int{outcomes[0].Result.Value, outcomes[1].Result.Value}, nil
}
