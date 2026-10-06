package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/skosovsky/flowy"
	pg "github.com/skosovsky/flowy/adapters/checkpointer/postgres"
	"github.com/skosovsky/flowy/checkpoint"
	flowyotel "github.com/skosovsky/flowy/ext/otel"
)

var errPublication = errors.New("host injected publication failure")

type faultStore struct {
	*pg.ExecutionStore

	toolFault  atomic.Bool
	childFault atomic.Bool
	enabled    bool
}

func (s *faultStore) CommitExecution(ctx context.Context, revision uint64, lease flowy.ExecutionLease,
	target flowy.ExecutionEnvelope,
) (flowy.ExecutionEnvelope, error) {
	if s.enabled && target.ExecutionID == parentID {
		if hasCompletedChild(target) && s.childFault.CompareAndSwap(false, true) {
			return flowy.ExecutionEnvelope{}, errPublication // BEFORE backend publication, AFTER isolated work.
		}
		if hasCompletedActivity(target) && s.toolFault.CompareAndSwap(false, true) {
			published, err := s.ExecutionStore.CommitExecution(ctx, revision, lease, target)
			if err != nil {
				return published, err
			}
			return flowy.ExecutionEnvelope{}, errPublication // AFTER PostgreSQL publication, lost reply.
		}
	}
	return s.ExecutionStore.CommitExecution(ctx, revision, lease, target)
}

func hasCompletedActivity(target flowy.ExecutionEnvelope) bool {
	var journal map[string]flowy.ActivityRecord
	if json.Unmarshal(target.JournalPayload, &journal) != nil {
		return false
	}
	for _, record := range journal {
		if record.State == flowy.ActivityCompleted {
			return true
		}
	}
	return false
}

func hasCompletedChild(target flowy.ExecutionEnvelope) bool {
	var groups map[string]flowy.ChildGroupRecord
	if json.Unmarshal(target.ChildrenPayload, &groups) != nil {
		return false
	}
	for _, group := range groups {
		for _, child := range group.Children {
			if child.State == flowy.ChildCompleted {
				return true
			}
		}
	}
	return false
}

type worker struct {
	pool   *pgxpool.Pool
	store  *faultStore
	runner *flowy.DurableRunner[parentState, effect]
}

func newWorker(ctx context.Context, dsn, schema string, h *host, faults bool) (*worker, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 4
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	store, err := pg.NewWaitExecutionStore(pool, waitProfile())
	if err != nil {
		pool.Close()
		return nil, err
	}
	w := &worker{pool: pool, store: &faultStore{ExecutionStore: store, enabled: faults}, runner: nil}
	w.runner, err = h.parentRunner(w.store)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return w, nil
}

func (h *host) parentRunner(store *faultStore) (*flowy.DurableRunner[parentState, effect], error) {
	b := flowy.NewGraph[parentState, effect](func(_, update parentState) parentState { return update })
	b.AddNode("tool", h.parentTool).AddNode("approval", h.approval)
	b.AddNode("children", func(ctx context.Context, s parentState) (parentState, flowy.Directive, error) {
		return h.children(ctx, s, store)
	})
	b.AddEdge("tool", "approval").AddEdge("approval", "children")
	b.AllowNoOutgoingRoute("children").SetEntryPoint("tool")
	b.Use(flowyotel.TracingMiddleware[parentState, effect](otel.Tracer("blueprint")))
	graph, err := b.Compile(flowy.WithMaxSteps(parentMaxSteps), flowy.WithNamedBudget("tool", 1),
		flowy.WithNamedBudget(computeCounter, 2))
	if err != nil {
		return nil, err
	}
	return flowy.NewDurableRunner(graph, store, descriptor("parent"), checkpoint.JSONSerializer[parentState]{},
		checkpoint.JSONSerializer[[]effect]{}, h.options())
}

func (h *host) parentTool(ctx context.Context, s parentState) (parentState, flowy.Directive, error) {
	result, err := h.callTool(ctx, 1)
	if err != nil {
		return s, flowy.End(), err
	}
	if err = flowy.UseBudget(ctx, "tool", result.Units); err != nil {
		return s, flowy.End(), err
	}
	s.Receipt = result.Reference
	return s, flowy.WithEffects(flowy.Completed(), []effect{{Kind: "tool", Units: result.Units}}), nil
}

func (h *host) approval(_ context.Context, s parentState) (parentState, flowy.Directive, error) {
	return s, flowy.Await(h.waitSpec()), nil
}

func (h *host) childRunner(store flowy.ExecutionStore) (*flowy.DurableRunner[childInput, effect], error) {
	b := flowy.NewGraph[childInput, effect](func(_, update childInput) childInput { return update })
	b.AddNode("child", func(ctx context.Context, s childInput) (childInput, flowy.Directive, error) {
		r, err := h.callTool(ctx, s.Value)
		if err != nil {
			return s, flowy.End(), err
		}
		if err = flowy.UseBudget(ctx, computeCounter, r.Units); err != nil {
			return s, flowy.End(), err
		}
		s.Value = r.Value
		return s, flowy.WithEffects(flowy.End(), []effect{{Kind: computeCounter, Units: r.Units}}), nil
	}).AllowNoOutgoingRoute("child").SetEntryPoint("child")
	b.Use(flowyotel.TracingMiddleware[childInput, effect](otel.Tracer("blueprint")))
	graph, err := b.Compile(flowy.WithMaxSteps(2), flowy.WithNamedBudget(computeCounter, 1))
	if err != nil {
		return nil, err
	}
	return flowy.NewDurableRunner(graph, store, descriptor("child"), checkpoint.JSONSerializer[childInput]{},
		checkpoint.JSONSerializer[[]effect]{}, h.options())
}

func (h *host) dispatcher(store flowy.ExecutionStore) (flowy.ChildDispatcher, error) {
	runner, err := h.childRunner(store)
	if err != nil {
		return nil, err
	}
	return flowy.TypedChildDispatcher(
		checkpoint.JSONSerializer[childInput]{},
		checkpoint.JSONSerializer[receipt]{},
		func(ctx context.Context, invocation flowy.TypedChildInvocation[childInput]) (flowy.TypedChildResult[receipt], error) {
			h.childCalls.Add(1)
			result, runErr := runner.Start(ctx, invocation.Runtime.ExecutionID, invocation.Input)
			if runErr != nil {
				return flowy.TypedChildResult[receipt]{}, runErr
			}
			if barrierErr := h.barrier.arrive(ctx); barrierErr != nil {
				return flowy.TypedChildResult[receipt]{}, barrierErr
			}
			return flowy.TypedChildResult[receipt]{State: flowy.ChildCompleted,
				Result: receipt{Value: result.State.Value, Units: result.RunMeta.BudgetCounts[computeCounter]}}, nil
		},
	)
}

func (h *host) children(ctx context.Context, s parentState, store flowy.ExecutionStore,
) (parentState, flowy.Directive, error) {
	if !s.Approved {
		return s, flowy.End(), errors.New("approval required by host")
	}
	specs, err := flowy.ProjectChildSpecs(ctx, s,
		[]flowy.ChildProjectionSpec{{ID: "b", Allocation: map[string]int{computeCounter: 1}},
			{ID: "a", Allocation: map[string]int{computeCounter: 1}}},
		checkpoint.JSONSerializer[parentState]{}, checkpoint.JSONSerializer[childInput]{},
		func(_ context.Context, parent parentState, id string) (childInput, error) {
			index := 0
			if id == "b" {
				index = 1
			}
			return childInput{Value: parent.Plan[index]}, nil
		})
	if err != nil {
		return s, flowy.End(), err
	}
	dispatch, err := h.dispatcher(store)
	if err != nil {
		return s, flowy.End(), err
	}
	group, err := flowy.RunChildren(ctx, flowy.ChildGroupPlan{Key: "team", Label: "host-projection-v1",
		MergeLabel: "id-order-v1", BudgetLabel: "compute-v1", CancelLabel: "host-v1", MaxConcurrency: 2,
		FailurePolicy: flowy.ChildCollectErrors, Children: specs}, map[string]int{computeCounter: 2}, dispatch)
	if err != nil {
		return s, flowy.End(), err
	}
	group, err = returnUsage(ctx, group)
	if err != nil {
		return s, flowy.End(), err
	}
	values, err := flowy.JoinTypedChildren(
		ctx,
		group,
		checkpoint.JSONSerializer[receipt]{},
		checkpoint.JSONSerializer[[]int]{},
		func(_ context.Context, outcomes []flowy.TypedChildOutcome[receipt]) ([]int, error) {
			h.joins.Add(1)
			return ordered(outcomes)
		},
	)
	if err != nil {
		return s, flowy.End(), err
	}
	if err = flowy.UseBudget(ctx, computeCounter, 2); err != nil {
		return s, flowy.End(), err
	}
	s.Values = values
	return s, flowy.WithEffects(flowy.End(), []effect{{Kind: "join", Units: 2}}), nil
}

func returnUsage(ctx context.Context, group flowy.ChildGroupRecord) (flowy.ChildGroupRecord, error) {
	for _, child := range group.Children {
		var measured receipt
		if err := json.Unmarshal(child.Result, &measured); err != nil {
			return group, err
		}
		if measured.Units != 1 {
			return group, fmt.Errorf("unexpected host usage: %d", measured.Units)
		}
		updated, err := flowy.ReturnChildBudget(ctx, group, flowy.ChildBudgetReturn{ChildID: child.Spec.ID,
			ChildRevision: child.Revision, DecisionID: "usage-" + child.Spec.ID, Reason: "host measured usage",
			Evidence: "persisted child receipt", Used: map[string]int{computeCounter: measured.Units}})
		if err != nil {
			return group, err
		}
		group = updated
	}
	return group, nil
}
