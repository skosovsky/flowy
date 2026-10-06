package flowy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

// reservationFixture is a host-owned external port, not a core financial schema.
type reservationFixture struct {
	held     atomic.Bool
	reserves atomic.Int32
	releases atomic.Int32
	queries  atomic.Int32
	children atomic.Int32
}

func (p *reservationFixture) reserve(context.Context, flowy.ActivityInvocation) ([]byte, error) {
	p.reserves.Add(1)
	p.held.Store(true)
	return []byte("opaque-host-receipt"), nil
}

func (p *reservationFixture) reconcileReserve(context.Context, flowy.ActivityRecord) ([]byte, error) {
	p.queries.Add(1)
	if !p.held.Load() {
		return nil, errors.New("reservation requires host investigation")
	}
	return []byte("opaque-host-receipt"), nil
}

func (p *reservationFixture) release(context.Context, flowy.ActivityInvocation) ([]byte, error) {
	p.releases.Add(1)
	p.held.Store(false)
	return []byte("opaque-host-release"), nil
}

func (p *reservationFixture) reconcileRelease(context.Context, flowy.ActivityRecord) ([]byte, error) {
	p.queries.Add(1)
	if p.held.Load() {
		return nil, errors.New("release requires host investigation")
	}
	return []byte("opaque-host-release"), nil
}

func reservationChildNode(port *reservationFixture) flowy.Node[durableTestState, flowy.NoEffect] {
	return func(ctx context.Context, state durableTestState) (durableTestState, flowy.Directive, error) {
		plan := persistedChildPlan()
		group, err := flowy.PrepareChildren(ctx, plan, nil)
		if err != nil {
			return state, flowy.End(), err
		}
		reservation, err := flowy.CallActivity(ctx, flowy.ActivityRequest{Key: "reserve/" + plan.Key,
			Implementation: "host-reserve", Input: []byte(group.Children[0].ExecutionID), Dispatch: port.reserve,
			Reconcile: port.reconcileReserve})
		if err != nil {
			return state, flowy.End(), err
		}
		group, err = flowy.RunChildren(
			ctx,
			plan,
			nil,
			func(context.Context, flowy.ChildInvocation) (flowy.ChildResult, error) {
				port.children.Add(1)
				if !port.held.Load() {
					return flowy.ChildResult{}, errors.New("child launched without confirmed reservation")
				}
				return flowy.ChildResult{State: flowy.ChildCompleted}, nil
			},
		)
		if err != nil {
			return state, flowy.End(), err
		}
		group, err = flowy.ReturnChildBudget(ctx, group, flowy.ChildBudgetReturn{
			ChildID:       group.Children[0].Spec.ID,
			ChildRevision: group.Children[0].Revision,
			DecisionID:    "usage",
			Reason:        "host usage",
			Evidence:      "host meter",
			Used:          nil,
		})
		if err == nil {
			_, err = flowy.JoinChildren(
				ctx,
				group,
				func(context.Context, []flowy.ChildRecord) ([]byte, error) { return nil, nil },
			)
		}
		if err == nil {
			_, err = flowy.CallActivity(ctx, flowy.ActivityRequest{
				Key:            "release/" + plan.Key,
				Implementation: "host-release",
				Input:          reservation.Payload,
				Dispatch:       port.release,
				Reconcile:      port.reconcileRelease,
			})
		}
		return state, flowy.End(), err
	}
}

func TestChildHostReservationPortRecoversWithoutRepeatingExternalEffects(t *testing.T) {
	for _, failAt := range []int32{5, 13} {
		t.Run(map[int32]string{5: "reserve outcome", 13: "release outcome"}[failAt], func(t *testing.T) {
			// Arrange: external host port mutates before its aggregate outcome commit fails.
			ctx := context.Background()
			store := &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil), failAt: failAt}
			port := &reservationFixture{}
			runner := childJoinRunner(t, store, reservationChildNode(port))
			// Act.
			first, err := runner.Start(ctx, "reservation", durableTestState{})
			if first == nil || !errors.Is(err, errInjectedCommit) {
				t.Fatalf("outcome fault missing: %v", err)
			}
			if failAt == 5 && port.children.Load() != 0 {
				t.Fatal("child launched before reservation was durably confirmed")
			}
			_, err = runner.Resume(ctx, first.ResumeToken)
			// Assert: unknown external effect was queried, not blindly replayed.
			if err != nil || port.reserves.Load() != 1 || port.releases.Load() != 1 || port.queries.Load() != 1 ||
				port.children.Load() != 1 ||
				port.held.Load() {
				t.Fatalf(
					"port recovery: %v reserve=%d release=%d queries=%d children=%d",
					err,
					port.reserves.Load(),
					port.releases.Load(),
					port.queries.Load(),
					port.children.Load(),
				)
			}
		})
	}
}
