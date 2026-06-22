package flowy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLeaseGuardCheckpointerDelegatesSaveWithOutbox(t *testing.T) {
	t.Parallel()

	type state struct{}
	inner := &transactionalMemoryCP[state, NoEffect]{memoryCP: newMemoryCP[state, NoEffect]()}
	guarded, ok := NewLeaseGuardCheckpointer[state, NoEffect](inner, &noopLeaseManager{}).(TransactionalCheckpointer[state, NoEffect])
	if !ok {
		t.Fatal("expected lease guard to implement TransactionalCheckpointer")
	}

	var enqueueCalled bool
	rev, err := guarded.SaveWithOutbox(context.Background(), 0, Snapshot[state, NoEffect]{
		ThreadID:         "lease-tx-th",
		ExecutionPointer: "work",
		State:            state{},
		RunMeta:          RunMetadata{HandoffStatus: HandoffStatusEnqueued},
	}, func(_ context.Context, tx TransactionHandle, savedRevision uint64) error {
		enqueueCalled = true
		if tx == nil {
			return errors.New("missing outbox tx token")
		}
		if savedRevision != 1 {
			return errors.New("unexpected saved revision")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("SaveWithOutbox: %v", err)
	}
	if rev != 1 || !enqueueCalled {
		t.Fatalf("expected rev=1 enqueueCalled=true, got rev=%d enqueue=%v", rev, enqueueCalled)
	}
}

type noopLeaseManager struct{}

func (noopLeaseManager) Acquire(context.Context, string, string, time.Duration) error { return nil }
func (noopLeaseManager) Renew(context.Context, string, string, time.Duration) error   { return nil }
func (noopLeaseManager) Release(context.Context, string, string) error                { return nil }
func (noopLeaseManager) IsHeld(context.Context, string) (bool, error)                 { return false, nil }
func (noopLeaseManager) Holder(context.Context, string) (string, bool, error) {
	return "", false, nil
}
