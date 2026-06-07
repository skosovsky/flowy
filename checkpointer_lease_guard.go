package flowy

import "context"

type leaseGuardCheckpointer[T, E any] struct {
	inner Checkpointer[T, E]
	lease LeaseManager
}

// NewLeaseGuardCheckpointer wraps a checkpointer with best-effort lease checks on DeleteIfIdle.
// Not atomic across separate stores; use adapter-native DeleteIfIdle for distributed deployments.
func NewLeaseGuardCheckpointer[T, E any](
	inner Checkpointer[T, E],
	lease LeaseManager,
) Checkpointer[T, E] {
	if inner == nil || lease == nil {
		return inner
	}
	return &leaseGuardCheckpointer[T, E]{inner: inner, lease: lease}
}

func (*leaseGuardCheckpointer[T, E]) isLeaseGuardCheckpointer() {}

func (c *leaseGuardCheckpointer[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
) (uint64, error) {
	return c.inner.Save(ctx, expectedRevision, snapshot)
}

func (c *leaseGuardCheckpointer[T, E]) Load(ctx context.Context, threadID string) (Snapshot[T, E], uint64, error) {
	return c.inner.Load(ctx, threadID)
}

func (c *leaseGuardCheckpointer[T, E]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]Snapshot[T, E], error) {
	return c.inner.GetHistory(ctx, threadID, limit)
}

func (c *leaseGuardCheckpointer[T, E]) Prune(ctx context.Context, threadID string, retainCount int) error {
	return c.inner.Prune(ctx, threadID, retainCount)
}

// Delete bypasses lease checks; runner retention policies use DeleteIfIdle only.
func (c *leaseGuardCheckpointer[T, E]) Delete(ctx context.Context, threadID string) error {
	return c.inner.Delete(ctx, threadID)
}

func (c *leaseGuardCheckpointer[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	holder, held, err := c.lease.Holder(ctx, threadID)
	if err != nil {
		return err
	}
	if held {
		caller := LeaseOwnerFromContext(ctx)
		if caller == "" || holder != caller {
			return ErrThreadLeaseBusy
		}
	}
	return c.inner.DeleteIfIdle(ctx, threadID)
}

func (c *leaseGuardCheckpointer[T, E]) transactionalCheckpointerInner() (TransactionalCheckpointer[T, E], bool) {
	txCP, ok := c.inner.(TransactionalCheckpointer[T, E])
	return txCP, ok
}

func (c *leaseGuardCheckpointer[T, E]) SaveWithOutbox(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
	enqueueFn func(context.Context) error,
) (uint64, error) {
	txCP, ok := c.transactionalCheckpointerInner()
	if !ok {
		return 0, ErrTransactionalOutboxUnsupported
	}
	return txCP.SaveWithOutbox(ctx, expectedRevision, snapshot, enqueueFn)
}

var _ TransactionalCheckpointer[any, any] = (*leaseGuardCheckpointer[any, any])(nil)
