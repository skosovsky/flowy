package flowy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrLeaseHeld is returned when a thread lease is owned by another worker.
var ErrLeaseHeld = errors.New("flowy: thread lease held by another owner")

// ErrThreadLeaseBusy is returned when a thread already has an active lease (including same owner).
var ErrThreadLeaseBusy = errors.New("flowy: thread already has an active lease")

// LeaseManager provides exclusive ownership of a thread for safe handoff.
type LeaseManager interface {
	Acquire(ctx context.Context, threadID, owner string, ttl time.Duration) (ExecutionLease, error)
	Renew(ctx context.Context, lease ExecutionLease, ttl time.Duration) (ExecutionLease, error)
	Release(ctx context.Context, lease ExecutionLease) error
	IsHeld(ctx context.Context, threadID string) (bool, error)
	// Holder returns the active lease owner when held.
	Holder(ctx context.Context, threadID string) (owner string, held bool, err error)
}

type leaseRecord struct {
	owner       string
	expiresAt   time.Time
	incarnation uint64
}

// MemoryLeaseManager is a dev-only in-process lease store with TTL.
type MemoryLeaseManager struct {
	mu      sync.Mutex
	leases  map[string]leaseRecord
	fences  map[string]uint64
	nowFunc func() time.Time
}

// NewMemoryLeaseManager creates an in-memory lease manager.
func NewMemoryLeaseManager() *MemoryLeaseManager {
	return &MemoryLeaseManager{
		mu:      sync.Mutex{},
		leases:  make(map[string]leaseRecord),
		fences:  make(map[string]uint64),
		nowFunc: time.Now,
	}
}

func (m *MemoryLeaseManager) now() time.Time {
	if m.nowFunc != nil {
		return m.nowFunc()
	}
	return time.Now()
}

func (m *MemoryLeaseManager) Acquire(
	_ context.Context,
	threadID, owner string,
	ttl time.Duration,
) (ExecutionLease, error) {
	if threadID == "" || owner == "" {
		return ExecutionLease{}, errors.New("flowy: lease acquire requires threadID and owner")
	}
	if ttl <= 0 {
		return ExecutionLease{}, errors.New("flowy: lease ttl must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	if rec, ok := m.leases[threadID]; ok && rec.expiresAt.After(now) {
		if rec.owner != owner {
			return ExecutionLease{}, fmt.Errorf("%w: %s", ErrLeaseHeld, rec.owner)
		}
		return ExecutionLease{}, fmt.Errorf("%w: %s", ErrThreadLeaseBusy, rec.owner)
	}
	if m.fences[threadID] == ^uint64(0) {
		return ExecutionLease{}, ErrExecutionCapability
	}
	m.fences[threadID]++
	record := leaseRecord{owner: owner, expiresAt: now.Add(ttl), incarnation: m.fences[threadID]}
	m.leases[threadID] = record
	return ExecutionLease{
		ExecutionID: threadID,
		Owner:       owner,
		Incarnation: record.incarnation,
		ExpiresAt:   record.expiresAt,
	}, nil
}

func (m *MemoryLeaseManager) Renew(_ context.Context, lease ExecutionLease, ttl time.Duration) (ExecutionLease, error) {
	if lease.ExecutionID == "" || lease.Owner == "" || lease.Incarnation == 0 || ttl <= 0 {
		return ExecutionLease{}, ErrLeaseLost
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	rec, ok := m.leases[lease.ExecutionID]
	if !ok || !rec.expiresAt.After(now) || rec.owner != lease.Owner || rec.incarnation != lease.Incarnation {
		return ExecutionLease{}, ErrLeaseLost
	}
	rec.expiresAt = now.Add(ttl)
	m.leases[lease.ExecutionID] = rec
	lease.ExpiresAt = rec.expiresAt
	return lease, nil
}

func (m *MemoryLeaseManager) IsHeld(ctx context.Context, threadID string) (bool, error) {
	_, held, err := m.Holder(ctx, threadID)
	return held, err
}

func (m *MemoryLeaseManager) Holder(_ context.Context, threadID string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.leases[threadID]
	if !ok || !rec.expiresAt.After(m.now()) {
		return "", false, nil
	}
	return rec.owner, true, nil
}

func (m *MemoryLeaseManager) Release(_ context.Context, lease ExecutionLease) error {
	if lease.ExecutionID == "" || lease.Owner == "" || lease.Incarnation == 0 {
		return ErrLeaseLost
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.leases[lease.ExecutionID]
	if !ok {
		return nil
	}
	if rec.owner != lease.Owner || rec.incarnation != lease.Incarnation {
		return ErrLeaseLost
	}
	delete(m.leases, lease.ExecutionID)
	return nil
}

type executionLeaseKey struct{}

// WithExecutionLease carries the acquisition handle for storage-fenced writes.
// The handle is not authorization; the host owns access policy.
func WithExecutionLease(ctx context.Context, lease ExecutionLease) context.Context {
	return context.WithValue(ctx, executionLeaseKey{}, lease)
}

// ExecutionLeaseFromContext returns the acquisition handle, if supplied.
func ExecutionLeaseFromContext(ctx context.Context) (ExecutionLease, bool) {
	lease, ok := ctx.Value(executionLeaseKey{}).(ExecutionLease)
	return lease, ok
}
