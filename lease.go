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

// ErrThreadBusy is returned when a thread already has an active lease (including same owner).
var ErrThreadBusy = errors.New("flowy: thread already has an active lease")

// LeaseManager provides exclusive ownership of a thread for safe handoff.
type LeaseManager interface {
	Acquire(ctx context.Context, threadID, owner string, ttl time.Duration) error
	Renew(ctx context.Context, threadID, owner string, ttl time.Duration) error
	Release(ctx context.Context, threadID, owner string) error
}

type leaseRecord struct {
	owner     string
	expiresAt time.Time
}

// MemoryLeaseManager is a dev-only in-process lease store with TTL.
type MemoryLeaseManager struct {
	mu      sync.Mutex
	leases  map[string]leaseRecord
	nowFunc func() time.Time
}

// NewMemoryLeaseManager creates an in-memory lease manager.
func NewMemoryLeaseManager() *MemoryLeaseManager {
	return &MemoryLeaseManager{
		mu:      sync.Mutex{},
		leases:  make(map[string]leaseRecord),
		nowFunc: time.Now,
	}
}

func (m *MemoryLeaseManager) now() time.Time {
	if m.nowFunc != nil {
		return m.nowFunc()
	}
	return time.Now()
}

func (m *MemoryLeaseManager) Acquire(_ context.Context, threadID, owner string, ttl time.Duration) error {
	if threadID == "" || owner == "" {
		return errors.New("flowy: lease acquire requires threadID and owner")
	}
	if ttl <= 0 {
		return errors.New("flowy: lease ttl must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	if rec, ok := m.leases[threadID]; ok && rec.expiresAt.After(now) {
		if rec.owner != owner {
			return fmt.Errorf("%w: %s", ErrLeaseHeld, rec.owner)
		}
		return fmt.Errorf("%w: %s", ErrThreadBusy, rec.owner)
	}
	m.leases[threadID] = leaseRecord{owner: owner, expiresAt: now.Add(ttl)}
	return nil
}

func (m *MemoryLeaseManager) Renew(_ context.Context, threadID, owner string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	rec, ok := m.leases[threadID]
	if !ok || rec.expiresAt.Before(now) || rec.owner != owner {
		return fmt.Errorf("%w: %s", ErrLeaseHeld, rec.owner)
	}
	rec.expiresAt = now.Add(ttl)
	m.leases[threadID] = rec
	return nil
}

func (m *MemoryLeaseManager) Release(_ context.Context, threadID, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.leases[threadID]
	if !ok {
		return nil
	}
	if rec.owner != owner {
		return fmt.Errorf("%w: %s", ErrLeaseHeld, rec.owner)
	}
	delete(m.leases, threadID)
	return nil
}

type leaseOwnerKey struct{}

// WithLeaseOwner stores the active lease owner id in ctx.
func WithLeaseOwner(ctx context.Context, owner string) context.Context {
	return context.WithValue(ctx, leaseOwnerKey{}, owner)
}

// LeaseOwnerFromContext returns lease owner id from ctx.
func LeaseOwnerFromContext(ctx context.Context) string {
	owner, _ := ctx.Value(leaseOwnerKey{}).(string)
	return owner
}
