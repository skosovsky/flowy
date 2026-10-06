package flowy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryLeaseReusedOwnerCannotMutateSuccessor(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	now := time.Now()
	manager := NewMemoryLeaseManager()
	manager.nowFunc = func() time.Time { return now }
	old, err := manager.Acquire(ctx, "run", "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	current, acquireErr := manager.Acquire(ctx, "run", "worker", time.Minute)
	if acquireErr != nil {
		t.Fatal(acquireErr)
	}
	// Act: the text owner is identical, but the acquisition is different.
	_, renewErr := manager.Renew(ctx, old, time.Hour)
	releaseErr := manager.Release(ctx, old)
	// Assert.
	if current.Incarnation <= old.Incarnation || !errors.Is(renewErr, ErrLeaseLost) ||
		!errors.Is(releaseErr, ErrLeaseLost) {
		t.Fatalf("ABA accepted: old=%+v current=%+v renew=%v release=%v", old, current, renewErr, releaseErr)
	}
	if _, conflictErr := manager.Acquire(ctx, "run", "other", time.Minute); !errors.Is(conflictErr, ErrLeaseHeld) {
		t.Fatalf("successor released: %v", conflictErr)
	}
	if validReleaseErr := manager.Release(ctx, current); validReleaseErr != nil {
		t.Fatal(validReleaseErr)
	}
	newest, newestErr := manager.Acquire(ctx, "run", "worker", time.Minute)
	if newestErr != nil || newest.Incarnation <= current.Incarnation {
		t.Fatalf("release recycled fence: %+v %v", newest, newestErr)
	}
}

func TestMemoryLeaseRejectsRenewAtExactExpiry(t *testing.T) {
	// Arrange.
	ctx := context.Background()
	now := time.Now()
	manager := NewMemoryLeaseManager()
	manager.nowFunc = func() time.Time { return now }
	lease, err := manager.Acquire(ctx, "run", "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	now = lease.ExpiresAt
	_, renewErr := manager.Renew(ctx, lease, time.Minute)
	// Assert: equality is expired, not still held.
	if !errors.Is(renewErr, ErrLeaseLost) {
		t.Fatalf("expired acquisition renewed: %v", renewErr)
	}
}
