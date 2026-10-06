//go:build integration

package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestNativeLeaseSameOwnerABA(t *testing.T) {
	// Arrange: independent connections reuse the same text owner after expiry.
	ctx, pool := racePool(t)
	id := testThreadID(t)
	first, second := mustPostgresLeaseManager(t, pool), mustPostgresLeaseManager(t, pool)
	old, err := first.Acquire(ctx, id, "reused-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Exact expiry is controlled by the test, not a timing-dependent sleep.
	if _, expireErr := pool.Exec(
		ctx,
		"UPDATE flowy_leases SET expires_at = clock_timestamp() - interval '1 second' WHERE thread_id = $1",
		id,
	); expireErr != nil {
		t.Fatal(expireErr)
	}
	current, acquireErr := second.Acquire(ctx, id, "reused-owner", time.Minute)
	if acquireErr != nil {
		t.Fatal(acquireErr)
	}
	// Act.
	_, renewErr := first.Renew(ctx, old, time.Hour)
	releaseErr := first.Release(ctx, old)
	// Assert: stale same-owner operations preserve the successor and its expiry.
	if current.Incarnation <= old.Incarnation || !errors.Is(renewErr, flowy.ErrLeaseLost) ||
		!errors.Is(releaseErr, flowy.ErrLeaseLost) {
		t.Fatalf("native ABA accepted: old=%+v current=%+v renew=%v release=%v", old, current, renewErr, releaseErr)
	}
	var incarnation uint64
	var expiry time.Time
	if scanErr := pool.QueryRow(ctx, "SELECT incarnation, expires_at FROM flowy_leases WHERE thread_id = $1", id).
		Scan(&incarnation, &expiry); scanErr != nil {
		t.Fatal(scanErr)
	}
	if incarnation != current.Incarnation || !expiry.Equal(current.ExpiresAt) {
		t.Fatal("stale mutation changed successor")
	}
	if validReleaseErr := second.Release(ctx, current); validReleaseErr != nil {
		t.Fatal(validReleaseErr)
	}
	newest, newestErr := first.Acquire(ctx, id, "reused-owner", time.Minute)
	if newestErr != nil || newest.Incarnation <= current.Incarnation {
		t.Fatalf("release reset counter: %+v %v", newest, newestErr)
	}
	if _, busyErr := second.Acquire(
		ctx,
		id,
		"reused-owner",
		time.Minute,
	); !errors.Is(
		busyErr,
		flowy.ErrThreadLeaseBusy,
	) {
		t.Fatalf("active same owner reacquired: %v", busyErr)
	}
}
