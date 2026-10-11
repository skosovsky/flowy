package postgres

import (
	"context"
	"testing"
	"time"
)

func TestFractionalTTLIsRoundedUpBeforeAcquireAndRenewSQL(t *testing.T) {
	t.Parallel()
	// Arrange: keep the lease long enough to avoid wall-clock expiry in the fake.
	db := &leaseFakeDB{}
	manager, err := NewLeaseManager(db)
	if err != nil {
		t.Fatal(err)
	}
	// Act.
	lease, err := manager.Acquire(context.Background(), "fraction", "worker", time.Second+time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Renew(context.Background(), lease, time.Second+time.Nanosecond)
	// Assert: both SQL commands receive a whole microsecond beyond one second.
	if err != nil || len(db.ttlArgs) != 2 || db.ttlArgs[0] != 1.000001 || db.ttlArgs[1] != 1.000001 {
		t.Fatalf("renew=%v ttlArgs=%v", err, db.ttlArgs)
	}
}
