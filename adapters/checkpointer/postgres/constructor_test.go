package postgres

import (
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

func TestConstructorsRejectNilCollaborators(t *testing.T) {
	t.Parallel()
	var typedDB *fakeDB
	var typedCodec *checkpoint.JSONSerializer[int]
	profile := flowy.WaitCapabilityProfile{
		Label:         "profile",
		JournalOwner:  "store",
		LeaseOwner:    "store",
		TimerOwner:    "host",
		ClockOwner:    "host",
		RetryOwner:    "runtime",
		RecoveryOwner: "runtime",
	}
	for _, db := range []DB{nil, typedDB} {
		// Arrange/Act.
		cp, cpErr := NewCheckpointer[int, flowy.NoEffect](db, checkpoint.JSONSerializer[int]{})
		store, storeErr := NewExecutionStore(db)
		waitStore, waitErr := NewWaitExecutionStore(db, profile)
		// Assert.
		if cp != nil || store != nil || waitStore != nil || !errors.Is(cpErr, flowy.ErrExecutionCapability) ||
			!errors.Is(storeErr, flowy.ErrExecutionCapability) ||
			!errors.Is(waitErr, flowy.ErrExecutionCapability) {
			t.Fatalf("cp=%v store=%v wait=%v", cpErr, storeErr, waitErr)
		}
	}
	db := &admissionDB{DB: nil, begins: 0}
	for _, codec := range []flowy.StateSerializer[int]{nil, typedCodec} {
		// Act/Assert: a present DB cannot make a missing codec usable.
		cp, err := NewCheckpointer[int, flowy.NoEffect](db, codec)
		if cp != nil || !errors.Is(err, flowy.ErrExecutionCapability) || db.begins != 0 {
			t.Fatalf("cp=%v err=%v begins=%d", cp, err, db.begins)
		}
	}
}
