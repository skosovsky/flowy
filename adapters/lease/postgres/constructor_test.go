package postgres

import (
	"errors"
	"testing"

	"github.com/skosovsky/flowy"
)

type nilDB struct{ DB }

func TestConstructorRejectsNilDatabase(t *testing.T) {
	t.Parallel()
	var typedDB *nilDB
	for _, db := range []DB{nil, typedDB} {
		// Arrange/Act.
		manager, err := NewLeaseManager(db)
		// Assert.
		if manager != nil || !errors.Is(err, flowy.ErrExecutionCapability) {
			t.Fatalf("manager=%v err=%v", manager, err)
		}
	}
}
