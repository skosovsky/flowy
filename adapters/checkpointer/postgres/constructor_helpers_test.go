package postgres

import (
	"testing"

	"github.com/skosovsky/flowy"
)

func mustCheckpointer[T, E any](t testing.TB, db DB, serializer flowy.StateSerializer[T]) *Checkpointer[T, E] {
	t.Helper()
	cp, err := NewCheckpointer[T, E](db, serializer)
	if err != nil {
		t.Fatal(err)
	}
	return cp
}
func mustExecutionStore(t testing.TB, db DB) *ExecutionStore {
	t.Helper()
	store, err := NewExecutionStore(db)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
