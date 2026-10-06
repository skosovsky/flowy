//go:build integration

package postgres

import (
	"testing"

	pglease "github.com/skosovsky/flowy/adapters/lease/postgres"
)

func mustPostgresLeaseManager(t testing.TB, db pglease.DB) *pglease.LeaseManager {
	t.Helper()
	manager, err := pglease.NewLeaseManager(db)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}
