//go:build integration

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/skosovsky/toolsy"
	"github.com/skosovsky/toolsy/adapters/execution/filejournal"
	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy"
	pg "github.com/skosovsky/flowy/adapters/checkpointer/postgres"
)

type processFixture struct {
	binary, dsn, schema, dir string
	pool                     *pgxpool.Pool
}

func (f *processFixture) command(t *testing.T, command, point string, wantSuccess bool) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, f.binary, command)
	cmd.Env = append(os.Environ(), "FLOWY_TEST_DATABASE_URL="+f.dsn, "APPROVAL_SCHEMA="+f.schema, "APPROVAL_DIR="+f.dir,
		"APPROVAL_SECRET="+hex.EncodeToString([]byte("01234567890123456789012345678901")), "APPROVAL_CRASH="+point)
	raw, err := cmd.CombinedOutput()
	if wantSuccess {
		require.NoError(t, err, string(raw))
	} else {
		require.Error(t, err, string(raw))
	}
	if point != "" {
		require.Contains(t, string(raw), "CRASH "+point)
		require.NotContains(t, string(raw), "ACK ")
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		require.Equal(t, 86, exit.ExitCode())
	}
	return string(raw)
}
func processSetup(t *testing.T, binary, dsn string) *processFixture {
	t.Helper()
	admin, err := pgxpool.New(t.Context(), dsn)
	require.NoError(t, err)
	var entropy [12]byte
	_, err = rand.Read(entropy[:])
	require.NoError(t, err)
	schema := "approval_" + hex.EncodeToString(entropy[:])
	_, err = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, dropErr := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		require.NoError(t, dropErr)
		admin.Close()
	})
	pool, err := openPool(t.Context(), dsn, schema)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(t.Context(), pg.ExecutionSchemaSQL())
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), evidenceSchema)
	require.NoError(t, err)
	return &processFixture{binary: binary, dsn: dsn, schema: schema, dir: t.TempDir(), pool: pool}
}

//nolint:gocognit // Explicit process phases preserve individual crash boundary assertions.
func TestProcessCrashRecovery(t *testing.T) {
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	require.NotEmpty(t, dsn, "mandatory PostgreSQL gate requires FLOWY_TEST_DATABASE_URL")
	binary := filepath.Join(t.TempDir(), "worker")
	build := exec.CommandContext(t.Context(), "go", "build", "-race", "-o", binary, ".")
	buildOutput, err := build.CombinedOutput()
	require.NoError(t, err, string(buildOutput))
	points := []string{
		"before_intent",
		"before_prepare",
		"after_challenge",
		"before_arm",
		"after_arm",
		"before_decision",
		"after_grant",
		"after_decision",
		"before_claim",
		"after_claim",
		"before_effect",
		"after_effect",
		"before_finish",
		"after_finish",
		"lost_delivery",
		"journal_commit",
		"state_commit",
		"after_capture",
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			// Arrange: every phase is a separate OS process; only PostgreSQL and journal files survive.
			f := processSetup(t, binary, dsn)
			startCrash := map[string]bool{
				"before_intent":   true,
				"before_prepare":  true,
				"after_challenge": true,
				"before_arm":      true,
				"after_arm":       true,
			}
			decisionCrash := point == "before_decision" || point == "after_grant" || point == "after_decision"
			if startCrash[point] {
				f.command(t, "start", point, false)
			} else {
				f.command(t, "start", "", true)
			}
			var generation string
			if point == "after_arm" {
				inspectStore, openErr := pg.NewWaitExecutionStore(f.pool, profile())
				require.NoError(t, openErr)
				head, loadErr := inspectStore.LoadExecution(t.Context(), "approval-example")
				require.NoError(t, loadErr)
				waits, waitErr := flowy.InspectExecutionWaits(head)
				require.NoError(t, waitErr)
				require.Len(t, waits, 1)
				generation = waits[0].Generation
				require.Equal(t, flowy.WaitArmed, waits[0].State)
			}
			var output string
			// Act: the killed process leaves a bounded lease; wait for its persisted expiry, never infer effect absence from it.
			time.Sleep(workerLease + 100*time.Millisecond)
			if point == "before_intent" {
				f.command(t, "start", "", true)
			} else if startCrash[point] {
				f.command(t, "resume", "", true)
			}
			if point == "after_arm" {
				inspectStore, openErr := pg.NewWaitExecutionStore(f.pool, profile())
				require.NoError(t, openErr)
				head, loadErr := inspectStore.LoadExecution(t.Context(), "approval-example")
				require.NoError(t, loadErr)
				waits, waitErr := flowy.InspectExecutionWaits(head)
				require.NoError(t, waitErr)
				require.Len(t, waits, 1)
				require.Equal(t, generation, waits[0].Generation)
				require.Equal(t, flowy.WaitArmed, waits[0].State)
			}
			if decisionCrash {
				f.command(t, "allow", point, false)
				time.Sleep(workerLease + 100*time.Millisecond)
			} else {
				f.command(t, "allow", "", true)
			}
			if point == "before_decision" || point == "after_grant" || point == "after_decision" {
				f.command(t, "allow", "", true)
			}
			effectCrash := !startCrash[point] && !decisionCrash
			if effectCrash {
				f.command(t, "resume", point, false)
				time.Sleep(workerLease + 100*time.Millisecond)
			}
			if point == "before_claim" || point == "after_claim" || point == "before_effect" {
				output = f.command(t, "resume", "", false)
				require.Contains(t, output, flowy.ErrActivityUnknown.Error())
				f.command(t, "resolve", "", false)
			} else {
				if point == "after_effect" || point == "before_finish" {
					output = f.command(t, "resume", "", false)
					require.Contains(t, output, flowy.ErrActivityUnknown.Error())
					f.command(t, "resolve", "", true)
				}
				output = f.command(t, "resume", "", true)
				var completed state
				require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(output)), &completed))
				require.NotNil(t, completed.Result)
				require.Equal(t, "logical-operation", completed.Result.OperationID)
				require.Equal(t, "provider-call", completed.CallID)
				replay := f.command(t, "resume", "", true)
				require.Equal(t, output, replay)
			}
			// Assert: actual external action and dispatch are independently counted in durable storage.
			var calls, writes int
			err = f.pool.QueryRow(t.Context(), `SELECT COALESCE(sum(calls),0),COALESCE(sum(writes),0) FROM approval_host_intents`).
				Scan(&calls, &writes)
			require.NoError(t, err)
			if point == "before_claim" || point == "after_claim" || point == "before_effect" {
				require.Zero(t, calls)
				require.Zero(t, writes)
			} else {
				require.Equal(t, 1, calls)
				require.Equal(t, 1, writes)
			}
			ops, openErr := filejournal.Open(filepath.Join(f.dir, "operations.json"), 0)
			require.NoError(t, openErr)
			store, storeErr := pg.NewWaitExecutionStore(f.pool, profile())
			require.NoError(t, storeErr)
			s, _, loadErr := inspectState(t.Context(), store, "approval-example")
			require.NoError(t, loadErr)
			record, found, inspectErr := ops.Inspect(t.Context(), s.Binding)
			require.NoError(t, inspectErr)
			if point == "before_claim" {
				require.False(t, found)
			} else {
				require.True(t, found)
				require.Equal(t, s.Binding, record.Binding)
				if calls == 1 {
					require.Equal(t, toolsy.OperationCompleted, record.State)
					require.NotEmpty(t, record.Result)
				}
			}
		})
	}
}

func TestPersistentDispatchCounter(t *testing.T) {
	// Arrange.
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	require.NotEmpty(t, dsn)
	f := processSetup(t, "unused", dsn)
	e := &postgresEvidence{pool: f.pool}
	m := mapping{OperationID: "op", CallID: "call", ActivityIdentity: "activity", Digest: "digest"}
	require.NoError(t, e.Map(t.Context(), m))
	// Act: a second external-service request is denied, but must still be counted.
	_, err := e.Write(t.Context(), "op", input{Value: "same"}, 1)
	require.NoError(t, err)
	_, err = e.Write(t.Context(), "op", input{Value: "same"}, 1)
	require.Error(t, err)
	// Assert.
	var calls, writes int
	require.NoError(
		t,
		f.pool.QueryRow(t.Context(), `SELECT calls,writes FROM approval_host_intents WHERE operation_id='op'`).
			Scan(&calls, &writes),
	)
	require.Equal(t, 2, calls)
	require.Equal(t, 1, writes)
}
