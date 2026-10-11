package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/skosovsky/toolsy/adapters/execution/filejournal"

	"github.com/skosovsky/flowy"
	pg "github.com/skosovsky/flowy/adapters/checkpointer/postgres"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

//nolint:funlen,gocognit // Finite CLI wiring mirrors the documented phase commands.
func run(
	ctx context.Context,
) error {
	if len(os.Args) != 2 {
		return errors.New("usage: approval_recovery start|allow|deny|edit|expired|revoked|resume|resolve|revoke")
	}
	dsn, schema, dir := os.Getenv("FLOWY_TEST_DATABASE_URL"), os.Getenv("APPROVAL_SCHEMA"), os.Getenv("APPROVAL_DIR")
	if dsn == "" || schema == "" || !filepath.IsAbs(dir) {
		return errors.New("explicit database URL, schema and absolute host directory required")
	}
	secret, err := hex.DecodeString(os.Getenv("APPROVAL_SECRET"))
	if err != nil || len(secret) < secretBytes {
		return errDenied
	}
	pool, err := openPool(ctx, dsn, schema)
	if err != nil {
		return err
	}
	defer pool.Close()
	ops, err := filejournal.Open(filepath.Join(dir, "operations.json"), 0)
	if err != nil {
		return err
	}
	h := &host{evidence: &postgresEvidence{pool: pool}, operations: ops, secret: secret, now: time.Now}
	fault := hook(func(point string) error {
		if point == os.Getenv("APPROVAL_CRASH") {
			fmt.Println("CRASH " + point)
			os.Exit(crashExit)
		}
		return nil
	})
	h.fault = fault
	store, err := pg.NewWaitExecutionStore(pool, profile())
	if err != nil {
		return err
	}
	wrapped := &faultExecutionStore{ExecutionStore: store, fault: fault}
	r, err := h.runner(wrapped)
	if err != nil {
		return err
	}
	const id = "approval-example"
	switch command := os.Args[1]; command {
	case "init":
		_, err = pool.Exec(ctx, pg.ExecutionSchemaSQL())
		if err == nil {
			_, err = pool.Exec(ctx, evidenceSchema)
		}
	case "start":
		// IDs are supplied by trusted host admission, checkpointed before preparation.
		_, err = r.Start(ctx, id, initial("provider-call", "logical-operation", input{Value: "approved value"}))
	case allowDecision, "deny", editDecision, expiredDecision, "revoked":
		d, requestErr := h.savedDecision(ctx, wrapped, id, command, dir)
		if requestErr != nil {
			return requestErr
		}
		_, err = h.deliver(ctx, r, wrapped, id, d)
	case "resume":
		_, token, loadErr := inspectState(ctx, wrapped, id)
		if loadErr != nil {
			return loadErr
		}
		result, resumeErr := r.Resume(ctx, token)
		if resumeErr != nil {
			return resumeErr
		}
		return json.NewEncoder(os.Stdout).Encode(result.State)
	case "resolve":
		s, _, loadErr := inspectState(ctx, wrapped, id)
		if loadErr != nil {
			return loadErr
		}
		err = h.resolveReceipt(ctx, operatorSubject, s)
	case "retry":
		_, token, loadErr := inspectState(ctx, wrapped, id)
		if loadErr != nil {
			return loadErr
		}
		_, err = h.authorizeRetry(ctx, operatorSubject, r, wrapped, token)
	case "revoke":
		err = h.evidence.Revoke(ctx, "logical-operation")
	default:
		return errors.New("unsupported command")
	}
	if err == nil {
		fmt.Println("ACK " + os.Args[1])
	}
	return err
}

var _ flowy.ExecutionStore = (*faultExecutionStore)(nil)
