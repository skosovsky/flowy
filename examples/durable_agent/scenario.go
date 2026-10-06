package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skosovsky/flowy"
	pg "github.com/skosovsky/flowy/adapters/checkpointer/postgres"
)

type scenarioReport struct {
	Final          *flowy.RunResult[parentState, effect]
	Replay         *flowy.RunResult[parentState, effect]
	Head           flowy.ExecutionEnvelope
	ChildHeads     map[string]flowy.ExecutionEnvelope
	Workers        []*worker
	ToolFault      bool
	ChildFault     bool
	Unresolved     bool
	ApprovalReplay bool
	TimerLost      bool
	Resolved       int
	Schema         string
}

func runScenario(ctx context.Context, dsn string, h *host) (scenarioReport, error) {
	var report scenarioReport
	admin, schema, err := createSchema(ctx, dsn)
	if err != nil {
		return report, err
	}
	defer admin.Close()
	report.Schema = schema
	report, err = executeScenario(ctx, dsn, h, report)
	return report, errors.Join(err, closeScenario(ctx, admin, report))
}

func executeScenario(ctx context.Context, dsn string, h *host, report scenarioReport) (scenarioReport, error) {
	w, err := report.open(ctx, dsn, h, true)
	if err != nil {
		return report, err
	}
	report.ToolFault, err = startTool(ctx, w, h)
	if err != nil {
		return report, err
	}
	// Replacement workers receive a fresh context, with no live trace parent.
	fresh, cancelRecovery := context.WithTimeout(context.Background(), time.Minute)
	defer cancelRecovery()
	ctx = fresh
	w, err = report.open(ctx, dsn, h, false)
	if err != nil {
		return report, err
	}
	err = recoverTool(ctx, w)
	if err != nil {
		return report, err
	}
	var token flowy.ResumeToken
	// Host clock advances to the timer deadline; the authenticated inbox is
	// drained before timer delivery. First committed wins, including late input.
	h.clock.at = h.deadline
	token, report.ApprovalReplay, report.TimerLost, err = deliverApproval(ctx, w, h)
	if err != nil {
		return report, err
	}
	w, err = report.open(ctx, dsn, h, true)
	if err != nil {
		return report, err
	}
	w.store.toolFault.Store(true) // Only the later parent/child fault is armed here.
	_, err = w.runner.Resume(ctx, token)
	if !errors.Is(err, errPublication) {
		return report, fmt.Errorf("child fault not reached: %w", err)
	}
	report.ChildFault = w.store.childFault.Load()
	w, err = report.open(ctx, dsn, h, false)
	if err != nil {
		return report, err
	}
	token, err = authoritativeToken(ctx, w.store, parentID)
	if err != nil {
		return report, err
	}
	_, err = w.runner.Resume(ctx, token)
	report.Unresolved = errors.Is(err, flowy.ErrChildrenUnresolved)
	if !report.Unresolved {
		return report, fmt.Errorf("expected unresolved child ownership: %w", err)
	}
	token, report.Resolved, err = resolveChildren(ctx, w)
	if err != nil {
		return report, err
	}
	err = report.finish(ctx, dsn, h, token)
	return report, err
}

func authoritativeToken(ctx context.Context, store flowy.ExecutionStore, id string) (flowy.ResumeToken, error) {
	head, err := store.LoadExecution(ctx, id)
	if err != nil {
		return flowy.ResumeToken{}, err
	}
	if err = flowy.ValidateExecutionIntegrity(head, id, head.Revision); err != nil {
		return flowy.ResumeToken{}, err
	}
	return flowy.ResumeToken{ThreadID: head.ExecutionID, SnapshotRevision: head.Revision}, nil
}

func deliverApproval(ctx context.Context, w *worker, h *host) (flowy.ResumeToken, bool, bool, error) {
	page, err := w.store.DiscoverDueWaits(ctx, h.clock.Now(), pg.DiscoveryCursor{}, discoveryLimit)
	if err != nil {
		return flowy.ResumeToken{}, false, false, err
	}
	if page.More || len(page.Diagnostics) != 0 || len(page.Waits) != 1 {
		return flowy.ResumeToken{}, false, false, fmt.Errorf("unexpected discovery page: %+v", page)
	}
	due := page.Waits[0]
	delivery := flowy.WaitDelivery{Generation: due.Wait.Generation, ID: "approved-event", Kind: flowy.WaitEvent,
		CorrelationID: "host-order", ExpectedRevision: due.Revision, Payload: []byte("approved")}
	// Authentication/authorization precede this call; core does not authenticate.
	accepted, err := w.runner.DeliverWait(ctx, parentID, delivery, h.deliveryContract())
	if err != nil {
		return flowy.ResumeToken{}, false, false, err
	}
	duplicate, err := w.runner.DeliverWait(ctx, parentID, delivery, h.deliveryContract())
	if err != nil {
		return flowy.ResumeToken{}, false, false, err
	}
	delivery.ID, delivery.Kind, delivery.Payload = "due-timer", flowy.WaitTimer, nil
	loser, err := w.runner.DeliverWait(ctx, parentID, delivery, h.deliveryContract())
	if err != nil {
		return flowy.ResumeToken{}, false, false, err
	}
	return loser.ResumeToken, duplicate.Replay &&
		duplicate.ResumeToken == accepted.ResumeToken, loser.Decision.Status == flowy.WaitLost, nil
}

func resolveChildren(ctx context.Context, w *worker) (flowy.ResumeToken, int, error) {
	token, err := authoritativeToken(ctx, w.store, parentID)
	if err != nil {
		return token, 0, err
	}
	head, err := w.store.LoadExecution(ctx, parentID)
	if err != nil {
		return token, 0, err
	}
	var groups map[string]flowy.ChildGroupRecord
	if err = json.Unmarshal(head.ChildrenPayload, &groups); err != nil {
		return token, 0, err
	}
	resolved := 0
	for _, group := range groups {
		for _, child := range group.Children {
			if child.State == flowy.ChildCompleted {
				continue
			}
			decision, decisionErr := verifiedDecision(ctx, w, group, child)
			if decisionErr != nil {
				return token, resolved, decisionErr
			}
			token, err = w.runner.ResolveChildOutcome(ctx, token, decision)
			if err != nil {
				return token, resolved, err
			}
			resolved++
		}
	}
	return token, resolved, nil
}

func createSchema(ctx context.Context, dsn string) (*pgxpool.Pool, string, error) {
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, "", err
	}
	var random [12]byte
	if _, err = rand.Read(random[:]); err != nil {
		admin.Close()
		return nil, "", err
	}
	schema := "flowy_blueprint_" + hex.EncodeToString(random[:])
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		return nil, "", err
	}
	return admin, schema, nil
}

func closeScenario(ctx context.Context, admin *pgxpool.Pool, report scenarioReport) error {
	for _, w := range report.Workers {
		w.pool.Close()
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	_, err := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{report.Schema}.Sanitize()+" CASCADE")
	return err
}

func verifiedDecision(ctx context.Context, w *worker, group flowy.ChildGroupRecord,
	child flowy.ChildRecord) (flowy.ChildOutcomeResolution, error) {
	childHead, err := w.store.LoadExecution(ctx, child.ExecutionID)
	if err != nil {
		return flowy.ChildOutcomeResolution{}, err
	}
	if err = flowy.ValidateExecutionIntegrity(childHead, child.ExecutionID, childHead.Revision); err != nil {
		return flowy.ChildOutcomeResolution{}, err
	}
	if childHead.Terminal == nil || childHead.Terminal.Status != flowy.RunStatusCompleted {
		return flowy.ChildOutcomeResolution{}, fmt.Errorf("child is not independently complete: %s", child.ExecutionID)
	}
	var state childInput
	if err = json.Unmarshal(childHead.Progress.StatePayload, &state); err != nil {
		return flowy.ChildOutcomeResolution{}, err
	}
	payload, err := json.Marshal(receipt{Value: state.Value, Units: childHead.RunMeta.BudgetCounts[computeCounter]})
	if err != nil {
		return flowy.ChildOutcomeResolution{}, err
	}
	// The host authenticates the operator and verifies this durable receipt.
	return flowy.ChildOutcomeResolution{Node: group.Node, Activation: group.Activation,
		GroupKey: group.Plan.Key, GroupLabel: group.Plan.Label, ChildID: child.Spec.ID,
		ExecutionID: child.ExecutionID, ChildRevision: child.Revision, DecisionID: "verified-" + child.Spec.ID,
		Reason: "host found durable terminal", Evidence: childHead.Digest,
		Result: flowy.ChildResult{State: flowy.ChildCompleted, Payload: payload}}, nil
}

func (report *scenarioReport) open(ctx context.Context, dsn string, h *host, faults bool) (*worker, error) {
	if len(report.Workers) > 0 {
		report.Workers[len(report.Workers)-1].pool.Close()
	}
	w, err := newWorker(ctx, dsn, report.Schema, h, faults)
	if err == nil {
		report.Workers = append(report.Workers, w)
	}
	return w, err
}

func startTool(ctx context.Context, w *worker, h *host) (bool, error) {
	if _, err := w.pool.Exec(ctx, pg.ExecutionSchemaSQL()); err != nil {
		return false, err
	}
	plan, err := h.model.Plan(ctx)
	if err != nil {
		return false, err
	}
	_, err = w.runner.Start(ctx, parentID, parentState{Plan: plan})
	if !errors.Is(err, errPublication) {
		return false, fmt.Errorf("tool fault not reached: %w", err)
	}
	return w.store.toolFault.Load(), nil
}

func (report *scenarioReport) finish(ctx context.Context, dsn string, h *host, token flowy.ResumeToken) error {
	w, err := report.open(ctx, dsn, h, false)
	if err != nil {
		return err
	}
	report.Final, err = w.runner.Resume(ctx, token)
	if err != nil {
		return err
	}
	report.Head, err = w.store.LoadExecution(ctx, parentID)
	if err != nil {
		return err
	}
	report.ChildHeads, err = loadChildHeads(ctx, w, report.Head)
	if err != nil {
		return err
	}
	w, err = report.open(ctx, dsn, h, false)
	if err != nil {
		return err
	}
	report.Replay, err = w.runner.Resume(ctx, report.Final.ResumeToken)
	return err
}

func recoverTool(ctx context.Context, w *worker) error {
	token, err := authoritativeToken(ctx, w.store, parentID)
	if err != nil {
		return err
	}
	_, err = w.runner.Resume(ctx, token)
	return err
}

func loadChildHeads(ctx context.Context, w *worker, parent flowy.ExecutionEnvelope,
) (map[string]flowy.ExecutionEnvelope, error) {
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(parent.ChildrenPayload, &groups); err != nil {
		return nil, err
	}
	heads := make(map[string]flowy.ExecutionEnvelope)
	for _, group := range groups {
		for _, child := range group.Children {
			head, err := w.store.LoadExecution(ctx, child.ExecutionID)
			if err != nil {
				return nil, err
			}
			if err = flowy.ValidateExecutionIntegrity(head, child.ExecutionID, head.Revision); err != nil {
				return nil, err
			}
			heads[child.Spec.ID] = head
		}
	}
	return heads, nil
}
