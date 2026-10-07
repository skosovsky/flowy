package main

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/skosovsky/toolsy"

	"github.com/skosovsky/flowy"
)

// authorizeRetry is a trusted operator protocol. Fencing an old downstream attempt
// and proving no receipt is stronger than observing timeout/lease expiry.
// Failure between its separate stores remains fail-closed and requires operator completion.
//
//nolint:gocognit // Each durable retry stage is explicitly checked for safe replay.
func (h *host) authorizeRetry(ctx context.Context, principal string, r *flowy.DurableRunner[state, flowy.NoEffect],
	store flowy.ExecutionStore, token flowy.ResumeToken) (flowy.ResumeToken, error) {
	if principal != operatorSubject {
		return flowy.ResumeToken{}, errDenied
	}
	s, current, err := inspectState(ctx, store, token.ThreadID)
	if err != nil {
		return flowy.ResumeToken{}, err
	}
	if current != token {
		return flowy.ResumeToken{}, flowy.ErrConcurrencyConflict
	}
	if err = s.validate(true); err != nil {
		return flowy.ResumeToken{}, err
	}
	head, err := store.LoadExecution(ctx, token.ThreadID)
	if err != nil {
		return flowy.ResumeToken{}, err
	}
	var journal map[string]flowy.ActivityRecord
	if err = json.Unmarshal(head.JournalPayload, &journal); err != nil {
		return flowy.ResumeToken{}, err
	}
	var activity flowy.ActivityRecord
	for _, record := range journal {
		if record.State == flowy.ActivityUnknown || record.State == flowy.ActivityPrepared {
			activity = record
		}
	}
	if activity.Identity == "" || len(activity.Attempts) != 1 {
		return flowy.ResumeToken{}, errEvidence
	}
	operation, found, err := h.operations.Inspect(ctx, s.Binding)
	if err != nil || !found {
		return flowy.ResumeToken{}, errors.Join(errEvidence, err)
	}
	const proof = "downstream-old-attempt-fenced-no-receipt"
	authorized := operation.State == toolsy.OperationRetryAuthorized && operation.ResolutionProofID == proof
	if !authorized && operation.State != toolsy.OperationUnknown &&
		(operation.State != toolsy.OperationInProgress || h.now().Before(operation.LeaseUntil)) {
		return flowy.ResumeToken{}, errEvidence
	}
	if err = h.evidence.FenceAbsence(ctx, s.OperationID, len(activity.Attempts)); err != nil {
		return flowy.ResumeToken{}, err
	}
	if err = h.fault.hit("after_retry_fence"); err != nil {
		return flowy.ResumeToken{}, err
	}
	if !authorized {
		if err = h.operations.Resolve(
			ctx,
			toolsy.OperationResolution{Binding: s.Binding, ExpectedAttemptID: operation.AttemptID,
				State: toolsy.OperationRetryAuthorized, ProofID: proof, Now: h.now()},
		); err != nil {
			return flowy.ResumeToken{}, err
		}
	}
	if err = h.fault.hit("after_retry_operation"); err != nil {
		return flowy.ResumeToken{}, err
	}
	if activity.State == flowy.ActivityPrepared {
		if len(activity.Resolutions) != 1 || activity.Resolutions[0].Evidence != proof ||
			activity.Resolutions[0].Action != flowy.ActivityResolveRetry {
			return flowy.ResumeToken{}, errEvidence
		}
		return token, nil
	}

	resolved, err := r.ResolveActivity(
		ctx,
		token,
		flowy.ActivityResolution{
			Identity:          activity.Identity,
			InputDigest:       activity.InputDigest,
			Implementation:    activity.Implementation,
			DecisionID:        "fenced-absence",
			Action:            flowy.ActivityResolveRetry,
			Reason:            "operator verified downstream absence and fenced original attempt",
			Evidence:          "downstream-old-attempt-fenced-no-receipt",
			SafeRetryContract: retryPolicy().SafeRetryContract,
		},
	)
	if err == nil {
		err = h.fault.hit("after_retry_activity")
	}
	return resolved, err
}
