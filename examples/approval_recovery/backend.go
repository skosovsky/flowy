package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/skosovsky/toolsy"
)

// gatedStore instruments the actual backend without replacing its binding checks.
type gatedStore struct {
	operationStore

	host *host
}

func (s *gatedStore) Claim(ctx context.Context, claim toolsy.OperationClaim) (toolsy.ClaimResult, error) {
	// Preparation has no grant and must reach real approval_required checks.
	if claim.GrantID != "" {
		if err := s.host.evidence.Allowed(ctx, claim.Binding.OperationID); err != nil {
			return toolsy.ClaimResult{}, err
		}
		if err := s.host.fault.hit("before_claim"); err != nil {
			return toolsy.ClaimResult{}, err
		}
	}
	result, err := s.operationStore.Claim(ctx, claim)
	if err == nil && result.Dispatch {
		err = s.host.fault.hit("after_claim")
	}
	return result, err
}
func (s *gatedStore) Finish(ctx context.Context, finish toolsy.OperationFinish) error {
	if finish.State == toolsy.OperationCompleted {
		if err := s.host.fault.hit("before_finish"); err != nil {
			return err
		}
	}
	if err := s.operationStore.Finish(ctx, finish); err != nil {
		return err
	}
	if finish.State == toolsy.OperationCompleted {
		return s.host.fault.hit("after_finish")
	}
	return nil
}
func (h *host) execute(ctx context.Context, s state, attempt int, yield func(toolsy.Chunk) error) error {
	if err := h.evidence.Allowed(ctx, s.OperationID); err != nil {
		return err
	}
	tool, err := toolsy.NewTool(toolName, "Write approved host value",
		func(ctx context.Context, _ *toolsy.RunEnv, in input) (receipt, error) {
			if s.GrantID == "" {
				return receipt{}, errDenied
			}
			if faultErr := h.fault.hit("before_effect"); faultErr != nil {
				return receipt{}, faultErr
			}
			r, writeErr := h.evidence.Write(ctx, s.OperationID, in, attempt)
			if writeErr != nil {
				return receipt{}, writeErr
			}
			if faultErr := h.fault.hit("after_effect"); faultErr != nil {
				return receipt{}, faultErr
			}
			return r, nil
		}, toolsy.WithRequiresConfirmation())
	if err != nil {
		return err
	}
	store := &gatedStore{operationStore: h.operations, host: h}
	p, err := toolsy.NewOperationProfile(toolsy.OperationProfileConfig{Store: store,
		Prepare: func(_ context.Context, call toolsy.PreparedCall) (toolsy.OperationIntent, error) {
			return toolsy.OperationIntent{
				Namespace:         envelopeLabel,
				Scope:             "tenant",
				Subject:           operatorSubject,
				OperationID:       s.OperationID,
				PolicyFingerprint: "current",
				CanonicalDigest:   s.Digest,
				CanonicalRules:    "host-json",
				DownstreamKey:     s.OperationID,
				AttemptID:         fmt.Sprintf("%s/%d", s.CallID, attempt),
				GrantID:           s.GrantID,
				DisplayJSON:       call.Input.ArgsJSON,
			}, nil
		}, Codec: toolsy.JSONResultCodec[receipt, string]{}, Issuer: hostOwner, Clock: h.now, Lease: workerLease})
	if err != nil {
		return err
	}
	reg, err := toolsy.NewRegistryBuilder(toolsy.WithExecutionProfile(p)).Add(tool).Build()
	if err != nil {
		return err
	}
	return reg.Execute(
		ctx,
		toolsy.ToolCall{ToolName: toolName, Input: toolsy.ToolInput{CallID: s.CallID, ArgsJSON: mustJSON(s.Input)}},
		yield,
	)
}

// resolveReceipt is an explicit authenticated operator action outside Reconcile.
// A stored downstream receipt proves the completed effect; a missing receipt never authorizes retry.
func (h *host) resolveReceipt(ctx context.Context, principal string, s state) error {
	if principal != operatorSubject {
		return errDenied
	}
	if err := s.validate(true); err != nil {
		return err
	}
	result, err := h.evidence.Receipt(ctx, s.OperationID)
	if err != nil || result.OperationID != s.OperationID || result.Value != s.Input.Value {
		return errors.Join(errEvidence, err)
	}
	record, found, err := h.operations.Inspect(ctx, s.Binding)
	if err != nil || !found {
		return errors.Join(errEvidence, err)
	}
	raw := mustJSON(result)
	chunk := toolsy.Chunk{
		Event:       toolsy.EventResult,
		Data:        raw,
		MimeType:    toolsy.MimeTypeJSON,
		TypedResult: result,
		Envelope: toolsy.NewResultEnvelope(
			result,
			raw,
			toolsy.MimeTypeJSON,
			toolsy.DeliveryClassStructured,
			toolsy.AudienceModel,
			nil,
		),
	}
	encoded, err := (toolsy.JSONResultCodec[receipt, string]{}).EncodeResult(chunk)
	if err != nil {
		return err
	}
	return h.operations.Resolve(ctx, toolsy.OperationResolution{Binding: s.Binding, ExpectedAttemptID: record.AttemptID,
		State: toolsy.OperationCompleted, Result: encoded, ProofID: "verified-downstream-receipt", Now: h.now()})
}
