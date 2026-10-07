package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/skosovsky/toolsy"

	"github.com/skosovsky/flowy"
)

type decision struct {
	Label       string                  `json:"label"`
	ID          string                  `json:"id"`
	Generation  string                  `json:"generation"`
	Revision    uint64                  `json:"revision"`
	CallID      string                  `json:"call_id"`
	OperationID string                  `json:"operation_id"`
	Binding     toolsy.OperationBinding `json:"binding"`
	Kind        string                  `json:"kind"`
	Input       input                   `json:"input"`
	IssuedAt    time.Time               `json:"issued_at"`
	ExpiresAt   time.Time               `json:"expires_at"`
}
type signedDecision struct {
	Decision  decision `json:"decision"`
	Signature []byte   `json:"signature"`
}

func (h *host) sign(d decision) signedDecision {
	mac := hmac.New(sha256.New, h.secret)
	_, _ = mac.Write(mustJSON(d))
	return signedDecision{Decision: d, Signature: mac.Sum(nil)}
}
func (h *host) authenticate(d signedDecision) error {
	if len(h.secret) < secretBytes {
		return errDenied
	}
	expected := h.sign(d.Decision)
	if !hmac.Equal(expected.Signature, d.Signature) {
		return errDenied
	}
	if d.Decision.Label != envelopeLabel || d.Decision.ID == "" || d.Decision.Generation == "" ||
		d.Decision.IssuedAt.IsZero() || !d.Decision.ExpiresAt.After(d.Decision.IssuedAt) {
		return errEvidence
	}
	switch d.Decision.Kind {
	case allowDecision, "deny", editDecision, expiredDecision, "revoked":
		return nil
	default:
		return errEvidence
	}
}
func (h *host) deliver(ctx context.Context, r *flowy.DurableRunner[state, flowy.NoEffect], store flowy.ExecutionStore,
	executionID string, signed signedDecision) (flowy.WaitDeliveryResult, error) {
	if err := h.authenticate(signed); err != nil {
		return flowy.WaitDeliveryResult{}, err
	}
	d := signed.Decision
	head, err := store.LoadExecution(ctx, executionID)
	if err != nil {
		return flowy.WaitDeliveryResult{}, err
	}
	waits, err := flowy.InspectExecutionWaits(head)
	if err != nil {
		return flowy.WaitDeliveryResult{}, err
	}
	var addressed *flowy.DurableWaitRecord
	for _, w := range waits {
		if w.Generation == d.Generation {
			copyWait := w
			addressed = &copyWait
			break
		}
	}
	if addressed == nil {
		return flowy.WaitDeliveryResult{}, flowy.ErrWaitStale
	}
	// Duplicate delivery is handled by the runtime's immutable ledger, before pure callbacks.
	payload := mustJSON(d)
	delivery := flowy.WaitDelivery{
		Generation:       d.Generation,
		ID:               d.ID,
		Kind:             flowy.WaitEvent,
		CorrelationID:    d.OperationID,
		ExpectedRevision: d.Revision,
		Payload:          payload,
	}
	if addressed.State == flowy.WaitArmed {
		if err = h.admit(ctx, head, d); err != nil {
			return flowy.WaitDeliveryResult{}, err
		}
	}

	result, err := r.DeliverWait(ctx, executionID, delivery, deliveryContract())
	if err == nil {
		err = h.fault.hit("after_decision")
	}
	return result, err
}
func (h *host) admit(ctx context.Context, head flowy.ExecutionEnvelope, d decision) error {
	var s state
	if err := json.Unmarshal(head.Progress.StatePayload, &s); err != nil {
		return err
	}
	if err := s.validate(true); err != nil {
		return err
	}
	if d.CallID != s.CallID || d.OperationID != s.OperationID || d.Binding != s.Binding {
		return errEvidence
	}
	if d.Kind != editDecision && d.Input != s.Input {
		return errEvidence
	}
	if err := h.fault.hit("before_decision"); err != nil {
		return err
	}
	if d.Kind == allowDecision {
		if err := h.evidence.Allowed(ctx, s.OperationID); err != nil {
			return err
		}
		if err := h.operations.PutGrant(ctx, toolsy.ApprovalGrant{ID: d.ID, Issuer: hostOwner, Binding: s.Binding,
			IssuedAt: d.IssuedAt, ExpiresAt: d.ExpiresAt, AllowRecovery: true}); err != nil {
			return err
		}
	}
	return h.fault.hit("after_grant")
}

func deliveryContract() flowy.WaitDeliveryContract[state] {
	return flowy.WaitDeliveryContract[state]{
		MatcherLabel:      envelopeLabel,
		PayloadCodec:      envelopeLabel,
		ContinuationLabel: envelopeLabel,
		Match: func(_ context.Context, payload []byte) (bool, error) {
			var d decision
			err := json.Unmarshal(payload, &d)
			return err == nil && d.Label == envelopeLabel, err
		},
		Apply: func(_ context.Context, s state, delivery flowy.WaitDelivery) (state, error) {
			if delivery.Kind == flowy.WaitTimer {
				s.Decision = expiredDecision
				return s, nil
			}
			var d decision
			if err := json.Unmarshal(delivery.Payload, &d); err != nil {
				return s, err
			}
			if d.Binding != s.Binding || d.CallID != s.CallID || d.OperationID != s.OperationID {
				return s, errEvidence
			}
			s.Decision = d.Kind
			if d.Kind == allowDecision {
				s.GrantID = d.ID
			}
			if d.Kind == editDecision {
				s.Input = d.Input
			}
			return s, nil
		},
	}
}
func inspectState(ctx context.Context, store flowy.ExecutionStore, id string) (state, flowy.ResumeToken, error) {
	head, err := store.LoadExecution(ctx, id)
	if err != nil {
		return state{}, flowy.ResumeToken{}, err
	}
	if err = flowy.ValidateExecutionIntegrity(head, id, head.Revision); err != nil {
		return state{}, flowy.ResumeToken{}, err
	}
	var s state
	err = json.Unmarshal(head.Progress.StatePayload, &s)
	if err != nil {
		return s, flowy.ResumeToken{}, err
	}
	token, err := flowy.InspectExecutionResume(ctx, store, id)
	if err == nil && token.SnapshotRevision != head.Revision {
		return s, flowy.ResumeToken{}, flowy.ErrConcurrencyConflict
	}
	return s, token, err
}

func (h *host) request(
	ctx context.Context,
	store flowy.ExecutionStore,
	id, kind string,
	edited input,
) (signedDecision, error) {
	s, _, err := inspectState(ctx, store, id)
	if err != nil {
		return signedDecision{}, err
	}
	head, err := store.LoadExecution(ctx, id)
	if err != nil {
		return signedDecision{}, err
	}
	waits, err := flowy.InspectExecutionWaits(head)
	if err != nil {
		return signedDecision{}, err
	}
	for _, w := range waits {
		if w.State == flowy.WaitArmed {
			if kind != editDecision {
				edited = s.Input
			}
			return h.sign(
				decision{
					Label:       envelopeLabel,
					ID:          w.Generation + "/" + kind,
					Generation:  w.Generation,
					Revision:    head.Revision,
					CallID:      s.CallID,
					OperationID: s.OperationID,
					Binding:     s.Binding,
					Kind:        kind,
					Input:       edited,
					IssuedAt:    h.now().UTC(),
					ExpiresAt:   h.now().UTC().Add(time.Hour),
				},
			), nil
		}
	}
	return signedDecision{}, errors.New("host: no armed approval")
}
