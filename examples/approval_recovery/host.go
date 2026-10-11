// Package main demonstrates host-owned authenticated approval and activity recovery.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/skosovsky/toolsy"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
)

const (
	envelopeLabel   = "approval-recovery"
	hostOwner       = "host"
	operatorSubject = "operator"
	toolName        = "write"
	allowDecision   = "allow"
	editDecision    = "edit"
	expiredDecision = "expired"
	maxSteps        = 32
	secretBytes     = 32
	crashExit       = 86
	workerLease     = 2 * time.Second
)

var errDenied = errors.New("host: authorization denied")
var errEvidence = errors.New("host: missing or conflicting evidence")
var errFault = errors.New("host: injected boundary failure")

type input struct {
	Value string `json:"value"`
}
type receipt struct {
	OperationID string `json:"operation_id"`
	Value       string `json:"value"`
	Accepted    bool   `json:"accepted"`
}
type state struct {
	Label          string                  `json:"label"`
	CallID         string                  `json:"call_id"`
	OperationID    string                  `json:"operation_id"`
	Input          input                   `json:"input"`
	Digest         string                  `json:"digest"`
	Binding        toolsy.OperationBinding `json:"binding"`
	GrantID        string                  `json:"grant_id"`
	Decision       string                  `json:"decision"`
	Round          int                     `json:"round"`
	Result         *receipt                `json:"result"`
	CapturePending bool                    `json:"capture_pending"`
}
type mapping struct {
	CallID           string                  `json:"call_id"`
	OperationID      string                  `json:"operation_id"`
	ActivityIdentity string                  `json:"activity_identity"`
	Digest           string                  `json:"digest"`
	Binding          toolsy.OperationBinding `json:"binding"`
}

// The host owns these ports. They are not shared core schemas.
type evidenceStore interface {
	Map(context.Context, mapping) error
	Lookup(context.Context, string) (mapping, error)
	Allowed(context.Context, string) error
	Revoke(context.Context, string) error
	Capture(context.Context, string, string) error
	Write(context.Context, string, input, int) (receipt, error)
	FenceAbsence(context.Context, string, int) error
	Receipt(context.Context, string) (receipt, error)
}
type operationStore interface {
	toolsy.OperationStore
	toolsy.ApprovalIssuerStore
}
type hook func(string) error

func (f hook) hit(point string) error {
	if f != nil {
		return f(point)
	}
	return nil
}

type host struct {
	evidence   evidenceStore
	operations operationStore
	secret     []byte
	now        func() time.Time
	fault      hook
}

func digest(in input) string {
	raw, _ := json.Marshal(in)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func initial(callID, operationID string, in input) state {
	return state{Label: envelopeLabel, CallID: callID, OperationID: operationID, Input: in, Digest: digest(in)}
}
func (s state) validate(bound bool) error {
	if s.Label != envelopeLabel || s.CallID == "" || s.OperationID == "" || s.Digest != digest(s.Input) {
		return errEvidence
	}
	if bound && (s.Binding.OperationID != s.OperationID || s.Binding.Subject != operatorSubject ||
		s.Binding.Scope != "tenant" || s.Binding.Namespace != envelopeLabel || s.Binding.Tool != toolName ||
		s.Binding.CanonicalDigest == "" || s.Binding.ManifestFingerprint == "" || s.Binding.PolicyFingerprint != "current" ||
		s.Binding.DownstreamKey != s.OperationID) {
		return errEvidence
	}
	return nil
}

func profile() flowy.WaitCapabilityProfile {
	return flowy.WaitCapabilityProfile{Label: envelopeLabel, JournalOwner: hostOwner, LeaseOwner: hostOwner,
		TimerOwner: hostOwner, ClockOwner: hostOwner, RetryOwner: "runtime", RecoveryOwner: hostOwner}
}
func (h *host) runner(store flowy.ExecutionStore) (*flowy.DurableRunner[state, flowy.NoEffect], error) {
	b := flowy.NewGraph[state, flowy.NoEffect](func(_, update state) state { return update })
	b.AddNode("prepare", h.prepare).
		AddNode("wait", h.wait).
		AddNode("decision", h.decision).
		AddNode("effect", h.effect).
		AddNode("capture", h.capture)
	b.AddEdge("prepare", "wait")
	b.AllowNoOutgoingRoute("wait").AllowNoOutgoingRoute("capture")
	b.AddEdge("effect", "capture")
	b.AddConditionalEdge("decision", func(_ context.Context, s state) (string, error) {
		if s.Decision == editDecision {
			return "prepare", nil
		}
		return "effect", nil
	}, "prepare", "effect").SetEntryPoint("prepare")
	g, err := b.Compile(flowy.WithMaxSteps(maxSteps))
	if err != nil {
		return nil, err
	}
	p := profile()
	return flowy.NewDurableRunner(g, store, flowy.ExecutionDescriptor{
		GraphID:           envelopeLabel,
		GraphRevision:     "bound-approval",
		StateCodec:        envelopeLabel,
		EffectsCodec:      "none",
		ExecutionContract: "sealed-approval",
		ReplayPolicy:      flowy.StepReplayPolicy{Label: "host-checkpoints", Mode: flowy.StepReplaySafe},
	},
		checkpoint.JSONSerializer[state]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "approval-worker", LeaseTTL: workerLease, WaitProfile: &p})
}

func (h *host) prepare(ctx context.Context, s state) (state, flowy.Directive, error) {
	if err := s.validate(false); err != nil {
		return s, flowy.End(), err
	}
	if err := h.fault.hit("before_prepare"); err != nil {
		return s, flowy.End(), err
	}
	// No grant is supplied during preparation. The actual backend must pause before dispatch.
	s.GrantID = ""
	var pending *toolsy.PendingApprovalError
	err := h.execute(ctx, s, 1, func(toolsy.Chunk) error { return nil })
	if !errors.As(err, &pending) {
		return s, flowy.End(), errors.Join(errEvidence, err)
	}
	s.Binding = pending.Challenge.Binding
	s.Decision = ""
	if err = s.validate(true); err != nil {
		return s, flowy.End(), err
	}
	if err = h.fault.hit("after_challenge"); err != nil {
		return s, flowy.End(), err
	}
	return s, flowy.Completed(), nil
}
func (h *host) wait(_ context.Context, s state) (state, flowy.Directive, error) {
	if err := s.validate(true); err != nil {
		return s, flowy.End(), err
	}
	return s, flowy.Await(flowy.DurableWaitSpec{
		ID:                "approval",
		CorrelationID:     s.OperationID,
		Deadline:          h.now().UTC().Add(time.Hour),
		MatcherLabel:      envelopeLabel,
		PayloadCodec:      envelopeLabel,
		ContinuationLabel: envelopeLabel,
		EventPointer:      "decision",
		TimeoutPointer:    "decision",
		WinnerPolicy:      flowy.WaitFirstCommitted,
	}), nil
}
func (h *host) decision(_ context.Context, s state) (state, flowy.Directive, error) {
	if s.Decision == editDecision {
		s.Digest = digest(s.Input)
		s.Binding = toolsy.OperationBinding{}
		s.GrantID = ""
		s.Round++
	}
	if s.Decision != allowDecision && s.Decision != editDecision {
		return s, flowy.End(), nil
	}
	return s, flowy.Completed(), nil
}
func (h *host) effect(ctx context.Context, s state) (state, flowy.Directive, error) {
	if err := s.validate(true); err != nil {
		return s, flowy.End(), err
	}
	// Authorization for new effects occurs inside Dispatch; replay/reconcile may recover an old outcome even after revocation.
	outcome, err := flowy.CallActivity(ctx, flowy.ActivityRequest{Key: toolName, Implementation: envelopeLabel,
		Input: mustJSON(s), Retry: retryPolicy(),
		Dispatch: func(ctx context.Context, invocation flowy.ActivityInvocation) ([]byte, error) {
			m := mapping{
				CallID:           s.CallID,
				OperationID:      s.OperationID,
				ActivityIdentity: invocation.Identity,
				Digest:           s.Digest,
				Binding:          s.Binding,
			}
			if mapErr := h.evidence.Map(ctx, m); mapErr != nil {
				return nil, mapErr
			}
			if authErr := h.evidence.Allowed(ctx, s.OperationID); authErr != nil {
				return nil, authErr
			}
			if captureErr := h.evidence.Capture(ctx, s.OperationID, "before"); captureErr != nil {
				return nil, captureErr
			}
			return h.dispatch(ctx, s, invocation.Attempt)
		}, Reconcile: func(ctx context.Context, record flowy.ActivityRecord) ([]byte, error) {
			return h.reconcile(ctx, s, record.Identity)
		}})
	if err != nil {
		return s, flowy.End(), err
	}
	var result receipt
	if err = json.Unmarshal(
		outcome.Payload,
		&result,
	); err != nil || result.OperationID != s.OperationID ||
		result.Value != s.Input.Value {
		return s, flowy.End(), errors.Join(errEvidence, err)
	}
	s.Result = &result
	return s, flowy.Completed(), nil
}

//nolint:nilerr // Capture failures publish an explicit suspension and preserve the completed effect.
func (h *host) capture(ctx context.Context, s state) (state, flowy.Directive, error) {
	if s.Result == nil || s.Result.OperationID != s.OperationID {
		return s, flowy.End(), errEvidence
	}
	if err := h.evidence.Capture(ctx, s.OperationID, "after"); err != nil {
		s.CapturePending = true
		return s, flowy.Suspend(
			"capture pending",
			flowy.ResumeAt("capture"),
		), nil
	}
	if err := h.fault.hit("after_capture"); err != nil {
		s.CapturePending = true
		return s, flowy.Suspend(
			"capture interrupted",
			flowy.ResumeAt("capture"),
		), nil
	}
	s.CapturePending = false
	return s, flowy.End(), nil
}
func (h *host) dispatch(ctx context.Context, s state, attempt int) ([]byte, error) {
	var result *receipt
	err := h.execute(ctx, s, attempt, func(chunk toolsy.Chunk) error {
		if chunk.Event != toolsy.EventResult {
			return nil
		}
		if faultErr := h.fault.hit("lost_delivery"); faultErr != nil {
			return faultErr
		}
		value, validationErr := validateReceiptChunk(chunk, s)
		if validationErr != nil {
			return validationErr
		}
		result = &value
		return nil
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errEvidence
	}
	return json.Marshal(result)
}
func (h *host) reconcile(ctx context.Context, s state, identity string) ([]byte, error) {
	if err := s.validate(true); err != nil {
		return nil, err
	}
	m, err := h.evidence.Lookup(ctx, s.OperationID)
	if err != nil ||
		m != (mapping{CallID: s.CallID, OperationID: s.OperationID, ActivityIdentity: identity, Digest: s.Digest, Binding: s.Binding}) {
		return nil, errors.Join(errEvidence, err)
	}
	record, found, err := h.operations.Inspect(ctx, s.Binding)
	if err != nil || !found || record.Binding != s.Binding || record.State != toolsy.OperationCompleted ||
		len(record.Result) == 0 {
		return nil, errors.Join(errEvidence, err)
	}
	if err = validateStoredResult(record.Result); err != nil {
		return nil, err
	}
	chunk, err := (toolsy.JSONResultCodec[receipt, string]{}).DecodeResult(record.Result)
	if err != nil {
		return nil, errors.Join(errEvidence, err)
	}
	result, err := validateReceiptChunk(chunk, s)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}
func mustJSON(value any) []byte { raw, _ := json.Marshal(value); return raw }

func validateReceiptChunk(chunk toolsy.Chunk, s state) (receipt, error) {
	result, ok := chunk.TypedResult.(receipt)
	env := chunk.ToolEnvelope()
	envelopeResult, envelopeOK := env.Result.(receipt)
	rawResult, rawErr := decodeReceipt(chunk.Data)
	if !ok || !envelopeOK || result != envelopeResult || chunk.Event != toolsy.EventResult || chunk.IsError ||
		chunk.MimeType != toolsy.MimeTypeJSON || env.MimeType != chunk.MimeType ||
		env.Kind != toolsy.ToolEnvelopeKindResult || env.Error != nil ||
		!bytes.Equal(env.Raw, chunk.Data) || len(chunk.Data) == 0 ||
		env.DeliveryClass != toolsy.DeliveryClassStructured || env.Audience != toolsy.AudienceModel {
		return receipt{}, errEvidence
	}
	if rawErr != nil || rawResult != result ||
		result.OperationID != s.OperationID || result.Value != s.Input.Value {
		return receipt{}, errEvidence
	}
	return result, nil
}

func retryPolicy() flowy.ActivityRetryPolicy {
	return flowy.ActivityRetryPolicy{Label: "operator-only", MaxAttempts: 2,
		Schedule: flowy.ActivityRetrySchedule{Kind: flowy.ActivityRetryFixed}, SafeRetryContract: "host-fenced-absence"}
}
