package flowy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrWaitInvalid      = errors.New("flowy: invalid durable wait")
	ErrWaitConflict     = errors.New("flowy: durable wait identity conflict")
	ErrWaitNotArmed     = errors.New("flowy: durable wait not armed")
	ErrWaitUnknown      = errors.New("flowy: unknown durable wait")
	ErrWaitStale        = errors.New("flowy: stale durable wait delivery")
	ErrWaitUnresolved   = errors.New("flowy: unresolved durable wait")
	ErrWaitRegistration = errors.New("flowy: durable wait registration failed")
	ErrWaitCanceled     = errors.New("flowy: durable wait canceled")
	ErrWaitExpired      = errors.New("flowy: durable wait expired")
)

// WaitState is independent of a transport acknowledgement or worker lifetime.
type WaitState string

const (
	WaitArmed    WaitState = "armed"
	WaitResolved WaitState = "resolved"
	WaitExpired  WaitState = "expired"
	WaitCanceled WaitState = "canceled"
)

// WaitWinnerPolicy is explicit; no implicit scheduler priority is inferred.
type WaitWinnerPolicy string

const WaitFirstCommitted WaitWinnerPolicy = "first_valid_committed"

// DurableWaitSpec labels the pure matcher, payload codec and continuation.
// Absolute deadlines survive recovery. Pointers are validated against the graph
// by the runner before registration, not inferred from the host payload.
type DurableWaitSpec struct {
	ID                string           `json:"id"`
	CorrelationID     string           `json:"correlation_id"`
	Deadline          time.Time        `json:"deadline"`
	MatcherLabel      string           `json:"matcher_label"`
	PayloadCodec      string           `json:"payload_codec"`
	ContinuationLabel string           `json:"continuation_label"`
	EventPointer      ExecutionPointer `json:"event_pointer"`
	TimeoutPointer    ExecutionPointer `json:"timeout_pointer"`
	WinnerPolicy      WaitWinnerPolicy `json:"winner_policy"`
}

func (s DurableWaitSpec) Validate() error {
	_, offset := s.Deadline.Zone()
	if s.ID == "" || s.CorrelationID == "" || s.Deadline.IsZero() || offset != 0 ||
		!validRuntimeText(s.ID, s.CorrelationID, s.MatcherLabel, s.PayloadCodec, s.ContinuationLabel,
			string(s.EventPointer), string(s.TimeoutPointer)) ||
		s.MatcherLabel == "" || s.PayloadCodec == "" || s.ContinuationLabel == "" ||
		s.EventPointer == "" || s.TimeoutPointer == "" || s.WinnerPolicy != WaitFirstCommitted {
		return ErrWaitInvalid
	}
	if _, err := s.Deadline.MarshalJSON(); err != nil {
		return ErrWaitInvalid
	}
	return nil
}

type WaitDeliveryKind string

const (
	WaitEvent WaitDeliveryKind = "event"
	WaitTimer WaitDeliveryKind = "timer"
)

// WaitDelivery names one immutable transport identity. ExpectedRevision may be
// refreshed on redelivery; changing kind/correlation/payload under ID may not.
type WaitDelivery struct {
	Generation       string           `json:"generation"`
	ID               string           `json:"id"`
	Kind             WaitDeliveryKind `json:"kind"`
	CorrelationID    string           `json:"correlation_id"`
	ExpectedRevision uint64           `json:"expected_revision"`
	Payload          []byte           `json:"payload,omitempty"`
}

type WaitDecisionStatus string

const (
	WaitAccepted         WaitDecisionStatus = "accepted"
	WaitUnmatched        WaitDecisionStatus = "unmatched"
	WaitLost             WaitDecisionStatus = "lost"
	WaitRejectedCanceled WaitDecisionStatus = "canceled"
)

// WaitDecision is a durable delivery result, not a storage acknowledgement.
type WaitDecision struct {
	ID             string             `json:"id"`
	Kind           WaitDeliveryKind   `json:"kind"`
	CorrelationID  string             `json:"correlation_id"`
	PayloadDigest  string             `json:"payload_digest"`
	Status         WaitDecisionStatus `json:"status"`
	SourceRevision uint64             `json:"source_revision"`
	Incarnation    uint64             `json:"incarnation"`
	At             time.Time          `json:"at"`
}

// DurableWaitRecord is sealed with execution state and decisions in the same
// aggregate. Generation includes the exact arm checkpoint, preventing ABA reuse.
type DurableWaitRecord struct {
	ExecutionID  string                  `json:"execution_id"`
	Node         ExecutionPointer        `json:"node"`
	Activation   uint64                  `json:"activation"`
	Generation   string                  `json:"generation"`
	ArmRevision  uint64                  `json:"arm_revision"`
	Spec         DurableWaitSpec         `json:"spec"`
	Profile      WaitCapabilityProfile   `json:"profile"`
	State        WaitState               `json:"state"`
	WinnerID     string                  `json:"winner_id,omitempty"`
	Decisions    map[string]WaitDecision `json:"decisions,omitempty"`
	Cancellation *WaitCancellationRecord `json:"cancellation,omitempty"`
}

func waitIdentity(execution string, node ExecutionPointer, activation uint64, id string) string {
	return waitDigest([]any{"flowy-wait", execution, node, activation, id})
}

func waitGeneration(identity string, revision uint64) string {
	return waitDigest([]any{"flowy-wait-generation", identity, revision})
}

func waitDigest(value any) string {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func planDurableWait(execution string, node ExecutionPointer, activation, revision uint64,
	spec DurableWaitSpec, profile WaitCapabilityProfile,
) (DurableWaitRecord, error) {
	if spec.Validate() != nil || profile.Validate() != nil || execution == "" || node == "" || activation == 0 ||
		revision == 0 {
		return DurableWaitRecord{}, ErrWaitInvalid
	}
	identity := waitIdentity(execution, node, activation, spec.ID)
	return DurableWaitRecord{
		ExecutionID: execution, Node: node, Activation: activation,
		Generation: waitGeneration(identity, revision), ArmRevision: revision,
		Spec: spec, Profile: profile, State: WaitArmed, WinnerID: "", Decisions: make(map[string]WaitDecision),
		Cancellation: nil,
	}, nil
}
