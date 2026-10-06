package flowy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"math"
	"slices"
	"time"
)

func waitPayloadDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func cloneDurableWait(record DurableWaitRecord) DurableWaitRecord {
	record.Decisions = maps.Clone(record.Decisions)
	if record.Cancellation != nil {
		cancellation := *record.Cancellation
		record.Cancellation = &cancellation
	}
	return record
}

// prepareWaitDecision is pure: persistence must publish this target together
// with winner state/cursor under the same source revision and live fence. An
// identical duplicate returns replay=true and needs no write or continuation.
func prepareWaitDecision(record DurableWaitRecord, delivery WaitDelivery,
	revision, incarnation uint64, now time.Time, matched bool,
) (DurableWaitRecord, WaitDecision, bool, error) {
	if validateDurableWait(record, revision) != nil || incarnation == 0 || now.IsZero() {
		return DurableWaitRecord{}, WaitDecision{}, false, ErrWaitInvalid
	}
	if err := validateWaitDelivery(record, delivery, revision); err != nil {
		return DurableWaitRecord{}, WaitDecision{}, false, err
	}
	if prior, ok := record.Decisions[delivery.ID]; ok {
		if prior.Kind != delivery.Kind || prior.CorrelationID != delivery.CorrelationID ||
			prior.PayloadDigest != waitPayloadDigest(delivery.Payload) {
			return DurableWaitRecord{}, WaitDecision{}, false, ErrWaitConflict
		}
		return cloneDurableWait(record), prior, true, nil
	}
	if revision == math.MaxUint64 {
		return DurableWaitRecord{}, WaitDecision{}, false, ErrWaitInvalid
	}
	if delivery.CorrelationID != record.Spec.CorrelationID ||
		(delivery.Kind == WaitTimer && len(delivery.Payload) != 0) {
		return DurableWaitRecord{}, WaitDecision{}, false, ErrWaitInvalid
	}
	if delivery.Kind == WaitTimer && now.Before(record.Spec.Deadline) {
		return DurableWaitRecord{}, WaitDecision{}, false, ErrWaitInvalid
	}
	if record.State == WaitArmed && delivery.ExpectedRevision != revision {
		return DurableWaitRecord{}, WaitDecision{}, false, ErrWaitStale
	}
	target := cloneDurableWait(record)
	decision := WaitDecision{
		ID: delivery.ID, Kind: delivery.Kind, CorrelationID: delivery.CorrelationID,
		PayloadDigest: waitPayloadDigest(delivery.Payload), SourceRevision: revision,
		Incarnation: incarnation, At: now.UTC(), Status: waitDecisionStatus(record, delivery.Kind, matched),
	}
	if decision.Status == WaitAccepted {
		target.WinnerID = decision.ID
		target.State = WaitResolved
		if delivery.Kind == WaitTimer {
			target.State = WaitExpired
		}
	}
	if target.Decisions == nil {
		target.Decisions = make(map[string]WaitDecision)
	}
	target.Decisions[decision.ID] = decision
	return target, decision, false, nil
}

func waitDecisionStatus(record DurableWaitRecord, kind WaitDeliveryKind, matched bool) WaitDecisionStatus {
	if record.State == WaitCanceled {
		return WaitRejectedCanceled
	}
	if record.State != WaitArmed {
		return WaitLost
	}
	if kind == WaitEvent && !matched {
		return WaitUnmatched
	}
	return WaitAccepted
}

func validateWaitDelivery(record DurableWaitRecord, delivery WaitDelivery, revision uint64) error {
	if delivery.Generation != record.Generation || delivery.ExpectedRevision < record.ArmRevision ||
		delivery.ExpectedRevision > revision {
		return ErrWaitStale
	}
	if delivery.ID == "" ||
		(delivery.Kind != WaitEvent && delivery.Kind != WaitTimer) {
		return ErrWaitInvalid
	}
	return nil
}

func executionWaits(envelope ExecutionEnvelope) (map[string]DurableWaitRecord, error) {
	waits := make(map[string]DurableWaitRecord)
	if len(envelope.WaitsPayload) == 0 {
		return waits, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope.WaitsPayload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&waits); err != nil || waits == nil {
		return nil, ErrExecutionCorrupt
	}
	// Unmarshal also rejects trailing data; the strict decoder rejects unknown fields.
	if !json.Valid(envelope.WaitsPayload) {
		return nil, ErrExecutionCorrupt
	}
	armed := 0
	for identity, record := range waits {
		if !validWaitEnvelopeAddress(envelope, identity, record) {
			return nil, ErrExecutionCorrupt
		}
		if record.State == WaitArmed {
			armed++
			if armed > 1 {
				return nil, ErrExecutionCorrupt
			}
		}
		if !validCanceledWaitTerminal(envelope, record) {
			return nil, ErrExecutionCorrupt
		}
	}
	return waits, nil
}

func validWaitEnvelopeAddress(envelope ExecutionEnvelope, identity string, record DurableWaitRecord) bool {
	return (envelope.RuntimeProfile == nil || record.Profile == *envelope.RuntimeProfile) &&
		identity == waitIdentity(record.ExecutionID, record.Node, record.Activation, record.Spec.ID) &&
		record.ExecutionID == envelope.ExecutionID && record.Activation <= envelope.Activation &&
		validateDurableWait(record, envelope.Revision) == nil &&
		(record.State != WaitArmed || (envelope.Terminal == nil && record.Activation == envelope.Activation &&
			record.Node == envelope.Progress.ExecutionPointer))
}

// InspectExecutionWaits reads sealed wait metadata without domain codecs, nodes
// or registration. The caller/store must supply the exact addressed envelope.
func InspectExecutionWaits(envelope ExecutionEnvelope) ([]DurableWaitRecord, error) {
	if err := ValidateExecutionIntegrity(envelope, envelope.ExecutionID, envelope.Revision); err != nil {
		return nil, err
	}
	waits, err := validateExecutionCollectionsWithWaits(envelope)
	if err != nil {
		return nil, err
	}
	identities := make([]string, 0, len(waits))
	for identity := range waits {
		identities = append(identities, identity)
	}
	slices.Sort(identities)
	result := make([]DurableWaitRecord, 0, len(identities))
	for _, identity := range identities {
		result = append(result, cloneDurableWait(waits[identity]))
	}
	return result, nil
}

func validateDurableWait(record DurableWaitRecord, revision uint64) error {
	identity := waitIdentity(record.ExecutionID, record.Node, record.Activation, record.Spec.ID)
	if record.ExecutionID == "" || record.Node == "" || record.Activation == 0 || record.Spec.Validate() != nil ||
		record.Profile.Validate() != nil ||
		record.ArmRevision == 0 || record.ArmRevision > revision ||
		record.Generation != waitGeneration(identity, record.ArmRevision) {
		return ErrWaitInvalid
	}
	if !validWaitCancellation(record, revision) {
		return ErrWaitInvalid
	}
	switch record.State {
	case WaitArmed, WaitCanceled:
		if record.WinnerID != "" {
			return ErrWaitInvalid
		}
	case WaitResolved, WaitExpired:
		if record.WinnerID == "" {
			return ErrWaitInvalid
		}
	default:
		return ErrWaitInvalid
	}
	return validateWaitDecisions(record, revision)
}

func validateWaitDecisions(record DurableWaitRecord, revision uint64) error {
	sources := make(map[uint64]bool)
	if record.Cancellation != nil {
		sources[record.Cancellation.SourceRevision] = true
	}
	winner, hasWinner := record.Decisions[record.WinnerID]
	if record.WinnerID != "" && (!hasWinner || winner.Status != WaitAccepted) {
		return ErrWaitInvalid
	}
	for id, decision := range record.Decisions {
		if id == "" || id != decision.ID || decision.CorrelationID != record.Spec.CorrelationID ||
			decision.Incarnation == 0 || decision.At.IsZero() ||
			decision.SourceRevision < record.ArmRevision || decision.SourceRevision >= revision ||
			sources[decision.SourceRevision] || !canonicalWaitDigest(decision.PayloadDigest) ||
			(decision.Kind != WaitEvent && decision.Kind != WaitTimer) {
			return ErrWaitInvalid
		}
		sources[decision.SourceRevision] = true
		if decision.Kind == WaitTimer && (decision.At.Before(record.Spec.Deadline) ||
			decision.PayloadDigest != waitPayloadDigest(nil)) {
			return ErrWaitInvalid
		}
		if !validWaitDecisionState(record, decision, winner) {
			return ErrWaitInvalid
		}
	}
	return nil
}

func canonicalWaitDigest(digest string) bool {
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == digest
}

func validWaitDecisionState(record DurableWaitRecord, decision, winner WaitDecision) bool {
	switch decision.Status {
	case WaitAccepted:
		return decision.ID == record.WinnerID &&
			((record.State == WaitResolved && decision.Kind == WaitEvent) ||
				(record.State == WaitExpired && decision.Kind == WaitTimer))
	case WaitUnmatched:
		return decision.Kind == WaitEvent &&
			(record.WinnerID == "" || decision.SourceRevision < winner.SourceRevision) &&
			(record.Cancellation == nil || decision.SourceRevision < record.Cancellation.SourceRevision)
	case WaitLost:
		return record.WinnerID != "" && decision.SourceRevision > winner.SourceRevision
	case WaitRejectedCanceled:
		return record.State == WaitCanceled && record.Cancellation != nil &&
			decision.SourceRevision > record.Cancellation.SourceRevision
	default:
		return false
	}
}
