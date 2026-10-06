package flowy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrActivityConflict rejects input or implementation changes for one identity.
var ErrActivityConflict = errors.New("flowy: activity key conflict")

// ErrActivityUnknown requires explicit reconciliation of a dispatched attempt.
var ErrActivityUnknown = errors.New("flowy: activity outcome unknown; reconciliation required")

// ErrActivityBusy rejects a second operation while the same live attempt runs.
var ErrActivityBusy = errors.New("flowy: activity attempt still running")

// ErrActivityJournalUnavailable means an activity transition could not be
// confirmed by storage. It does not prove rollback or absence of remote effects.
// The original storage error remains available through [errors.Is].
var ErrActivityJournalUnavailable = errors.New("flowy: activity journal unavailable")

// ActivityOrigin identifies how an outcome was obtained.
type ActivityOrigin string

const (
	ActivityLive       ActivityOrigin = "live"
	ActivityReplayed   ActivityOrigin = "replayed"
	ActivityReconciled ActivityOrigin = "reconciled"
	ActivityManual     ActivityOrigin = "manual"
	ActivitySimulated  ActivityOrigin = "simulated"
)

// ActivityState is the persisted external-dispatch state machine.
type ActivityState string

const (
	ActivityPrepared  ActivityState = "prepared"
	ActivityRunning   ActivityState = "running"
	ActivityCompleted ActivityState = "completed"
	ActivityFailed    ActivityState = "failed"
	ActivityUnknown   ActivityState = "unknown"
)

// ActivityAttempt retains abandoned dispatch attempts instead of discarding them.
type ActivityAttempt struct {
	Number         int                            `json:"number"`
	Incarnation    uint64                         `json:"incarnation"`
	State          ActivityState                  `json:"state"`
	Error          string                         `json:"error,omitempty"`
	Classification ActivityFailureClass           `json:"classification,omitempty"`
	StartedAt      time.Time                      `json:"started_at"`
	RetrySchedule  *ActivityRetryScheduleDecision `json:"retry_schedule,omitempty"`
	FinishedAt     time.Time                      `json:"finished_at,omitzero"`
}

// ActivityRecord is stored inside the same aggregate as step state and cursor.
type ActivityRecord struct {
	Identity       string                     `json:"identity"`
	ExecutionID    string                     `json:"execution_id"`
	Node           ExecutionPointer           `json:"node"`
	Activation     uint64                     `json:"activation"`
	Key            string                     `json:"key"`
	InputDigest    string                     `json:"input_digest"`
	Implementation string                     `json:"implementation"`
	State          ActivityState              `json:"state"`
	Outcome        []byte                     `json:"outcome,omitempty"`
	Origin         ActivityOrigin             `json:"origin,omitempty"`
	Attempts       []ActivityAttempt          `json:"attempts"`
	Retry          ActivityRetryPolicy        `json:"retry"`
	Classification ActivityFailureClass       `json:"classification,omitempty"`
	NextAttemptAt  time.Time                  `json:"next_attempt_at,omitzero"`
	Resolutions    []ActivityResolutionRecord `json:"resolutions,omitempty"`
}

// ActivityRequest defines a raw host-dispatched operation. Reconcile may confirm
// an unknown outcome using downstream evidence; it must not blindly dispatch it.
// Retries require an explicit persisted safe-retry policy. The host owns
// authorization and must disable any competing adapter retry loop.
// Classify may supply a generic NotBefore hint under the persisted HintLabel.
type ActivityRequest struct {
	Key            string
	Implementation string
	Input          []byte
	Dispatch       func(context.Context, ActivityInvocation) ([]byte, error)
	Reconcile      func(context.Context, ActivityRecord) ([]byte, error)
	Retry          ActivityRetryPolicy
	Classify       func(error) ActivityFailureDecision
}

// ActivityInvocation supplies the persisted identity to the host dispatcher.
// Identity is stable across recovery of one activation, not across graph cycles.
// A host may use it as a downstream idempotency key; runtime alone cannot prove
// that the downstream service honors that key.
type ActivityInvocation struct {
	Identity string
	Input    []byte
	Attempt  int
}

// ActivityOutcome contains a detached payload and its execution provenance.
type ActivityOutcome struct {
	Payload  []byte
	Origin   ActivityOrigin
	Revision uint64
	Identity string
}

type activityContextKey struct{}
type activityBackend interface {
	callActivity(context.Context, ActivityRequest) (ActivityOutcome, error)
}

// CallActivity invokes the activity boundary in a durable node. The logical key
// is scoped to run/node/activation, separating graph cycles from replay.
func CallActivity(ctx context.Context, request ActivityRequest) (ActivityOutcome, error) {
	backend, ok := ctx.Value(activityContextKey{}).(activityBackend)
	if !ok || request.Key == "" || request.Implementation == "" || request.Dispatch == nil ||
		!validRuntimeText(request.Key, request.Implementation) {
		return ActivityOutcome{}, ErrExecutionCapability
	}
	if err := validateActivityRetry(request.Retry); err != nil {
		return ActivityOutcome{}, err
	}
	request.Input = bytes.Clone(request.Input)
	return backend.callActivity(ctx, request)
}

func activityIdentity(envelope ExecutionEnvelope, key string) string {
	return activityAddressIdentity(envelope.ExecutionID, envelope.Progress.ExecutionPointer, envelope.Activation, key)
}

func activityAddressIdentity(id string, node ExecutionPointer, activation uint64, key string) string {
	encoded, _ := json.Marshal(
		[]any{id, node, activation, key},
	)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (c *executionCheckpointer[T, E]) callActivity(
	ctx context.Context,
	request ActivityRequest,
) (ActivityOutcome, error) {
	request, policyErr := c.forkActivityRequest(request)
	if policyErr != nil {
		return ActivityOutcome{}, policyErr
	}
	c.mu.Lock()
	identity := boundActivityIdentity(c.envelope, request.Key)
	digest := sha256.Sum256(request.Input)
	inputDigest := hex.EncodeToString(digest[:])
	journal, err := c.readJournal()
	if err != nil {
		c.mu.Unlock()
		return ActivityOutcome{}, err
	}
	record, exists := journal[identity]
	if exists &&
		(record.InputDigest != inputDigest || record.Implementation != request.Implementation || record.Retry != request.Retry) {
		c.mu.Unlock()
		return ActivityOutcome{}, ErrActivityConflict
	}
	if exists && record.State == ActivityCompleted {
		result := ActivityOutcome{
			Payload:  bytes.Clone(record.Outcome),
			Origin:   ActivityReplayed,
			Revision: c.envelope.Revision,
			Identity: identity,
		}
		c.mu.Unlock()
		return result, nil
	}
	if exists && (record.State == ActivityRunning || record.State == ActivityUnknown) {
		return c.recoverActivityLocked(ctx, request, journal, record)
	}
	if exists {
		if readyErr := preparedActivityError(record, c.envelope.Revision, c.clock.Now()); readyErr != nil {
			c.mu.Unlock()
			return ActivityOutcome{}, readyErr
		}
	}
	if !exists {
		record = ActivityRecord{
			Identity:       identity,
			ExecutionID:    c.envelope.ExecutionID,
			Node:           c.envelope.Progress.ExecutionPointer,
			Activation:     c.envelope.Activation,
			Key:            request.Key,
			InputDigest:    inputDigest,
			Implementation: request.Implementation,
			State:          ActivityPrepared,
			Outcome:        nil,
			Origin:         "",
			Attempts:       []ActivityAttempt{},
			Retry:          request.Retry,
			Classification: "",
			NextAttemptAt:  time.Time{},
			Resolutions:    nil,
		}
		if err := c.persistActivity(ctx, journal, record); err != nil {
			c.mu.Unlock()
			return ActivityOutcome{}, err
		}
	}
	record.State = ActivityRunning
	// Origin describes the current outcome, not an earlier operator decision.
	// Decision provenance stays in Resolutions when a new attempt starts.
	record.Origin = ""
	record.NextAttemptAt, record.Classification = time.Time{}, ""
	record.Attempts = append(
		record.Attempts,
		ActivityAttempt{
			Number:         len(record.Attempts) + 1,
			Incarnation:    c.lease.Incarnation,
			State:          ActivityRunning,
			Error:          "",
			Classification: "",
			StartedAt:      c.clock.Now().UTC(),
			FinishedAt:     time.Time{},
			RetrySchedule:  nil,
		},
	)
	if err := c.persistActivity(ctx, journal, record); err != nil {
		c.mu.Unlock()
		return ActivityOutcome{}, err
	}
	c.mu.Unlock()
	payload, dispatchErr := request.Dispatch(ctx, ActivityInvocation{
		Identity: identity,
		Input:    bytes.Clone(request.Input),
		Attempt:  len(record.Attempts),
	})
	var classification ActivityFailureDecision
	if dispatchErr != nil {
		classification = classifyActivityFailure(request, dispatchErr)
	}
	return c.finishActivity(ctx, identity, payload, dispatchErr, c.activityDispatchOrigin(), classification)
}

// recoverActivityLocked consumes the mutex held by callActivity. Host
// reconciliation runs outside the mutex, while its outcome remains fenced.
func (c *executionCheckpointer[T, E]) recoverActivityLocked(
	ctx context.Context,
	request ActivityRequest,
	journal map[string]ActivityRecord,
	record ActivityRecord,
) (ActivityOutcome, error) {
	if record.State == ActivityRunning && len(record.Attempts) == 0 {
		c.mu.Unlock()
		return ActivityOutcome{}, ErrActivityConflict
	}
	if record.State == ActivityRunning && record.Attempts[len(record.Attempts)-1].Incarnation == c.lease.Incarnation {
		c.mu.Unlock()
		return ActivityOutcome{}, ErrActivityBusy
	}
	if record.State == ActivityRunning {
		record.State = ActivityUnknown
		record.Attempts[len(record.Attempts)-1].State = ActivityUnknown
		record.Classification = ActivityAmbiguous
		record.Attempts[len(record.Attempts)-1].Classification = ActivityAmbiguous
		if err := c.persistActivity(ctx, journal, record); err != nil {
			c.mu.Unlock()
			return ActivityOutcome{}, err
		}
	}
	c.mu.Unlock()
	if request.Reconcile == nil {
		return ActivityOutcome{}, ErrActivityUnknown
	}
	payload, err := request.Reconcile(ctx, record)
	if err != nil {
		return ActivityOutcome{}, errors.Join(ErrActivityUnknown, err)
	}
	var decision ActivityFailureDecision
	return c.finishActivity(ctx, record.Identity, payload, nil, ActivityReconciled, decision)
}

func (c *executionCheckpointer[T, E]) readJournal() (map[string]ActivityRecord, error) {
	return executionActivityJournal(c.envelope)
}

func (c *executionCheckpointer[T, E]) persistActivity(
	ctx context.Context,
	journal map[string]ActivityRecord,
	record ActivityRecord,
) error {
	if err := validateActivityRecord(record); err != nil {
		return err
	}
	journal[record.Identity] = record
	payload, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	target := cloneExecutionEnvelope(c.envelope)
	target.JournalPayload = payload
	committed, err := c.store.CommitExecution(ctx, c.envelope.Revision, c.lease, target)
	if err != nil {
		c.persistenceFailed = true
		return activityJournalCommitError(err)
	}
	c.envelope = cloneExecutionEnvelope(committed)
	return nil
}

func activityJournalCommitError(err error) error {
	for _, classified := range []error{
		ErrConcurrencyConflict, ErrLeaseLost, ErrExecutionCorrupt, ErrInvalidSnapshot,
		ErrExecutionCapability, ErrExecutionIncompatible, context.Canceled, context.DeadlineExceeded,
	} {
		if errors.Is(err, classified) {
			return err
		}
	}
	return errors.Join(ErrActivityJournalUnavailable, err)
}

func (c *executionCheckpointer[T, E]) finishActivity(
	ctx context.Context,
	identity string,
	payload []byte,
	dispatchErr error,
	origin ActivityOrigin,
	classification ActivityFailureDecision,
) (ActivityOutcome, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	journal, err := c.readJournal()
	if err != nil {
		return ActivityOutcome{}, err
	}
	record, exists := journal[identity]
	if !exists || len(record.Attempts) == 0 {
		return ActivityOutcome{}, ErrActivityConflict
	}
	if record.State == ActivityCompleted {
		if dispatchErr != nil || !bytes.Equal(record.Outcome, payload) {
			return ActivityOutcome{}, ErrActivityConflict
		}
		return ActivityOutcome{
			Payload:  bytes.Clone(record.Outcome),
			Origin:   ActivityReplayed,
			Revision: c.envelope.Revision,
			Identity: identity,
		}, nil
	}
	if dispatchErr != nil {
		c.recordActivityFailure(&record, dispatchErr, classification)
	} else {
		record.State = ActivityCompleted
		record.Outcome, record.Origin = bytes.Clone(payload), origin
		if origin != ActivityReconciled && origin != ActivityManual {
			record.Attempts[len(record.Attempts)-1].State = ActivityCompleted
			record.Attempts[len(record.Attempts)-1].FinishedAt = c.clock.Now().UTC()
		}
	}
	if err := c.persistActivity(ctx, journal, record); err != nil {
		return ActivityOutcome{}, errors.Join(ErrActivityUnknown, err)
	}
	if dispatchErr != nil {
		return ActivityOutcome{}, fmt.Errorf("%w: %w", activityRecordError(record, c.envelope.Revision), dispatchErr)
	}
	return ActivityOutcome{
		Payload:  bytes.Clone(record.Outcome),
		Origin:   origin,
		Revision: c.envelope.Revision,
		Identity: identity,
	}, nil
}
