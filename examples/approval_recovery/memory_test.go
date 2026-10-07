package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/skosovsky/toolsy"
	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type memoryEvidence struct {
	mu             sync.Mutex
	mappings       map[string]mapping
	receipts       map[string]receipt
	revoked        map[string]bool
	captures       map[string]int
	fences         map[string]int
	calls          int
	writes         int
	captureFailure string
}

func newMemoryEvidence() *memoryEvidence {
	return &memoryEvidence{fences: map[string]int{},
		mappings: map[string]mapping{},
		receipts: map[string]receipt{},
		revoked:  map[string]bool{},
		captures: map[string]int{},
	}
}
func (e *memoryEvidence) Map(_ context.Context, m mapping) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if old, ok := e.mappings[m.OperationID]; ok && old != m {
		return errEvidence
	}
	e.mappings[m.OperationID] = m
	return nil
}
func (e *memoryEvidence) Lookup(_ context.Context, id string) (mapping, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.mappings[id]
	if !ok {
		return mapping{}, errEvidence
	}
	return m, nil
}
func (e *memoryEvidence) Allowed(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.revoked[id] {
		return errDenied
	}
	return nil
}
func (e *memoryEvidence) Revoke(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.revoked[id] = true
	return nil
}
func (e *memoryEvidence) Capture(_ context.Context, id, phase string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.captureFailure == phase {
		return errFault
	}
	e.captures[id+"/"+phase] = 1
	return nil
}
func (e *memoryEvidence) Write(_ context.Context, id string, in input, attempt int) (receipt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.revoked[id] {
		return receipt{}, errDenied
	}
	e.calls++
	current := e.fences[id]
	if current == 0 {
		current = 1
	}
	if attempt != current {
		return receipt{}, errDenied
	}
	if _, exists := e.receipts[id]; exists {
		return receipt{}, errors.New("host: duplicate dispatch")
	}
	r := receipt{OperationID: id, Value: in.Value, Accepted: in.Value != "reject"}
	if r.Accepted {
		e.writes++
	}
	e.receipts[id] = r
	return r, nil
}
func (e *memoryEvidence) Receipt(_ context.Context, id string) (receipt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.receipts[id]
	if !ok {
		return receipt{}, errEvidence
	}
	return r, nil
}

type fixture struct {
	h     *host
	e     *memoryEvidence
	op    *toolsy.MemoryOperationStore
	store *faultExecutionStore
	r     *flowy.DurableRunner[state, flowy.NoEffect]
}

func setup(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{e: newMemoryEvidence(), op: toolsy.NewMemoryOperationStore()}
	f.h = &host{evidence: f.e, operations: f.op, secret: []byte("01234567890123456789012345678901"), now: time.Now}
	f.store = &faultExecutionStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil)}
	var err error
	f.r, err = f.h.runner(f.store)
	require.NoError(t, err)
	return f
}
func (f *fixture) start(t *testing.T, id string) state {
	t.Helper()
	result, err := f.r.Start(t.Context(), id, initial("call-"+id, "op-"+id, input{Value: "same"}))
	require.NoError(t, err)
	require.Nil(t, result.State.Result)
	s, _, err := inspectState(t.Context(), f.store, id)
	require.NoError(t, err)
	return s
}
func (f *fixture) approve(t *testing.T, id, kind string) (signedDecision, flowy.WaitDeliveryResult) {
	t.Helper()
	d, err := f.h.request(t.Context(), f.store, id, kind, input{Value: "edited"})
	require.NoError(t, err)
	result, err := f.h.deliver(t.Context(), f.r, f.store, id, d)
	require.NoError(t, err)
	return d, result
}
func (f *fixture) resume(t *testing.T, id string) (*flowy.RunResult[state, flowy.NoEffect], error) {
	t.Helper()
	_, token, err := inspectState(t.Context(), f.store, id)
	require.NoError(t, err)
	r, err := f.h.runner(f.store)
	require.NoError(t, err)
	return r.Resume(t.Context(), token)
}

func TestApprovalDecisions(t *testing.T) {
	for _, kind := range []string{"allow", "deny", "edit", "expired", "revoked"} {
		t.Run(kind, func(t *testing.T) {
			// Arrange.
			f := setup(t)
			original := f.start(t, kind)
			// Act.
			_, accepted := f.approve(t, kind, kind)
			result, err := f.r.Resume(t.Context(), accepted.ResumeToken)
			require.NoError(t, err)
			if kind == "edit" {
				require.Equal(t, 0, f.e.calls)
				require.Equal(t, original.OperationID, result.State.OperationID)
				require.NotEqual(t, original.Binding, result.State.Binding)
				_, accepted = f.approve(t, kind, "allow")
				result, err = f.r.Resume(t.Context(), accepted.ResumeToken)
				require.NoError(t, err)
			}
			// Assert.
			if kind == "allow" || kind == "edit" {
				require.Equal(t, 1, f.e.writes)
				require.NotNil(t, result.State.Result)
			} else {
				require.Zero(t, f.e.writes)
				require.Nil(t, result.State.Result)
			}
		})
	}
}
func TestIdentityAndDeliveryAdmission(t *testing.T) {
	// Arrange: identical arguments are separate intentions.
	f := setup(t)
	first := f.start(t, "first")
	second := f.start(t, "second")
	require.NotEqual(t, first.OperationID, second.OperationID)
	d, err := f.h.request(t.Context(), f.store, "first", "allow", input{})
	require.NoError(t, err)
	// Act/Assert: authentication and addressing failures do not consume a grant or dispatch.
	forged := d
	forged.Signature = []byte("forged")
	_, err = f.h.deliver(t.Context(), f.r, f.store, "first", forged)
	require.ErrorIs(t, err, errDenied)
	foreign := d.Decision
	foreign.OperationID = second.OperationID
	_, err = f.h.deliver(t.Context(), f.r, f.store, "first", f.h.sign(foreign))
	require.ErrorIs(t, err, errEvidence)
	changed := d.Decision
	changed.Input.Value = "other"
	_, err = f.h.deliver(t.Context(), f.r, f.store, "first", f.h.sign(changed))
	require.ErrorIs(t, err, errEvidence)
	require.Empty(t, f.op.Snapshot().Consumed)
	require.Zero(t, f.e.calls)
	accepted, err := f.h.deliver(t.Context(), f.r, f.store, "first", d)
	require.NoError(t, err)
	duplicate, err := f.h.deliver(t.Context(), f.r, f.store, "first", d)
	require.NoError(t, err)
	require.True(t, duplicate.Replay)
	changed = d.Decision
	changed.Kind = "deny"
	_, err = f.h.deliver(t.Context(), f.r, f.store, "first", f.h.sign(changed))
	require.Error(t, err)
	stale := d.Decision
	stale.Generation = "stale"
	_, err = f.h.deliver(t.Context(), f.r, f.store, "first", f.h.sign(stale))
	require.ErrorIs(t, err, flowy.ErrWaitStale)
	_, err = f.r.Resume(t.Context(), accepted.ResumeToken)
	require.NoError(t, err)
	replay, err := f.resume(t, "first")
	require.NoError(t, err)
	require.Equal(t, first.CallID, replay.State.CallID)
	require.Equal(t, 1, f.e.calls)
	require.Equal(t, 1, f.e.writes)
}
func TestRealBackendBindingExpiryAndRevocation(t *testing.T) {
	for _, mode := range []string{"binding", "expiry", "revocation", "effect-revocation"} {
		t.Run(mode, func(t *testing.T) {
			// Arrange.
			f := setup(t)
			s := f.start(t, mode)
			_, accepted := f.approve(t, mode, "allow")
			// Act: change the actual dispatch request or current policy after decision acceptance.
			switch mode {
			case "binding":
				s.GrantID = accepted.Decision.ID
				s.Input.Value = "changed"
				s.Digest = digest(s.Input)
				err := f.h.execute(t.Context(), s, 1, func(toolsy.Chunk) error { return nil })
				var opErr *toolsy.OperationError
				require.ErrorAs(t, err, &opErr)
				require.Equal(t, "binding_mismatch", opErr.Kind)
			case "expiry":
				f.h.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
				_, err := f.r.Resume(t.Context(), accepted.ResumeToken)
				require.Error(t, err)
			case "revocation":
				require.NoError(t, f.e.Revoke(t.Context(), s.OperationID))
				_, err := f.r.Resume(t.Context(), accepted.ResumeToken)
				require.ErrorIs(t, err, errDenied)
			case "effect-revocation":
				f.h.fault = func(point string) error {
					if point == "before_effect" {
						return f.e.Revoke(t.Context(), s.OperationID)
					}
					return nil
				}
				_, err := f.r.Resume(t.Context(), accepted.ResumeToken)
				require.Error(t, err)
			}
			// Assert.
			require.Zero(t, f.e.writes)
			require.Zero(t, f.e.calls)
		})
	}
}
func TestLostDeliveryAndJournalFailure(t *testing.T) {
	for _, point := range []string{"lost_delivery", "journal_commit", "state_commit"} {
		t.Run(point, func(t *testing.T) {
			// Arrange.
			f := setup(t)
			f.start(t, point)
			_, accepted := f.approve(t, point, "allow")
			hit := false
			fault := func(p string) error {
				if p == point && !hit {
					hit = true
					return errFault
				}
				return nil
			}
			f.h.fault, f.store.fault = fault, fault
			// Act.
			_, err := f.r.Resume(t.Context(), accepted.ResumeToken)
			require.Error(t, err)
			require.True(t, hit)
			f.h.fault, f.store.fault = nil, nil
			result, err := f.resume(t, point)
			require.NoError(t, err)
			// Assert.
			require.Equal(t, 1, f.e.calls)
			require.Equal(t, 1, f.e.writes)
			require.NotNil(t, result.State.Result)
			replay, err := f.resume(t, point)
			require.NoError(t, err)
			require.Equal(t, result.State.Result, replay.State.Result)
			require.Equal(t, 1, f.e.calls)
		})
	}
}
func TestCaptureRecovery(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			// Arrange.
			f := setup(t)
			original := f.start(t, phase)
			_, accepted := f.approve(t, phase, "allow")
			f.e.captureFailure = phase
			// Act.
			pending, err := f.r.Resume(t.Context(), accepted.ResumeToken)
			if phase == "before" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, flowy.RunStatusSuspended, pending.Status)
			}
			f.e.captureFailure = ""
			result, err := f.resume(t, phase)
			// Assert.
			if phase == "before" {
				require.ErrorIs(t, err, flowy.ErrActivityUnknown)
				require.Zero(t, f.e.calls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, f.e.calls)
				require.Equal(t, 1, f.e.writes)
				require.Equal(t, original.OperationID, result.State.OperationID)
				require.Equal(t, 1, f.e.captures[original.OperationID+"/after"])
			}
		})
	}
}

//nolint:gocognit // Each branch corresponds to one crash-matrix recovery outcome.
func TestCrashMatrixAndOperatorRecovery(
	t *testing.T,
) {
	for _, point := range []string{"before_intent", "before_prepare", "after_challenge", "before_arm", "after_arm", "before_decision", "after_decision",
		"before_claim", "after_claim", "before_effect", "after_effect", "before_finish", "after_finish", "after_capture"} {
		t.Run(point, func(t *testing.T) {
			// Arrange.
			f := setup(t)
			hit := false
			fault := func(p string) error {
				if p == point && !hit {
					hit = true
					return errFault
				}
				return nil
			}
			f.h.fault, f.store.fault = fault, fault
			// Act: the hook may interrupt start, delivery or dispatch.
			_, startErr := f.r.Start(t.Context(), point, initial("call", "op", input{Value: "same"}))
			if point == "before_prepare" || point == "after_challenge" {
				require.Error(t, startErr)
				require.True(t, hit)
				require.Zero(t, f.e.calls)
				require.Empty(t, f.op.Snapshot().Records)
				return
			}
			if startErr == nil {
				d, reqErr := f.h.request(t.Context(), f.store, point, "allow", input{})
				require.NoError(t, reqErr)
				accepted, deliveryErr := f.h.deliver(t.Context(), f.r, f.store, point, d)
				if deliveryErr == nil {
					_, _ = f.r.Resume(t.Context(), accepted.ResumeToken)
				}
			}
			require.True(t, hit, "fault boundary not reached")
			f.h.fault, f.store.fault = nil, nil
			// Assert: recover preparation/decision safely; an ambiguous effect needs evidence.
			if point == "before_intent" {
				require.Zero(t, f.e.calls)
				require.Empty(t, f.op.Snapshot().Records)
				return
			}
			_, resumeErr := f.resume(t, point)
			if resumeErr == nil {
				if d, requestErr := f.h.request(t.Context(), f.store, point, "allow", input{}); requestErr == nil {
					accepted, deliveryErr := f.h.deliver(t.Context(), f.r, f.store, point, d)
					require.NoError(t, deliveryErr)
					_, resumeErr = f.r.Resume(t.Context(), accepted.ResumeToken)
				}
			}
			if point == "after_claim" || point == "before_effect" || point == "before_claim" {
				require.ErrorIs(t, resumeErr, flowy.ErrActivityUnknown)
				require.Zero(t, f.e.calls)
				s, _, loadErr := inspectState(t.Context(), f.store, point)
				require.NoError(t, loadErr)
				require.Error(t, f.h.resolveReceipt(t.Context(), "operator", s))
				return
			}
			if point == "after_effect" || point == "before_finish" {
				require.ErrorIs(t, resumeErr, flowy.ErrActivityUnknown)
				s, _, loadErr := inspectState(t.Context(), f.store, point)
				require.NoError(t, loadErr)
				require.ErrorIs(t, f.h.resolveReceipt(t.Context(), "attacker", s), errDenied)
				require.NoError(t, f.h.resolveReceipt(t.Context(), "operator", s))
				_, resumeErr = f.resume(t, point)
			}
			require.NoError(t, resumeErr)
			require.Equal(t, 1, f.e.calls)
			require.Equal(t, 1, f.e.writes)
		})
	}
}

func (e *memoryEvidence) FenceAbsence(_ context.Context, id string, attempt int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.receipts[id]; ok {
		return errEvidence
	}
	current := e.fences[id]
	if current == 0 {
		current = 1
	}
	if current == attempt+1 {
		return nil
	}
	if current != attempt {
		return errEvidence
	}
	e.fences[id] = attempt + 1
	return nil
}
func TestConsumedGapWithVerifiedAbsence(t *testing.T) {
	// Arrange: the actual backend consumed the grant but has not dispatched an effect.
	f := setup(t)
	original := f.start(t, "gap")
	_, accepted := f.approve(t, "gap", "allow")
	f.h.fault = func(point string) error {
		if point == "before_effect" {
			return errFault
		}
		return nil
	}
	_, err := f.r.Resume(t.Context(), accepted.ResumeToken)
	require.ErrorIs(t, err, flowy.ErrActivityUnknown)
	snapshot := f.op.Snapshot()
	require.Len(t, snapshot.Consumed, 1)
	require.Zero(t, f.e.calls)
	f.h.fault = nil
	_, token, err := inspectState(t.Context(), f.store, "gap")
	require.NoError(t, err)
	// Act: fenced downstream absence is evidence; timeout alone is not.
	_, err = f.h.authorizeRetry(t.Context(), "attacker", f.r, f.store, token)
	require.ErrorIs(t, err, errDenied)
	token, err = f.h.authorizeRetry(t.Context(), "operator", f.r, f.store, token)
	require.NoError(t, err)
	_, err = f.e.Write(t.Context(), original.OperationID, original.Input, 1)
	require.ErrorIs(t, err, errDenied)
	result, err := f.r.Resume(t.Context(), token)
	// Assert: the same intent/grant is completed by one permitted new attempt.
	require.NoError(t, err)
	require.NotNil(t, result.State.Result)
	require.Equal(t, original.CallID, result.State.CallID)
	require.Equal(t, original.OperationID, result.State.OperationID)
	require.Equal(t, 1, f.e.writes)
	record, found, err := f.op.Inspect(t.Context(), original.Binding)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, record.Attempts, 2)
	require.Equal(t, 2, f.e.calls) // includes the fenced old request, not hidden by the counter.
}

func TestRetryProtocolCrashStages(t *testing.T) {
	for _, point := range []string{"after_retry_fence", "after_retry_operation", "after_retry_activity"} {
		t.Run(point, func(t *testing.T) {
			// Arrange: a consumed grant and a verified absence remain outside Reconcile.
			f := setup(t)
			original := f.start(t, point)
			_, accepted := f.approve(t, point, "allow")
			f.h.fault = func(p string) error {
				if p == "before_effect" {
					return errFault
				}
				return nil
			}
			_, err := f.r.Resume(t.Context(), accepted.ResumeToken)
			require.ErrorIs(t, err, flowy.ErrActivityUnknown)
			_, token, err := inspectState(t.Context(), f.store, point)
			require.NoError(t, err)
			f.h.fault = func(p string) error {
				if p == point {
					return errFault
				}
				return nil
			}
			// Act: replay the exact operator protocol after losing one stage's response.
			_, err = f.h.authorizeRetry(t.Context(), operatorSubject, f.r, f.store, token)
			require.ErrorIs(t, err, errFault)
			f.h.fault = nil
			_, token, err = inspectState(t.Context(), f.store, point)
			require.NoError(t, err)
			token, err = f.h.authorizeRetry(t.Context(), operatorSubject, f.r, f.store, token)
			require.NoError(t, err)
			token, err = f.h.authorizeRetry(t.Context(), operatorSubject, f.r, f.store, token)
			require.NoError(t, err)
			result, err := f.r.Resume(t.Context(), token)
			// Assert.
			require.NoError(t, err)
			require.Equal(t, original.OperationID, result.State.OperationID)
			require.Equal(t, 1, f.e.calls)
			require.Equal(t, 1, f.e.writes)
		})
	}
}
