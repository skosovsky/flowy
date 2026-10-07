package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/skosovsky/toolsy"
	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy"
)

type inspectStore struct {
	operationStore

	record toolsy.OperationRecord
	found  bool
	err    error
}

func (s *inspectStore) Inspect(context.Context, toolsy.OperationBinding) (toolsy.OperationRecord, bool, error) {
	return s.record, s.found, s.err
}
func TestReconcileRejectsUnprovenEvidence(t *testing.T) {
	// Arrange: a real effect commits, but its delivery is lost.
	f := setup(t)
	s := f.start(t, "unknown")
	_, accepted := f.approve(t, "unknown", "allow")
	f.h.fault = func(point string) error {
		if point == "lost_delivery" {
			return errFault
		}
		return nil
	}
	_, err := f.r.Resume(t.Context(), accepted.ResumeToken)
	require.ErrorIs(t, err, flowy.ErrActivityUnknown)
	m, err := f.e.Lookup(t.Context(), s.OperationID)
	require.NoError(t, err)
	record, found, err := f.op.Inspect(t.Context(), s.Binding)
	require.NoError(t, err)
	require.True(t, found)
	before := f.op.Snapshot()
	cases := map[string]toolsy.OperationRecord{
		"running":    {Binding: s.Binding, State: toolsy.OperationInProgress},
		"unknown":    {Binding: s.Binding, State: toolsy.OperationUnknown},
		"incomplete": {Binding: s.Binding, State: toolsy.OperationCompleted},
		"corrupt":    {Binding: s.Binding, State: toolsy.OperationCompleted, Result: []byte(`{}`)},
		"foreign":    {State: toolsy.OperationCompleted, Result: record.Result},
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			// Act.
			h := *f.h
			h.operations = &inspectStore{operationStore: f.op, record: bad, found: true}
			_, probeErr := h.reconcile(t.Context(), s, m.ActivityIdentity)
			// Assert.
			require.Error(t, probeErr)
			require.Equal(t, before, f.op.Snapshot())
			require.Equal(t, 1, f.e.calls)
		})
	}
	h := *f.h
	h.operations = &inspectStore{operationStore: f.op, found: false}
	_, err = h.reconcile(t.Context(), s, m.ActivityIdentity)
	require.Error(t, err)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = f.h.reconcile(canceled, s, m.ActivityIdentity)
	require.Error(t, err)
	deadline, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stop()
	_, err = f.h.reconcile(deadline, s, m.ActivityIdentity)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = f.h.reconcile(t.Context(), s, "wrong-activity")
	require.Error(t, err)
	s.Label = "unsupported"
	_, err = f.h.reconcile(t.Context(), s, m.ActivityIdentity)
	require.Error(t, err)
	require.Equal(t, before, f.op.Snapshot())
	require.Equal(t, 1, f.e.writes)
}
func TestConcurrentReconcileIsReadOnly(t *testing.T) {
	// Arrange.
	f := setup(t)
	s := f.start(t, "parallel")
	_, accepted := f.approve(t, "parallel", "allow")
	f.h.fault = func(point string) error {
		if point == "lost_delivery" {
			return errFault
		}
		return nil
	}
	_, err := f.r.Resume(t.Context(), accepted.ResumeToken)
	require.Error(t, err)
	m, err := f.e.Lookup(t.Context(), s.OperationID)
	require.NoError(t, err)
	before := f.op.Snapshot()
	captures := len(f.e.captures)
	// Act.
	var wg sync.WaitGroup
	results := make(chan error, 12)
	for range 12 {
		wg.Go(func() {
			raw, probeErr := f.h.reconcile(t.Context(), s, m.ActivityIdentity)
			if probeErr == nil {
				var r receipt
				probeErr = json.Unmarshal(raw, &r)
				if r.OperationID != s.OperationID {
					probeErr = errEvidence
				}
			}
			results <- probeErr
		})
	}
	wg.Wait()
	close(results)
	// Assert.
	for probeErr := range results {
		require.NoError(t, probeErr)
	}
	require.Equal(t, before, f.op.Snapshot())
	require.Equal(t, 1, f.e.calls)
	require.Equal(t, 1, f.e.writes)
	require.Len(t, f.e.captures, captures)
}
func TestTerminalBusinessRejection(t *testing.T) {
	// Arrange.
	f := setup(t)
	_, err := f.r.Start(t.Context(), "reject", initial("call", "op", input{Value: "reject"}))
	require.NoError(t, err)
	_, accepted := f.approve(t, "reject", "allow")
	// Act.
	result, err := f.r.Resume(t.Context(), accepted.ResumeToken)
	// Assert: orchestration completion does not imply business acceptance.
	require.NoError(t, err)
	require.NotNil(t, result.State.Result)
	require.False(t, result.State.Result.Accepted)
	require.Equal(t, 1, f.e.calls)
	require.Zero(t, f.e.writes)
}
func TestEnvelopeAndMappingConflicts(t *testing.T) {
	for _, mode := range []string{"label", "call", "operation", "digest", "binding"} {
		t.Run(mode, func(t *testing.T) {
			// Arrange.
			s := initial("call", "op", input{Value: "same"})
			switch mode {
			case "label":
				s.Label = "unknown"
			case "call":
				s.CallID = ""
			case "operation":
				s.OperationID = ""
			case "digest":
				s.Input.Value = "tampered"
			case "binding":
				s.Binding.OperationID = "other"
			}
			// Act/Assert.
			require.Error(t, s.validate(true))
		})
	}
	f := setup(t)
	m := mapping{CallID: "call", OperationID: "op", ActivityIdentity: "activity", Digest: "digest"}
	require.NoError(t, f.e.Map(t.Context(), m))
	m.ActivityIdentity = "other"
	require.Error(t, f.e.Map(t.Context(), m))
	require.Zero(t, f.e.writes)
}

func TestConflictingTerminalEvidence(t *testing.T) {
	// Arrange: mutate the persisted real backend result, not a permissive mock codec.
	f := setup(t)
	s := f.start(t, "evidence")
	_, accepted := f.approve(t, "evidence", "allow")
	f.h.fault = func(point string) error {
		if point == "lost_delivery" {
			return errFault
		}
		return nil
	}
	_, err := f.r.Resume(t.Context(), accepted.ResumeToken)
	require.Error(t, err)
	m, err := f.e.Lookup(t.Context(), s.OperationID)
	require.NoError(t, err)
	record, found, err := f.op.Inspect(t.Context(), s.Binding)
	require.NoError(t, err)
	require.True(t, found)
	for _, mode := range []string{"contradiction", "raw", "mime", "incomplete", "audience", "codec"} {
		t.Run(mode, func(t *testing.T) {
			// Act.
			var wire map[string]any
			require.NoError(t, json.Unmarshal(record.Result, &wire))
			switch mode {
			case "contradiction":
				wire["envelope_result"] = map[string]any{
					"operation_id": s.OperationID,
					"value":        s.Input.Value,
					"accepted":     false,
				}
			case "raw":
				wire["data"] = nil
			case "mime":
				wire["mime"] = ""
			case "incomplete":
				wire["has_envelope_result"] = false
			case "audience":
				wire["audience"] = "untrusted"
			case "codec":
				wire["format"] = "unknown"
			}
			raw, marshalErr := json.Marshal(wire)
			require.NoError(t, marshalErr)
			altered := record
			altered.Result = raw
			h := *f.h
			h.operations = &inspectStore{operationStore: f.op, record: altered, found: true}
			_, probeErr := h.reconcile(t.Context(), s, m.ActivityIdentity)
			// Assert.
			require.Error(t, probeErr)
			require.Equal(t, 1, f.e.writes)
		})
	}
}

func TestReceiptRequiresExplicitFields(t *testing.T) {
	for _, raw := range []string{`{"operation_id":"op","value":"reject"}`, `{"operation_id":"op","value":"","accepted":null}`,
		`{"operation_id":"op","accepted":true}`, `{"value":"same","accepted":true}`} {
		// Arrange/Act/Assert: an absent field is not a zero-valued receipt.
		_, err := decodeReceipt([]byte(raw))
		require.Error(t, err)
	}
	r, err := decodeReceipt([]byte(`{"operation_id":"op","value":"","accepted":false}`))
	require.NoError(t, err)
	require.False(t, r.Accepted)
}
