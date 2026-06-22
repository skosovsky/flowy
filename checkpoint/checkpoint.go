// Package checkpoint provides storage helpers for flowy snapshots.
package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/skosovsky/flowy"
)

// ErrNoSnapshot is returned when thread snapshot is absent.
var ErrNoSnapshot = errors.New("checkpoint: no snapshot")

// ErrInvalidRecord is returned when checkpoint record metadata is incomplete or inconsistent.
var ErrInvalidRecord = errors.New("checkpoint: invalid record")

// JSONSerializer is the default JSON serializer for state T.
type JSONSerializer[T any] struct{}

func (JSONSerializer[T]) Marshal(state T) ([]byte, error) {
	return json.Marshal(state)
}

func (JSONSerializer[T]) Unmarshal(data []byte) (T, error) {
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	return out, nil
}

type sanitizingSerializer[T any] struct {
	base     flowy.StateSerializer[T]
	sanitize func(*T)
}

// WithSanitizer wraps serializer with pre/post sanitize hook.
func WithSanitizer[T any](
	base flowy.StateSerializer[T],
	sanitize func(*T),
) flowy.StateSerializer[T] {
	return &sanitizingSerializer[T]{base: base, sanitize: sanitize}
}

func (s *sanitizingSerializer[T]) Marshal(state T) ([]byte, error) {
	if s.sanitize != nil {
		s.sanitize(&state)
	}
	return s.base.Marshal(state)
}

func (s *sanitizingSerializer[T]) Unmarshal(data []byte) (T, error) {
	state, err := s.base.Unmarshal(data)
	if err != nil {
		var zero T
		return zero, err
	}
	if s.sanitize != nil {
		s.sanitize(&state)
	}
	return state, nil
}

// Record is the storage-facing checkpoint envelope used by adapters.
type Record struct {
	ThreadID     string          `json:"thread_id"`
	Revision     uint64          `json:"revision"`
	NodeID       string          `json:"node_id"`
	StatePayload json.RawMessage `json:"state_payload"`
	RunMeta      json.RawMessage `json:"run_meta"`
	Effects      json.RawMessage `json:"effects"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// DecodeRecordOptions validates storage metadata against the checkpoint envelope.
type DecodeRecordOptions struct {
	ExpectedThreadID         string
	ExpectedRevision         uint64
	ExpectedExecutionPointer flowy.ExecutionPointer
}

// EncodeRecord converts typed snapshot to a storage checkpoint record.
func EncodeRecord[T, E any](
	snapshot flowy.Snapshot[T, E],
	serializer flowy.StateSerializer[T],
) (Record, error) {
	statePayload, err := serializer.Marshal(snapshot.State)
	if err != nil {
		return Record{}, fmt.Errorf("checkpoint: marshal state: %w", err)
	}
	metaPayload, err := json.Marshal(snapshot.RunMeta)
	if err != nil {
		return Record{}, fmt.Errorf("checkpoint: marshal run meta: %w", err)
	}
	effectsPayload, err := json.Marshal(snapshot.Effects)
	if err != nil {
		return Record{}, fmt.Errorf("checkpoint: marshal effects: %w", err)
	}

	return Record{
		ThreadID:     snapshot.ThreadID,
		Revision:     snapshot.Revision,
		NodeID:       string(snapshot.ExecutionPointer),
		StatePayload: statePayload,
		RunMeta:      metaPayload,
		Effects:      effectsPayload,
		UpdatedAt:    time.Now().UTC(),
	}, nil
}

// DecodeRecord converts a storage checkpoint record to a typed snapshot.
func DecodeRecord[T, E any](
	record Record,
	serializer flowy.StateSerializer[T],
	opts DecodeRecordOptions,
) (flowy.Snapshot[T, E], error) {
	if record.ThreadID == "" {
		return flowy.Snapshot[T, E]{}, invalidRecordError("empty thread_id")
	}
	if record.Revision == 0 {
		return flowy.Snapshot[T, E]{}, invalidRecordError("zero revision")
	}
	if record.NodeID == "" {
		return flowy.Snapshot[T, E]{}, invalidRecordError("empty node_id")
	}
	if opts.ExpectedThreadID != "" && record.ThreadID != opts.ExpectedThreadID {
		return flowy.Snapshot[T, E]{}, invalidRecordError(fmt.Sprintf(
			"thread_id mismatch: record=%q expected=%q",
			record.ThreadID,
			opts.ExpectedThreadID,
		))
	}
	if opts.ExpectedRevision != 0 && record.Revision != opts.ExpectedRevision {
		return flowy.Snapshot[T, E]{}, invalidRecordError(fmt.Sprintf(
			"revision mismatch: record=%d expected=%d",
			record.Revision,
			opts.ExpectedRevision,
		))
	}
	if opts.ExpectedExecutionPointer != "" &&
		flowy.ExecutionPointer(record.NodeID) != opts.ExpectedExecutionPointer {
		return flowy.Snapshot[T, E]{}, invalidRecordError(fmt.Sprintf(
			"execution pointer mismatch: record=%q expected=%q",
			record.NodeID,
			opts.ExpectedExecutionPointer,
		))
	}
	state, err := serializer.Unmarshal(record.StatePayload)
	if err != nil {
		return flowy.Snapshot[T, E]{}, invalidRecordCauseError("unmarshal state", err)
	}
	var meta flowy.RunMetadata
	if err := json.Unmarshal(record.RunMeta, &meta); err != nil {
		return flowy.Snapshot[T, E]{}, invalidRecordCauseError("unmarshal run meta", err)
	}
	var effects []E
	if len(record.Effects) > 0 {
		if err := json.Unmarshal(record.Effects, &effects); err != nil {
			return flowy.Snapshot[T, E]{}, invalidRecordCauseError("unmarshal effects", err)
		}
	}

	return flowy.Snapshot[T, E]{
		ThreadID:         record.ThreadID,
		Revision:         record.Revision,
		ExecutionPointer: flowy.ExecutionPointer(record.NodeID),
		State:            state,
		RunMeta:          meta,
		Effects:          effects,
	}, nil
}

func invalidRecordError(reason string) error {
	return fmt.Errorf("%w: %w: %s", flowy.ErrSnapshotEnvelopeInvalid, ErrInvalidRecord, reason)
}

func invalidRecordCauseError(reason string, err error) error {
	return fmt.Errorf("%w: checkpoint: %s: %w", flowy.ErrSnapshotEnvelopeInvalid, reason, err)
}
