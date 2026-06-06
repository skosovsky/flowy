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

// StoredSnapshot is persistence payload used by adapters.
type StoredSnapshot struct {
	ThreadID     string          `json:"thread_id"`
	Revision     int             `json:"revision"`
	NodeID       string          `json:"node_id"`
	StatePayload json.RawMessage `json:"state_payload"`
	RunMeta      json.RawMessage `json:"run_meta"`
	Effects      json.RawMessage `json:"effects"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// EncodeStoredSnapshot converts typed snapshot to serialized DB payload.
func EncodeStoredSnapshot[T, E any](
	snapshot flowy.Snapshot[T, E],
	serializer flowy.StateSerializer[T],
) (StoredSnapshot, error) {
	statePayload, err := serializer.Marshal(snapshot.State)
	if err != nil {
		return StoredSnapshot{}, fmt.Errorf("checkpoint: marshal state: %w", err)
	}
	metaPayload, err := json.Marshal(snapshot.RunMeta)
	if err != nil {
		return StoredSnapshot{}, fmt.Errorf("checkpoint: marshal run meta: %w", err)
	}
	effectsPayload, err := json.Marshal(snapshot.Effects)
	if err != nil {
		return StoredSnapshot{}, fmt.Errorf("checkpoint: marshal effects: %w", err)
	}

	return StoredSnapshot{
		ThreadID:     snapshot.ThreadID,
		Revision:     snapshot.Revision,
		NodeID:       string(snapshot.ExecutionPointer),
		StatePayload: statePayload,
		RunMeta:      metaPayload,
		Effects:      effectsPayload,
		UpdatedAt:    time.Now().UTC(),
	}, nil
}

// DecodeStoredSnapshot converts DB payload to typed snapshot.
func DecodeStoredSnapshot[T, E any](
	stored StoredSnapshot,
	serializer flowy.StateSerializer[T],
) (flowy.Snapshot[T, E], error) {
	state, err := serializer.Unmarshal(stored.StatePayload)
	if err != nil {
		return flowy.Snapshot[T, E]{}, fmt.Errorf("checkpoint: unmarshal state: %w", err)
	}
	var meta flowy.RunMetadata
	if err := json.Unmarshal(stored.RunMeta, &meta); err != nil {
		return flowy.Snapshot[T, E]{}, fmt.Errorf("checkpoint: unmarshal run meta: %w", err)
	}
	var effects []E
	if len(stored.Effects) > 0 {
		if err := json.Unmarshal(stored.Effects, &effects); err != nil {
			return flowy.Snapshot[T, E]{}, fmt.Errorf("checkpoint: unmarshal effects: %w", err)
		}
	}

	return flowy.Snapshot[T, E]{
		ThreadID:         stored.ThreadID,
		Revision:         stored.Revision,
		ExecutionPointer: flowy.ExecutionPointer(stored.NodeID),
		State:            state,
		RunMeta:          meta,
		Effects:          effects,
	}, nil
}
