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
func EncodeStoredSnapshot[T any](
	snapshot flowy.Snapshot[T],
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
		NodeID:       snapshot.NodeID,
		StatePayload: statePayload,
		RunMeta:      metaPayload,
		Effects:      effectsPayload,
		UpdatedAt:    time.Now().UTC(),
	}, nil
}

// DecodeStoredSnapshot converts DB payload to typed snapshot.
func DecodeStoredSnapshot[T any](
	stored StoredSnapshot,
	serializer flowy.StateSerializer[T],
) (flowy.Snapshot[T], error) {
	state, err := serializer.Unmarshal(stored.StatePayload)
	if err != nil {
		return flowy.Snapshot[T]{}, fmt.Errorf("checkpoint: unmarshal state: %w", err)
	}
	var meta flowy.RunMetadata
	if err := json.Unmarshal(stored.RunMeta, &meta); err != nil {
		return flowy.Snapshot[T]{}, fmt.Errorf("checkpoint: unmarshal run meta: %w", err)
	}
	var effects []any
	if len(stored.Effects) > 0 {
		if err := json.Unmarshal(stored.Effects, &effects); err != nil {
			return flowy.Snapshot[T]{}, fmt.Errorf("checkpoint: unmarshal effects: %w", err)
		}
	}

	return flowy.Snapshot[T]{
		ThreadID: stored.ThreadID,
		Revision: stored.Revision,
		NodeID:   stored.NodeID,
		State:    state,
		RunMeta:  meta,
		Effects:  effects,
	}, nil
}

// UnmarshalJSON keeps backward-read compatibility with legacy payload key "version".
func (s *StoredSnapshot) UnmarshalJSON(data []byte) error {
	type snapshotAlias struct {
		ThreadID     string          `json:"thread_id"`
		Revision     int             `json:"revision"`
		Version      int             `json:"version"`
		NodeID       string          `json:"node_id"`
		StatePayload json.RawMessage `json:"state_payload"`
		RunMeta      json.RawMessage `json:"run_meta"`
		Effects      json.RawMessage `json:"effects"`
		UpdatedAt    time.Time       `json:"updated_at"`
	}
	var raw snapshotAlias
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.ThreadID = raw.ThreadID
	if raw.Revision != 0 {
		s.Revision = raw.Revision
	} else {
		s.Revision = raw.Version
	}
	s.NodeID = raw.NodeID
	s.StatePayload = raw.StatePayload
	s.RunMeta = raw.RunMeta
	s.Effects = raw.Effects
	s.UpdatedAt = raw.UpdatedAt
	return nil
}
