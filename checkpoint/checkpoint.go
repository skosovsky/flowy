// Package checkpoint provides persistence types and state envelope encoding for graph snapshots.
// The execution core (flowy) is stateless; adapters implement Checkpointer using these types.
package checkpoint

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Checkpoint is a serialized graph snapshot at a particular execution step.
type Checkpoint struct {
	ID        string
	ThreadID  string
	RunID     string
	Node      string
	Next      string
	StateData []byte // JSON storage envelope persisted by the checkpointer.
	CreatedAt time.Time
}

// LoadedCheckpoint contains raw checkpoint metadata and the decoded state.
type LoadedCheckpoint[T any] struct {
	Checkpoint

	State T
}

// Checkpointer persists and loads graph checkpoints.
// Save must be idempotent by Checkpoint.ID.
// LoadLatest and GetHistory return checkpoints ordered by CreatedAt newest-first,
// breaking ties by Checkpoint.ID descending.
type Checkpointer interface {
	Save(ctx context.Context, cp Checkpoint) error
	LoadLatest(ctx context.Context, threadID string) (Checkpoint, error)
	GetHistory(ctx context.Context, threadID string, limit int) ([]Checkpoint, error)
}

// StateSerializer converts graph state to bytes and back.
// Marshal output is wrapped into a JSON storage envelope before persistence.
type StateSerializer[T any] interface {
	Marshal(state T) ([]byte, error)
	Unmarshal(data []byte, state *T) error
}

// JSONSerializer is the default state serializer for checkpointing.
type JSONSerializer[T any] struct{}

func (JSONSerializer[T]) Marshal(state T) ([]byte, error) {
	return json.Marshal(state)
}

func (JSONSerializer[T]) Unmarshal(data []byte, state *T) error {
	return json.Unmarshal(data, state)
}

// ErrNoCheckpoint is returned when no checkpoint exists for a thread.
var ErrNoCheckpoint = errors.New("checkpoint: no checkpoint")

const stateEnvelopeKind = "flowy-checkpoint-state/v2"

type stateEnvelope struct {
	Kind          string          `json:"$flowy_checkpoint_state"`
	Encoding      string          `json:"encoding"`
	PayloadJSON   json.RawMessage `json:"payload_json,omitempty"`
	PayloadBase64 string          `json:"payload_base64,omitempty"`
}

// EncodeStateData wraps serialized state bytes in a versioned JSON envelope.
func EncodeStateData(data []byte) ([]byte, error) {
	if json.Valid(data) {
		//nolint:exhaustruct // JSON branch uses only Kind, Encoding, PayloadJSON
		encoded, err := json.Marshal(stateEnvelope{
			Kind:        stateEnvelopeKind,
			Encoding:    "json",
			PayloadJSON: append(json.RawMessage(nil), data...),
		})
		if err != nil {
			return nil, fmt.Errorf("checkpoint: encode state envelope: %w", err)
		}
		return encoded, nil
	}

	//nolint:exhaustruct // base64 branch uses only Kind, Encoding, PayloadBase64
	payload := stateEnvelope{
		Kind:          stateEnvelopeKind,
		Encoding:      "base64",
		PayloadBase64: base64.StdEncoding.EncodeToString(data),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: encode state envelope: %w", err)
	}
	return encoded, nil
}

// DecodeStateData extracts raw payload bytes from EncodeStateData output.
func DecodeStateData(data []byte) ([]byte, error) {
	var payload stateEnvelope
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("checkpoint: decode state envelope: %w", err)
	}
	if payload.Kind != stateEnvelopeKind {
		return nil, fmt.Errorf("checkpoint: unsupported state envelope kind %q", payload.Kind)
	}

	switch payload.Encoding {
	case "json":
		if len(payload.PayloadJSON) == 0 {
			return nil, errors.New("checkpoint: state envelope missing payload_json")
		}
		return append([]byte(nil), payload.PayloadJSON...), nil
	case "base64":
		if payload.PayloadBase64 == "" {
			return nil, errors.New("checkpoint: state envelope missing payload_base64")
		}
		decoded, err := base64.StdEncoding.DecodeString(payload.PayloadBase64)
		if err != nil {
			return nil, fmt.Errorf("checkpoint: decode state envelope: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("checkpoint: unsupported state encoding %q", payload.Encoding)
	}
}

// NewSortableID returns a new time-ordered checkpoint ID.
func NewSortableID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("checkpoint: generate uuidv7: %w", err)
	}
	return id.String(), nil
}
