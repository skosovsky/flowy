package flowy

import (
	"context"
	"errors"
)

// Node is the basic computation unit. It receives the current state and returns
// the updated state (or a delta to be merged by the Reducer).
type Node[T any] func(ctx context.Context, state T) (T, error)

// ConditionalEdge is a routing function that decides which node to execute next.
type ConditionalEdge[T any] func(ctx context.Context, state T) (string, error)

// Reducer defines how to merge the current state with the update returned by a node.
type Reducer[T any] func(current T, update T) T

// EventType identifies the kind of stream event.
type EventType string

const (
	// EventNodeStart is emitted before a node runs.
	EventNodeStart EventType = "node_start"
	// EventNodeEnd is emitted after a node completes successfully.
	EventNodeEnd EventType = "node_end"
	// EventInterrupt is emitted when execution is suspended (HITL).
	EventInterrupt EventType = "interrupt"
	// EventError is emitted when a node returns an error.
	EventError EventType = "error"
)

// Event is a single item streamed during graph execution.
type Event[T any] struct {
	Type     EventType
	NodeName string
	State    T
	Err      error
}

// Sentinel errors for flow control and validation.
var (
	// ErrInterrupt is returned when execution is suspended at an interrupt point (HITL).
	ErrInterrupt = errors.New("flowy: interrupt")
	// ErrMaxStepsExceeded is returned when the step limit is reached (e.g. infinite loop).
	ErrMaxStepsExceeded = errors.New("flowy: max steps exceeded")
	// ErrThreadNotFound is returned by Resume when the threadID is not in the checkpointer.
	ErrThreadNotFound = errors.New("flowy: thread not found")
)
