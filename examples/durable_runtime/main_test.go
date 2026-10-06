package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
)

func TestDurableRuntimeExample(t *testing.T) {
	// Arrange: bounded execution of all self-verifying BYOT examples.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Act.
	err := run(ctx)
	// Assert: every example validates its own outcomes and operation counts.
	if err != nil {
		t.Fatal(err)
	}
}

func TestMigrationExampleRejectsMalformedSource(t *testing.T) {
	// Arrange: target transformation must not replace an unreadable source with a zero value.
	source := flowy.ExecutionProgress{ExecutionPointer: workNode, StatePayload: []byte("{")}
	// Act.
	result, err := correctExecutionProgress(source)
	// Assert: preserve the failed source and propagate its decode error.
	if err == nil || result.ExecutionPointer != source.ExecutionPointer ||
		!bytes.Equal(result.StatePayload, source.StatePayload) {
		t.Fatalf("malformed source converted: %+v %v", result, err)
	}
}
