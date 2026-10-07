package flowy_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy"
)

type resumeInspectionStore struct {
	flowy.ExecutionStore

	head flowy.ExecutionEnvelope
}

func (s resumeInspectionStore) LoadExecution(context.Context, string) (flowy.ExecutionEnvelope, error) {
	return s.head, nil
}
func TestInspectExecutionResumeReadOnly(t *testing.T) {
	// Arrange: seed a valid stored execution using the established history fixture.
	store, seed := seedHistoryInspection(t)
	head, loadErr := store.LoadExecution(t.Context(), seed.ExecutionID)
	require.NoError(t, loadErr)
	// Act.
	token, err := flowy.InspectExecutionResume(t.Context(), store, head.ExecutionID)
	// Assert: core supplies an exact address without adding a checkpoint.
	require.NoError(t, err)
	require.Equal(t, head.ExecutionID, token.ThreadID)
	require.Equal(t, head.Revision, token.SnapshotRevision)
	after, err := store.LoadExecution(t.Context(), head.ExecutionID)
	require.NoError(t, err)
	require.Equal(t, head, after)
}
func TestInspectExecutionResumeRejectsCorruptAndWrongAddress(t *testing.T) {
	// Arrange.
	store, head := seedHistoryInspection(t)
	for _, mode := range []string{"seal", "address", "empty"} {
		t.Run(mode, func(t *testing.T) {
			corrupt := head
			id := head.ExecutionID
			switch mode {
			case "seal":
				corrupt.Digest = "tampered"
			case "address":
				id = "other"
			case "empty":
				id = ""
			}
			// Act.
			token, err := flowy.InspectExecutionResume(
				t.Context(),
				resumeInspectionStore{ExecutionStore: store, head: corrupt},
				id,
			)
			// Assert.
			require.Error(t, err)
			require.Empty(t, token)
		})
	}
}
