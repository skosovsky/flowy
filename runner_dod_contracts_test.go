package flowy

import (
	"testing"
)

func TestDoDNoCheckpointCollectorWithValue(t *testing.T) {
	t.Parallel()

	strictForbidden := []string{
		"checkpointCollectorKey",
		"checkpointErrorCollectorKey",
		"softWarnCollectorKey",
		"type checkpointCollector",
		"type checkpointErrorCollector",
		"type softWarnCollector",
	}
	prodForbidden := []string{
		"checkpointCollector",
		"checkpointErrorCollector",
		"softWarnCollector",
	}
	assertNoCheckpointCollectorNeedles(t, prodGoFilesForDoDScan(t), append(strictForbidden, prodForbidden...))
	assertNoCheckpointCollectorNeedles(t, testGoFilesForDoDScan(t), strictForbidden)
}

func TestDoDNoRemovedHandoffSymbols(t *testing.T) {
	t.Parallel()

	removedHandoffForbidden := []string{
		"HandoffScheduler",
		"syncHandoffRunMetaFromCheckpointer",
		"ErrStaleResumeToken",
		"HandoffGeneration",
		"type HandoffGeneration",
	}
	assertNoCheckpointCollectorNeedles(t, prodGoFilesForDoDScan(t), removedHandoffForbidden)
	assertNoCheckpointCollectorNeedles(t, testGoFilesForDoDScan(t), removedHandoffForbidden)
}
