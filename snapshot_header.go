package flowy

import "fmt"

// ValidateSnapshotHeader checks storage-facing identity and revision metadata
// without calling a host codec. Save assigns its new stored revision before this
// check; the caller's snapshot revision does not replace expectedRevision/OCC.
func ValidateSnapshotHeader(threadID string, revision uint64, pointer ExecutionPointer) error {
	if threadID == "" || pointer == "" || revision == 0 || !validRuntimeText(threadID, string(pointer)) {
		return fmt.Errorf(
			"%w: nonempty UTF-8 identity, pointer and positive revision required",
			ErrSnapshotEnvelopeInvalid,
		)
	}
	return nil
}
