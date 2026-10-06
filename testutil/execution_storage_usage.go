package testutil

import "encoding/json"

// ExecutionStorageUsage measures serialized retained payload and permanent
// metadata separately. It excludes Go allocator/map overhead; benchmark B/op
// measures allocations. Metadata includes IDs, revisions, fences and anchors.
type ExecutionStorageUsage struct {
	PayloadBytes  int
	MetadataBytes int
	Revisions     int
	Executions    int
}

// StorageUsage observes a detached consistent snapshot for conformance workloads.
func (s *MemoryExecutionStore) StorageUsage() (ExecutionStorageUsage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	usage := ExecutionStorageUsage{Executions: len(s.heads), PayloadBytes: 0, MetadataBytes: 0, Revisions: 0}
	for _, history := range s.history {
		for _, payload := range history {
			usage.PayloadBytes += len(payload)
			usage.Revisions++
		}
	}
	metadata, err := json.Marshal(map[string]any{
		"heads": s.heads, "fences": s.fences, "leases": s.leases,
		"fork": s.lineage, "incoming": s.incoming, "outgoing": s.outgoing,
	})
	if err != nil {
		return ExecutionStorageUsage{}, err
	}
	usage.MetadataBytes = len(metadata)
	return usage, nil
}
