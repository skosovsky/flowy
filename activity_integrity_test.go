package flowy

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestActivityJournalRejectsInvalidRecords(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*ActivityRecord){
		"identity":               func(r *ActivityRecord) { r.Identity = "bad" },
		"run address":            func(r *ActivityRecord) { r.ExecutionID = "other" },
		"node address":           func(r *ActivityRecord) { r.Node = "other" },
		"activation address":     func(r *ActivityRecord) { r.Activation++ },
		"input digest":           func(r *ActivityRecord) { r.InputDigest = "bad" },
		"key":                    func(r *ActivityRecord) { r.Key = "" },
		"implementation":         func(r *ActivityRecord) { r.Implementation = "" },
		"state":                  func(r *ActivityRecord) { r.State = "invented" },
		"missing attempts":       func(r *ActivityRecord) { r.Attempts = nil },
		"attempt number":         func(r *ActivityRecord) { r.Attempts[0].Number = 2 },
		"incarnation":            func(r *ActivityRecord) { r.Attempts[0].Incarnation = 0 },
		"start timestamp":        func(r *ActivityRecord) { r.Attempts[0].StartedAt = time.Time{} },
		"attempt state":          func(r *ActivityRecord) { r.Attempts[0].State = ActivityPrepared },
		"running finished":       func(r *ActivityRecord) { r.Attempts[0].FinishedAt = time.Now() },
		"running classification": func(r *ActivityRecord) { r.Classification = ActivityAmbiguous },
		"running outcome":        func(r *ActivityRecord) { r.Outcome = []byte("fabricated") },
		"retry policy":           func(r *ActivityRecord) { r.Retry.MaxAttempts = 2 },
		"unknown attempt":        func(r *ActivityRecord) { r.State = ActivityUnknown; r.Classification = ActivityAmbiguous },
		"unproven completed":     func(r *ActivityRecord) { r.State = ActivityCompleted; r.Origin = ActivityLive },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			record := validRunningActivityForTest()
			identity := record.Identity
			if err := validateActivityRecord(record); err != nil {
				t.Fatal(err)
			}
			// Act.
			mutate(&record)
			payload, err := json.Marshal(map[string]ActivityRecord{identity: record})
			if err != nil {
				t.Fatal(err)
			}
			_, err = decodeActivityJournal(payload)
			// Assert.
			if !errors.Is(err, ErrExecutionCorrupt) {
				t.Fatalf("invalid record accepted: %v", err)
			}
		})
	}
}

func validRunningActivityForTest() ActivityRecord {
	return ActivityRecord{
		Identity: activityAddressIdentity(
			"run",
			"node",
			1,
			"operation",
		),
		ExecutionID:    "run",
		Node:           "node",
		Activation:     1,
		Key:            "operation",
		InputDigest:    strings.Repeat("b", 64),
		Implementation: "host-contract",
		State:          ActivityRunning,
		Attempts: []ActivityAttempt{
			{Number: 1, Incarnation: 1, State: ActivityRunning, StartedAt: time.Now().UTC()},
		},
	}
}

func TestActivityJournalRejectsNonObjectsAndAllowsEmpty(t *testing.T) {
	t.Parallel()
	for _, payload := range []string{"null", "[]", "1", "{", `{"bad":{}}`} {
		// Arrange/Act.
		_, err := decodeActivityJournal([]byte(payload))
		// Assert.
		if !errors.Is(err, ErrExecutionCorrupt) {
			t.Fatalf("invalid journal %q accepted: %v", payload, err)
		}
	}
	for _, payload := range [][]byte{nil, []byte("{}")} {
		journal, err := decodeActivityJournal(payload)
		if err != nil || journal == nil || len(journal) != 0 {
			t.Fatalf("empty journal rejected: %v", err)
		}
	}
}

func TestActivityJournalRetainsUnknownAttemptAfterReconciliation(t *testing.T) {
	t.Parallel()
	for _, origin := range []ActivityOrigin{ActivityManual, ActivityReconciled} {
		// Arrange: successful reconciliation does not fabricate dispatch completion.
		record := validRunningActivityForTest()
		record.Attempts[0].State = ActivityUnknown
		record.Attempts[0].Classification = ActivityAmbiguous
		record.State, record.Origin = ActivityCompleted, origin
		if origin == ActivityManual {
			record.Resolutions = []ActivityResolutionRecord{
				{
					DecisionID:     "confirmed",
					Attempt:        1,
					Action:         ActivityResolveComplete,
					PriorState:     ActivityUnknown,
					SourceRevision: 1,
					Incarnation:    2,
					Reason:         "confirmed",
					Evidence:       "receipt",
					At:             time.Now().UTC(),
				},
			}
		}
		record.Classification = ActivityAmbiguous
		// Act.
		err := validateActivityRecord(record)
		// Assert.
		if err != nil {
			t.Fatalf("valid reconciled record rejected: %v", err)
		}
	}
}

func TestActivityJournalValidatesExecutionAddressAndRetainsHistoricalNode(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, id   string
		activation uint64
		corrupt    bool
	}{
		{name: "foreign execution", id: "other", activation: 2, corrupt: true},
		{name: "future activation", id: "run", activation: 0, corrupt: true},
		{name: "historical node", id: "run", activation: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			// Arrange: source node address remains valid after the execution cursor moves.
			record := validRunningActivityForTest()
			record.State, record.Origin = ActivityCompleted, ActivityLive
			record.Attempts[0].State = ActivityCompleted
			record.Attempts[0].FinishedAt = time.Now().UTC()
			payload, err := json.Marshal(map[string]ActivityRecord{record.Identity: record})
			if err != nil {
				t.Fatal(err)
			}
			envelope := ExecutionEnvelope{ExecutionID: test.id, Activation: test.activation,
				Progress: MigrationState{ExecutionPointer: "migrated-node"}, JournalPayload: payload}
			// Act.
			journal, err := executionActivityJournal(envelope)
			// Assert.
			if test.corrupt {
				if !errors.Is(err, ErrExecutionCorrupt) {
					t.Fatalf("foreign/future activity accepted: %v", err)
				}
				return
			}
			if err != nil || journal[record.Identity].Node != "node" {
				t.Fatalf("historical address lost: %v", err)
			}
		})
	}
}

func TestActivityJournalRejectsUnresolvedOrDanglingMigrationReferences(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		refs       map[string]string
		activation uint64
		valid      bool
	}{
		{name: "missing binding", activation: 1},
		{name: "dangling binding", activation: 1, refs: map[string]string{"operation": "missing"}},
		{name: "wrong key", activation: 1, refs: map[string]string{"different": activityAddressIdentity("run", "node", 1, "operation")}},
		{name: "past unresolved activation", activation: 2},
		{name: "explicit binding", activation: 1, refs: map[string]string{"operation": activityAddressIdentity("run", "node", 1, "operation")}, valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			// Arrange.
			record := validRunningActivityForTest()
			payload, err := json.Marshal(map[string]ActivityRecord{record.Identity: record})
			if err != nil {
				t.Fatal(err)
			}
			envelope := ExecutionEnvelope{
				ExecutionID: "run",
				Activation:  test.activation,
				Progress: MigrationState{
					ExecutionPointer:  "migrated",
					JournalReferences: test.refs,
				},
				JournalPayload: payload,
			}
			// Act.
			_, err = executionActivityJournal(envelope)
			// Assert.
			if test.valid {
				if err != nil {
					t.Fatalf("explicit binding rejected: %v", err)
				}
			} else if !errors.Is(err, ErrExecutionCorrupt) {
				t.Fatalf("unsafe binding accepted: %v", err)
			}
		})
	}
}
