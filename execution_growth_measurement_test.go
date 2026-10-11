package flowy_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

//nolint:gocognit // Keep workload commits and measured serialization assertions together.
func TestExecutionWaitDeliveryRewriteGrowth(t *testing.T) {
	for _, count := range []int{16, 64} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			// Arrange: an armed wait retains every unique unmatched delivery.
			ctx := context.Background()
			store := newWaitRegistrationStore()
			var calls, matches, applies atomic.Int32
			runner := deliveryRunner(t, store, &calls, checkpoint.JSONSerializer[durableTestState]{})
			if _, err := runner.Start(ctx, "run", durableTestState{}); err != nil {
				t.Fatal(err)
			}
			delivery := deliveryForArmedWait(t, store)
			contract := deliveryContract(&matches, &applies)
			delivery.Payload = []byte("unmatched")
			written := 0
			// Act: exact current revisions admit distinct nonwinning deliveries.
			for i := range count {
				delivery.ID = fmt.Sprintf("unmatched-%d", i)
				result, err := runner.DeliverWait(ctx, "run", delivery, contract)
				if err != nil {
					t.Fatal(err)
				}
				delivery.ExpectedRevision = result.ResumeToken.SnapshotRevision
				head, err := store.LoadExecution(ctx, "run")
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(head)
				if err != nil {
					t.Fatal(err)
				}
				written += len(encoded)
			}
			head, err := store.LoadExecution(ctx, "run")
			if err != nil {
				t.Fatal(err)
			}
			waits, err := flowy.InspectExecutionWaits(head)
			// Assert: no delivery disappeared; report serialized rewrite volume, excluding backend overhead.
			if err != nil || len(waits) != 1 || len(waits[0].Decisions) != count || applies.Load() != 0 {
				t.Fatalf("waits=%+v err=%v applies=%d", waits, err, applies.Load())
			}
			encoded, err := json.Marshal(head)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("deliveries=%d aggregate-B=%d rewritten-B=%d", count, len(encoded), written)
		})
	}
}

//nolint:gocognit // Keep per-attempt accounting and measured rewrite assertions together.
func TestExecutionAttemptRewriteGrowth(t *testing.T) {
	for _, count := range []int{16, 64} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			// Arrange: zero-delay retries are distinct calls for one activation/key.
			store := &executionGrowthStore{ExecutionStore: testutil.NewMemoryExecutionStore(nil)}
			schedule, err := flowy.NewFixedActivityRetrySchedule(0)
			if err != nil {
				t.Fatal(err)
			}
			b := flowy.NewGraph[int, flowy.NoEffect](func(_, update int) int { return update })
			b.AddNode("attempts", func(ctx context.Context, state int) (int, flowy.Directive, error) {
				request := flowy.ActivityRequest{
					Key:            "same",
					Implementation: "retry-v1",
					Retry: flowy.ActivityRetryPolicy{
						Label:             "fixed",
						MaxAttempts:       count,
						Schedule:          schedule,
						SafeRetryContract: "host-idempotent-v1",
					},
					Dispatch: func(context.Context, flowy.ActivityInvocation) ([]byte, error) {
						return nil, errors.New("known failure")
					},
					Classify: func(error) flowy.ActivityFailureDecision {
						return flowy.ActivityFailureDecision{Class: flowy.ActivityRetryable}
					},
				}
				for i := range count {
					_, callErr := flowy.CallActivity(ctx, request)
					expected := flowy.ErrActivityRetryPending
					if i == count-1 {
						expected = flowy.ErrActivityAttemptsExhausted
					}
					if !errors.Is(callErr, expected) {
						return state, flowy.End(), callErr
					}
				}
				return count, flowy.End(), nil
			}).SetEntryPoint("attempts").AllowNoOutgoingRoute("attempts")
			graph, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			runner, err := flowy.NewDurableRunner(graph, store, durableDescriptor("attempt-growth"),
				checkpoint.JSONSerializer[int]{}, checkpoint.JSONSerializer[[]flowy.NoEffect]{},
				flowy.DurableOptions{Owner: "test", LeaseTTL: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			result, err := runner.Start(context.Background(), "growth", 0)
			// Assert: attempts remain in the authoritative journal without TTL compaction.
			if err != nil || result.State != count {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			head, err := store.LoadExecution(context.Background(), "growth")
			if err != nil {
				t.Fatal(err)
			}
			var records map[string]flowy.ActivityRecord
			err = json.Unmarshal(head.JournalPayload, &records)
			if err != nil || len(records) != 1 || attemptCount(records) != count {
				t.Fatalf("records=%+v err=%v", records, err)
			}
			t.Logf(
				"attempts=%d commits=%d aggregate-B=%d rewritten-B=%d",
				count,
				store.commits,
				store.aggregateBytes,
				store.historyBytes,
			)
		})
	}
}

func attemptCount(records map[string]flowy.ActivityRecord) int {
	total := 0
	for _, record := range records {
		total += len(record.Attempts)
	}
	return total
}
