//go:build integration

package main

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/skosovsky/flowy"
	flowyotel "github.com/skosovsky/flowy/ext/otel"
)

func TestPostgresBlueprintRecovery(t *testing.T) {
	// Arrange: actual PostgreSQL, external fake surviving all six workers and
	// an initial trace parent that replacement contexts do not carry.
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("integration requires FLOWY_TEST_DATABASE_URL; memory/skip is not backend evidence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	now := time.Now().UTC()
	m, service := &fakeModel{}, newFakeTool()
	h := &host{model: m, tool: service, clock: &hostClock{at: now}, deadline: now.Add(time.Hour),
		barrier: &childBarrier{ready: make(chan struct{})}}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	flowyotel.InstallTelemetryBridge()
	if err := flowyotel.InstallLifecycleObserverWithTracing(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		flowy.SetLifecycleObserver(nil)
		flowy.SetTelemetryBridge(nil)
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	initial, parent := provider.Tracer("host").Start(ctx, "authenticated-request")
	// Act: lost tool reply, wait arbitration, rejected parent publication,
	// addressed operator recovery, join and a final replay on another pool.
	report, err := runScenario(initial, dsn, h)
	parent.End()
	if err != nil {
		t.Fatal(err)
	}
	// Assert: business outputs and fake external facts, not stdout/exit code.
	if !report.ToolFault || !report.ChildFault || !report.Unresolved || !report.ApprovalReplay || !report.TimerLost {
		t.Fatalf("fault/arbitration proof missing: %+v", report)
	}
	if report.Resolved < 1 || report.Resolved > 2 || h.childCalls.Load() != 2 || h.barrier.completed.Load() != 2 ||
		h.joins.Load() != 1 || h.matches.Load() != 1 || h.applies.Load() != 1 || m.calls.Load() != 1 {
		t.Fatalf("counts: resolved=%d child=%d joined=%d match=%d apply=%d model=%d",
			report.Resolved, h.childCalls.Load(), h.joins.Load(), h.matches.Load(), h.applies.Load(), m.calls.Load())
	}
	assertExternalWrites(t, report, service)
	assertFinal(t, report)
	assertFreshWorkers(t, report)
	assertTraces(t, recorder.Ended(), parent.SpanContext())
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var schemas int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM pg_namespace WHERE nspname=$1", report.Schema).
		Scan(&schemas); err != nil {
		t.Fatal(err)
	}
	if schemas != 0 {
		t.Fatal("owned schema leaked")
	}
}

//nolint:gocognit // Check all persisted scenario authorities together after the worker restart.
func assertFinal(t *testing.T, report scenarioReport) {
	t.Helper()
	final := report.Final
	if final.Status != flowy.RunStatusCompleted || !final.State.Approved || final.State.Receipt == "" ||
		!reflect.DeepEqual(final.State.Plan, []int{2, 3}) || !reflect.DeepEqual(final.State.Values, []int{4, 6}) ||
		!reflect.DeepEqual(final.Effects, []effect{{Kind: "tool", Units: 1}, {Kind: "join", Units: 2}}) ||
		!reflect.DeepEqual(final.RunMeta.BudgetCounts, map[string]int{"tool": 1, "compute": 2}) {
		t.Fatalf("wrong business outcome: %+v", final)
	}
	if report.Head.Terminal == nil || report.Head.Terminal.Status != flowy.RunStatusCompleted ||
		final.ResumeToken.ThreadID != report.Head.ExecutionID || final.ResumeToken.SnapshotRevision != report.Head.Revision ||
		!reflect.DeepEqual(report.Replay, final) {
		t.Fatalf("final authority/replay mismatch: head=%+v final=%+v replay=%+v", report.Head, final, report.Replay)
	}
	if err := flowy.ValidateExecutionIntegrity(report.Head, parentID, final.ResumeToken.SnapshotRevision); err != nil {
		t.Fatal(err)
	}
	var groups map[string]flowy.ChildGroupRecord
	if err := json.Unmarshal(report.Head.ChildrenPayload, &groups); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 {
		t.Fatalf("groups=%d", len(groups))
	}
	for _, group := range groups {
		if len(group.Children) != 2 || len(group.BudgetReturns) != 2 ||
			!reflect.DeepEqual(group.MergedIDs, []string{"a", "b"}) {
			t.Fatalf("persisted join/allocation return: %+v", group)
		}
		for _, child := range group.Children {
			returned, found := group.BudgetReturns[child.Spec.ID]
			if child.State != flowy.ChildCompleted || child.ExecutionID == parentID || !found ||
				!reflect.DeepEqual(returned.Used, map[string]int{computeCounter: 1}) {
				t.Errorf("child usage/result: %+v", child)
			}
			if head, exists := report.ChildHeads[child.Spec.ID]; !exists || head.ExecutionID != child.ExecutionID {
				t.Errorf("independent child head absent/wrong address: %+v", child)
			}
		}
	}
}

func assertExternalWrites(t *testing.T, report scenarioReport, service *fakeTool) {
	t.Helper()
	if len(report.ChildHeads) != 2 {
		t.Fatalf("independent child heads=%d", len(report.ChildHeads))
	}
	expected := make(map[string]receipt)
	parent := persistedReceipt(t, report.Head, parentID, "tool", 2)
	expected[parent.Reference] = parent
	if report.Final.State.Receipt != parent.Reference {
		t.Error("parent receipt does not address persisted external action")
	}
	for id, value := range map[string]int{"a": 4, "b": 6} {
		head, exists := report.ChildHeads[id]
		if !exists || head.ExecutionID == parentID || head.Terminal == nil ||
			head.Terminal.Status != flowy.RunStatusCompleted {
			t.Fatalf("child %s lacks independent terminal: %+v", id, head)
		}
		var state childInput
		if err := json.Unmarshal(head.Progress.StatePayload, &state); err != nil {
			t.Fatal(err)
		}
		if state.Value != value || !reflect.DeepEqual(head.RunMeta.BudgetCounts, map[string]int{computeCounter: 1}) {
			t.Fatalf("child %s terminal outcome/usage: %+v", id, head)
		}
		result := persistedReceipt(t, head, head.ExecutionID, "child", value)
		expected[result.Reference] = result
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(expected) != 3 || !reflect.DeepEqual(service.writes, expected) {
		t.Errorf("applied writes: actual=%v expected=%v", service.writes, expected)
	}
	counts := make(map[string]int)
	for id := range expected {
		counts[id] = 1
	}
	if !reflect.DeepEqual(service.dispatches, counts) {
		t.Errorf("dispatch identities/counts: actual=%v expected=%v", service.dispatches, counts)
	}
}

func persistedReceipt(t *testing.T, head flowy.ExecutionEnvelope, owner, node string, value int) receipt {
	t.Helper()
	var journal map[string]flowy.ActivityRecord
	if err := json.Unmarshal(head.JournalPayload, &journal); err != nil {
		t.Fatal(err)
	}
	if len(journal) != 1 {
		t.Fatalf("%s journal entries=%d", owner, len(journal))
	}
	for identity, record := range journal {
		if identity == "" || record.Identity != identity || record.ExecutionID != owner ||
			string(record.Node) != node ||
			record.Activation != 1 ||
			record.Key != "write" ||
			record.State != flowy.ActivityCompleted ||
			len(record.Attempts) != 1 {
			t.Fatalf("wrong external activity address: %+v", record)
		}
		var result receipt
		if err := json.Unmarshal(record.Outcome, &result); err != nil {
			t.Fatal(err)
		}
		if result.Reference != identity || result.Value != value || result.Units != 1 {
			t.Fatalf("wrong persisted external receipt: %+v", result)
		}
		return result
	}
	t.Fatal("persisted external receipt absent")
	return receipt{}
}

func assertFreshWorkers(t *testing.T, report scenarioReport) {
	t.Helper()
	if len(report.Workers) != 6 {
		t.Fatalf("workers=%d", len(report.Workers))
	}
	for i, w := range report.Workers {
		if w.pool.Stat().TotalConns() != 0 {
			t.Errorf("worker %d pool not closed", i)
		}
		for j := range i {
			if w.pool == report.Workers[j].pool || w.store == report.Workers[j].store ||
				w.store.ExecutionStore == report.Workers[j].store.ExecutionStore {
				t.Fatal("worker-side adapter/pool reused")
			}
		}
	}
}

//nolint:gocognit // One scenario verifies continuity, lost-ACK reporting and privacy on the same trace set.
func assertTraces(t *testing.T, spans []sdktrace.ReadOnlySpan, parent trace.SpanContext) {
	t.Helper()
	required := map[string]bool{
		"flowy.lifecycle.checkpoint.committed": false, "flowy.lifecycle.activity.failed": false,
		"flowy.lifecycle.activity.replayed": false, "flowy.lifecycle.wait_delivery.committed": false,
		"flowy.lifecycle.child_resolve.committed": false, "flowy.lifecycle.child_join.committed": false,
		"flowy.lifecycle.terminal.replayed": false,
	}
	if len(spans) > 250 {
		t.Fatalf("finite scenario generated %d spans", len(spans))
	}
	remoteToolNode := false
	for _, span := range spans {
		if span.Name() == "flowy.node.tool" && span.Parent().IsRemote() &&
			span.SpanContext().TraceID() == parent.TraceID() {
			remoteToolNode = true
		}
		if _, wanted := required[span.Name()]; wanted {
			required[span.Name()] = true
			if span.SpanContext().TraceID() != parent.TraceID() {
				t.Errorf("trace lost: %s", span.Name())
			}
			if span.Name() == "flowy.lifecycle.terminal.replayed" && !span.Parent().IsRemote() {
				t.Errorf("fresh replay lacked restored remote parent: %s", span.Name())
			}
		}
		for _, attr := range span.Attributes() {
			if strings.Contains(attr.Value.AsString(), "host found durable terminal") ||
				strings.Contains(attr.Value.AsString(), errPublication.Error()) {
				t.Errorf("raw evidence/error exported: %v", attr)
			}
		}
		// The initial parent activity result is stored but its lost response
		// cannot produce a committed-success observation for that activity.
		if span.Name() == "flowy.lifecycle.activity.committed" {
			for _, attr := range span.Attributes() {
				if string(attr.Key) == "thread_id" && attr.Value.AsString() == parentID {
					t.Error("lost acknowledgement reported as committed")
				}
			}
		}
	}
	for name, found := range required {
		if !found {
			t.Errorf("required observation missing: %s", name)
		}
	}
	if !remoteToolNode {
		t.Error("fresh worker node lacked restored remote trace parent")
	}
}
