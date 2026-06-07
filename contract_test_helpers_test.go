package flowy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Contract map (Task18 clear-break test taxonomy):
//
// Sync vs stream
//   - Start/Resume return RunResult synchronously; terminal error is the returned error.
//   - Stream/ResumeStream expose Events(); terminal outcome is authoritative from Wait().
//
// Terminal event delivery
//   - Strict: consumer drains Events(); terminal RunEvent is required (requireTerminalEventReason).
//   - Persist-vs-event (BD): checkpoint/snapshot is authoritative; terminal event may be dropped
//     (persist-vs-event tests assert Wait error / snapshot directly after ConsumeEventsAndWait or AwaitStreamCollect).
//
// Guards
//   - Session guard (in-process): ErrThreadAlreadyRunning on duplicate Start/Resume or second Stream Wait().
//   - Lease guard (distributed): ErrThreadLeaseBusy on lease acquire conflict; ErrLeaseLost on expiry/takeover.
//
// Handoff
//   - Graph Handoff directive: persisted snapshot + optional HandoffOutbox (HandoffStatus FSM).
//   - RequestLocalHandoff: in-process active-run API on this Runner instance only.
//
// Reason taxonomy
//   - User reason: Suspend/Handoff directive string.
//   - System constants: ReasonSuspendedCheckpointSkipped, ReasonHandoffSaveFailed, etc.
//   - Raw error text: infra failures, context cancel, lease lost.
//
// HTB + lease timing
//   - lease_lost_before_handoff subtests call RequestLocalHandoff before forceLeaseTakeover intentionally
//     (deterministic anti-flake ordering; handoff may win with nil, or lose with ErrLeaseLost).
//   - HTB stream tests: BeginStreamCollect before RequestLocalHandoff; handoff API waits session.done.
//   - stream Wait after parent cancel or RequestStop may return nil when cancel persisted (consumer stop).
//
// Timing helpers: waitForLeaseTTLExpiry / waitForHandoffCoordination cover TTL tests.
// waitForHeartbeatWindow is intentionally omitted (no heartbeat-window contract tests yet).
//
// Stream drain pattern: use BeginStreamCollect / AwaitStreamCollect (stream_helpers.go) or
// CollectEventsAndWait / ConsumeEventsAndWait. Never block on Events drain alone before Wait().
//
// Goleak: goleak_test.go uses TestMain; leak-sensitive tests must not use t.Parallel().

// awaitStreamCollect waits for BeginStreamCollect with a test timeout.
// On timeout it calls RequestStop before failing so the drain goroutine can exit.
func awaitStreamCollect[T, E any](
	t *testing.T,
	handle StreamHandle[T, E],
	out <-chan StreamCollectResult[T, E],
	timeout time.Duration,
) ([]RunEvent[T, E], error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	result, err := AwaitStreamCollect(ctx, handle, out)
	if errors.Is(err, context.DeadlineExceeded) {
		handle.RequestStop()
		t.Fatalf("stream drain+wait timed out after %s", timeout)
	}
	return result.Events, err
}

// requireStreamCancelReasonMatchesSync accepts persist-vs-event on RequestStop: terminal event may
// be dropped after consumer stop closes the sink, but snapshot segment reason must match sync.
func assertSkipOnSaveErrorSuspendStreamSyncReasonMatch[T, E any](
	t *testing.T,
	runner Runner[T, E],
	initial T,
	events []RunEvent[T, E],
) {
	t.Helper()
	syncRes, syncErr := runner.Start(context.Background(), "skip-on-save-sync-th", initial,
		WithCheckpointErrorPolicy[T, E](CheckpointPolicySkipOnSaveError),
	)
	if syncErr != nil {
		t.Fatalf("sync skip-on-save: %v", syncErr)
	}
	if syncRes.Reason != ReasonSuspendedCheckpointSkipped {
		t.Fatalf("expected sync reason %q, got %q", ReasonSuspendedCheckpointSkipped, syncRes.Reason)
	}
	assertTerminalEventReasonMatchesSync(t, events, EventSuspended, syncRes.Reason)
}

func requireSyncContextCanceledResult[T, E any](
	t *testing.T,
	g *Graph[T, E],
	cp Checkpointer[T, E],
	threadID string,
) *RunResult[T, E] {
	t.Helper()
	syncCtx, syncCancel := context.WithCancel(context.Background())
	syncCancel()
	syncRes, syncErr := g.NewRunner(cp).Start(syncCtx, threadID, *new(T))
	if syncErr == nil {
		t.Fatalf("expected sync cancel error, got %+v", syncRes)
	}
	if syncRes.Reason != "context_canceled" {
		t.Fatalf("expected sync context_canceled reason, got %q", syncRes.Reason)
	}
	return syncRes
}

func requireStreamCancelReasonMatchesSync[T, E any](
	t *testing.T,
	events []RunEvent[T, E],
	cp Checkpointer[T, E],
	threadID string,
	syncReason string,
) {
	t.Helper()
	snap := requireSnapshotPresent(t, cp, threadID)
	if snap.RunMeta.Segment.EndReason != SegmentEndContextCanceled {
		t.Fatalf("snapshot segment: got %q want %q", snap.RunMeta.Segment.EndReason, SegmentEndContextCanceled)
	}
	if term, ok := terminalEvent(events); ok {
		requireTerminalEventReason(t, events, EventContextCanceled, syncReason)
		if term.Reason != syncReason {
			t.Fatalf("terminal event reason %q != sync reason %q", term.Reason, syncReason)
		}
	}
}

func requireTerminalEventReason[T, E any](
	t *testing.T,
	events []RunEvent[T, E],
	wantType EventType,
	wantReason string,
) {
	t.Helper()
	term, ok := terminalEvent(events)
	if !ok {
		t.Fatalf("expected terminal event type %s with reason %q, events=%+v", wantType, wantReason, events)
	}
	if term.Type != wantType {
		t.Fatalf("terminal event type: got %s want %s", term.Type, wantType)
	}
	if term.Reason != wantReason {
		t.Fatalf("terminal event reason: got %q want %q", term.Reason, wantReason)
	}
}

func requireEventFailedReason[T, E any](t *testing.T, events []RunEvent[T, E], wantReason string) {
	t.Helper()
	requireTerminalEventReason(t, events, EventFailed, wantReason)
}

func terminalEventReason[T, E any](events []RunEvent[T, E], eventType EventType) string {
	for _, ev := range events {
		if ev.Type == eventType {
			return ev.Reason
		}
	}
	return ""
}

func assertTerminalEventReasonMatchesSync[T, E any](
	t *testing.T,
	events []RunEvent[T, E],
	eventType EventType,
	syncReason string,
) {
	t.Helper()
	terminalReason := terminalEventReason(events, eventType)
	if terminalReason != syncReason {
		t.Fatalf("event reason %q != sync result reason %q events=%+v", terminalReason, syncReason, events)
	}
}

func assertTerminalErrorStreamReasonMatchesSync[tState any, tEffect any](
	t *testing.T,
	g *Graph[tState, tEffect],
	cp Checkpointer[tState, tEffect],
	syncThreadID, streamThreadID string,
	opts []RunOption[tState, tEffect],
	wantErr error,
	wantReason string,
) {
	t.Helper()
	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), syncThreadID, *new(tState), opts...)
	if !errors.Is(syncErr, wantErr) {
		t.Fatalf("sync error: want %v, got res=%+v err=%v", wantErr, syncRes, syncErr)
	}
	if syncRes == nil || syncRes.Reason != wantReason {
		t.Fatalf("sync reason: want %q, got res=%+v", wantReason, syncRes)
	}

	handle, err := g.NewRunner(cp).Stream(context.Background(), streamThreadID, *new(tState), opts...)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if !errors.Is(waitErr, wantErr) {
		t.Fatalf("stream Wait: want %v, got %v", wantErr, waitErr)
	}
	requireEventFailedReason(t, events, wantReason)
}

func requireSnapshotPresent[T, E any](t *testing.T, cp Checkpointer[T, E], threadID string) Snapshot[T, E] {
	t.Helper()
	snap, _, err := cp.Load(context.Background(), threadID)
	if err != nil {
		t.Fatalf("expected snapshot for %q: %v", threadID, err)
	}
	return snap
}

func requireSnapshotMissing[T, E any](t *testing.T, cp Checkpointer[T, E], threadID string) {
	t.Helper()
	_, _, err := cp.Load(context.Background(), threadID)
	if err == nil {
		t.Fatalf("expected no snapshot for %q", threadID)
	}
	if !errors.Is(err, ErrThreadNotFound) {
		t.Fatalf("Load(%q): got %v want ErrThreadNotFound", threadID, err)
	}
}

func requireResumeTokenMatchesSnapshot[T, E any](
	t *testing.T,
	token ResumeToken,
	snap Snapshot[T, E],
) {
	t.Helper()
	if token.ThreadID != snap.ThreadID {
		t.Fatalf("token thread %q != snapshot thread %q", token.ThreadID, snap.ThreadID)
	}
	if token.SnapshotRevision != snap.Revision {
		t.Fatalf("token revision %d != snapshot revision %d", token.SnapshotRevision, snap.Revision)
	}
}

func terminalEvent[T, E any](events []RunEvent[T, E]) (RunEvent[T, E], bool) {
	return terminalEventFromEvents(events)
}

type blockingSaveCP[T, E any] struct {
	inner       *memoryCP[T, E]
	saveEntered chan struct{}
	releaseSave chan struct{}
	enteredOnce sync.Once
}

func newBlockingSaveCP[T, E any]() *blockingSaveCP[T, E] {
	return &blockingSaveCP[T, E]{
		inner:       newMemoryCP[T, E](),
		saveEntered: make(chan struct{}),
		releaseSave: make(chan struct{}),
	}
}

func (b *blockingSaveCP[T, E]) Save(
	_ context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
) (uint64, error) {
	b.enteredOnce.Do(func() { close(b.saveEntered) })
	<-b.releaseSave
	return b.inner.Save(context.Background(), expectedRevision, snapshot)
}

func (b *blockingSaveCP[T, E]) Load(ctx context.Context, threadID string) (Snapshot[T, E], uint64, error) {
	return b.inner.Load(ctx, threadID)
}

func (b *blockingSaveCP[T, E]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]Snapshot[T, E], error) {
	return b.inner.GetHistory(ctx, threadID, limit)
}

func (b *blockingSaveCP[T, E]) Prune(ctx context.Context, threadID string, retainCount int) error {
	return b.inner.Prune(ctx, threadID, retainCount)
}

func (b *blockingSaveCP[T, E]) Delete(ctx context.Context, threadID string) error {
	return b.inner.Delete(ctx, threadID)
}

func (b *blockingSaveCP[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	return b.inner.DeleteIfIdle(ctx, threadID)
}

func (b *blockingSaveCP[T, E]) unblockSave() {
	close(b.releaseSave)
}

func waitForLeaseTTLExpiry() {
	time.Sleep(150 * time.Millisecond)
}

// waitForHandoffCoordination yields briefly so an active Start/Stream can reach a handoff-ready state.
func waitForHandoffCoordination() {
	time.Sleep(10 * time.Millisecond)
}

// waitForStreamBufferBackpressure yields until a slow stream consumer creates backpressure.
func waitForStreamBufferBackpressure() {
	time.Sleep(25 * time.Millisecond)
}

func collectEventsFromChannel[T, E any](
	ch <-chan RunEvent[T, E],
	timeout time.Duration,
) ([]RunEvent[T, E], bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	out := make([]RunEvent[T, E], 0)
	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return out, false
			}
			out = append(out, event)
		case <-timer.C:
			return out, true
		}
	}
}

func collectEvents[T, E any](t *testing.T, stream <-chan RunEvent[T, E], timeout time.Duration) []RunEvent[T, E] {
	t.Helper()
	events, timedOut := collectEventsFromChannel(stream, timeout)
	if timedOut {
		t.Fatalf("stream timeout after %s", timeout)
	}
	return events
}

func assertHandoffRunMetaNoneOnSkip[T, E any](t *testing.T, res *RunResult[T, E]) {
	t.Helper()
	if res == nil {
		t.Fatal("expected non-nil RunResult")
	}
	if res.RunMeta.HandoffStatus != HandoffStatusNone {
		t.Fatalf("expected none handoff status on skip-on-save, got %q", res.RunMeta.HandoffStatus)
	}
	if !res.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatalf("expected zero HandoffPendingAt on skip-on-save, got %v", res.RunMeta.HandoffPendingAt)
	}
}

func seedRecoverSnapshot[T, E any](
	t *testing.T,
	cp Checkpointer[T, E],
	threadID string,
	state T,
	meta RunMetadata,
) {
	t.Helper()
	if _, err := cp.Save(context.Background(), 0, Snapshot[T, E]{
		ThreadID:         threadID,
		ExecutionPointer: "work",
		State:            state,
		RunMeta:          meta,
	}); err != nil {
		t.Fatalf("seed recover snapshot: %v", err)
	}
}

func newRecoverWorkRunner[T, E any](
	t *testing.T,
	cp Checkpointer[T, E],
	outbox HandoffOutbox,
	opts ...RunnerOption[T, E],
) Runner[T, E] {
	t.Helper()
	b := NewGraph[T, E](func(_ T, u T) T { return u })
	b.AddNode("work", func(_ context.Context, s T) (T, Directive, error) {
		return s, End(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile recover work graph: %v", err)
	}
	runOpts := make([]RunnerOption[T, E], 0, len(opts)+1)
	if outbox != nil {
		runOpts = append(runOpts, WithRunnerHandoffOutbox[T, E](outbox))
	}
	runOpts = append(runOpts, opts...)
	return g.NewRunnerWithOptions(cp, runOpts)
}

func assertEnqueuedHandoffSnapshot[T, E any](
	t *testing.T,
	cp Checkpointer[T, E],
	threadID string,
) {
	t.Helper()
	snap, _, loadErr := cp.Load(context.Background(), threadID)
	if loadErr != nil {
		t.Fatalf("load snapshot after recovery: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusEnqueued {
		t.Fatalf("expected enqueued handoff status, got %q", snap.RunMeta.HandoffStatus)
	}
	if !snap.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatalf("expected HandoffPendingAt cleared on enqueued snapshot, got %v", snap.RunMeta.HandoffPendingAt)
	}
}

func assertOrphanedHandoffSnapshot[T, E any](
	t *testing.T,
	cp Checkpointer[T, E],
	threadID string,
	res *RunResult[T, E],
	directiveReason string,
) {
	t.Helper()
	snap, _, loadErr := cp.Load(context.Background(), threadID)
	if loadErr != nil {
		t.Fatalf("load snapshot after enqueue fail: %v", loadErr)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusOrphaned {
		t.Fatalf("expected orphaned handoff status, got %q", snap.RunMeta.HandoffStatus)
	}
	if !snap.RunMeta.HandoffPendingAt.IsZero() {
		t.Fatalf("expected HandoffPendingAt cleared on orphaned snapshot, got %v", snap.RunMeta.HandoffPendingAt)
	}
	if res != nil && directiveReason != "" {
		wantReason := deriveHandoffTerminalReason(directiveReason, HandoffStatusOrphaned)
		if res.Reason != wantReason {
			t.Fatalf("reason %q != expected %q for orphaned handoff", res.Reason, wantReason)
		}
	}
}

// handoffPatchFailCP fails Save when snapshot carries the configured HandoffStatus.
type handoffPatchFailCP[T, E any] struct {
	*memoryCP[T, E]

	failOnStatus   HandoffStatus
	failOnStatuses map[HandoffStatus]struct{}
}

func (c *handoffPatchFailCP[T, E]) ensureMemoryCP() {
	if c.memoryCP == nil {
		c.memoryCP = newMemoryCP[T, E]()
	}
}

func (c *handoffPatchFailCP[T, E]) Save(
	_ context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
) (uint64, error) {
	if c.failOnStatus != "" && snapshot.RunMeta.HandoffStatus == c.failOnStatus {
		return 0, errors.New("handoff patch save failed")
	}
	if len(c.failOnStatuses) > 0 {
		if _, ok := c.failOnStatuses[snapshot.RunMeta.HandoffStatus]; ok {
			return 0, errors.New("handoff patch save failed")
		}
	}
	c.ensureMemoryCP()
	return c.memoryCP.Save(context.Background(), expectedRevision, snapshot)
}

func (c *handoffPatchFailCP[T, E]) Load(ctx context.Context, threadID string) (Snapshot[T, E], uint64, error) {
	c.ensureMemoryCP()
	return c.memoryCP.Load(ctx, threadID)
}

func (c *handoffPatchFailCP[T, E]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]Snapshot[T, E], error) {
	c.ensureMemoryCP()
	return c.memoryCP.GetHistory(ctx, threadID, limit)
}

func (c *handoffPatchFailCP[T, E]) Prune(ctx context.Context, threadID string, retainCount int) error {
	c.ensureMemoryCP()
	return c.memoryCP.Prune(ctx, threadID, retainCount)
}

func (c *handoffPatchFailCP[T, E]) Delete(ctx context.Context, threadID string) error {
	c.ensureMemoryCP()
	return c.memoryCP.Delete(ctx, threadID)
}

func (c *handoffPatchFailCP[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	c.ensureMemoryCP()
	return c.memoryCP.DeleteIfIdle(ctx, threadID)
}

func assertHandoffTokenRevisionContract[T, E any](
	t *testing.T,
	outbox *stubHandoffOutbox,
	res *RunResult[T, E],
	cp Checkpointer[T, E],
	threadID string,
) {
	t.Helper()
	snap, rev, err := cp.Load(context.Background(), threadID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil RunResult")
	}
	if res.ResumeToken.SnapshotRevision != rev {
		t.Fatalf("result token revision %d != snapshot revision %d", res.ResumeToken.SnapshotRevision, rev)
	}
	if len(outbox.calls) == 0 {
		t.Fatal("expected outbox enqueue call")
	}
	token := outbox.calls[len(outbox.calls)-1]
	if token.SnapshotRevision != rev-1 {
		t.Fatalf("outbox token revision %d != pending revision %d", token.SnapshotRevision, rev-1)
	}
	if token.SnapshotRevision == res.ResumeToken.SnapshotRevision {
		t.Fatalf("outbox token must differ from result token when enqueued: %+v", token)
	}
	_ = snap
}

func assertHandoffFailureTokenMatchesSnapshot[T, E any](
	t *testing.T,
	res *RunResult[T, E],
	cp Checkpointer[T, E],
	threadID string,
) {
	t.Helper()
	snap, rev, err := cp.Load(context.Background(), threadID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if res == nil || res.ResumeToken.ThreadID == "" {
		t.Fatalf("expected populated ResumeToken, got %+v", res)
	}
	if res.ResumeToken.SnapshotRevision != rev {
		t.Fatalf("result token revision %d != snapshot revision %d", res.ResumeToken.SnapshotRevision, rev)
	}
	if snap.RunMeta.HandoffStatus != HandoffStatusOrphaned {
		t.Fatalf("expected orphaned handoff status, got %q", snap.RunMeta.HandoffStatus)
	}
}

// transactionalMemoryCP implements TransactionalCheckpointer for unit tests (save → enqueue, like postgres).
type transactionalMemoryCP[T, E any] struct {
	*memoryCP[T, E]
}

func (t *transactionalMemoryCP[T, E]) ensureMemoryCP() {
	if t.memoryCP == nil {
		t.memoryCP = newMemoryCP[T, E]()
	}
}

func (t *transactionalMemoryCP[T, E]) SaveWithOutbox(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
	enqueueFn func(context.Context) error,
) (uint64, error) {
	t.ensureMemoryCP()
	pending := snapshot
	pending.Revision = expectedRevision + 1
	txToken := struct{}{}
	txCtx := ContextWithOutboxTx(ctx, txToken)
	if _, err := t.memoryCP.Save(txCtx, expectedRevision, pending); err != nil {
		return 0, err
	}
	if enqueueFn != nil {
		if err := enqueueFn(txCtx); err != nil {
			_ = t.memoryCP.Delete(ctx, snapshot.ThreadID)
			return 0, err
		}
	}
	return pending.Revision, nil
}

func (t *transactionalMemoryCP[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	snapshot Snapshot[T, E],
) (uint64, error) {
	t.ensureMemoryCP()
	return t.memoryCP.Save(ctx, expectedRevision, snapshot)
}

func (t *transactionalMemoryCP[T, E]) Load(ctx context.Context, threadID string) (Snapshot[T, E], uint64, error) {
	t.ensureMemoryCP()
	return t.memoryCP.Load(ctx, threadID)
}

func (t *transactionalMemoryCP[T, E]) GetHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]Snapshot[T, E], error) {
	t.ensureMemoryCP()
	return t.memoryCP.GetHistory(ctx, threadID, limit)
}

func (t *transactionalMemoryCP[T, E]) Prune(ctx context.Context, threadID string, retainCount int) error {
	t.ensureMemoryCP()
	return t.memoryCP.Prune(ctx, threadID, retainCount)
}

func (t *transactionalMemoryCP[T, E]) Delete(ctx context.Context, threadID string) error {
	t.ensureMemoryCP()
	return t.memoryCP.Delete(ctx, threadID)
}

func (t *transactionalMemoryCP[T, E]) DeleteIfIdle(ctx context.Context, threadID string) error {
	t.ensureMemoryCP()
	return t.memoryCP.DeleteIfIdle(ctx, threadID)
}

func assertSegmentEndReason[T, E any](
	t *testing.T,
	snap Snapshot[T, E],
	label string,
	want SegmentEndReason,
) {
	t.Helper()
	if snap.RunMeta.Segment.EndReason != want {
		t.Fatalf("%s: segment end=%q want %q", label, snap.RunMeta.Segment.EndReason, want)
	}
}

func assertHandoffReasonMatchesStatus[T, E any](
	t *testing.T,
	res *RunResult[T, E],
	cp Checkpointer[T, E],
	threadID string,
	directiveReason string,
) {
	t.Helper()
	snap, _, err := cp.Load(context.Background(), threadID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil RunResult")
	}
	wantReason := deriveHandoffTerminalReason(directiveReason, snap.RunMeta.HandoffStatus)
	if res.Reason != wantReason {
		t.Fatalf("reason %q != expected %q for handoff_status %q", res.Reason, wantReason, snap.RunMeta.HandoffStatus)
	}
}

func assertRunMetaHandoffStatusMatchesSnapshot[T, E any](
	t *testing.T,
	res *RunResult[T, E],
	cp Checkpointer[T, E],
	threadID string,
) {
	t.Helper()
	snap, _, err := cp.Load(context.Background(), threadID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil RunResult")
	}
	if res.RunMeta.HandoffStatus != snap.RunMeta.HandoffStatus {
		t.Fatalf("result handoff status %q != snapshot %q", res.RunMeta.HandoffStatus, snap.RunMeta.HandoffStatus)
	}
	if !res.RunMeta.HandoffPendingAt.Equal(snap.RunMeta.HandoffPendingAt) {
		t.Fatalf(
			"result HandoffPendingAt %v != snapshot %v",
			res.RunMeta.HandoffPendingAt,
			snap.RunMeta.HandoffPendingAt,
		)
	}
}

func resumeAfterOCCConflict[T, E any](
	t *testing.T,
	runner Runner[T, E],
	cp Checkpointer[T, E],
	staleToken ResumeToken,
) (*RunResult[T, E], error) {
	t.Helper()
	_, err := runner.Resume(context.Background(), staleToken)
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict on stale token, got %v", err)
	}
	snap, rev, loadErr := cp.Load(context.Background(), staleToken.ThreadID)
	if loadErr != nil {
		t.Fatalf("reload: %v", loadErr)
	}
	freshToken := ResumeToken{ThreadID: snap.ThreadID, SnapshotRevision: rev}
	return runner.Resume(context.Background(), freshToken)
}
