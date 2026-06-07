package flowy

import (
	"context"
	"errors"
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
//   - Graph Handoff directive: persisted snapshot + optional HandoffScheduler.
//   - RequestLocalHandoff: in-process active-run API on this Runner instance only.
//
// Reason taxonomy
//   - User reason: Suspend/Handoff directive string.
//   - System constants: ReasonSuspendedCheckpointSkipped, ReasonHandoffScheduleFailed, etc.
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
	snap, err := cp.Load(context.Background(), threadID)
	if err != nil {
		t.Fatalf("expected snapshot for %q: %v", threadID, err)
	}
	return snap
}

func requireSnapshotMissing[T, E any](t *testing.T, cp Checkpointer[T, E], threadID string) {
	t.Helper()
	_, err := cp.Load(context.Background(), threadID)
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
}

func newBlockingSaveCP[T, E any]() *blockingSaveCP[T, E] {
	return &blockingSaveCP[T, E]{
		inner:       newMemoryCP[T, E](),
		saveEntered: make(chan struct{}),
		releaseSave: make(chan struct{}),
	}
}

func (b *blockingSaveCP[T, E]) Save(_ context.Context, snapshot Snapshot[T, E]) error {
	select {
	case <-b.saveEntered:
	default:
		close(b.saveEntered)
	}
	<-b.releaseSave
	return b.inner.Save(context.Background(), snapshot)
}

func (b *blockingSaveCP[T, E]) Load(ctx context.Context, threadID string) (Snapshot[T, E], error) {
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
