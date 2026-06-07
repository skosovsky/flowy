package flowy

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type stubHandoffScheduler struct {
	mu    sync.Mutex
	calls []ResumeToken
	err   error
}

func (s *stubHandoffScheduler) ScheduleContinuation(_ context.Context, token ResumeToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, token)
	return s.err
}

func (s *stubHandoffScheduler) lastToken() ResumeToken {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		return ResumeToken{}
	}
	return s.calls[len(s.calls)-1]
}

type deleteSpyCP[T, E any] struct {
	memoryCP[T, E]

	deleteCalls int
}

func retentionFailureGraph[T any, E any](t *testing.T, directive Directive) (*Graph[T, E], *failingMemoryCP[T, E]) {
	t.Helper()
	cp := &failingMemoryCP[T, E]{
		memoryCP:  *newMemoryCP[T, E](),
		failPrune: true,
	}
	b := NewGraph[T, E](func(_ T, u T) T { return u })
	b.AddNode("work", func(_ context.Context, s T) (T, Directive, error) {
		return s, directive, nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile(WithRetentionLimit(2))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, cp
}

type infraTestState struct{}

func infraFailureHandoffResolveGraph(
	t *testing.T,
) (*Graph[infraTestState, NoEffect], Checkpointer[infraTestState, NoEffect], []RunOption[infraTestState, NoEffect]) {
	t.Helper()
	cp := newMemoryCP[infraTestState, NoEffect]()
	b := NewGraph[infraTestState, NoEffect](func(_ infraTestState, u infraTestState) infraTestState { return u })
	b.AddNode("work", func(_ context.Context, s infraTestState) (infraTestState, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	opts := []RunOption[infraTestState, NoEffect]{
		WithSuspendPointerResolver[infraTestState, NoEffect](
			func(_ infraTestState, _ ExecutionPointer) (ExecutionPointer, error) {
				return "", errors.New("invalid pointer")
			},
		),
	}
	return g, cp, opts
}

func infraFailureHandoffSaveGraph(
	t *testing.T,
) (*Graph[infraTestState, NoEffect], Checkpointer[infraTestState, NoEffect], []RunOption[infraTestState, NoEffect]) {
	t.Helper()
	cp := &failingMemoryCP[infraTestState, NoEffect]{failSave: true}
	b := NewGraph[infraTestState, NoEffect](func(_ infraTestState, u infraTestState) infraTestState { return u })
	b.AddNode("router", func(_ context.Context, s infraTestState) (infraTestState, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AddNode("work", func(_ context.Context, s infraTestState) (infraTestState, Directive, error) {
		return s, Handoff("bg"), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.AllowNoOutgoingRoute("router")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	opts := []RunOption[infraTestState, NoEffect]{
		WithSuspendPointerResolver[infraTestState, NoEffect](
			func(_ infraTestState, _ ExecutionPointer) (ExecutionPointer, error) {
				return "router", nil
			},
		),
	}
	return g, cp, opts
}

func assertInfraFailureStreamSync[T, E any](
	t *testing.T,
	g *Graph[T, E],
	cp Checkpointer[T, E],
	opts []RunOption[T, E],
	wantStatus RunStatus,
	wantEvent EventType,
	wantPointer string,
	wantReason string,
) {
	t.Helper()
	syncRes, syncErr := g.NewRunner(cp).Start(context.Background(), "infra-sync-th", *new(T), opts...)
	if syncErr == nil {
		t.Fatalf("expected sync infra failure, got res=%+v", syncRes)
	}
	if syncRes == nil || syncRes.Status != wantStatus {
		t.Fatalf("sync status: want %s, got res=%+v", wantStatus, syncRes)
	}
	if syncRes.Reason != wantReason {
		t.Fatalf("sync reason: want %q, got %q", wantReason, syncRes.Reason)
	}
	if wantPointer != "" && string(syncRes.ExecutionPointer) != wantPointer {
		t.Fatalf("sync pointer: want %q, got %q", wantPointer, syncRes.ExecutionPointer)
	}

	handle, err := g.NewRunner(cp).Stream(context.Background(), "infra-stream-th", *new(T), opts...)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	events, waitErr := CollectEventsAndWait(context.Background(), handle)
	if waitErr == nil {
		t.Fatal("expected stream infra failure")
	}
	switch wantEvent {
	case EventFailed:
		assertEventFailedReasonMatchesSync(t, events, wantReason)
	case EventContextCanceled:
		assertTerminalEventReasonMatchesSync(t, events, wantEvent, wantReason)
	default:
		reason := terminalEventReason(events, wantEvent)
		if reason != wantReason {
			t.Fatalf("stream event reason: want %q, got %q events=%+v", wantReason, reason, events)
		}
	}
}

func assertEventFailedReasonMatchesSync[T, E any](t *testing.T, events []RunEvent[T, E], wantReason string) {
	t.Helper()
	requireEventFailedReason(t, events, wantReason)
}

func leaseLostBlockingGraph(t *testing.T, ready chan struct{}) *Graph[struct{}, NoEffect] {
	t.Helper()
	b := NewGraph[struct{}, NoEffect](func(_ struct{}, u struct{}) struct{} { return u })
	b.AddNode("work", func(ctx context.Context, s struct{}) (struct{}, Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func forceLeaseTakeover(t *testing.T, lease *MemoryLeaseManager, threadID string) {
	t.Helper()
	if relErr := lease.Release(context.Background(), threadID, "worker-a"); relErr != nil {
		t.Fatalf("release %q: %v", threadID, relErr)
	}
	if acqErr := lease.Acquire(context.Background(), threadID, "worker-b", time.Minute); acqErr != nil {
		t.Fatalf("acquire b %q: %v", threadID, acqErr)
	}
}

func stealLeaseAndWait(t *testing.T, lease *MemoryLeaseManager, threadID string) {
	t.Helper()
	forceLeaseTakeover(t, lease, threadID)
	waitForLeaseTTLExpiry()
}
func blockingHandoffWorkGraph[T any, E any](t *testing.T) (*Graph[T, E], chan struct{}) {
	t.Helper()
	ready := make(chan struct{})
	b := NewGraph[T, E](func(_ T, u T) T { return u })
	b.AddNode("work", func(ctx context.Context, s T) (T, Directive, error) {
		close(ready)
		<-ctx.Done()
		return s, Completed(), nil
	})
	b.AllowNoOutgoingRoute("work")
	b.SetEntryPoint("work")
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, ready
}

func prodGoFilesForDoDScan(t *testing.T) []string {
	t.Helper()
	return collectGoFilesForDoDScan(t, false)
}

func testGoFilesForDoDScan(t *testing.T) []string {
	t.Helper()
	return collectGoFilesForDoDScan(t, true)
}

func collectGoFilesForDoDScan(t *testing.T, testsOnly bool) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		isTest := strings.HasSuffix(path, "_test.go")
		if testsOnly != isTest {
			return nil
		}
		if strings.HasPrefix(path, "examples/") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return files
}

func assertNoCheckpointCollectorNeedles(t *testing.T, files, forbidden []string) {
	t.Helper()
	for _, file := range files {
		if strings.HasSuffix(file, "runner_dod_contracts_test.go") {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		content := string(data)
		for _, needle := range forbidden {
			if strings.Contains(content, needle) {
				t.Fatalf("%s contains forbidden %q", file, needle)
			}
		}
	}
}

type pointerSpyCP[T, E any] struct {
	memoryCP[T, E]

	savedPointers []ExecutionPointer
}

func (p *pointerSpyCP[T, E]) Save(_ context.Context, snapshot Snapshot[T, E]) error {
	p.savedPointers = append(p.savedPointers, snapshot.ExecutionPointer)
	return p.memoryCP.Save(context.Background(), snapshot)
}

type countingFailingMemoryCP[T, E any] struct {
	memoryCP[T, E]

	saveCount int
}

func (c *countingFailingMemoryCP[T, E]) Save(_ context.Context, snapshot Snapshot[T, E]) error {
	c.saveCount++
	if c.saveCount > 1 {
		return errors.New("save failed")
	}
	return c.memoryCP.Save(context.Background(), snapshot)
}
