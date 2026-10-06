package flowy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func ownershipGraph(t *testing.T, node Node[int, NoEffect]) *Graph[int, NoEffect] {
	t.Helper()
	builder := NewGraph[int, NoEffect](func(_, update int) int { return update })
	builder.AddNode("work", node).SetEntryPoint("work").AllowNoOutgoingRoute("work")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return graph
}

func TestStaleAndRejectedStreamStopOwnsOnlyItsRun(t *testing.T) {
	t.Parallel()
	// Arrange: complete A, then block B at the same thread identity.
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	graph := ownershipGraph(t, func(ctx context.Context, state int) (int, Directive, error) {
		if state == 0 {
			return state, End(), nil
		}
		entered <- ctx
		select {
		case <-release:
			return state, End(), nil
		case <-ctx.Done():
			return state, End(), ctx.Err()
		}
	})
	runner := graph.NewRunner(newMemoryCP[int, NoEffect]())
	old, err := runner.Stream(context.Background(), "same", 0)
	if err != nil {
		t.Fatal(err)
	}
	if waitErr := old.Wait(); waitErr != nil {
		t.Fatal(waitErr)
	}
	current, err := runner.Stream(context.Background(), "same", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(current.RequestStop)
	currentContext := <-entered
	duplicate, err := runner.Stream(context.Background(), "same", 2)
	if err != nil {
		t.Fatal(err)
	}
	if waitErr := duplicate.Wait(); !errors.Is(waitErr, ErrThreadAlreadyRunning) {
		t.Fatalf("duplicate error=%v", waitErr)
	}
	// Act: neither completed nor rejected handle owns B's cancellation.
	old.RequestStop()
	duplicate.RequestStop()
	// Assert: synchronous stop cannot alter B's live context.
	if contextErr := currentContext.Err(); contextErr != nil {
		t.Fatalf("stale stop canceled B: %v", contextErr)
	}
	once.Do(func() { close(release) })
	result, err := current.WaitResult()
	if err != nil || result.Status != RunStatusCompleted {
		t.Fatalf("B result=%+v err=%v", result, err)
	}
}

//nolint:gocognit // Independent entry-point and error-shape contract matrix.
func TestExecuteCancelsOwnedNodeContextOnEveryReturn(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"end", "error", "suspend-resume"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			// Arrange: a live cancellable parent and captured node contexts.
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			contexts := make(chan context.Context, 2)
			graph := ownershipGraph(t, func(ctx context.Context, state int) (int, Directive, error) {
				contexts <- ctx
				if mode == "error" {
					return state, End(), errors.New("node error")
				}
				if mode == "suspend-resume" && state == 0 {
					return 1, Suspend("pause"), nil
				}
				return state, End(), nil
			})
			runner := graph.NewRunner(newMemoryCP[int, NoEffect]())
			// Act.
			result, err := runner.Start(parent, "owned", 0)
			if mode != "error" && err != nil {
				t.Fatal(err)
			}
			// Assert.
			if ctx := <-contexts; ctx.Err() == nil {
				t.Fatal("node context remains attached after return")
			}
			if mode == "suspend-resume" {
				_, err = runner.Resume(parent, result.ResumeToken)
				if err != nil {
					t.Fatal(err)
				}
				if ctx := <-contexts; ctx.Err() == nil {
					t.Fatal("resume node context remains active")
				}
			}
			if parent.Err() != nil {
				t.Fatal("owned cleanup canceled parent")
			}
		})
	}
}

// triggeredDeadlineContext supplies a deterministic deadline signal without a TTL race.
// Its clock is controlled by the test; Go's derived cancel context observes Err on Done.
type triggeredDeadlineContext struct{ done chan struct{} }

func (*triggeredDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *triggeredDeadlineContext) Done() <-chan struct{}     { return c.done }
func (c *triggeredDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}
func (*triggeredDeadlineContext) Value(any) any { return nil }

//nolint:gocognit // Independent entry-point and error-shape contract matrix.
func TestActiveDeadlineNodeErrorPersistsContinuation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"start", "stream", "resume", "resume-stream"} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", mode, wrapped), func(t *testing.T) {
				t.Parallel()
				// Arrange: prepare a resumable boundary before triggering the deadline.
				parent := &triggeredDeadlineContext{done: make(chan struct{})}
				graph := ownershipGraph(t, func(ctx context.Context, state int) (int, Directive, error) {
					if state == 0 {
						return 42, Suspend("pause"), nil
					}
					close(parent.done)
					<-ctx.Done()
					err := ctx.Err()
					if wrapped {
						err = fmt.Errorf("node deadline: %w", err)
					}
					return state, End(), err
				})
				cp := newMemoryCP[int, NoEffect]()
				runner := graph.NewRunner(cp)
				var token ResumeToken
				revisions := 1
				if mode == "resume" || mode == "resume-stream" {
					prepared, err := runner.Start(context.Background(), "deadline", 0)
					if err != nil {
						t.Fatal(err)
					}
					token = prepared.ResumeToken
					revisions++
				}
				// Act.
				var result *RunResult[int, NoEffect]
				var err error
				switch mode {
				case "start":
					result, err = runner.Start(parent, "deadline", 42)
				case "resume":
					result, err = runner.Resume(parent, token)
				case "stream", "resume-stream":
					var handle StreamHandle[int, NoEffect]
					if mode == "stream" {
						handle, err = runner.Stream(parent, "deadline", 42)
					} else {
						handle, err = runner.ResumeStream(parent, token)
					}
					if err != nil {
						t.Fatal(err)
					}
					result, err = handle.WaitResult()
				}
				// Assert.
				if !errors.Is(err, context.DeadlineExceeded) || result == nil ||
					result.Status != RunStatusContextCanceled ||
					result.ResumeToken.SnapshotRevision == 0 {
					t.Fatalf("result=%+v err=%v", result, err)
				}
				if len(cp.hist["deadline"]) != revisions || cp.last.State != 42 {
					t.Fatal("deadline lost continuation")
				}
			})
		}
	}
}

func TestLocalNodeDeadlineWithLiveRunRemainsFailure(t *testing.T) {
	t.Parallel()
	// Arrange: a node-local timeout has not canceled the active run.
	graph := ownershipGraph(t, func(_ context.Context, state int) (int, Directive, error) {
		return state, End(), fmt.Errorf("local timeout: %w", context.DeadlineExceeded)
	})
	cp := newMemoryCP[int, NoEffect]()
	// Act.
	result, err := graph.NewRunner(cp).Start(context.Background(), "local", 42)
	// Assert.
	if !errors.Is(err, context.DeadlineExceeded) || result.Status != RunStatusFailed || len(cp.hist) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if result.RunMeta.Segment.EndTime.IsZero() || result.RunMeta.Segment.EndTime.Location() != time.UTC ||
		result.RunMeta.Segment.EndReason != "fail" {
		t.Fatalf("metadata=%+v", result.RunMeta)
	}
}

func TestStreamStopBeforeSessionRegistration(t *testing.T) {
	t.Parallel()
	// Arrange: hold the stream callback before execute can register a session.
	entered, release := make(chan struct{}), make(chan struct{})
	graph := ownershipGraph(t, func(_ context.Context, state int) (int, Directive, error) {
		t.Error("node dispatched after early stop")
		return state, End(), nil
	})
	runner := graph.NewRunner(newMemoryCP[int, NoEffect]()).(*graphRunner[int, NoEffect])
	inv, err := applyRunOptions[int, NoEffect]()
	if err != nil {
		t.Fatal(err)
	}
	handle := runner.startStream(
		context.Background(),
		inv,
		func(ctx context.Context, sink eventSink[int, NoEffect]) (*RunResult[int, NoEffect], error) {
			close(entered)
			<-release
			return runner.execute(ctx, "early", graph.entryPoint, 42, newRunMetadata(), nil, 0, sink, inv)
		},
	)
	<-entered
	// Act.
	handle.RequestStop()
	close(release)
	// Assert.
	result, waitErr := handle.WaitResult()
	if waitErr != nil || result == nil || result.Status != RunStatusContextCanceled ||
		result.ResumeToken.SnapshotRevision == 0 {
		t.Fatalf("early stop result=%+v err=%v", result, waitErr)
	}
}
