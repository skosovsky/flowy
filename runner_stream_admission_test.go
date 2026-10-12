package flowy

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// Regression fixture retains the original audit invocation and allocation costs.
func auditStream(tb testing.TB, attempts int, withClone bool) (int, int) {
	tb.Helper()
	// Arrange: no consumer until completion; the bounded channel can retain 32 events.
	runner := &graphRunner[[]byte, NoEffect]{}
	payload := make([]byte, 64<<10)
	payload[0] = 42
	clones := 0
	inv := runInvocationOptions[[]byte, NoEffect]{}
	if withClone {
		inv.cloneState = func(v []byte) []byte { clones++; return append([]byte(nil), v...) }
	}
	// Act: direct sink boundary isolates streaming from graph/business/storage work.
	h := runner.startStream(
		context.Background(),
		inv,
		func(ctx context.Context, sink eventSink[[]byte, NoEffect]) (*RunResult[[]byte, NoEffect], error) {
			for range attempts {
				if !sink(ctx, RunEvent[[]byte, NoEffect]{State: payload}) {
					return nil, errors.New("unexpected stopped stream")
				}
			}
			return &RunResult[[]byte, NoEffect]{State: payload}, nil
		},
	)
	if err := h.Wait(); err != nil {
		tb.Fatal(err)
	}
	payload[0] = 99
	delivered := 0
	for event := range h.Events() {
		delivered++
		if withClone && event.State[0] != 42 {
			tb.Fatal("event alias")
		}
	}
	// Assert: completed stream, bounded delivery, producer detachment maintained.
	if delivered != min(attempts, streamEventBufferSize) {
		tb.Fatalf("delivered %d", delivered)
	}
	return clones, delivered
}
func TestAuditDroppedEventClones(t *testing.T) {
	clones, delivered := auditStream(t, 1024, true)
	if clones != 32 || delivered != 32 {
		t.Fatalf("clones=%d delivered=%d", clones, delivered)
	}
	t.Logf(
		"attempts=1024 clones=%d delivered=%d dropped=%d copied_bytes=%d",
		clones,
		delivered,
		1024-delivered,
		clones*(64<<10),
	)
}
func BenchmarkAuditStreamSaturated(b *testing.B) {
	for _, clone := range []bool{false, true} {
		for _, n := range []int{32, 256, 1024} {
			b.Run(fmt.Sprintf("clone=%t/events=%d", clone, n), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					auditStream(b, n, clone)
				}
			})
		}
	}
}

//nolint:gocognit // Explicit independent cloner and ownership assertions.
func TestStreamAdmissionClonerMatrix(t *testing.T) {
	for _, mode := range []string{"nil", "state", "effect", "both"} {
		t.Run(mode, func(t *testing.T) {
			// Arrange: alternate HasEffect with an undrained buffer and mutable payloads.
			state, effect := []byte{42}, []byte{43}
			stateClones, effectClones := 0, 0
			inv := runInvocationOptions[[]byte, []byte]{}
			if mode == "state" || mode == "both" {
				inv.cloneState = func(v []byte) []byte { stateClones++; return append([]byte(nil), v...) }
			}
			if mode == "effect" || mode == "both" {
				inv.cloneEffect = func(v []byte) []byte { effectClones++; return append([]byte(nil), v...) }
			}
			runner := &graphRunner[[]byte, []byte]{}
			want := &RunResult[[]byte, []byte]{State: state, Status: RunStatusCompleted}
			// Act: terminal event is attempted only after saturation.
			h := runner.startStream(
				context.Background(),
				inv,
				func(ctx context.Context, sink eventSink[[]byte, []byte]) (*RunResult[[]byte, []byte], error) {
					for i := range 1024 {
						if !sink(ctx, RunEvent[[]byte, []byte]{State: state, Effect: effect, HasEffect: i%2 == 0}) {
							return nil, errors.New("unexpected stop")
						}
					}
					sink(ctx, RunEvent[[]byte, []byte]{Type: EventCompleted, State: state})
					return want, nil
				},
			)
			got, err := h.WaitResult()
			state[0], effect[0] = 99, 99
			// Assert: authoritative result and bounded, detached observations.
			if err != nil || got != want {
				t.Fatalf("result=%v err=%v", got, err)
			}
			delivered, effects := 0, 0
			for e := range h.Events() {
				delivered++
				if e.Type == EventCompleted {
					t.Fatal("terminal event admitted at full buffer")
				}
				if inv.cloneState != nil && e.State[0] != 42 {
					t.Fatal("state alias")
				}
				if e.HasEffect {
					effects++
					if inv.cloneEffect != nil && e.Effect[0] != 43 {
						t.Fatal("effect alias")
					}
				}
			}
			if delivered != 32 || effects != 16 {
				t.Fatalf("delivered=%d effects=%d", delivered, effects)
			}
			wantState, wantEffect := 0, 0
			if inv.cloneState != nil {
				wantState = 32
			}
			if inv.cloneEffect != nil {
				wantEffect = 16
			}
			if stateClones != wantState || effectClones != wantEffect {
				t.Fatalf("state clones=%d effect clones=%d", stateClones, effectClones)
			}
		})
	}
}

func TestStreamAdmissionConcurrentDrain(t *testing.T) {
	// Arrange: force one publication/drain per round while producer mutates sources.
	ack := make(chan struct{})
	state, effect := []byte{42}, []byte{43}
	clones := 0
	inv := runInvocationOptions[[]byte, []byte]{
		cloneState:  func(v []byte) []byte { clones++; return append([]byte(nil), v...) },
		cloneEffect: func(v []byte) []byte { return append([]byte(nil), v...) },
	}
	runner := &graphRunner[[]byte, []byte]{}
	// Act: the producer mutates sources after each send while the consumer drains.
	h := runner.startStream(
		context.Background(),
		inv,
		func(ctx context.Context, sink eventSink[[]byte, []byte]) (*RunResult[[]byte, []byte], error) {
			for range 1024 {
				state[0], effect[0] = 42, 43
				if !sink(ctx, RunEvent[[]byte, []byte]{State: state, Effect: effect, HasEffect: true}) {
					return nil, errors.New("unexpected stop")
				}
				state[0], effect[0] = 99, 99
				<-ack
			}
			return &RunResult[[]byte, []byte]{Status: RunStatusCompleted}, nil
		},
	)
	delivered := 0
	valid := true
	for e := range h.Events() {
		delivered++
		valid = valid && e.State[0] == 42 && e.Effect[0] == 43
		// Consumer mutation cannot affect sources or subsequent observations.
		e.State[0], e.Effect[0] = 11, 12
		ack <- struct{}{}
	}
	_, err := h.WaitResult()
	// Assert.
	if err != nil || !valid || delivered != 1024 || clones != 1024 {
		t.Fatalf("err=%v valid=%t delivered=%d clones=%d", err, valid, delivered, clones)
	}
}

//nolint:gocognit // Deterministic before/during/full-buffer cancellation matrix.
func TestStreamAdmissionCancellation(t *testing.T) {
	for _, mode := range []string{"stop-before", "cancel-before", "stop-during", "cancel-during", "stop-full", "cancel-full"} {
		t.Run(mode, func(t *testing.T) {
			// Arrange: explicit barriers make cancellation deterministic.
			entered, release, cloning := make(chan struct{}), make(chan struct{}), make(chan struct{})
			eventCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clones := 0
			inv := runInvocationOptions[int, NoEffect]{cloneState: func(v int) int {
				clones++
				if mode == "stop-during" || mode == "cancel-during" {
					close(cloning)
					<-release
				}
				return v
			}}
			accepted := true
			runner := &graphRunner[int, NoEffect]{}
			h := runner.startStream(
				context.Background(),
				inv,
				func(_ context.Context, sink eventSink[int, NoEffect]) (*RunResult[int, NoEffect], error) {
					if mode == "stop-full" || mode == "cancel-full" {
						for range 32 {
							sink(eventCtx, RunEvent[int, NoEffect]{})
						}
					}
					close(entered)
					if mode != "stop-during" && mode != "cancel-during" {
						<-release
					}
					accepted = sink(eventCtx, RunEvent[int, NoEffect]{})
					return &RunResult[int, NoEffect]{Status: RunStatusCompleted}, nil
				},
			)
			<-entered
			// Act.
			if mode == "stop-during" || mode == "cancel-during" {
				<-cloning
			}
			if mode == "stop-before" || mode == "stop-during" || mode == "stop-full" {
				h.RequestStop()
			} else {
				cancel()
			}
			close(release)
			_, err := h.WaitResult()
			delivered := 0
			for range h.Events() {
				delivered++
			}
			// Assert: cancellation prevents publication, including after detachment.
			wantClones, wantDelivered := 0, 0
			if mode == "stop-during" || mode == "cancel-during" {
				wantClones = 1
			}
			if mode == "stop-full" || mode == "cancel-full" {
				wantClones, wantDelivered = 32, 32
			}
			if err != nil || accepted || clones != wantClones || delivered != wantDelivered {
				t.Fatalf("err=%v accepted=%t clones=%d delivered=%d", err, accepted, clones, delivered)
			}
		})
	}
}
