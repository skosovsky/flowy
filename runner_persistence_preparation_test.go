package flowy

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type preparationState struct{ Value string }

type preparationInterceptor struct {
	calls  int
	reject error
}

func (i *preparationInterceptor) BeforeSave(_ context.Context, state *preparationState) error {
	i.calls++
	if i.reject != nil {
		return i.reject
	}
	state.Value = "encoded:" + state.Value
	return nil
}

func (*preparationInterceptor) AfterLoad(_ context.Context, state *preparationState) error {
	state.Value = strings.TrimPrefix(state.Value, "encoded:")
	return nil
}

func preparationGraph(t *testing.T) *Graph[preparationState, NoEffect] {
	t.Helper()
	builder := NewGraph[preparationState, NoEffect](
		func(_ preparationState, update preparationState) preparationState { return update },
	)
	builder.AddNode("work", func(_ context.Context, state preparationState) (preparationState, Directive, error) {
		return state, Handoff("background"), nil
	})
	builder.AllowNoOutgoingRoute("work")
	builder.SetEntryPoint("work")
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return graph
}

func TestHandoffPreparationParity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                    string
		transactional, rejected bool
	}{
		{"ordinary/transform", false, false}, {"ordinary/reject", false, true},
		{"transactional/transform", true, false}, {"transactional/reject", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertHandoffPreparationParity(t, tc.transactional, tc.rejected)
		})
	}
}

func assertHandoffPreparationParity(t *testing.T, transactional, rejected bool) {
	t.Helper()
	// Arrange: non-idempotent encoding and a domain-only invariant.
	memory := newMemoryCP[preparationState, NoEffect]()
	var cp Checkpointer[preparationState, NoEffect] = memory
	if transactional {
		cp = &transactionalMemoryCP[preparationState, NoEffect]{memoryCP: memory}
	}
	interceptor := &preparationInterceptor{}
	rejection := errors.New("redaction rejected")
	if rejected {
		interceptor.reject = rejection
	}
	outbox := &stubHandoffOutbox{}
	runner := preparationGraph(t).NewRunner(cp, interceptor)
	invariantCalls := 0
	// Act.
	result, err := runner.Start(context.Background(), "preparation", preparationState{Value: "42"},
		WithHandoffOutbox[preparationState, NoEffect](outbox),
		WithInvariantValidator[preparationState, NoEffect](func(state preparationState) error {
			invariantCalls++
			if state.Value != "42" {
				return errors.New("encoded state is not domain state")
			}
			return nil
		}))
	// Assert: reject before any persistence/notification; otherwise encode exactly once.
	if interceptor.calls != 1 {
		t.Fatalf("BeforeSave calls = %d, want 1", interceptor.calls)
	}
	if invariantCalls == 0 {
		t.Fatal("domain invariant was bypassed")
	}
	if rejected {
		if !errors.Is(err, rejection) {
			t.Fatalf("error = %v, want rejection", err)
		}
		if len(memory.hist["preparation"]) != 0 || outbox.lastToken().ThreadID != "" {
			t.Fatal("rejection mutated snapshot/outbox")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if result.State.Value != "42" {
		t.Fatalf("runtime state changed: %+v", result.State)
	}
	for _, snapshot := range memory.hist["preparation"] {
		if snapshot.State.Value != "encoded:42" {
			t.Fatalf("persisted representation: %+v", snapshot.State)
		}
	}
	if len(memory.hist["preparation"]) == 0 || outbox.lastToken().ThreadID != "preparation" {
		t.Fatal("missing checkpoint/outbox")
	}
}

func TestPreparationInvariantRejectsBeforeHooksAndPersistence(t *testing.T) {
	t.Parallel()
	for _, transactional := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "transactional"}[transactional], func(t *testing.T) {
			t.Parallel()
			// Arrange: preserve an existing healthy head/history on rejection.
			memory := newMemoryCP[preparationState, NoEffect]()
			snapshot := Snapshot[preparationState, NoEffect]{
				ThreadID:         "healthy",
				ExecutionPointer: "work",
				State:            preparationState{Value: "healthy"},
			}
			if _, err := memory.Save(context.Background(), 0, snapshot); err != nil {
				t.Fatal(err)
			}
			var cp Checkpointer[preparationState, NoEffect] = memory
			if transactional {
				cp = &transactionalMemoryCP[preparationState, NoEffect]{memoryCP: memory}
			}
			interceptor := &preparationInterceptor{}
			outbox := &stubHandoffOutbox{}
			runner := &graphRunner[preparationState, NoEffect]{
				checkpointer: cp,
				interceptors: []StateInterceptor[preparationState]{interceptor},
			}
			rejection := errors.New("domain invalid")
			inv := runInvocationOptions[preparationState, NoEffect]{
				invariantValidator: func(preparationState) error { return rejection },
			}
			// Act.
			var err error
			if transactional {
				_, _, err = runner.tryTransactionalHandoffSave(
					context.Background(),
					"healthy",
					1,
					snapshot,
					snapshot.RunMeta,
					inv,
					outbox,
					&RunResult[preparationState, NoEffect]{Reason: "background"},
				)
			} else {
				_, _, err = runner.persistSnapshot(context.Background(), 1, snapshot, nil, "work", snapshot.State, inv)
			}
			// Assert.
			if !errors.Is(err, rejection) || interceptor.calls != 0 {
				t.Fatalf("err=%v, hook calls=%d", err, interceptor.calls)
			}
			if len(memory.hist["healthy"]) != 1 || memory.last.State.Value != "healthy" ||
				outbox.lastToken().ThreadID != "" {
				t.Fatal("rejection changed head/history/outbox")
			}
		})
	}
}

func TestHandoffMetadataRecoveryDoesNotReencodeState(t *testing.T) {
	t.Parallel()
	// Arrange: orphan a successfully prepared ordinary handoff.
	memory := newMemoryCP[preparationState, NoEffect]()
	interceptor := &preparationInterceptor{}
	outbox := &stubHandoffOutbox{err: errors.New("notification unavailable")}
	runner := preparationGraph(t).NewRunner(memory, interceptor)
	result, err := runner.Start(
		context.Background(),
		"recover-encoded",
		preparationState{Value: "42"},
		WithHandoffOutbox[preparationState, NoEffect](outbox),
	)
	if !errors.Is(err, ErrHandoffEnqueueFailed) || result.RunMeta.HandoffStatus != HandoffStatusOrphaned {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	outbox.err = nil
	// Act: load persisted state and patch metadata without another encode/decode.
	recovery, err := runner.RecoverStaleHandoff(context.Background(), "recover-encoded", WithRecoverOutbox(outbox))
	// Assert.
	if err != nil || !recovery.Recovered {
		t.Fatalf("recovery=%+v err=%v", recovery, err)
	}
	if interceptor.calls != 1 {
		t.Fatalf("BeforeSave calls=%d, want 1", interceptor.calls)
	}
	for _, snapshot := range memory.hist["recover-encoded"] {
		if snapshot.State.Value != "encoded:42" {
			t.Fatalf("metadata patch changed storage state: %+v", snapshot.State)
		}
	}
}
