package flowy

import (
	"context"
	"errors"
	"testing"
	"time"
)

type preparationLeaseSpy struct {
	LeaseManager

	calls int
}

func (s *preparationLeaseSpy) Acquire(context.Context, string, string, time.Duration) (ExecutionLease, error) {
	s.calls++
	return ExecutionLease{}, errors.New("unexpected acquisition")
}

type preparationPlainOutbox struct{}

func (*preparationPlainOutbox) EnqueueIntent(context.Context, HandoffIntent) error { return nil }

func TestAtomicHandoffPreflightRejectsBeforeExecution(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"no-checkpointer-capability", "no-outbox-capability", "no-outbox"} {
		for _, entry := range []string{"start", "resume", "stream", "resume-stream"} {
			t.Run(kind+"/"+entry, func(t *testing.T) {
				t.Parallel()
				assertAtomicPreflightRejection(t, kind, entry)
			})
		}
	}
}

func assertAtomicPreflightRejection(t *testing.T, kind, entry string) {
	t.Helper()
	// Arrange: preflight must reject even before lease acquisition/load.
	memory := newMemoryCP[preparationState, NoEffect]()
	var cp Checkpointer[preparationState, NoEffect] = memory
	var outbox HandoffOutbox = &stubHandoffOutbox{}
	if kind != "no-checkpointer-capability" {
		cp = &transactionalMemoryCP[preparationState, NoEffect]{memoryCP: memory}
	}
	if kind == "no-outbox-capability" {
		outbox = &preparationPlainOutbox{}
	}
	if kind == "no-outbox" {
		outbox = nil
	}
	lease := &preparationLeaseSpy{}
	interceptor := &preparationInterceptor{}
	runner := preparationGraph(
		t,
	).NewRunnerWithOptions(cp, []RunnerOption[preparationState, NoEffect]{WithLeaseManager[preparationState, NoEffect](lease)}, interceptor)
	opts := []RunOption[preparationState, NoEffect]{
		WithAtomicHandoff[preparationState, NoEffect](),
		WithHandoffOutbox[preparationState, NoEffect](outbox),
		WithRunLease[preparationState, NoEffect]("owner", time.Minute),
	}
	// Act.
	var err error
	switch entry {
	case "start":
		_, err = runner.Start(context.Background(), "atomic", preparationState{}, opts...)
	case "resume":
		_, err = runner.Resume(context.Background(), ResumeToken{ThreadID: "atomic", SnapshotRevision: 1}, opts...)
	case "stream":
		_, err = runner.Stream(context.Background(), "atomic", preparationState{}, opts...)
	case "resume-stream":
		_, err = runner.ResumeStream(
			context.Background(),
			ResumeToken{ThreadID: "atomic", SnapshotRevision: 1},
			opts...)
	}
	// Assert.
	if !errors.Is(err, ErrTransactionalOutboxUnsupported) {
		t.Fatalf("err=%v", err)
	}
	if lease.calls != 0 || interceptor.calls != 0 || len(memory.hist) != 0 {
		t.Fatal("unsupported admission performed work")
	}
}

func TestAtomicHandoffUsesRunnerDefaultOutbox(t *testing.T) {
	t.Parallel()
	// Arrange.
	memory := newMemoryCP[preparationState, NoEffect]()
	cp := &transactionalMemoryCP[preparationState, NoEffect]{memoryCP: memory}
	outbox := &stubHandoffOutbox{}
	interceptor := &preparationInterceptor{}
	runner := preparationGraph(
		t,
	).NewRunnerWithOptions(cp, []RunnerOption[preparationState, NoEffect]{WithRunnerHandoffOutbox[preparationState, NoEffect](outbox)}, interceptor)
	// Act.
	result, err := runner.Start(
		context.Background(),
		"atomic",
		preparationState{Value: "42"},
		WithAtomicHandoff[preparationState, NoEffect](),
	)
	// Assert: one transaction revision, one preparation, recorded intent at that revision.
	if err != nil {
		t.Fatal(err)
	}
	if len(memory.hist["atomic"]) != 1 || interceptor.calls != 1 || result.ResumeToken != outbox.lastToken() {
		t.Fatalf("history=%+v hooks=%d result=%+v", memory.hist["atomic"], interceptor.calls, result)
	}
}

type preparationFailureCP struct {
	*memoryCP[preparationState, NoEffect]

	err error
}

func (c *preparationFailureCP) Save(context.Context, uint64, Snapshot[preparationState, NoEffect]) (uint64, error) {
	return 0, c.err
}

func TestSkipOnSaveErrorPreservesContractRejections(t *testing.T) {
	t.Parallel()
	for _, rejection := range []error{ErrConcurrencyConflict, ErrInvalidSnapshot, ErrLeaseLost, ErrThreadLeaseBusy, ErrExecutionCapability, ErrInvalidHandoffIntent, ErrTransactionalOutboxUnsupported} {
		t.Run(rejection.Error(), func(t *testing.T) {
			t.Parallel()
			// Arrange.
			memory := newMemoryCP[preparationState, NoEffect]()
			cp := &preparationFailureCP{memoryCP: memory, err: rejection}
			outbox := &stubHandoffOutbox{}
			runner := preparationGraph(t).NewRunner(cp)
			// Act.
			result, err := runner.Start(
				context.Background(),
				"skip-rejection",
				preparationState{Value: "42"},
				WithHandoffOutbox[preparationState, NoEffect](outbox),
				WithCheckpointErrorPolicy[preparationState, NoEffect](CheckpointPolicySkipOnSaveError),
			)
			// Assert.
			if !errors.Is(err, rejection) {
				t.Fatalf("err=%v result=%+v", err, result)
			}
			if len(memory.hist) != 0 || outbox.lastToken().ThreadID != "" {
				t.Fatal("contract error produced persistence/outbox")
			}
		})
	}
}

func TestSkipOnSaveErrorDoesNotSuppressPreparationRejection(t *testing.T) {
	t.Parallel()
	// Arrange.
	memory := newMemoryCP[preparationState, NoEffect]()
	rejection := errors.New("host redaction rejected")
	interceptor := &preparationInterceptor{reject: rejection}
	outbox := &stubHandoffOutbox{}
	runner := preparationGraph(t).NewRunner(memory, interceptor)
	// Act.
	result, err := runner.Start(
		context.Background(),
		"skip-preparation",
		preparationState{},
		WithHandoffOutbox[preparationState, NoEffect](outbox),
		WithCheckpointErrorPolicy[preparationState, NoEffect](CheckpointPolicySkipOnSaveError),
	)
	// Assert.
	if !errors.Is(err, rejection) || result.ResumeToken.ThreadID != "" {
		t.Fatalf("err=%v result=%+v", err, result)
	}
	if len(memory.hist) != 0 || outbox.lastToken().ThreadID != "" || interceptor.calls != 1 {
		t.Fatal("preparation rejection performed publication")
	}
}
