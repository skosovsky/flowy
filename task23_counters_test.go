package flowy_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/checkpoint"
	"github.com/skosovsky/flowy/testutil"
)

type task23CounterRunner interface {
	Start(
		context.Context,
		string,
		int,
		...flowy.RunOption[int, flowy.NoEffect],
	) (*flowy.RunResult[int, flowy.NoEffect], error)
	Resume(
		context.Context,
		flowy.ResumeToken,
		...flowy.RunOption[int, flowy.NoEffect],
	) (*flowy.RunResult[int, flowy.NoEffect], error)
	Stream(
		context.Context,
		string,
		int,
		...flowy.RunOption[int, flowy.NoEffect],
	) (flowy.StreamHandle[int, flowy.NoEffect], error)
	ResumeStream(
		context.Context,
		flowy.ResumeToken,
		...flowy.RunOption[int, flowy.NoEffect],
	) (flowy.StreamHandle[int, flowy.NoEffect], error)
}

func newTask23CounterRunner(
	t *testing.T,
	g *flowy.Graph[int, flowy.NoEffect],
	durable bool,
) (task23CounterRunner, flowy.ExecutionStore) {
	t.Helper()
	if !durable {
		return g.NewRunner(testutil.NewMemoryCheckpointer[int, flowy.NoEffect]()), nil
	}
	store := testutil.NewMemoryExecutionStore(nil)
	r, err := flowy.NewDurableRunner(
		g,
		store,
		durableDescriptor("counters"),
		checkpoint.JSONSerializer[int]{},
		checkpoint.JSONSerializer[[]flowy.NoEffect]{},
		flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	return r, store
}

func task23RunCounter(
	t *testing.T,
	r task23CounterRunner,
	stream bool,
	token *flowy.ResumeToken,
) (*flowy.RunResult[int, flowy.NoEffect], error) {
	t.Helper()
	ctx := context.Background()
	if !stream {
		if token != nil {
			return r.Resume(ctx, *token)
		}
		return r.Start(ctx, "counters", 0)
	}
	var h flowy.StreamHandle[int, flowy.NoEffect]
	var err error
	if token != nil {
		h, err = r.ResumeStream(ctx, *token)
	} else {
		h, err = r.Stream(ctx, "counters", 0)
	}
	if err != nil {
		return nil, err
	}
	return h.WaitResult()
}

//nolint:gocognit // full durable/stream/retry counter matrix with committed assertions
func TestTask23RetryAndResumeCounters(t *testing.T) {
	for _, durable := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, retry := range []bool{false, true} {
				t.Run(fmt.Sprintf("durable=%t/stream=%t/retry=%t", durable, stream, retry), func(t *testing.T) {
					// Arrange.
					calls := 0
					b := flowy.NewGraph[int, flowy.NoEffect](func(_, u int) int { return u })
					b.AddNode("work", func(ctx context.Context, s int) (int, flowy.Directive, error) {
						calls++
						if err := flowy.UseBudget(ctx, "units", 1); err != nil {
							return s, flowy.Fail("budget"), err
						}
						if retry {
							return s + 1, flowy.Retry(4), nil
						}
						if s == 0 {
							return s + 1, flowy.Suspend("pause"), nil
						}
						return s + 1, flowy.End(), nil
					}).SetEntryPoint("work").AllowNoOutgoingRoute("work")
					if retry {
						b.AddRetryRoute("work", "work")
					}
					g, err := b.Compile(flowy.WithMaxSteps(1))
					if err != nil {
						t.Fatal(err)
					}
					r, store := newTask23CounterRunner(t, g, durable)
					// Act.
					first, err := task23RunCounter(t, r, stream, nil)
					// Assert.
					if first == nil || calls != 1 || first.RunMeta.StepCount != 1 ||
						first.RunMeta.BudgetCounts["units"] != 1 {
						t.Fatalf("first=%+v calls=%d err=%v", first, calls, err)
					}
					if retry {
						if !errors.Is(err, flowy.ErrMaxStepsExceeded) || first.RunMeta.RetryCounts["work"] != 1 {
							t.Fatalf("retry first=%+v err=%v", first, err)
						}
						assertTask23CounterEnvelope(t, store, 1, 1)
						return
					}
					if err != nil || first.Status != flowy.RunStatusSuspended {
						t.Fatalf("first=%+v err=%v", first, err)
					}
					second, err := task23RunCounter(t, r, stream, &first.ResumeToken)
					if err != nil || second.Status != flowy.RunStatusCompleted || calls != 2 || second.State != 2 ||
						second.RunMeta.StepCount != 1 ||
						second.RunMeta.BudgetCounts["units"] != 2 {
						t.Fatalf("second=%+v calls=%d err=%v", second, calls, err)
					}
					assertTask23CounterEnvelope(t, store, 2, 0)
				})
			}
		}
	}
}

func assertTask23CounterEnvelope(t *testing.T, store flowy.ExecutionStore, budget, retries int) {
	t.Helper()
	if store == nil {
		return
	}
	envelope, err := store.LoadExecution(context.Background(), "counters")
	if err != nil || envelope.RunMeta.StepCount != 1 || envelope.RunMeta.BudgetCounts["units"] != budget ||
		envelope.RunMeta.RetryCounts["work"] != retries {
		t.Fatalf("committed=%+v err=%v", envelope, err)
	}
}
