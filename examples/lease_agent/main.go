// Package main demonstrates lease-aware execution with MemoryLeaseManager.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type workState struct {
	Step int
}

const demoLeaseTTL = 30 * time.Second

func main() {
	cp := testutil.NewMemoryCheckpointer[workState, flowy.NoEffect]()
	lease := flowy.NewMemoryLeaseManager()

	b := flowy.NewGraph[workState, flowy.NoEffect](func(_ workState, u workState) workState { return u })
	b.AddNode("step", func(_ context.Context, s workState) (workState, flowy.Directive, error) {
		s.Step++
		if s.Step < 2 {
			return s, flowy.Suspend("more"), nil
		}
		return s, flowy.End(), nil
	})
	b.SetEntryPoint("step")
	b.AllowNoOutgoingRoute("step")
	// WithDeleteOnSuccess: checkpoint removed after successful completion once lease is released.
	g, err := b.Compile(flowy.WithDeleteOnSuccess(true))
	if err != nil {
		log.Fatal(err)
	}

	runner := g.NewRunnerWithOptions(cp, []flowy.RunnerOption[workState, flowy.NoEffect]{
		flowy.WithLeaseManager[workState, flowy.NoEffect](lease),
	})
	leaseOpts := flowy.WithRunLease[workState, flowy.NoEffect]("worker-1", demoLeaseTTL)

	first, err := runner.Start(context.Background(), "lease-thread", workState{}, leaseOpts)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("first status=%s step=%d\n", first.Status, first.State.Step)

	second, err := runner.Resume(context.Background(), "lease-thread", leaseOpts)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("second status=%s step=%d\n", second.Status, second.State.Step)

	if _, loadErr := cp.Load(context.Background(), "lease-thread"); loadErr == nil {
		log.Fatal("expected checkpoint deleted after successful completion with DeleteOnSuccess")
	}
	fmt.Println("checkpoint deleted after success (DeleteIfIdle after lease release)")
}
