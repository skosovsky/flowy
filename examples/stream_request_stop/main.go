// Package main demonstrates BeginStreamCollect + RequestStop without deadlock.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type workState struct {
	Ticks int
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ready := make(chan struct{})
	graph, err := flowy.NewGraph[workState, flowy.NoEffect](func(_ workState, u workState) workState { return u }).
		AddNode("work", func(ctx context.Context, s workState) (workState, flowy.Directive, error) {
			select {
			case <-ready:
			default:
				close(ready)
			}
			<-ctx.Done()
			return s, flowy.Completed(), nil
		}).
		AllowNoOutgoingRoute("work").
		SetEntryPoint("work").
		Compile()
	if err != nil {
		return err
	}

	cp := testutil.NewMemoryCheckpointer[workState, flowy.NoEffect]()
	threadID := "request-stop-thread"
	handle, err := graph.NewRunner(cp).Stream(context.Background(), threadID, workState{})
	if err != nil {
		return err
	}

	out := flowy.BeginStreamCollect(handle)
	<-ready
	handle.RequestStop()

	result, waitErr := flowy.AwaitStreamCollectWithSnapshot(context.Background(), handle, out, cp, threadID)
	if waitErr != nil {
		return waitErr
	}
	fmt.Printf("events=%d wait_err=%v\n", len(result.Events), waitErr)
	if result.Snapshot == nil {
		return errors.New("expected snapshot after RequestStop")
	}
	fmt.Printf(
		"checkpoint node=%s ticks=%d segment_end=%s\n",
		result.Snapshot.ExecutionPointer,
		result.Snapshot.State.Ticks,
		result.Snapshot.RunMeta.Segment.EndReason,
	)
	fmt.Printf("resume_token thread=%s revision=%d\n", result.ResumeToken.ThreadID, result.ResumeToken.SnapshotRevision)
	return nil
}
