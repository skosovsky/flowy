// Package main demonstrates Handoff Outbox: persist handoff, enqueue resume intent, recover stale pending.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type jobState struct {
	Step int
}

type memoryOutbox struct {
	mu    sync.Mutex
	queue []flowy.HandoffIntent
}

func (o *memoryOutbox) EnqueueIntent(_ context.Context, intent flowy.HandoffIntent) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.queue = append(o.queue, intent)
	fmt.Printf("outbox enqueue thread=%s rev=%d status=%s\n",
		intent.ThreadID, intent.SnapshotRevision, intent.HandoffStatus)
	return nil
}

func (o *memoryOutbox) pop() (flowy.HandoffIntent, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.queue) == 0 {
		return flowy.HandoffIntent{}, false
	}
	intent := o.queue[0]
	o.queue = o.queue[1:]
	return intent, true
}

func runStaleRecoveryDemo(
	graph *flowy.Graph[jobState, flowy.NoEffect],
	cp flowy.Checkpointer[jobState, flowy.NoEffect],
	outbox *memoryOutbox,
	bgRunner flowy.Runner[jobState, flowy.NoEffect],
) {
	staleThreadID := "stale-pending-demo"
	staleRunner := graph.NewRunnerWithOptions(cp, []flowy.RunnerOption[jobState, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[jobState, flowy.NoEffect](outbox),
	})
	// Demo fixture only: production apps must not Save HandoffStatus manually — use RecoverStaleHandoff.
	if _, saveErr := cp.Save(context.Background(), 0, flowy.Snapshot[jobState, flowy.NoEffect]{
		ThreadID:         staleThreadID,
		ExecutionPointer: "work",
		State:            jobState{Step: 1},
		RunMeta: flowy.RunMetadata{
			HandoffStatus:    flowy.HandoffStatusPending,
			HandoffPendingAt: time.Now().UTC().Add(-10 * time.Minute),
		},
	}); saveErr != nil {
		log.Fatal(saveErr)
	}
	recovery, recoverErr := staleRunner.RecoverStaleHandoff(context.Background(), staleThreadID,
		flowy.WithRecoverStaleAfter(time.Minute),
	)
	if recoverErr != nil {
		log.Fatalf("recover stale pending: %v", recoverErr)
	}
	staleIntent, ok := outbox.pop()
	if !ok {
		log.Fatal("expected outbox message after stale recovery")
	}
	staleSnap, staleRev, err := cp.Load(context.Background(), staleThreadID)
	if err != nil {
		log.Fatal(err)
	}
	if staleSnap.RunMeta.HandoffStatus != flowy.HandoffStatusEnqueued {
		log.Fatalf("expected enqueued after recovery, got %q", staleSnap.RunMeta.HandoffStatus)
	}
	if recovery.ResumeToken.SnapshotRevision != staleRev {
		log.Fatalf("recovery token rev %d != snapshot rev %d", recovery.ResumeToken.SnapshotRevision, staleRev)
	}
	decision, err := bgRunner.EvaluateResume(context.Background(), staleIntent.ResumeToken)
	if err != nil {
		log.Fatalf("evaluate recovered outbox intent: decision=%+v err=%v", decision, err)
	}
	if decision.ResumeToken != recovery.ResumeToken {
		log.Fatalf("decision token %+v != recovery token %+v", decision.ResumeToken, recovery.ResumeToken)
	}
	recovered, err := bgRunner.Resume(context.Background(), decision.ResumeToken)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("stale recovery resume status=%s step=%d\n", recovered.Status, recovered.State.Step)
}

func main() {
	cp := testutil.NewMemoryCheckpointer[jobState, flowy.NoEffect]()
	outbox := &memoryOutbox{}
	threadID := "handoff-outbox-demo"

	graph, err := flowy.NewGraph[jobState, flowy.NoEffect](func(_ jobState, u jobState) jobState { return u }).
		AddNode("work", func(_ context.Context, s jobState) (jobState, flowy.Directive, error) {
			s.Step++
			if s.Step < 2 {
				return s, flowy.Handoff("background"), nil
			}
			return s, flowy.End(), nil
		}).
		AllowNoOutgoingRoute("work").
		SetEntryPoint("work").
		Compile()
	if err != nil {
		log.Fatal(err)
	}

	runner := graph.NewRunnerWithOptions(cp, []flowy.RunnerOption[jobState, flowy.NoEffect]{
		flowy.WithRunnerHandoffOutbox[jobState, flowy.NoEffect](outbox),
	})

	res, err := runner.Start(context.Background(), threadID, jobState{},
		flowy.WithHandoffOutbox[jobState, flowy.NoEffect](outbox),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("foreground handoff status=%s token_rev=%d\n", res.Status, res.ResumeToken.SnapshotRevision)

	outboxIntent, ok := outbox.pop()
	if !ok {
		log.Fatal("expected outbox message")
	}
	snap, rev, err := cp.Load(context.Background(), threadID)
	if err != nil {
		log.Fatal(err)
	}
	if snap.ThreadID != threadID {
		log.Fatalf("snapshot thread %q != %q", snap.ThreadID, threadID)
	}
	if rev != res.ResumeToken.SnapshotRevision {
		log.Fatalf(
			"loaded rev %d != result token rev %d",
			rev,
			res.ResumeToken.SnapshotRevision,
		)
	}

	bgRunner := graph.NewRunner(cp)
	decision, err := bgRunner.EvaluateResume(context.Background(), outboxIntent.ResumeToken)
	if err != nil {
		log.Fatalf("evaluate outbox intent: decision=%+v err=%v", decision, err)
	}
	if decision.ResumeToken != res.ResumeToken {
		log.Fatalf("decision token %+v != result token %+v", decision.ResumeToken, res.ResumeToken)
	}
	completed, err := bgRunner.Resume(context.Background(), decision.ResumeToken)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("background resume status=%s step=%d\n", completed.Status, completed.State.Step)

	runStaleRecoveryDemo(graph, cp, outbox, bgRunner)
}
