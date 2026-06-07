// Package main demonstrates Handoff Outbox: persist handoff, enqueue resume intent, recover stale pending.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	flowyotel "github.com/skosovsky/flowy/ext/otel"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type jobState struct {
	Step int
}

type memoryOutbox struct {
	mu    sync.Mutex
	queue []flowy.ResumeToken
}

func (o *memoryOutbox) EnqueueIntent(_ context.Context, token flowy.ResumeToken) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.queue = append(o.queue, token)
	fmt.Printf("outbox enqueue thread=%s rev=%d\n", token.ThreadID, token.SnapshotRevision)
	return nil
}

func (o *memoryOutbox) pop() (flowy.ResumeToken, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.queue) == 0 {
		return flowy.ResumeToken{}, false
	}
	token := o.queue[0]
	o.queue = o.queue[1:]
	return token, true
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
	if recoverErr := staleRunner.RecoverStaleHandoff(context.Background(), staleThreadID,
		flowy.WithRecoverStaleAfter(time.Minute),
	); recoverErr != nil {
		log.Fatalf("recover stale pending: %v", recoverErr)
	}
	staleToken, ok := outbox.pop()
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
	if staleToken.SnapshotRevision != staleRev-1 {
		log.Fatalf("outbox token rev %d != pending rev %d", staleToken.SnapshotRevision, staleRev-1)
	}
	recovered, err := bgRunner.Resume(context.Background(), flowy.ResumeToken{
		ThreadID:         staleThreadID,
		SnapshotRevision: staleRev,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("stale recovery resume status=%s step=%d\n", recovered.Status, recovered.State.Step)
}

func main() {
	if err := flowyotel.InstallLifecycleObserver(); err != nil {
		log.Fatal(err)
	}

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

	outboxToken, ok := outbox.pop()
	if !ok {
		log.Fatal("expected outbox message")
	}
	snap, rev, err := cp.Load(context.Background(), threadID)
	if err != nil {
		log.Fatal(err)
	}
	if outboxToken.SnapshotRevision != rev-1 {
		log.Fatalf("outbox token rev %d != pending rev %d", outboxToken.SnapshotRevision, rev-1)
	}
	freshToken := flowy.ResumeToken{ThreadID: snap.ThreadID, SnapshotRevision: rev}
	if freshToken.SnapshotRevision != res.ResumeToken.SnapshotRevision {
		log.Fatalf(
			"loaded rev %d != result token rev %d",
			freshToken.SnapshotRevision,
			res.ResumeToken.SnapshotRevision,
		)
	}

	bgRunner := graph.NewRunner(cp)
	completed, err := bgRunner.Resume(context.Background(), freshToken)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("background resume status=%s step=%d\n", completed.Status, completed.State.Step)

	runStaleRecoveryDemo(graph, cp, outbox, bgRunner)
}
