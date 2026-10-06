package flowy

import (
	"context"
	"errors"
)

type failingCaptureCheckpointer[T, E any] struct {
	captureCheckpointer[T, E]

	failSave bool
	failLoad bool
}

// bumpRevisionOnLoadCP simulates inner OCC mismatch after slot seed for tests.
type bumpRevisionOnLoadCP[T, E any] struct {
	captureCheckpointer[T, E]
}

func (b *bumpRevisionOnLoadCP[T, E]) Load(
	ctx context.Context,
	threadID string,
) (Snapshot[T, E], uint64, error) {
	snap, rev, err := b.captureCheckpointer.Load(ctx, threadID)
	if err != nil {
		return snap, 0, err
	}
	snap.Revision = rev + 1
	return snap, snap.Revision, nil
}

func (f *failingCaptureCheckpointer[T, E]) Save(
	ctx context.Context,
	expectedRevision uint64,
	s Snapshot[T, E],
) (uint64, error) {
	if f.failSave {
		return 0, errors.New("subgraph seed save failed")
	}
	return f.captureCheckpointer.Save(ctx, expectedRevision, s)
}

func (f *failingCaptureCheckpointer[T, E]) Load(
	ctx context.Context,
	threadID string,
) (Snapshot[T, E], uint64, error) {
	if f.failLoad {
		var zero Snapshot[T, E]
		return zero, 0, errors.New("subgraph slot load failed")
	}
	return f.captureCheckpointer.Load(ctx, threadID)
}

type subgraphTestMode int

const (
	subgraphTestModeNone subgraphTestMode = iota
	subgraphTestModeFailSeedSave
	subgraphTestModeFailSlotLoad
	subgraphTestModeStaleInnerRevision
)

type subgraphTestModeKey struct{}

// withSubgraphTestMode configures ephemeral subgraph checkpointer behavior for tests.
func withSubgraphTestMode(ctx context.Context, mode subgraphTestMode) context.Context {
	return context.WithValue(ctx, subgraphTestModeKey{}, mode)
}

func newSubgraphCheckpointer[Sub, E any](ctx context.Context) Checkpointer[Sub, E] {
	mode, _ := ctx.Value(subgraphTestModeKey{}).(subgraphTestMode)
	switch mode {
	case subgraphTestModeFailSeedSave:
		base := newCaptureCheckpointer[Sub, E]()
		return &failingCaptureCheckpointer[Sub, E]{
			captureCheckpointer: *base,
			failSave:            true,
			failLoad:            false,
		}
	case subgraphTestModeFailSlotLoad:
		base := newCaptureCheckpointer[Sub, E]()
		return &failingCaptureCheckpointer[Sub, E]{
			captureCheckpointer: *base,
			failSave:            false,
			failLoad:            true,
		}
	case subgraphTestModeStaleInnerRevision:
		base := newCaptureCheckpointer[Sub, E]()
		return &bumpRevisionOnLoadCP[Sub, E]{captureCheckpointer: *base}
	default:
		return newCaptureCheckpointer[Sub, E]()
	}
}

func faultSubgraphNodeWithSlot[Parent, Sub, E any](
	sub *Graph[Sub, E], mapIn func(Parent) Sub,
	loadSlot func(Parent) (SubgraphSlot[Sub, E], bool),
	storeSlot func(Parent, SubgraphSlot[Sub, E]) Parent,
	mapOut func(Parent, Sub) Parent,
) Node[Parent, E] {
	return subgraphNodeWithCheckpointer(sub, mapIn, loadSlot, storeSlot, mapOut, newSubgraphCheckpointer[Sub, E])
}
