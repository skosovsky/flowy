package flowy_test

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/skosovsky/flowy/checkpoint"

	"github.com/skosovsky/flowy"
	"github.com/skosovsky/flowy/testutil"
)

type task23MutableStreamer interface {
	Stream(
		context.Context,
		string,
		task23Mutable,
		...flowy.RunOption[task23Mutable, map[string]int],
	) (flowy.StreamHandle[task23Mutable, map[string]int], error)
}

//nolint:gocognit // coordinated producer/consumer mutation in ordinary and durable modes
func TestTask23ConcurrentMutableConsumer(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%t", durable), func(t *testing.T) {
			// Arrange: keep the producer live while the consumer mutates a published value.
			ready := make(chan struct{})
			release := make(chan struct{})
			p := 0
			initial := task23Mutable{Values: map[string]int{"v": 0}, Items: []int{0}, Pointer: &p}
			effect := map[string]int{"v": 1}
			b := flowy.NewGraph[task23Mutable, map[string]int](func(_, u task23Mutable) task23Mutable { return u })
			b.AddNode("first", func(_ context.Context, s task23Mutable) (task23Mutable, flowy.Directive, error) {
				s.Values["v"] = 1
				s.Items[0] = 1
				*s.Pointer = 1
				return s, flowy.Effect(flowy.Completed(), effect), nil
			})
			b.AddNode("last", func(ctx context.Context, s task23Mutable) (task23Mutable, flowy.Directive, error) {
				close(ready)
				select {
				case <-release:
				case <-ctx.Done():
					return s, flowy.Completed(), ctx.Err()
				}
				for range 1000 {
					s.Values["v"] = 2
					s.Items[0] = 2
					*s.Pointer = 2
					effect["v"] = 2
				}
				return s, flowy.End(), nil
			})
			b.AddEdge("first", "last").SetEntryPoint("first").AllowNoOutgoingRoute("last")
			g, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			cp := testutil.NewMemoryCheckpointerWithCloners(
				cloneTask23Mutable,
				flowy.ValueCloner[map[string]int](maps.Clone[map[string]int]),
			)
			var r task23MutableStreamer = g.NewRunner(cp)
			if durable {
				r, err = flowy.NewDurableRunner(
					g,
					testutil.NewMemoryExecutionStore(nil),
					durableDescriptor("mutable"),
					checkpoint.JSONSerializer[task23Mutable]{},
					checkpoint.JSONSerializer[[]map[string]int]{},
					flowy.DurableOptions{Owner: "worker", LeaseTTL: time.Minute},
				)
				if err != nil {
					t.Fatal(err)
				}
			}
			// Act.
			h, err := r.Stream(
				context.Background(),
				"concurrent",
				initial,
				flowy.WithEventCloners(
					cloneTask23Mutable,
					flowy.ValueCloner[map[string]int](maps.Clone[map[string]int]),
				),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer h.RequestStop()
			<-ready
			found := false
			for event := range h.Events() {
				if !event.HasEffect {
					continue
				}
				found = true
				close(release)
				for range 1000 {
					event.State.Values["v"] = 9
					event.State.Items[0] = 9
					*event.State.Pointer = 9
					event.Effect["v"] = 9
				}
				break
			}
			result, err := h.WaitResult()
			// Assert: consumer writes cannot change producer or committed snapshots.
			if err != nil || !found || result.State.Values["v"] != 2 || result.State.Items[0] != 2 ||
				*result.State.Pointer != 2 ||
				result.Effects[0]["v"] != 2 {
				t.Fatalf("result=%+v found=%t err=%v", result, found, err)
			}
			if durable {
				return
			}
			history, err := cp.GetHistory(context.Background(), "concurrent", 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, snapshot := range history {
				if snapshot.State.Values["v"] == 9 {
					t.Fatal("consumer mutated snapshot")
				}
			}
		})
	}
}
