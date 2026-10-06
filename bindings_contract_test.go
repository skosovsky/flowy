package flowy

import (
	"context"
	"sync"
	"testing"
)

func TestBindingsSameTypeSlotAndFrozenConcurrentReads(t *testing.T) {
	t.Parallel()
	// Arrange: two same-type sentinels intentionally address one slot.
	var first, second BindingKey[int]
	bindings := NewRunBindings()
	Bind(bindings, first, 10)
	Bind(bindings, second, 21)
	ctx := bindings.WithContext(context.Background()) // setup is now frozen by discipline
	results := make(chan int, 8)
	var readers sync.WaitGroup
	// Act: only reads are shared after publication.
	for range 8 {
		readers.Go(func() {
			value, ok := BindingFromContext(ctx, first)
			if !ok {
				results <- -1
				return
			}
			results <- value
		})
	}
	readers.Wait()
	close(results)
	// Assert: every reader sees the single type slot's last setup value.
	for value := range results {
		if value != 21 {
			t.Fatalf("value=%d", value)
		}
	}
}
