package flowy

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSentinelErrors_Is(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{"ErrInterrupt", ErrInterrupt, ErrInterrupt, true},
		{"ErrMaxStepsExceeded", ErrMaxStepsExceeded, ErrMaxStepsExceeded, true},
		{"ErrThreadNotFound", ErrThreadNotFound, ErrThreadNotFound, true},
		{"wrapped ErrInterrupt", errWrap{err: ErrInterrupt}, ErrInterrupt, true},
		{"different sentinels", ErrInterrupt, ErrMaxStepsExceeded, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := errors.Is(tt.err, tt.target)
			assert.Equal(t, tt.want, got)
		})
	}
}

type errWrap struct{ err error }

func (e errWrap) Error() string { return e.err.Error() }
func (e errWrap) Unwrap() error { return e.err }

func TestEvent_ZeroValue(t *testing.T) {
	var e Event[int]
	require.Empty(t, e.Type)
	require.Empty(t, e.NodeName)
	require.Zero(t, e.State)
	require.NoError(t, e.Err)
}

func TestNode_Call(t *testing.T) {
	called := false
	n := Node[string](func(_ context.Context, state string) (string, error) {
		called = true
		return state + "_updated", nil
	})
	ctx := context.Background()
	out, err := n(ctx, "hello")
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, "hello_updated", out)
}

func TestReducer_Identity(t *testing.T) {
	r := Reducer[int](func(current, update int) int {
		return current + update
	})
	assert.Equal(t, 10, r(3, 7))
}

func ExampleNewGraph() {
	ctx := context.Background()
	reducer := func(_, u string) string { return u }
	b := NewGraph[string](reducer)
	b.AddNode("greet", func(_ context.Context, s string) (string, error) { return "hello " + s, nil })
	b.AddNode("bye", func(_ context.Context, s string) (string, error) { return s + " bye", nil })
	b.AddEdge("greet", "bye")
	b.SetEntryPoint("greet")
	b.SetFinishPoint("bye")
	graph, err := b.Compile()
	if err != nil {
		return
	}
	out, _ := graph.Invoke(ctx, "world")
	fmt.Println(out)
	// Output: hello world bye
}

func ExampleGraph_Stream() {
	ctx := context.Background()
	reducer := func(_, u string) string { return u }
	b := NewGraph[string](reducer)
	b.AddNode("a", func(_ context.Context, s string) (string, error) { return s + "a", nil })
	b.AddNode("b", func(_ context.Context, s string) (string, error) { return s + "b", nil })
	b.AddEdge("a", "b")
	b.SetEntryPoint("a")
	b.SetFinishPoint("b")
	graph, err := b.Compile()
	if err != nil {
		return
	}
	ch := graph.Stream(ctx, ".")
	for e := range ch {
		if e.Type == EventNodeEnd {
			fmt.Println(e.NodeName, e.State)
		}
	}
	// Output:
	// a .a
	// b .ab
}
