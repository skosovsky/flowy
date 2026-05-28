package flowy

import "context"

type bindingsKey struct{}

// RunBindings holds ephemeral runtime dependencies that are never persisted in snapshots.
type RunBindings struct {
	values map[string]any
}

// NewRunBindings creates an empty binding container.
func NewRunBindings() *RunBindings {
	return &RunBindings{values: make(map[string]any)}
}

// Set stores a binding by name.
func (b *RunBindings) Set(name string, value any) {
	if b.values == nil {
		b.values = make(map[string]any)
	}
	b.values[name] = value
}

// Get returns a binding value.
func (b *RunBindings) Get(name string) (any, bool) {
	if b == nil || b.values == nil {
		return nil, false
	}
	v, ok := b.values[name]
	return v, ok
}

// WithContext attaches bindings to ctx for node handlers.
func (b *RunBindings) WithContext(ctx context.Context) context.Context {
	if b == nil {
		return ctx
	}
	return context.WithValue(ctx, bindingsKey{}, b)
}

// BindingsFromContext returns bindings attached to ctx.
func BindingsFromContext(ctx context.Context) (*RunBindings, bool) {
	b, ok := ctx.Value(bindingsKey{}).(*RunBindings)
	return b, ok
}

// BindingFromContext loads a typed binding from ctx.
func BindingFromContext[T any](ctx context.Context, name string) (T, bool) {
	var zero T
	bindings, ok := BindingsFromContext(ctx)
	if !ok {
		return zero, false
	}
	raw, ok := bindings.Get(name)
	if !ok {
		return zero, false
	}
	typed, ok := raw.(T)
	return typed, ok
}
