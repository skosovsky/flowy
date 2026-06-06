package flowy

import "context"

type bindingsKey struct{}

// BindingKey identifies a typed dependency slot in RunBindings.
// Use package-level sentinels (var DBKey BindingKey[*sql.DB]). One zero-value key per type T.
type BindingKey[T any] struct{}

// RunBindings holds ephemeral runtime dependencies that are never persisted in snapshots.
type RunBindings struct {
	m map[any]any
}

// NewRunBindings creates an empty binding container.
func NewRunBindings() *RunBindings {
	return &RunBindings{m: make(map[any]any)}
}

// Bind stores a typed dependency under key.
func Bind[T any](b *RunBindings, key BindingKey[T], val T) {
	if b == nil {
		return
	}
	if b.m == nil {
		b.m = make(map[any]any)
	}
	b.m[key] = val
}

// Extract returns a typed dependency bound under key.
func Extract[T any](b *RunBindings, key BindingKey[T]) (T, bool) {
	var zero T
	if b == nil || b.m == nil {
		return zero, false
	}
	raw, ok := b.m[key]
	if !ok {
		return zero, false
	}
	typed, ok := raw.(T)
	return typed, ok
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
func BindingFromContext[T any](ctx context.Context, key BindingKey[T]) (T, bool) {
	bindings, ok := BindingsFromContext(ctx)
	if !ok {
		var zero T
		return zero, false
	}
	return Extract(bindings, key)
}
