package flowy

// NoEffect is the default effect type for graphs without structured side effects.
type NoEffect struct{}

// EffectMarker is implemented by typed effect unions for stream consumers.
type EffectMarker interface {
	effectMarker()
}

// WithEffects wraps a base directive with effects in execution order.
func WithEffects[E any](base Directive, effects []E) Directive {
	for _, effect := range effects {
		base = Effect(base, effect)
	}
	return base
}
