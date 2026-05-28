package flowy

// NoEffect is the default effect type for graphs without structured side effects.
type NoEffect struct{}

// EffectMarker is implemented by typed effect unions for stream consumers.
type EffectMarker interface {
	effectMarker()
}
