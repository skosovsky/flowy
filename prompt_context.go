package flowy

// PromptRenderContext carries typed prompt input through the graph without
// committing to rendered messages before the final network node.
type PromptRenderContext[T any] struct {
	PromptID string
	Input    T
}
