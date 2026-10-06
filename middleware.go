package flowy

import (
	"context"
	"fmt"
	"slices"
)

func wrapNodeWithMiddlewares[T, E any](node Node[T, E], middlewares []NodeMiddleware[T, E]) Node[T, E] {
	if len(middlewares) == 0 {
		return node
	}
	wrapped := node
	for _, mw := range slices.Backward(middlewares) {
		if mw == nil {
			continue
		}
		wrapped = mw(wrapped)
	}
	return wrapped
}

// RecoverMiddleware catches panic inside its node/middleware chain, preserving
// input state and wrapping error panic causes. Register it before middleware it
// should cover. It does not wrap the surrounding runner's routing, reducers or
// persistence. Synchronous callbacks invoked by the node are within its call chain;
// shared mutable input and already-dispatched effects are not rolled back.
func RecoverMiddleware[T, E any]() NodeMiddleware[T, E] {
	return func(next Node[T, E]) Node[T, E] {
		return func(ctx context.Context, state T) (out T, directive Directive, err error) { //nolint:nonamedreturns // defer writes return slots
			defer func() {
				if recovered := recover(); recovered != nil {
					out = state
					if cause, ok := recovered.(error); ok {
						err = fmt.Errorf("flowy: recovered panic: %w", cause)
					} else {
						err = fmt.Errorf("flowy: recovered panic: %v", recovered)
					}
					directive = Fail(err.Error())
				}
			}()
			return next(ctx, state)
		}
	}
}
