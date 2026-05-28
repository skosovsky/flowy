package flowy

import (
	"context"
	"fmt"
)

func wrapNodeWithMiddlewares[T, E any](node Node[T, E], middlewares []NodeMiddleware[T, E]) Node[T, E] {
	if len(middlewares) == 0 {
		return node
	}
	wrapped := node
	for i := len(middlewares) - 1; i >= 0; i-- {
		mw := middlewares[i]
		if mw == nil {
			continue
		}
		wrapped = mw(wrapped)
	}
	return wrapped
}

// RecoverMiddleware catches panic in node/middleware chain and converts it to error.
func RecoverMiddleware[T, E any]() NodeMiddleware[T, E] {
	return func(next Node[T, E]) Node[T, E] {
		return func(ctx context.Context, state T) (T, Directive, error) {
			var (
				out       T
				directive Directive
				err       error
			)
			defer func() {
				if recovered := recover(); recovered != nil {
					out = state
					directive = End()
					err = fmt.Errorf("flowy: recovered panic: %v", recovered)
				}
			}()
			out, directive, err = next(ctx, state)
			return out, directive, err
		}
	}
}
