package flowy

import "context"

type outboxTxContextKey struct{}

// ContextWithOutboxTx attaches the active SaveWithOutbox storage transaction to ctx.
// enqueueFn receives this context; storage-specific outbox adapters read it via OutboxTxFromContext.
func ContextWithOutboxTx(ctx context.Context, tx any) context.Context {
	if tx == nil {
		return ctx
	}
	return context.WithValue(ctx, outboxTxContextKey{}, tx)
}

// OutboxTxFromContext returns the transaction passed to ContextWithOutboxTx during SaveWithOutbox.
func OutboxTxFromContext(ctx context.Context) (any, bool) {
	tx := ctx.Value(outboxTxContextKey{})
	return tx, tx != nil
}
