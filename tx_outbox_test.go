package flowy

import (
	"context"
	"testing"
)

func TestOutboxTxContext(t *testing.T) {
	t.Parallel()

	tx := struct{ id int }{id: 1}
	ctx := ContextWithOutboxTx(context.Background(), tx)
	got, ok := OutboxTxFromContext(ctx)
	if !ok {
		t.Fatal("expected tx in context")
	}
	if got.(struct{ id int }).id != 1 {
		t.Fatalf("unexpected tx: %+v", got)
	}
	_, ok = OutboxTxFromContext(context.Background())
	if ok {
		t.Fatal("expected no tx in bare context")
	}
}
