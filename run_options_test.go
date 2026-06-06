package flowy

import (
	"context"
	"testing"
)

func TestBudgetUsedEmptyContext(t *testing.T) {
	t.Parallel()

	if got := BudgetUsed(context.Background(), "tokens"); got != 0 {
		t.Fatalf("expected 0 without run metadata, got %d", got)
	}
}

func TestBudgetUsedUnknownBudget(t *testing.T) {
	t.Parallel()

	ctx := ContextWithRunMetadata(context.Background(), RunMetadataInput{
		BudgetCounts: map[string]int{"tokens": 2},
	})
	if got := BudgetUsed(ctx, "missing"); got != 0 {
		t.Fatalf("expected 0 for unknown budget, got %d", got)
	}
}

func TestContextWithRunMetadataIsolatedExecution(t *testing.T) {
	t.Parallel()

	ctx := ContextWithRunMetadata(context.Background(), RunMetadataInput{
		BudgetCounts: map[string]int{"tokens": 2},
	})
	if got := BudgetUsed(ctx, "tokens"); got != 2 {
		t.Fatalf("expected seeded budget 2, got %d", got)
	}
	if err := UseBudget(ctx, "tokens", 1); err != nil {
		t.Fatalf("use budget: %v", err)
	}
	if got := BudgetUsed(ctx, "tokens"); got != 3 {
		t.Fatalf("expected budget 3 after use, got %d", got)
	}
}

func TestContextWithRunMetadataInputMutationIsolation(t *testing.T) {
	t.Parallel()

	input := RunMetadataInput{
		BudgetCounts: map[string]int{"tokens": 2},
	}
	ctx := ContextWithRunMetadata(context.Background(), input)
	input.BudgetCounts["tokens"] = 99

	if got := BudgetUsed(ctx, "tokens"); got != 2 {
		t.Fatalf("expected isolated copy 2, got %d after mutating input", got)
	}
}

func TestContextWithRunMetadataTelemetryIsolation(t *testing.T) {
	t.Parallel()

	input := RunMetadataInput{
		TelemetryContext: map[string]string{"trace_id": "abc"},
	}
	ctx := ContextWithRunMetadata(context.Background(), input)
	input.TelemetryContext["trace_id"] = "mutated"

	meta, ok := runMetadataFromContext(ctx)
	if !ok || meta == nil {
		t.Fatal("expected run metadata in context")
	}
	if meta.TelemetryContext["trace_id"] != "abc" {
		t.Fatalf("expected isolated telemetry copy, got %q", meta.TelemetryContext["trace_id"])
	}
}
