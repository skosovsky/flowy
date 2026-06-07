package flowy

import (
	"context"
	"errors"
	"testing"
)

func TestRunSessionFinishStoresCompletionErrorAfterReturn(t *testing.T) {
	t.Parallel()

	_, cancel := context.WithCancelCause(context.Background())
	session := newRunSession(cancel)

	want := errors.New("handoff save failed")
	session.finish(want)

	got := session.completionError()
	if got == nil {
		t.Fatal("expected completion error, got nil")
	}
	if !errors.Is(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestRunSessionFinishNilErrorCompletionErrorNil(t *testing.T) {
	t.Parallel()

	_, cancel := context.WithCancelCause(context.Background())
	session := newRunSession(cancel)

	session.finish(nil)

	if got := session.completionError(); got != nil {
		t.Fatalf("expected nil completion error, got %v", got)
	}
}
