package patterns

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/skosovsky/flowy"
)

func TestTask23PatternsPreserveEffects(t *testing.T) {
	for _, name := range []string{"react", "supervisor", "optimizer"} {
		t.Run(name, func(t *testing.T) {
			// Arrange.
			node := func(label string) flowy.Node[int, string] {
				return func(_ context.Context, s int) (int, flowy.Directive, error) {
					s++
					d := flowy.Effect(
						flowy.Effect(flowy.Completed(), fmt.Sprintf("%s:%d:a", label, s)),
						fmt.Sprintf("%s:%d:b", label, s),
					)
					return s, d, nil
				}
			}
			var b *flowy.GraphBuilder[int, string]
			var buildErr error
			var want []string
			switch name {
			case "react":
				b, buildErr = BuildReAct(node("reason"), node("action"), func(s int) bool { return s < 3 }, 2)
				want = []string{"reason:1:a", "reason:1:b", "action:2:a", "action:2:b", "reason:3:a", "reason:3:b"}
			case "supervisor":
				b, buildErr = BuildDispatchGraph(
					node("supervisor"),
					map[string]flowy.Node[int, string]{"worker": node("worker")},
					func(int) string { return "route" },
					RouteMap{"route": "worker"},
				)
				want = []string{"supervisor:1:a", "supervisor:1:b", "worker:2:a", "worker:2:b"}
			case "optimizer":
				b, buildErr = BuildEvaluatorOptimizer(
					node("generate"),
					node("evaluate"),
					func(s int) bool { return s >= 4 },
					2,
				)
				want = []string{
					"generate:1:a",
					"generate:1:b",
					"evaluate:2:a",
					"evaluate:2:b",
					"generate:3:a",
					"generate:3:b",
					"evaluate:4:a",
					"evaluate:4:b",
				}
			}
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			graph, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			// Act.
			result, err := graph.NewRunner(memCP[int, string]{}).Start(context.Background(), name, 0)
			// Assert.
			if err != nil || !reflect.DeepEqual(result.Effects, want) {
				t.Fatalf("effects=%v want=%v err=%v", result.Effects, want, err)
			}
		})
	}
}

func TestTask23PatternsPreserveTerminalWrappers(t *testing.T) {
	for _, base := range []flowy.Directive{flowy.End(), flowy.Suspend("pause"), flowy.Handoff("handoff"), flowy.Fail("failed"), flowy.Retry(1)} {
		t.Run(base.Type(), func(t *testing.T) {
			// Arrange.
			reason := func(_ context.Context, s int) (int, flowy.Directive, error) {
				return s, flowy.Effect(flowy.Effect(base, "a"), "b"), nil
			}
			b, buildErr := BuildReAct[int, string](reason, reason, func(int) bool { return false }, 2)
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			// Act. Check wrapper before runtime terminal routing or retry policy.
			graph, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			result, _ := graph.NewRunner(memCP[int, string]{}).Start(context.Background(), base.Type(), 0)
			// Assert.
			if result == nil || !reflect.DeepEqual(result.Effects, []string{"a", "b"}) {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}
