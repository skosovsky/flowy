package flowy_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestExamplesSmoke(t *testing.T) {
	t.Parallel()

	dirs := []string{
		"react_agent",
		"streaming_agent",
		"stream_request_stop",
		"hitl_agent",
		"middleware_agent",
		"multi_agent",
		"context_deadline",
		"conditional_routing",
		"subgraph_agent",
		"subgraph_slot_agent",
		"lease_agent",
		"bindings_agent",
		"semantic_cache_agent",
		"late_prompt_agent",
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	examplesRoot := filepath.Join(root, "examples")

	for _, dir := range dirs {
		t.Run(dir, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command("go", "run", ".")
			cmd.Dir = filepath.Join(examplesRoot, dir)
			out, runErr := cmd.CombinedOutput()
			if runErr != nil {
				t.Fatalf("go run %s failed: %v\n%s", dir, runErr, string(out))
			}
			if strings.TrimSpace(string(out)) == "" {
				t.Fatalf("go run %s produced no output", dir)
			}
		})
	}
}
