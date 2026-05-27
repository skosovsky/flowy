package flowy_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCookbookExamplesRun(t *testing.T) {
	root := findModuleRoot(t)
	examples := []string{
		"react_agent",
		"streaming_agent",
		"hitl_agent",
		"middleware_agent",
		"multi_agent",
		"context_deadline",
		"conditional_routing",
		"subgraph_agent",
	}
	for _, name := range examples {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(root, "examples", name)
			cmd := exec.Command("go", "run", "main.go")
			cmd.Dir = dir
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("go run failed: %v\n%s", err, out)
			}
			if len(strings.TrimSpace(string(out))) == 0 {
				t.Fatalf("expected non-empty output from %s", name)
			}
		})
	}
}

func findModuleRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
