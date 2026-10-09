//go:build integration

package scripts_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestIntegrationModuleDiscovery(t *testing.T) {
	// Arrange: the real Makefile, with hidden/vendor modules that must not participate.
	dir := t.TempDir()
	write(t, filepath.Join(dir, "Makefile"), read(t, filepath.Join(repoRoot(t), "Makefile")))
	for _, module := range []string{".", "packages/one", "adapters/two", ".hidden/child", "vendor/dependency", "packages/.hidden/child", "packages/vendor/child"} {
		write(t, filepath.Join(dir, module, "go.mod"), []byte("module example.invalid/test\n"))
	}
	// Act.
	got := command(t, dir, "make", "--no-print-directory", "-s", "modules")
	// Assert.
	if got != ".\nadapters/two\npackages/one" {
		t.Fatalf("unexpected module inventory: %q", got)
	}
}

func TestIntegrationReleaseRejectsInvalidModulePath(t *testing.T) {
	// Arrange.
	f := newReleaseFixture(t)
	write(t, filepath.Join(f.repo, "packages/test/go.mod"), []byte("module example.invalid/wrong\n\ngo 1.27.2\n"))
	command(t, f.repo, "git", "add", ".")
	command(t, f.repo, "git", "commit", "-m", "incorrect module")
	// Act.
	output, err := f.invoke(t, "patch")
	// Assert.
	if err == nil || !strings.Contains(output, "unexpected module path") {
		t.Fatalf("invalid path accepted: %v %s", err, output)
	}
	if got := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); got != f.initialRefs {
		t.Fatal("remote changed")
	}
}

func TestIntegrationReleaseCancelledConfirmationRetainsCandidate(t *testing.T) {
	// Arrange.
	f := newReleaseFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/bash", "scripts/release.sh", "patch")
	cmd.Dir = f.repo
	cmd.Stdin = strings.NewReader("n\n")
	// Act.
	output, err := cmd.CombinedOutput()
	// Assert: no publication, and recovery uses the same prepared candidate.
	if err == nil || !strings.Contains(string(output), "candidate retained") {
		t.Fatalf("unexpected confirmation result: %v %s", err, output)
	}
	if got := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); got != f.initialRefs {
		t.Fatal("cancel published")
	}
	record := filepath.Join(f.repo, ".git/library-releases/active")
	candidate := string(read(t, filepath.Join(record, "candidate")))
	if output, err := f.invoke(t, "finish"); err == nil {
		t.Fatalf("unfinished release archived: %s", output)
	}
	for _, operation := range []string{"inspect", "resume"} {
		if output, err := f.invoke(t, operation); err != nil {
			t.Fatalf("%s: %v %s", operation, err, output)
		}
	}
	if got := string(read(t, filepath.Join(record, "candidate"))); got != candidate {
		t.Fatal("candidate replaced")
	}
}
