//go:build integration

package scripts_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type releaseFixture struct{ repo, remote, source, initialRefs string }

func newReleaseFixture(t *testing.T) releaseFixture {
	t.Helper()
	base := t.TempDir()
	repo := filepath.Join(base, "caller")
	remote := filepath.Join(base, "remote.git")
	command(t, base, "git", "init", "--bare", "--quiet", remote)
	command(t, base, "git", "init", "--quiet", "-b", "main", repo)
	for _, pair := range [][2]string{{"user.name", "Fixture"}, {"user.email", "fixture@example.invalid"}, {"commit.gpgsign", "false"}, {"tag.gpgsign", "false"}} {
		command(t, repo, "git", "config", pair[0], pair[1])
	}
	command(t, repo, "git", "remote", "add", "origin", remote)
	write(t, filepath.Join(repo, "scripts/release.sh"), read(t, filepath.Join(repoRoot(t), "scripts/release.sh")))
	write(t, filepath.Join(repo, "go.mod"), []byte("module example.invalid/fixture\n\ngo 1.27.2\n"))
	write(t, filepath.Join(repo, "fixture.go"), []byte("package fixture\n"))
	write(
		t,
		filepath.Join(repo, "packages/test/go.mod"),
		[]byte(
			"module example.invalid/fixture/packages/test\n\ngo 1.27.2\n\nrequire example.invalid/fixture v0.0.0\n\nreplace example.invalid/fixture => ../..\n",
		),
	)
	write(
		t,
		filepath.Join(repo, "packages/test/adapter.go"),
		[]byte("package adapter\nimport _ \"example.invalid/fixture\"\n"),
	)
	write(t, filepath.Join(repo, "Makefile"), []byte(`lint test test-integration test-e2e:
	@printf '%s\n' '$@' >> .gate-order
	@test ! -f reject-check
modules:
	@find . -type d \( -name '.*' ! -name '.' -o -name vendor \) -prune -o -name go.mod -exec dirname {} \; | sed 's|^./||' | sort
`))
	command(t, repo, "git", "add", ".")
	command(t, repo, "git", "commit", "--quiet", "-m", "fixture")
	command(t, repo, "git", "push", "origin", "main")
	initialRefs := command(t, repo, "git", "ls-remote", "--refs", remote)
	write(t, filepath.Join(repo, "source.txt"), []byte("unpublished source\n"))
	command(t, repo, "git", "add", "source.txt")
	command(t, repo, "git", "commit", "--quiet", "-m", "source")
	source := command(t, repo, "git", "rev-parse", "HEAD")
	return releaseFixture{repo: repo, remote: remote, source: source, initialRefs: initialRefs}
}

func (f releaseFixture) invoke(t *testing.T, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/bash", append([]string{"scripts/release.sh"}, args...)...)
	cmd.Dir = f.repo
	cmd.Env = append(os.Environ(), "GOWORK=off", "GONOSUMDB=example.invalid/*")
	cmd.Stdin = strings.NewReader("y\n")
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestIntegrationReleasePreservesCallerAndPublishesExactRefs(t *testing.T) {
	// Arrange: unrelated local tag and private untracked contents.
	f := newReleaseFixture(t)
	command(t, f.repo, "git", "tag", "unrelated")
	write(t, filepath.Join(f.repo, "private.txt"), []byte("private"))
	before := command(t, f.repo, "git", "status", "--porcelain")
	// Act.
	out, err := f.invoke(t, "patch", f.source)
	// Assert.
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if got := command(t, f.repo, "git", "rev-parse", "HEAD"); got != f.source {
		t.Fatal("caller HEAD changed")
	}
	if got := command(t, f.repo, "git", "status", "--porcelain"); got != before {
		t.Fatal("caller contents changed")
	}
	refs := command(t, f.repo, "git", "ls-remote", "--refs", f.remote)
	if !strings.Contains(refs, "refs/tags/v0.0.1") || strings.Contains(refs, "unrelated") {
		t.Fatalf("wrong refs %s", refs)
	}
	for _, op := range []string{"inspect", "resume", "finish"} {
		if out, err := f.invoke(t, op); err != nil {
			t.Fatalf("%s: %v\n%s", op, err, out)
		}
	}
}

func TestIntegrationReleaseRejectsDirtySourceAndFailedGate(t *testing.T) {
	for _, mode := range []string{"dirty", "staged", "gate"} {
		t.Run(mode, func(t *testing.T) {
			// Arrange.
			f := newReleaseFixture(t)
			if mode == "gate" {
				write(t, filepath.Join(f.repo, "reject-check"), []byte("fail"))
				command(t, f.repo, "git", "add", "reject-check")
				command(t, f.repo, "git", "commit", "--quiet", "-m", "reject gate")
				f.source = command(t, f.repo, "git", "rev-parse", "HEAD")
			} else {
				write(t, filepath.Join(f.repo, "fixture.go"), []byte("package changed\n"))
				if mode == "staged" {
					command(t, f.repo, "git", "add", "fixture.go")
				}
			}
			// Act.
			out, err := f.invoke(t, "patch", f.source)
			// Assert.
			if err == nil {
				t.Fatalf("unexpected success: %s", out)
			}
			if refs := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); refs != f.initialRefs {
				t.Fatalf("gate published %s", refs)
			}
		})
	}
}

func TestIntegrationReleaseRecoveryAfterRejectedPush(t *testing.T) {
	// Arrange: actual remote rejecting hook.
	f := newReleaseFixture(t)
	hook := filepath.Join(f.remote, "hooks/pre-receive")
	write(t, hook, []byte("#!/bin/sh\nexit 1\n"))
	if err := os.Chmod(hook, 0700); err != nil {
		t.Fatal(err)
	}
	// Act: failure keeps immutable candidate; subsequent retry publishes that same object.
	out, err := f.invoke(t, "patch", f.source)
	if err == nil {
		t.Fatalf("rejected push succeeded: %s", out)
	}
	if got := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); got != f.initialRefs {
		t.Fatal("rejected atomic push changed remote refs")
	}
	record := filepath.Join(f.repo, ".git/library-releases/active")
	candidate := string(read(t, filepath.Join(record, "candidate")))
	if err = os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	if out, err = f.invoke(t, "resume"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	// Assert.
	if string(read(t, filepath.Join(record, "candidate"))) != candidate {
		t.Fatal("retry replaced candidate")
	}
	if !strings.Contains(
		command(t, f.repo, "git", "ls-remote", "--refs", f.remote),
		strings.TrimSpace(candidate),
	) {
		t.Fatal("wrong published object")
	}
}

func TestIntegrationReleaseRejectsChangedCandidateAndUnknownOutcome(t *testing.T) {
	for _, mode := range []string{"candidate", "unknown", "destination", "collision"} {
		t.Run(mode, func(t *testing.T) {
			// Arrange: retain a candidate after rejecting publication.
			f := newReleaseFixture(t)
			hook := filepath.Join(f.remote, "hooks/pre-receive")
			write(t, hook, []byte("#!/bin/sh\nexit 1\n"))
			if err := os.Chmod(hook, 0700); err != nil {
				t.Fatal(err)
			}
			if out, err := f.invoke(t, "patch", f.source); err == nil {
				t.Fatalf("expected rejection: %s", out)
			}
			record := filepath.Join(f.repo, ".git/library-releases/active")
			switch mode {
			case "candidate":
				write(t, filepath.Join(record, "checkout/fixture.go"), []byte("package changed\n"))
			case "unknown":
				write(t, filepath.Join(record, "status"), []byte("unknown\n"))
			case "destination":
				command(t, f.repo, "git", "remote", "set-url", "origin", f.remote+"-other")
			case "collision":
				write(t, filepath.Join(record, "refs"), []byte("refs/tags/v0.0.1 "+strings.Repeat("0", 40)+"\n"))
			}
			// Act / Assert.
			if out, err := f.invoke(t, "resume"); err == nil {
				t.Fatalf("unsafe resume: %s", out)
			}
		})
	}
}

func TestIntegrationReleaseChecksSelectedSource(t *testing.T) {
	// Arrange: current HEAD differs from the explicitly selected, failing source.
	f := newReleaseFixture(t)
	write(t, filepath.Join(f.repo, "reject-check"), []byte("failure"))
	command(t, f.repo, "git", "add", "reject-check")
	command(t, f.repo, "git", "commit", "--quiet", "-m", "failing source")
	selected := command(t, f.repo, "git", "rev-parse", "HEAD")
	command(t, f.repo, "git", "rm", "reject-check")
	command(t, f.repo, "git", "commit", "--quiet", "-m", "passing head")
	// Act.
	out, err := f.invoke(t, "patch", selected)
	// Assert: checking only the current passing HEAD would incorrectly publish.
	if err == nil {
		t.Fatalf("unverified source published: %s", out)
	}
	if refs := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); refs != f.initialRefs {
		t.Fatal(refs)
	}
}

func TestIntegrationReleaseRequiresAtomicPush(t *testing.T) {
	// Arrange: server does not advertise the required atomic capability.
	f := newReleaseFixture(t)
	command(t, f.repo, "git", "--git-dir="+f.remote, "config", "receive.advertiseAtomic", "false")
	// Act.
	out, err := f.invoke(t, "patch", f.source)
	// Assert: no silent fallback to non-atomic publication.
	if err == nil {
		t.Fatalf("non-atomic release succeeded: %s", out)
	}
	if refs := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); refs != f.initialRefs {
		t.Fatal(refs)
	}
}

func TestIntegrationReleaseUnknownRequiresSuccessfulInspection(t *testing.T) {
	// Arrange: prepared candidate after server rejection.
	f := newReleaseFixture(t)
	hook := filepath.Join(f.remote, "hooks/pre-receive")
	write(t, hook, []byte("#!/bin/sh\nexit 1\n"))
	if err := os.Chmod(hook, 0700); err != nil {
		t.Fatal(err)
	}
	if out, err := f.invoke(t, "patch", f.source); err == nil {
		t.Fatal(out)
	}
	moved := f.remote + "-offline"
	if err := os.Rename(f.remote, moved); err != nil {
		t.Fatal(err)
	}
	// Act: unavailable remote observation records unknown.
	if out, err := f.invoke(t, "inspect"); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	record := filepath.Join(f.repo, ".git/library-releases/active")
	if strings.TrimSpace(string(read(t, filepath.Join(record, "status")))) != "unknown" {
		t.Fatal("unknown not retained")
	}
	if err := os.Rename(moved, f.remote); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	// Assert: resume remains blocked until inspect resolves uncertainty.
	if out, err := f.invoke(t, "resume"); err == nil {
		t.Fatalf("unknown resume succeeded: %s", out)
	}
	if out, err := f.invoke(t, "inspect"); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if out, err := f.invoke(t, "resume"); err != nil {
		t.Fatalf("%v %s", err, out)
	}
}

func TestIntegrationReleaseRetainsInterruptedCandidate(t *testing.T) {
	// Arrange: emulate interruption after candidate/ref creation, before phase bookkeeping.
	f := newReleaseFixture(t)
	hook := filepath.Join(f.remote, "hooks/pre-receive")
	write(t, hook, []byte("#!/bin/sh\nexit 1\n"))
	if err := os.Chmod(hook, 0700); err != nil {
		t.Fatal(err)
	}
	if out, err := f.invoke(t, "patch", f.source); err == nil {
		t.Fatal(out)
	}
	record := filepath.Join(f.repo, ".git/library-releases/active")
	candidate := string(read(t, filepath.Join(record, "candidate")))
	write(t, filepath.Join(record, "phase"), []byte("preparing\n"))
	if err := os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	// Act.
	out, err := f.invoke(t, "resume")
	// Assert.
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if string(read(t, filepath.Join(record, "candidate"))) != candidate {
		t.Fatal("candidate replaced after interruption")
	}
}

func TestIntegrationReleaseRejectsExistingTags(t *testing.T) {
	for _, location := range []string{"local", "remote"} {
		t.Run(location, func(t *testing.T) {
			// Arrange: an annotated tag already reserves the next adapter version.
			f := newReleaseFixture(t)
			tag := "packages/test/v0.0.1"
			command(t, f.repo, "git", "tag", "-a", tag, "-m", "reserved")
			if location == "remote" {
				command(t, f.repo, "git", "push", "origin", "refs/tags/"+tag)
				command(t, f.repo, "git", "tag", "-d", tag)
			}
			before := command(t, f.repo, "git", "ls-remote", "--refs", f.remote)
			// Act.
			out, err := f.invoke(t, "patch", f.source)
			// Assert: no overwritten tag and no partial root publication.
			if err == nil || !strings.Contains(out, "tag collision") {
				t.Fatalf("collision accepted: %v %s", err, out)
			}
			if got := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); got != before {
				t.Fatal("remote changed after collision")
			}
		})
	}
}

func TestIntegrationReleaseObservesSuccessfulPushDespiteTransportError(t *testing.T) {
	// Arrange: transport reports failure after the real atomic push has completed.
	f := newReleaseFixture(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	shim := filepath.Join(bin, "git")
	write(t, shim, []byte(`#!/bin/bash
case " $* " in
*' push --atomic '*) "$REAL_GIT" "$@"; exit 75 ;;
*) exec "$REAL_GIT" "$@" ;;
esac
`))
	if err = os.Chmod(shim, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REAL_GIT", realGit)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	// Act.
	out, err := f.invoke(t, "patch", f.source)
	// Assert: exact remote identities resolve ambiguity, without a second candidate.
	if err != nil || !strings.Contains(out, "Published and verified") {
		t.Fatalf("publication was not observed: %v %s", err, out)
	}
}

func TestIntegrationReleaseBreakVersionPolicy(t *testing.T) {
	for _, baseline := range []string{"v0.8.1", "v1.2.3"} {
		t.Run(baseline, func(t *testing.T) {
			// Arrange: version selection must use the destination's root tags.
			f := newReleaseFixture(t)
			command(t, f.repo, "git", "tag", baseline)
			command(t, f.repo, "git", "push", "origin", "refs/tags/"+baseline)
			before := command(t, f.repo, "git", "ls-remote", "--refs", f.remote)
			// Act.
			out, err := f.invoke(t, "break", f.source)
			// Assert.
			refs := command(t, f.repo, "git", "ls-remote", "--refs", f.remote)
			if baseline == "v1.2.3" {
				if err == nil || !strings.Contains(out, "import-version migration") || refs != before {
					t.Fatalf("unprepared major migration: %v %s", err, out)
				}
			} else if err != nil || !strings.Contains(refs, "refs/tags/packages/test/v0.9.0") {
				t.Fatalf("incorrect v0 break: %v %s", err, out)
			}
		})
	}
}

func TestIntegrationReleasePublishesSourceAndPreparedManifests(t *testing.T) {
	// Arrange: release an older ancestor while the caller remains ahead.
	f := newReleaseFixture(t)
	write(t, filepath.Join(f.repo, "later.txt"), []byte("later\n"))
	command(t, f.repo, "git", "add", "later.txt")
	command(t, f.repo, "git", "commit", "--quiet", "-m", "later")
	head := command(t, f.repo, "git", "rev-parse", "HEAD")
	// Act.
	out, err := f.invoke(t, "patch", f.source)
	// Assert.
	if err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if got := command(t, f.repo, "git", "--git-dir="+f.remote, "rev-parse", "main"); got != f.source {
		t.Fatalf("main = %s, want source %s", got, f.source)
	}
	if got := command(t, f.repo, "git", "rev-parse", "HEAD"); got != head {
		t.Fatal("local main moved")
	}
	gateOrder := string(read(t, filepath.Join(f.repo, ".git/library-releases/active/checkout/.gate-order")))
	if gateOrder != "lint\ntest\ntest-integration\ntest-e2e\n" {
		t.Fatalf("source gate order: %q", gateOrder)
	}
	candidate := strings.TrimSpace(string(read(t, filepath.Join(f.repo, ".git/library-releases/active/candidate"))))
	for _, tag := range []string{"v0.0.1", "packages/test/v0.0.1"} {
		if got := command(t, f.repo, "git", "--git-dir="+f.remote, "rev-parse", tag); got != candidate {
			t.Fatalf("%s points to %s, want %s", tag, got, candidate)
		}
	}
	changed := command(t, f.repo, "git", "--git-dir="+f.remote, "diff", "--name-only", "main", "v0.0.1")
	if changed != "packages/test/go.mod" {
		t.Fatalf("release must not manufacture library checksums: %s", changed)
	}
	sourceMod := command(t, f.repo, "git", "--git-dir="+f.remote, "show", "main:packages/test/go.mod")
	candidateMod := command(t, f.repo, "git", "--git-dir="+f.remote, "show", "v0.0.1:packages/test/go.mod")
	if !strings.Contains(sourceMod, "replace") || strings.Contains(candidateMod, "replace") ||
		!strings.Contains(candidateMod, "v0.0.1") {
		t.Fatalf("source/candidate manifests: %s / %s", sourceMod, candidateMod)
	}
}

func TestIntegrationReleaseRejectsInvalidMain(t *testing.T) {
	for _, mode := range []string{"detached", "feature", "foreign-source", "diverged", "missing-remote-main", "legacy-record", "previous-format"} {
		t.Run(mode, func(t *testing.T) {
			// Arrange.
			f := newReleaseFixture(t)
			switch mode {
			case "detached":
				command(t, f.repo, "git", "checkout", "--detach")
			case "feature":
				command(t, f.repo, "git", "checkout", "-b", "feature")
			case "foreign-source":
				command(t, f.repo, "git", "checkout", "-b", "feature")
				command(t, f.repo, "git", "commit", "--allow-empty", "-m", "foreign")
				f.source = command(t, f.repo, "git", "rev-parse", "HEAD")
				command(t, f.repo, "git", "checkout", "main")
			case "diverged":
				command(t, f.repo, "git", "checkout", "-b", "remote-change", "HEAD^")
				command(t, f.repo, "git", "commit", "--allow-empty", "-m", "remote")
				command(t, f.repo, "git", "push", "origin", "HEAD:main")
				command(t, f.repo, "git", "checkout", "main")
			case "missing-remote-main":
				command(t, f.repo, "git", "--git-dir="+f.remote, "update-ref", "-d", "refs/heads/main")
			case "previous-format":
				write(t, filepath.Join(f.repo, ".git/library-releases/active/format"), []byte("2\n"))
			case "legacy-record":
				if err := os.MkdirAll(filepath.Join(f.repo, ".git/legacy-releases/shell-active"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			before := command(t, f.repo, "git", "ls-remote", "--refs", f.remote)
			// Act.
			out, err := f.invoke(t, "patch", f.source)
			// Assert.
			if err == nil {
				t.Fatalf("unsafe release: %s", out)
			}
			if got := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); got != before {
				t.Fatal("remote changed")
			}
		})
	}
}

func TestIntegrationReleaseRecoveryAfterMainAdvances(t *testing.T) {
	// Arrange: complete a publication, then move remote main forward.
	f := newReleaseFixture(t)
	if out, err := f.invoke(t, "patch", f.source); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	record := filepath.Join(f.repo, ".git/library-releases/active")
	candidate := string(read(t, filepath.Join(record, "candidate")))
	command(t, f.repo, "git", "commit", "--allow-empty", "-m", "next work")
	command(t, f.repo, "git", "push", "origin", "main")
	before := command(t, f.repo, "git", "ls-remote", "--refs", f.remote)
	// Act.
	out, err := f.invoke(t, "resume")
	// Assert: ancestry proves source delivery; recovery must never rewind main.
	if err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if got := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); got != before {
		t.Fatal("recovery changed refs")
	}
	if got := string(read(t, filepath.Join(record, "candidate"))); got != candidate {
		t.Fatal("candidate changed")
	}
}

func TestIntegrationReleaseRejectsMainAdvancingDuringPreparation(t *testing.T) {
	// Arrange: a prepared candidate was retained after rejected push.
	f := newReleaseFixture(t)
	hook := filepath.Join(f.remote, "hooks/pre-receive")
	write(t, hook, []byte("#!/bin/sh\nexit 1\n"))
	if err := os.Chmod(hook, 0700); err != nil {
		t.Fatal(err)
	}
	if out, err := f.invoke(t, "patch", f.source); err == nil {
		t.Fatal(out)
	}
	if err := os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	command(t, f.repo, "git", "commit", "--allow-empty", "-m", "newer main")
	command(t, f.repo, "git", "push", "origin", "main")
	before := command(t, f.repo, "git", "ls-remote", "--refs", f.remote)
	// Act.
	out, err := f.invoke(t, "resume")
	// Assert: unpublished tags cannot cause the branch to rewind to the older source.
	if err == nil || !strings.Contains(out, "fast-forward") {
		t.Fatalf("%v %s", err, out)
	}
	if got := command(t, f.repo, "git", "ls-remote", "--refs", f.remote); got != before {
		t.Fatal("remote changed")
	}
}

func TestIntegrationReleaseRecoversMissingRemoteMainBeforePreparation(t *testing.T) {
	// Arrange: initial remote prerequisite fails before any candidate is prepared.
	f := newReleaseFixture(t)
	baseline := strings.Fields(f.initialRefs)[0]
	command(t, f.repo, "git", "--git-dir="+f.remote, "update-ref", "-d", "refs/heads/main")
	if out, err := f.invoke(t, "patch", f.source); err == nil {
		t.Fatal(out)
	}
	command(t, f.repo, "git", "--git-dir="+f.remote, "update-ref", "refs/heads/main", baseline)
	// Act.
	out, err := f.invoke(t, "resume")
	// Assert: initialized inventory permits safe preparation after the prerequisite is restored.
	if err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if got := command(t, f.repo, "git", "--git-dir="+f.remote, "rev-parse", "main"); got != f.source {
		t.Fatal("source was not published")
	}
}
