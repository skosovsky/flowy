"""AAA release regressions; all origins are disposable local bare repositories."""

import contextlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest
import zipfile
from unittest import mock

import release


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        signing = mock.patch.dict(os.environ, {"GIT_CONFIG_COUNT": "1",
                                             "GIT_CONFIG_KEY_0": "commit.gpgsign",
                                             "GIT_CONFIG_VALUE_0": "false"})
        signing.start()
        self.addCleanup(signing.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="flowy-release-test-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.source = self.base / "source"
        self.origin = self.base / "origin.git"
        self.source.mkdir()
        release.git(self.source, "init", "--quiet")
        release.git(self.source, "config", "user.name", "Release Fixture")
        release.git(self.source, "config", "user.email", "fixture@example.invalid")
        subprocess.run(["git", "init", "--quiet", "--bare", str(self.origin)], check=True)
        release.git(self.source, "remote", "add", "origin", str(self.origin))
        (self.source / "go.mod").write_text("module example.com/flowy\n\ngo 1.20\n")
        (self.source / "hello.go").write_text("package flowy\n\nfunc Hello() string { return \"hello\" }\n")
        (self.source / "adapter").mkdir()
        (self.source / "adapter/go.mod").write_text(
            "module example.com/flowy/adapter\n\ngo 1.20\n\nrequire example.com/flowy v0.0.0\n"
            "\nreplace example.com/flowy => ..\n")
        (self.source / "adapter/adapter.go").write_text(
            'package adapter\nimport "example.com/flowy"\nfunc Hello() string { return flowy.Hello() }\n')
        release.git(self.source, "add", ".")
        release.git(self.source, "commit", "--quiet", "-m", "fixture")
        self.original = self.snapshot()
        self.attempt = self.base / "attempt"

    def snapshot(self):
        return (release.git(self.source, "symbolic-ref", "HEAD"),
                release.git(self.source, "rev-parse", "HEAD"),
                release.git(self.source, "status", "--porcelain"),
                release.git(self.source, "show-ref"),
                (self.source / ".git/index").read_bytes(),
                (self.source / "go.mod").read_bytes(),
                (self.source / "adapter/go.mod").read_bytes())

    def prepare(self, kind="patch"):
        self.attempt.mkdir()
        with mock.patch("release.tempfile.mkdtemp", return_value=str(self.attempt)), \
                mock.patch("builtins.input", return_value="y"), contextlib.redirect_stdout(io.StringIO()):
            return release.prepare(self.source, kind, ". adapter")

    def test_exact_scope_and_source_unchanged(self):
        # Arrange: unrelated untracked secret and unrelated local tag.
        (self.source / "private-untracked.txt").write_text("fixture secret")
        release.git(self.source, "tag", "scratch-local")
        release.git(self.source, "tag", "v9.9.9")
        before = self.snapshot()
        # Act.
        attempt, manifest = self.prepare()
        status = release.publish(attempt, manifest)
        # Assert: local tags cannot drive version; only exact release refs/tree are published.
        self.assertEqual(status, 0)
        self.assertEqual(manifest["version"], "v0.0.1")
        self.assertEqual(set(release.remote_refs(str(self.origin))), set(manifest["refs"]))
        self.assertNotIn("private-untracked.txt", release.git(self.origin, "ls-tree", "-r", "--name-only", "v0.0.1"))
        self.assertEqual(self.snapshot(), before)
        self.assertEqual((self.source / "private-untracked.txt").read_text(), "fixture secret")
        changed = release.git(attempt / "repo", "diff", "--name-only", self.original[1], manifest["commit"])
        self.assertEqual(changed, "adapter/go.mod")
        self.assertNotIn("replace", (attempt / "repo/adapter/go.mod").read_text())
        self.assertIn("v0.0.1", (attempt / "repo/adapter/go.mod").read_text())

    def test_rejected_atomic_push_and_exact_retry(self):
        # Arrange: reject one module ref; atomic publication must reject every ref.
        hook = self.origin / "hooks/update"
        hook.write_text('#!/bin/sh\ncase "$1" in refs/tags/adapter/*) exit 1;; esac\n')
        hook.chmod(0o755)
        attempt, manifest = self.prepare()
        # Act.
        self.assertEqual(release.publish(attempt, manifest), 1)
        # Assert.
        self.assertEqual(release.publication(manifest), "none")
        self.assertEqual(self.snapshot(), self.original)
        self.assertEqual(release.resume(attempt), manifest)
        hook.unlink()
        self.assertEqual(release.publish(attempt, release.resume(attempt)), 0)
        self.assertEqual(release.publication(manifest), "complete")
        self.assertEqual(release.resume(attempt)["version"], "v0.0.1")
        self.assertEqual(release.publish(attempt, manifest), 0)  # Lost-ACK replay is read-only.

    def test_partial_publication_retries_only_recorded_operation(self):
        # Arrange: a remote has only the root tag from the recorded commit.
        attempt, manifest = self.prepare()
        release.git(attempt / "repo", "push", str(self.origin), manifest["refs"][0])
        self.assertEqual(release.publication(manifest), "partial")
        # Act.
        status = release.publish(attempt, release.resume(attempt))
        # Assert.
        self.assertEqual(status, 0)
        self.assertEqual(release.publication(manifest), "complete")
        self.assertEqual(self.snapshot(), self.original)

    def test_atomic_unsupported_has_no_fallback(self):
        release.git(self.origin, "config", "receive.advertiseAtomic", "false")
        attempt, manifest = self.prepare()
        self.assertEqual(release.publish(attempt, manifest), 1)
        self.assertEqual(release.publication(manifest), "none")
        self.assertEqual(release.resume(attempt), manifest)
        self.assertEqual(self.snapshot(), self.original)

    def test_lost_push_ack_queries_complete_publication(self):
        attempt, manifest = self.prepare()
        original_run = subprocess.run
        def lose_ack(command, **kwargs):
            result = original_run(command, **kwargs)
            if "push" in command:
                return subprocess.CompletedProcess(command, 1)
            return result
        with mock.patch("release.subprocess.run", side_effect=lose_ack):
            self.assertEqual(release.publish(attempt, manifest), 0)
        self.assertEqual(release.publication(manifest), "complete")
        self.assertEqual(self.snapshot(), self.original)

    def test_clean_consumers_install_prepared_modules_without_replaces(self):
        # Arrange: local module proxy populated from the prepared release artifacts.
        attempt, manifest = self.prepare()
        proxy = self.base / "proxy"
        for directory, module in ((".", "example.com/flowy"), ("adapter", "example.com/flowy/adapter")):
            artifacts = proxy / module / "@v"
            artifacts.mkdir(parents=True)
            version = manifest["version"]
            (artifacts / (version + ".mod")).write_bytes((attempt / "repo" / directory / "go.mod").read_bytes())
            (artifacts / (version + ".info")).write_text(json.dumps({"Version": version, "Time": "2026-10-06T00:00:00Z"}))
            (artifacts / "list").write_text(version + "\n")
            with zipfile.ZipFile(artifacts / (version + ".zip"), "w") as archive:
                for name in ("go.mod", "hello.go" if directory == "." else "adapter.go"):
                    archive.write(attempt / "repo" / directory / name, f"{module}@{version}/{name}")
        consumer = self.base / "consumer"
        consumer.mkdir()
        (consumer / "go.mod").write_text("module consumer\n\ngo 1.20\n")
        (consumer / "main.go").write_text('package main\nimport ("example.com/flowy"; "example.com/flowy/adapter")\nfunc main() { println(flowy.Hello(), adapter.Hello()) }\n')
        env = dict(os.environ, GOPROXY=proxy.as_uri(), GOSUMDB="off", GOWORK="off",
                   GOMODCACHE=str(self.base / "modcache"), GOCACHE=str(self.base / "buildcache"))
        # Act: fresh consumer resolves the two module versions entirely through the proxy.
        subprocess.run(["go", "get", "example.com/flowy@" + version,
                        "example.com/flowy/adapter@" + version], cwd=consumer, env=env, check=True)
        subprocess.run(["go", "build", "-mod=readonly", "."], cwd=consumer, env=env, check=True)
        # Assert: no workspace/local replace masked an unbuildable release.
        self.assertNotIn("replace", (consumer / "go.mod").read_text())
        self.assertEqual(self.snapshot(), self.original)

    def test_unknown_remote_retains_exact_attempt(self):
        # Arrange: destination becomes unreachable after preparation.
        attempt, manifest = self.prepare()
        hidden = self.base / "hidden.git"
        self.origin.rename(hidden)
        # Act.
        status = release.publish(attempt, manifest)
        # Assert: no speculative deletion or new version.
        self.assertEqual(status, 1)
        self.assertEqual(release.publication(manifest), "unknown")
        self.assertEqual(release.resume(attempt), manifest)
        self.assertEqual(self.snapshot(), self.original)
        hidden.rename(self.origin)
        self.assertEqual(release.publish(attempt, manifest), 0)

    def test_conflicting_remote_not_overwritten(self):
        attempt, manifest = self.prepare()
        release.git(self.source, "tag", "v0.0.1")
        release.git(self.source, "push", "origin", "refs/tags/v0.0.1")
        original_remote = release.remote_refs(str(self.origin))
        self.assertEqual(release.publication(manifest), "conflicting")
        self.assertEqual(release.publish(attempt, manifest), 1)
        self.assertEqual(release.remote_refs(str(self.origin)), original_remote)

    def test_failures_before_and_during_tagging_preserve_source(self):
        original_git = release.git
        for stage in ("edit", "tag"):
            with self.subTest(stage=stage):
                if self.attempt.exists():
                    shutil.rmtree(self.attempt)
                def fail_tag(directory, *args):
                    if args[0] == "update-ref":
                        raise subprocess.CalledProcessError(1, args)
                    return original_git(directory, *args)
                patcher = mock.patch("release.edit_modules", side_effect=ValueError("edit failed")) \
                    if stage == "edit" else mock.patch("release.git", side_effect=fail_tag)
                with patcher, self.assertRaises((ValueError, subprocess.CalledProcessError)):
                    self.prepare()
                self.assertEqual(self.snapshot(), self.original)
                self.assertEqual(release.remote_refs(str(self.origin)), {})
                self.assertFalse((self.attempt / "manifest.json").exists())

    def test_failure_after_tags_before_push_has_manifest(self):
        attempt, manifest = self.prepare()
        self.assertEqual(release.resume(attempt), manifest)
        self.assertEqual(release.remote_refs(str(self.origin)), {})
        self.assertEqual(self.snapshot(), self.original)

    def test_dirty_tracked_and_incomplete_modules_rejected_before_preparation(self):
        with self.assertRaises(ValueError):
            release.prepare(self.source, "patch", ".")
        (self.source / "hello.go").write_text("dirty")
        with self.assertRaises(ValueError):
            release.prepare(self.source, "patch", ". adapter")
        self.assertFalse(self.attempt.exists())
        self.assertEqual(release.remote_refs(str(self.origin)), {})

    def test_v2_rejected_before_preparation(self):
        release.git(self.source, "tag", "v1.2.3")
        release.git(self.source, "push", "origin", "refs/tags/v1.2.3")
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, "semantic import"):
            self.prepare("break")
        self.assertEqual(self.snapshot(), before)
        self.assertFalse((self.attempt / "manifest.json").exists())

    def test_changed_prepared_clone_rejected(self):
        attempt, _ = self.prepare()
        (attempt / "repo/hello.go").write_text("changed")
        with self.assertRaisesRegex(ValueError, "clone changed"):
            release.resume(attempt)

    def test_shell_entrypoint_prepare_only_and_resume(self):
        script = Path(__file__).resolve().with_name("release.sh")
        result = subprocess.run(["bash", str(script), "patch", ". adapter", "--prepare-only"],
                                cwd=self.source, input="y\n", text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        match = re.search(r"Prepared attempt directory: (.+)", result.stdout)
        self.assertIsNotNone(match)
        attempt = Path(match[1])
        self.addCleanup(shutil.rmtree, attempt, ignore_errors=True)
        self.assertEqual(release.remote_refs(str(self.origin)), {})
        resumed = subprocess.run(["bash", str(script), "--resume", str(attempt)],
                                 cwd=self.source, text=True, capture_output=True)
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        self.assertEqual(release.publication(release.resume(attempt)), "complete")
        self.assertEqual(self.snapshot(), self.original)

    def test_incomplete_manifest_is_rejected(self):
        attempt, manifest = self.prepare()
        manifest["refs"] = manifest["refs"][:1]
        (attempt / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "complete release ref allowlist"):
            release.resume(attempt)
        self.assertEqual(release.remote_refs(str(self.origin)), {})

    def test_portable_block_and_inline_edits(self):
        # Arrange: block requirements/replaces, external replace and nonzero internal version.
        content = ('module example.com/flowy/adapter\n\ngo 1.20\nrequire (\n'
                   '\texample.com/flowy v0.8.0 // internal\n\texample.net/external v1.0.0\n)\n'
                   'replace ( // comment\n\texample.com/flowy v0.8.0 => ..\n'
                   '\texample.net/external => ../external\n)\n')
        (self.source / "adapter/go.mod").write_text(content)
        # Act.
        release.edit_modules(self.source, ["adapter"], "example.com/flowy", "v0.9.0")
        # Assert.
        edited = (self.source / "adapter/go.mod").read_text()
        self.assertIn("example.com/flowy v0.9.0 // internal", edited)
        self.assertNotIn("example.com/flowy v0.8.0 =>", edited)
        self.assertIn("example.net/external => ../external", edited)


if __name__ == "__main__":
    unittest.main()
