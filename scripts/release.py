#!/usr/bin/env python3
"""Prepare isolated releases and publish only recorded refs atomically."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def git(directory, *args):
    return subprocess.check_output(["git", "-C", str(directory), *args], text=True,
                                   env=dict(os.environ, GIT_OPTIONAL_LOCKS="0")).strip()


def remote_refs(destination):
    output = subprocess.check_output(["git", "ls-remote", "--refs", "--tags", destination], text=True)
    return dict(line.split()[::-1] for line in output.splitlines())


def publication(manifest):
    try:
        refs = remote_refs(manifest["destination"])
    except subprocess.CalledProcessError:
        return "unknown"
    expected = manifest["refs"]
    if any(ref in refs and refs[ref] != manifest["commit"] for ref in expected):
        return "conflicting"
    matched = sum(refs.get(ref) == manifest["commit"] for ref in expected)
    return "complete" if matched == len(expected) else "partial" if matched else "none"


def modules_at(source, requested):
    tracked = git(source, "ls-files").splitlines()
    actual = sorted(str(Path(p).parent) for p in tracked if Path(p).name == "go.mod")
    supplied = sorted(str(Path(p)) for p in requested.split())
    if supplied != actual or len(set(supplied)) != len(supplied):
        raise ValueError("MODULES must list each tracked Go module exactly once")
    return actual


def edit_modules(clone, modules, root, version):
    for module in modules:
        path = clone / module / "go.mod"
        metadata = json.loads(subprocess.check_output(
            ["go", "mod", "edit", "-json", str(path)], text=True))
        flags = ["-fmt"]
        for requirement in metadata.get("Require") or []:
            dependency = requirement["Path"]
            if dependency == root or dependency.startswith(root + "/"):
                flags.append(f"-require={dependency}@{version}")
        for replacement in metadata.get("Replace") or []:
            old = replacement["Old"]
            if old["Path"] == root or old["Path"].startswith(root + "/"):
                identity = old["Path"] + ("@" + old["Version"] if old.get("Version") else "")
                flags.append(f"-dropreplace={identity}")
        subprocess.run(["go", "mod", "edit", *flags, str(path)], check=True)


def prepare(source, release_type, requested):
    source = source.resolve()
    if git(source, "rev-parse", "--show-toplevel") != str(source):
        raise ValueError("run release from the repository root")
    if git(source, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("tracked worktree and index must be clean")
    modules = modules_at(source, requested)
    root = re.search(r"^module\s+(\S+)", (source / "go.mod").read_text(), re.M)[1]
    destination = git(source, "remote", "get-url", "--push", "origin")
    if ":" not in destination and not destination.startswith("/"):
        destination = str((source / destination).resolve())
    versions = [tuple(map(int, m.groups())) for ref in remote_refs(destination)
                if (m := re.fullmatch(r"refs/tags/v(\d+)\.(\d+)\.(\d+)", ref))]
    major, minor, patch = max(versions, default=(0, 0, 0))
    if release_type == "patch":
        patch += 1
    elif major == 0:
        minor, patch = minor + 1, 0
    else:
        major, minor, patch = major + 1, 0, 0
    if major >= 2:
        raise ValueError("v2+ requires semantic import path migration and consumer verification")
    version = f"v{major}.{minor}.{patch}"
    if input(f"Release {version} ({release_type})? [y/N] ").lower() != "y":
        raise ValueError("aborted")
    attempt = Path(tempfile.mkdtemp(prefix="flowy-release-"))
    print(f"Prepared attempt directory: {attempt}", flush=True)
    clone = attempt / "repo"
    subprocess.run(["git", "clone", "--quiet", "--no-local", "--no-checkout", str(source), str(clone)], check=True)
    git(clone, "checkout", "--quiet", "--detach", git(source, "rev-parse", "HEAD"))
    for tag in git(clone, "tag", "-l").splitlines():
        git(clone, "tag", "-d", tag)
    for key in ("user.name", "user.email"):
        git(clone, "config", key, git(source, "config", key))
    edit_modules(clone, modules, root, version)
    git(clone, "add", "--", *[str(Path(module) / "go.mod") for module in modules])
    if git(clone, "diff", "--cached", "--name-only"):
        git(clone, "commit", "--quiet", "-m", f"chore: release {version}")
    commit = git(clone, "rev-parse", "HEAD")
    refs = [f"refs/tags/{version}"] + [f"refs/tags/{module}/{version}" for module in modules if module != "."]
    for ref in refs:
        git(clone, "update-ref", ref, commit)
    manifest = {"version": version, "commit": commit, "refs": refs, "destination": destination}
    (attempt / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return attempt, manifest


def resume(attempt):
    manifest = json.loads((attempt / "manifest.json").read_text())
    clone = attempt / "repo"
    version = manifest["version"]
    if not isinstance(version, str) or not re.fullmatch(r"v[01]\.\d+\.\d+", version):
        raise ValueError("invalid prepared release version")
    tracked = git(clone, "ls-files").splitlines()
    modules = sorted(str(Path(p).parent) for p in tracked if Path(p).name == "go.mod")
    expected = [f"refs/tags/{version}"] + [f"refs/tags/{module}/{version}"
                                              for module in modules if module != "."]
    if manifest["refs"] != expected:
        raise ValueError("manifest differs from complete release ref allowlist")
    for ref in manifest["refs"]:
        if not ref.startswith("refs/tags/") or git(clone, "rev-parse", ref) != manifest["commit"]:
            raise ValueError("prepared ref differs from immutable manifest")
    if git(clone, "status", "--porcelain") or git(clone, "rev-parse", "HEAD") != manifest["commit"]:
        raise ValueError("prepared clone changed")
    return manifest


def publish(attempt, manifest):
    state = publication(manifest)
    if state == "complete":
        print(f"Release {manifest['version']} already published.")
        return 0
    if state in ("unknown", "conflicting"):
        print(f"Publication: {state}; resolve destination state before retry.", file=sys.stderr)
        return 1
    subprocess.run(["git", "-C", str(attempt / "repo"), "push", "--atomic", manifest["destination"],
                    *[f"{ref}:{ref}" for ref in manifest["refs"]]])
    state = publication(manifest)
    print(f"Publication: {state}")
    if state != "complete":
        print(f"Retry exact attempt: python3 scripts/release.py --resume {attempt}")
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release_type", nargs="?", choices=("patch", "break"))
    parser.add_argument("modules", nargs="?")
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    try:
        if args.resume:
            attempt = args.resume.resolve()
            manifest = resume(attempt)
        else:
            if not args.release_type or not args.modules:
                parser.error("release type and MODULES are required unless --resume is used")
            attempt, manifest = prepare(Path.cwd().resolve(), args.release_type, args.modules)
        if args.prepare_only:
            print(f"Prepared {manifest['version']}; verify consumers before --resume {attempt}")
            return 0
        return publish(attempt, manifest)
    except (ValueError, OSError, subprocess.CalledProcessError, KeyError, TypeError) as error:
        print(f"Release failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
