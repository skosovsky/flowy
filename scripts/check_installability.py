#!/usr/bin/env python3
"""Build all committed project modules through a disposable local Go proxy."""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import tempfile
import zipfile

import release


def artifacts(source, attempt, commit):
    """Only committed files are admitted; generated caches/untracked inputs are excluded."""
    tracked = release.git(source, "ls-tree", "-r", "--name-only", commit).splitlines()
    modules = sorted(str(Path(name).parent) for name in tracked if Path(name).name == "go.mod")
    if len(modules) != 7:
        raise ValueError(f"expected seven committed modules, found {len(modules)}")
    clone = attempt / "source"
    clone.mkdir()
    archive = subprocess.check_output(["git", "-C", str(source), "archive", commit])
    subprocess.run(["tar", "-xf", "-", "-C", str(clone)], input=archive, check=True)
    root = json.loads(subprocess.check_output(
        ["go", "mod", "edit", "-json", str(clone / "go.mod")], text=True))["Module"]["Path"]
    version = "v0.0.0-task28"
    release.edit_modules(clone, modules, root, version)
    proxy = attempt / "proxy"
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    paths = []
    for directory in modules:
        module_dir = clone / directory
        metadata = json.loads(subprocess.check_output(
            ["go", "mod", "edit", "-json", str(module_dir / "go.mod")], text=True))
        if metadata.get("Replace"):
            raise ValueError(f"unremoved replace directive in {directory}")
        name = metadata["Module"]["Path"]
        if any(char.isupper() for char in name):
            raise ValueError("proxy path escaping needs explicit support for uppercase module names")
        output = proxy / name / "@v"
        output.mkdir(parents=True)
        (output / "list").write_text(version + "\n")
        (output / f"{version}.mod").write_bytes((module_dir / "go.mod").read_bytes())
        (output / f"{version}.info").write_text(json.dumps({"Version": version, "Time": timestamp}))
        # Nested modules get their own archives; do not leak their files into the parent zip.
        prefix = "" if directory == "." else directory + "/"
        nested = [m + "/" for m in modules if m != directory and
                  (directory == "." or m.startswith(prefix))]
        with zipfile.ZipFile(output / f"{version}.zip", "w", zipfile.ZIP_DEFLATED) as bundle:
            for tracked_name in tracked:
                if not tracked_name.startswith(prefix) or any(tracked_name.startswith(n) for n in nested):
                    continue
                relative = tracked_name[len(prefix):]
                bundle.write(module_dir / relative, f"{name}@{version}/{relative}")
        paths.append(name)
    return paths, version, proxy


def verify(source, attempt):
    commit = release.git(source, "rev-parse", "HEAD")
    paths, version, proxy = artifacts(source, attempt, commit)
    consumer = attempt / "consumer"
    consumer.mkdir()
    env = dict(os.environ, GOWORK="off", GOMODCACHE=str(attempt / "modcache"),
               GOCACHE=str(attempt / "buildcache"), GOBIN=str(attempt / "bin"),
               GOPROXY=f"{proxy.as_uri()},https://proxy.golang.org", GOSUMDB="off",
               GOENV="off", GOPRIVATE="none", GONOPROXY="none", GONOSUMDB="none", GOFLAGS="")
    # Synthetic first-party versions only exist in this local proxy; public deps use their pinned versions.
    (consumer / "go.mod").write_text("module example.com/flowy-consumer\n\ngo 1.27.1\n\nrequire (\n" +
                                    "".join(f"\t{name} {version}\n" for name in paths) + ")\n")
    libraries = [name for name in paths if "/examples/" not in name]
    (consumer / "main.go").write_text("package main\n\nimport (\n" +
                                     "".join(f'\t_ "{name}"\n' for name in libraries) +
                                     ")\n\nfunc main() {}\n")
    for name in paths:
        metadata = json.loads(subprocess.check_output(
            ["go", "mod", "download", "-json", f"{name}@{version}"], cwd=consumer, env=env, text=True))
        if metadata.get("Error") or metadata["Version"] != version:
            raise ValueError(f"unconfirmed artifact: {name}")
        if Path(metadata["Zip"]).read_bytes() != (proxy / name / "@v" / f"{version}.zip").read_bytes():
            raise ValueError(f"artifact not downloaded from intended local proxy: {name}")
    subprocess.run(["go", "mod", "tidy"], cwd=consumer, env=env, check=True)
    subprocess.run(["go", "build", "./..."], cwd=consumer, env=env, check=True)
    for name in paths:
        if "/examples/" in name:
            subprocess.run(["go", "install", f"{name}@{version}"], cwd=consumer, env=env, check=True)
    resolved = json.loads(subprocess.check_output(
        ["go", "mod", "edit", "-json"], cwd=consumer, env=env, text=True))
    if resolved.get("Replace"):
        raise ValueError("consumer verification must not use replace directives")
    result = {"source_commit": commit, "version": version,
              "modules": paths, "consumer_build": True, "blueprint_install": True,
              "local_replaces": False, "fresh_cache": True, "proxy": str(proxy)}
    (attempt / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parent.parent)
    args = parser.parse_args()
    attempt = Path(tempfile.mkdtemp(prefix="flowy-task28-consumer-"))
    print(f"Consumer attempt: {attempt}", flush=True)
    verify(args.source.resolve(), attempt)


if __name__ == "__main__":
    main()
