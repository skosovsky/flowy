#!/usr/bin/env python3
"""Discover actual Go fuzz probes and run each exact target; never pass empty coverage."""
import argparse
from pathlib import Path
import subprocess


def modules(root):
    return sorted(p.parent for p in root.rglob("go.mod")
                  if not any(part.startswith(".") or part == "vendor"
                             for part in p.relative_to(root).parts))


def discover(root):
    probes = []
    for module in modules(root):
        packages = subprocess.check_output(["go", "list", "./..."], cwd=module, text=True).splitlines()
        for package in packages:
            listing = subprocess.check_output(["go", "test", "-list", "^Fuzz", package],
                                              cwd=module, text=True)
            probes.extend((module, package, name) for name in listing.splitlines()
                          if name.startswith("Fuzz"))
    if not probes:
        raise ValueError("no fuzz probes discovered; no fuzz coverage established")
    return probes


def run(root, seconds, workers):
    probes = discover(root)
    for module, package, name in probes:
        print(f"fuzz {module.relative_to(root)} {package} {name}: {seconds}s", flush=True)
        subprocess.run(["go", "test", "-run=^$", f"-fuzz=^{name}$", f"-fuzztime={seconds}s",
                        f"-parallel={workers}", package], cwd=module, check=True)
    print(f"Completed {len(probes)} fuzz probes; packages without probes have no fuzz claim.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.seconds <= 0 or args.workers <= 0:
        parser.error("seconds and workers must be positive")
    run(Path(__file__).resolve().parent.parent, args.seconds, args.workers)


if __name__ == "__main__":
    main()
