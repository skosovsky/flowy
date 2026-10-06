#!/usr/bin/env python3
"""Require measured benchmark cases and bounded allocation counts; no time SLA."""

import argparse
import json
import math
import re
import sys


def check(manifest, output):
    required = manifest["benchmarks"]
    seen = set()
    errors = []
    if "PASS" not in output.splitlines() or any(
        line.startswith(("FAIL", "--- FAIL")) for line in output.splitlines()
    ):
        errors.append("benchmark command did not finish with PASS")
    for line in output.splitlines():
        fields = line.split()
        if not fields or not fields[0].startswith("Benchmark"):
            continue
        name = re.sub(r"-\d+$", "", fields[0])
        if name not in required:
            continue
        if len(fields) < 8:
            errors.append(f"{name}: no complete benchmark measurement")
            continue
        try:
            iterations = int(fields[1])
            values = {fields[i + 1]: float(fields[i]) for i in range(2, len(fields) - 1, 2)}
            if iterations < 1 or not all(math.isfinite(v) and v >= 0 for v in values.values()):
                raise ValueError("invalid measurement")
            for metric in ("ns/op", "B/op", "allocs/op"):
                if metric not in values:
                    raise ValueError(f"missing {metric}")
            for metric, maximum in required[name].items():
                if metric not in values or values[metric] > maximum:
                    errors.append(f"{name}: {metric}={values.get(metric)} exceeds {maximum}")
            seen.add(name)
        except (ValueError, IndexError) as exc:
            errors.append(f"{name}: {exc}")
    errors.extend(f"missing measured benchmark: {name}" for name in sorted(set(required) - seen))
    if not required:
        errors.append("empty benchmark manifest")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest")
    parser.add_argument("output")
    args = parser.parse_args()
    with open(args.manifest, encoding="utf-8") as file:
        manifest = json.load(file)
    with open(args.output, encoding="utf-8") as file:
        errors = check(manifest, file.read())
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"verified {len(manifest['benchmarks'])} measured benchmark cases")
    return 0


if __name__ == "__main__":
    sys.exit(main())
