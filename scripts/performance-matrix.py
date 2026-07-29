#!/usr/bin/env python3
"""Collect, summarize, and compare repeatable Shiba benchmark runs.

This deliberately has no PostgreSQL or Rust knowledge.  A workload is an
executable command which accepts ``--profile`` and ``--json-out`` and writes
one JSON object whose numeric measurements are in ``metrics``.  Keeping this
layer separate from workloads lets the matrix report comparable measurements
without turning the correctness gate into a hardware benchmark.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import platform
import shlex
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


PROFILE_DEFAULTS = {
    "smoke": {"warmups": 1, "repetitions": 3},
    "full": {"warmups": 2, "repetitions": 9},
}
KNOWN_FIELDS = {"metrics", "metadata", "name", "profile", "version"}


def die(message: str) -> None:
    raise SystemExit(f"performance matrix: {message}")


def git_revision() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def cpu_model() -> str:
    if sys.platform == "darwin":
        try:
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    if sys.platform.startswith("linux"):
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
        except OSError:
            pass
    return platform.processor() or "unknown"


def host() -> dict[str, Any]:
    identity = {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "cpu_model": cpu_model(),
        "cpu_count": os.cpu_count(),
        "node": platform.node(),
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return {"id": hashlib.sha256(encoded.encode()).hexdigest()[:16], **identity}


def numeric_metrics(payload: dict[str, Any]) -> dict[str, float]:
    raw = payload.get("metrics")
    if raw is None:
        raw = {key: value for key, value in payload.items() if key not in KNOWN_FIELDS}
    if not isinstance(raw, dict):
        die("workload JSON field 'metrics' must be an object")
    metrics: dict[str, float] = {}
    for name, value in raw.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            die(f"metric {name!r} must be a number")
        metrics[name] = float(value)
    if not metrics:
        die("workload JSON contains no numeric metrics")
    return metrics


def metadata_for(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("metadata", {})
    if not isinstance(metadata, dict):
        die("workload JSON field 'metadata' must be an object")
    result = dict(metadata)
    # The benchmark harness uses this top-level spelling because it describes
    # the whole PostgreSQL cluster; accept it in the smaller workload contract
    # too so callers do not need a wrapper solely to move a field.
    if "environment_fingerprint" in payload:
        result["environment_fingerprint"] = payload["environment_fingerprint"]
    return result


def payload_measurements(payload: dict[str, Any]) -> list[tuple[str | None, dict[str, float], dict[str, Any]]]:
    """Expand either one workload result or the harness's scenario run JSON."""
    scenarios = payload.get("scenarios")
    if scenarios is None:
        return [(None, numeric_metrics(payload), metadata_for(payload))]
    if not isinstance(scenarios, list) or not scenarios:
        die("workload JSON field 'scenarios' must be a non-empty array")
    root_metadata = metadata_for(payload)
    result: list[tuple[str | None, dict[str, float], dict[str, Any]]] = []
    seen: set[str] = set()
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            die("each scenario must be an object")
        name = scenario.get("scenario")
        if not isinstance(name, str) or not name:
            die("each scenario needs a non-empty string 'scenario'")
        if name in seen:
            die(f"duplicate scenario {name!r} in one workload result")
        seen.add(name)
        scenario_metadata = metadata_for(scenario)
        result.append((name, numeric_metrics(scenario), {**root_metadata, **scenario_metadata}))
    return result


def load_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        die(f"cannot read JSON {path}: {error}")
    if not isinstance(raw, dict):
        die(f"JSON {path} must contain an object")
    return raw


def parse_case(value: str) -> tuple[str, str]:
    name, separator, command = value.partition("=")
    if not separator or not name or not command:
        die("--case must be NAME=COMMAND")
    return name, command


def cases_from_manifest(path: Path) -> list[tuple[str, str]]:
    manifest = load_json(path)
    raw_cases = manifest.get("cases")
    if not isinstance(raw_cases, list) or not raw_cases:
        die("manifest must contain a non-empty 'cases' array")
    cases: list[tuple[str, str]] = []
    for item in raw_cases:
        if not isinstance(item, dict):
            die("each manifest case must be an object")
        name, command = item.get("name"), item.get("command")
        if not isinstance(name, str) or not isinstance(command, str):
            die("each manifest case needs string 'name' and 'command'")
        cases.append((name, command))
    return cases


def command_for(command: str, profile: str, json_out: Path) -> list[str]:
    quoted_output = shlex.quote(str(json_out))
    if "{profile}" in command or "{json_out}" in command:
        expanded = command.replace("{profile}", shlex.quote(profile)).replace(
            "{json_out}", quoted_output
        )
    else:
        expanded = f"{command} --profile {shlex.quote(profile)} --json-out {quoted_output}"
    return ["bash", "-o", "pipefail", "-c", expanded]


def percentile(values: list[float], percentage: float) -> float:
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentage
    lower, upper = int(index), min(int(index) + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def summarize(samples: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    measurements: dict[str, list[float]] = {}
    for sample in samples:
        for name, value in sample["metrics"].items():
            measurements.setdefault(name, []).append(value)
    return {
        name: {
            "count": len(values),
            "min": min(values),
            "median": statistics.median(values),
            "mean": statistics.fmean(values),
            "p95": percentile(values, 0.95),
            "max": max(values),
        }
        for name, values in sorted(measurements.items())
    }


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def write_csv(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["case", "sample", "metric", "value"])
        for case in report["cases"]:
            for index, sample in enumerate(case["samples"], start=1):
                for name, value in sorted(sample["metrics"].items()):
                    writer.writerow([case["name"], index, name, value])


def print_report(report: dict[str, Any]) -> None:
    print(f"profile={report.get('profile', 'imported')} host={report['host']['id']}")
    for case in report["cases"]:
        print(f"\n[{case['name']}]")
        for name, summary in case["summary"].items():
            print(
                f"  {name}: median={summary['median']:.6g} "
                f"p95={summary['p95']:.6g} n={int(summary['count'])}"
            )


def run(args: argparse.Namespace) -> int:
    cases = cases_from_manifest(Path(args.manifest)) if args.manifest else [parse_case(x) for x in args.case]
    if not cases:
        die("supply --manifest or at least one --case")
    names = [name for name, _ in cases]
    if len(names) != len(set(names)):
        die("case names must be unique")
    defaults = PROFILE_DEFAULTS[args.profile]
    warmups = defaults["warmups"] if args.warmups is None else args.warmups
    repetitions = defaults["repetitions"] if args.repetitions is None else args.repetitions
    if warmups < 0 or repetitions < 1:
        die("warmups must be >= 0 and repetitions must be >= 1")

    all_case_samples: dict[str, list[dict[str, Any]]] = {}
    all_case_commands: dict[str, str] = {}
    for name, command in cases:
        print(f"==> benchmark {name} ({warmups} warmup, {repetitions} samples)", flush=True)
        for invocation in range(warmups + repetitions):
            with tempfile.TemporaryDirectory(prefix="shiba-performance-") as directory:
                output = Path(directory) / "workload.json"
                started = time.monotonic()
                completed = subprocess.run(command_for(command, args.profile, output))
                wall_seconds = time.monotonic() - started
                if completed.returncode != 0:
                    die(f"case {name!r} failed on invocation {invocation + 1}")
                payload = load_json(output)
            if invocation < warmups:
                continue
            for scenario, metrics, metadata in payload_measurements(payload):
                case_name = name if scenario is None else f"{name}/{scenario}"
                if case_name not in all_case_samples:
                    all_case_samples[case_name] = []
                    all_case_commands[case_name] = command
                metrics.setdefault("wall_seconds", wall_seconds)
                all_case_samples[case_name].append({"metrics": metrics, "metadata": metadata})
    all_cases: list[dict[str, Any]] = []
    for name, samples in all_case_samples.items():
        if len(samples) != repetitions:
            die(f"case {name!r} appeared in only {len(samples)} of {repetitions} measured runs")
        all_cases.append({"name": name, "command": all_case_commands[name], "samples": samples, "summary": summarize(samples)})

    report = {
        "format": "shiba-performance-matrix/v1",
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "profile": args.profile,
        "git_revision": git_revision(),
        "host": host(),
        "cases": all_cases,
    }
    write_json(Path(args.output), report)
    if args.csv:
        write_csv(Path(args.csv), report)
    print_report(report)
    return 0


def normalize_report(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("format") == "shiba-performance-matrix/v1":
        return payload
    # A single workload result is useful for a one-off diagnostic run too.
    measurements = payload_measurements(payload)
    cases = []
    for scenario, metrics, metadata in measurements:
        sample = {"metrics": metrics, "metadata": metadata}
        cases.append({"name": scenario or payload.get("name", "imported"), "samples": [sample], "summary": summarize([sample])})
    return {
        "format": "shiba-performance-matrix/v1",
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "profile": payload.get("profile", "imported"),
        "git_revision": git_revision(),
        "host": host(),
        "cases": cases,
    }


def report(args: argparse.Namespace) -> int:
    normalized = normalize_report(load_json(Path(args.input)))
    write_json(Path(args.output), normalized)
    if args.csv:
        write_csv(Path(args.csv), normalized)
    print_report(normalized)
    return 0


def parse_policy(value: str) -> tuple[str, str, float]:
    parts = value.split(":")
    if len(parts) not in {2, 3} or parts[1] not in {"lower", "higher"}:
        die("--metric must be NAME:lower|higher[:allowed-regression]")
    try:
        threshold = float(parts[2]) if len(parts) == 3 else 0.25
    except ValueError:
        die(f"invalid regression threshold in {value!r}")
    if threshold < 0:
        die("regression threshold must be non-negative")
    return parts[0], parts[1], threshold


def case_index(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {case["name"]: case for case in report["cases"]}


def environment_fingerprint(case: dict[str, Any]) -> str | None:
    """Return one stable workload environment identity for a case.

    The workload, rather than this generic collector, knows the PostgreSQL
    version, extension build, and GUCs that affect a measurement.  A changed
    value between repetitions is itself an invalid benchmark sample.
    """
    fingerprints: set[str] = set()
    missing = False
    for sample in case["samples"]:
        metadata = sample.get("metadata", {})
        if not isinstance(metadata, dict) or "environment_fingerprint" not in metadata:
            missing = True
            continue
        value = metadata["environment_fingerprint"]
        fingerprints.add(json.dumps(value, sort_keys=True, separators=(",", ":")))
    if missing and fingerprints:
        die(f"case {case['name']!r} has environment_fingerprint in only some samples")
    if len(fingerprints) > 1:
        die(f"case {case['name']!r} changed environment_fingerprint during one run")
    return next(iter(fingerprints), None)


def regression_fraction(baseline: float, candidate: float, direction: str) -> tuple[float | None, bool]:
    """Return regression fraction and whether it is mathematically infinite."""
    if direction == "lower":
        if baseline == 0.0:
            return (None, candidate > 0.0)
        return (candidate / baseline - 1.0, False)
    if candidate == 0.0:
        return (None, baseline > 0.0)
    return (baseline / candidate - 1.0, False)


def compare(args: argparse.Namespace) -> int:
    baseline = normalize_report(load_json(Path(args.baseline)))
    candidate = normalize_report(load_json(Path(args.candidate)))
    if baseline.get("profile") != candidate.get("profile"):
        die(
            "baseline and candidate profiles differ; collect the same workload "
            "size before comparing"
        )
    same_host = baseline["host"].get("id") == candidate["host"].get("id")
    if not same_host and not args.allow_cross_host:
        die("baseline and candidate host fingerprints differ; collect both on one host or pass --allow-cross-host")
    if baseline.get("profile") != candidate.get("profile") and not args.allow_environment_mismatch:
        die(
            "baseline and candidate profiles differ; collect both with the same profile "
            "or pass --allow-environment-mismatch"
        )
    policies = [parse_policy(value) for value in args.metric]
    if not policies:
        die("supply at least one --metric; direction is a workload decision")
    baseline_cases, candidate_cases = case_index(baseline), case_index(candidate)
    rows: list[dict[str, Any]] = []
    failed = False
    for name, direction, threshold in policies:
        for case_name in sorted(set(baseline_cases) & set(candidate_cases)):
            baseline_environment = environment_fingerprint(baseline_cases[case_name])
            candidate_environment = environment_fingerprint(candidate_cases[case_name])
            if baseline_environment != candidate_environment and not args.allow_environment_mismatch:
                die(
                    f"case {case_name!r} environment fingerprints differ; "
                    "collect with the same PostgreSQL/GUC/extension configuration "
                    "or pass --allow-environment-mismatch"
                )
            old = baseline_cases[case_name]["summary"].get(name)
            new = candidate_cases[case_name]["summary"].get(name)
            if not old or not new:
                continue
            old_value, new_value = old["median"], new["median"]
            change, infinite = regression_fraction(old_value, new_value, direction)
            regressed = infinite or (change is not None and change > threshold)
            failed = failed or regressed
            rows.append({
                "case": case_name, "metric": name, "direction": direction,
                "baseline_median": old_value, "candidate_median": new_value,
                "regression": change, "infinite_regression": infinite,
                "threshold": threshold, "regressed": regressed,
                "environment_fingerprint": baseline_environment,
            })
    comparison = {
        "format": "shiba-performance-comparison/v1",
        "same_host": same_host,
        "baseline": str(args.baseline),
        "candidate": str(args.candidate),
        "comparisons": rows,
    }
    if args.output:
        write_json(Path(args.output), comparison)
    for row in rows:
        state = "REGRESSION" if row["regressed"] else "ok"
        change = "+infinity" if row["infinite_regression"] else f"{row['regression']:+.1%}"
        print(
            f"{state:10} {row['case']} {row['metric']} "
            f"{row['baseline_median']:.6g} -> {row['candidate_median']:.6g} "
            f"({change}; allowance {row['threshold']:.1%})"
        )
    if not rows:
        die("no selected metrics existed in matching cases")
    if failed and args.fail_on_regression:
        return 1
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    subcommands = root.add_subparsers(dest="subcommand", required=True)
    collect = subcommands.add_parser("run", help="run workload commands repeatedly")
    collect.add_argument("--profile", choices=PROFILE_DEFAULTS, default="smoke")
    collect.add_argument("--manifest", help="JSON manifest with [{name, command}] cases")
    collect.add_argument("--case", action="append", default=[], help="NAME=COMMAND; repeatable")
    collect.add_argument("--warmups", type=int)
    collect.add_argument("--repetitions", type=int)
    collect.add_argument("--output", required=True, help="matrix JSON artifact")
    collect.add_argument("--csv", help="optional per-sample CSV artifact")
    collect.set_defaults(function=run)
    imported = subcommands.add_parser("report", help="normalize one workload or matrix JSON file")
    imported.add_argument("--input", required=True)
    imported.add_argument("--output", required=True)
    imported.add_argument("--csv")
    imported.set_defaults(function=report)
    comparison = subcommands.add_parser("compare", help="compare median metrics with a same-host baseline")
    comparison.add_argument("--baseline", required=True)
    comparison.add_argument("--candidate", required=True)
    comparison.add_argument("--metric", action="append", default=[], help="NAME:lower|higher[:allowed-regression]")
    comparison.add_argument("--output")
    comparison.add_argument("--allow-cross-host", action="store_true")
    comparison.add_argument("--allow-environment-mismatch", action="store_true")
    comparison.add_argument("--fail-on-regression", action="store_true")
    comparison.set_defaults(function=compare)
    return root


def main() -> int:
    args = parser().parse_args()
    return args.function(args)


if __name__ == "__main__":
    raise SystemExit(main())
