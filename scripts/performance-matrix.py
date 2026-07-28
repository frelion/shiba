#!/usr/bin/env python3
"""Run the complete Shiba operator and end-to-end performance matrix.

The runner uses only the Python standard library and PostgreSQL command-line
programs.  Each scenario gets a fresh database inside one isolated temporary
cluster.  A plain PostgreSQL schema and a Shiba source schema receive identical
data and mutations so source-side overhead can be compared without changing
the workload.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import platform
import random
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "benchmarks"))
from operator_matrix import ALL_OPERATOR_KINDS, Action, Scenario, build_scenarios  # noqa: E402


class BenchmarkError(RuntimeError):
    pass


def env_int(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value <= 0:
        raise BenchmarkError(f"{name} must be positive")
    return value


RUN_ID = os.environ.get(
    "SHIBA_MATRIX_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
)
OUTPUT_DIR = Path(
    os.environ.get(
        "SHIBA_MATRIX_OUTPUT_DIR",
        PROJECT_ROOT / "performance" / "matrix-results" / RUN_ID,
    )
)
PG_CONFIG = Path(
    os.environ.get("PG_CONFIG", "/opt/homebrew/opt/postgresql@17/bin/pg_config")
)
PORT = env_int("SHIBA_MATRIX_PORT", 55436)
ROWS = env_int("SHIBA_MATRIX_ROWS", 20_000)
GROUPS = env_int("SHIBA_MATRIX_GROUPS", 100)
MUTATIONS = env_int("SHIBA_MATRIX_MUTATIONS", 20)
REPETITIONS = env_int("SHIBA_MATRIX_REPETITIONS", 3)
QUERY_SECONDS = env_int("SHIBA_MATRIX_QUERY_SECONDS", 5)
QUERY_CLIENTS = env_int("SHIBA_MATRIX_QUERY_CLIENTS", 4)
LATENCY_PROBES = env_int("SHIBA_MATRIX_LATENCY_PROBES", 40)
LARGE_TRANSACTION_ROWS = env_int("SHIBA_MATRIX_LARGE_TX_ROWS", 5_000)
RESOURCE_SAMPLE_MS = env_int("SHIBA_MATRIX_RESOURCE_SAMPLE_MS", 100)
SEED = env_int("SHIBA_MATRIX_SEED", 20260725)
KEEP_CLUSTER = os.environ.get("SHIBA_KEEP_MATRIX_CLUSTER", "0") == "1"
SKIP_BUILD = os.environ.get("SHIBA_MATRIX_SKIP_BUILD", "0") == "1"
SCENARIO_FILTER = {
    item.strip()
    for item in os.environ.get("SHIBA_MATRIX_SCENARIOS", "").split(",")
    if item.strip()
}

PG_BIN = Path(
    subprocess.check_output([str(PG_CONFIG), "--bindir"], text=True).strip()
)
DATA_DIR = Path(tempfile.mkdtemp(prefix="shiba-matrix-data.", dir="/tmp"))
SOCKET_DIR = Path(tempfile.mkdtemp(prefix="shiba-matrix-socket.", dir="/tmp"))
PG_LOG = DATA_DIR / "postgresql.log"
DATABASE = "shiba_matrix"
METRICS: list[dict[str, Any]] = []
ACTION_SAMPLES: list[dict[str, Any]] = []
SCENARIO_SUMMARIES: list[dict[str, Any]] = []
RESOURCE_ROWS: list[dict[str, Any]] = []
POSTMASTER_PID: int | None = None
RUNTIME_OBSERVATIONS: list[dict[str, Any]] = []


def run(
    command: list[str],
    *,
    input_text: str | None = None,
    check: bool = True,
    capture: bool = True,
    cwd: Path = PROJECT_ROOT,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        input=input_text,
        text=True,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if check and completed.returncode != 0:
        raise BenchmarkError(
            f"command failed ({completed.returncode}): {' '.join(command)}\n"
            f"stdout:\n{completed.stdout or ''}\nstderr:\n{completed.stderr or ''}"
        )
    return completed


def psql(
    sql: str,
    *,
    database: str = DATABASE,
    tuples: bool = False,
    timeout_ms: int = 300_000,
) -> str:
    command = [
        str(PG_BIN / "psql"),
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        str(SOCKET_DIR),
        "-p",
        str(PORT),
        "-d",
        database,
    ]
    if tuples:
        command.extend(["-A", "-t", "-q"])
    pg_env = os.environ.copy()
    pg_env["PGOPTIONS"] = (
        f"-c statement_timeout={timeout_ms} -c lock_timeout=30000 "
        "-c application_name=shiba_performance_matrix"
    )
    return run(command, input_text=sql, env=pg_env).stdout.strip()


def scalar(sql: str, *, database: str = DATABASE) -> str:
    output = psql(sql, database=database, tuples=True)
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        raise BenchmarkError(f"query returned no scalar value: {sql}")
    return lines[-1]


LEGACY_SHIBA_BACKEND_TYPES = (
    "shiba worker",
    "shiba dag worker",
    "shiba router",
    "shiba executor",
)


def runtime_topology() -> list[dict[str, int]]:
    output = psql(
        """
SELECT json_build_object(
  'owner_pid',state.owner_pid
)::text
FROM shiba_internal.runtime_state state
JOIN pg_stat_activity activity
  ON activity.pid=state.owner_pid
 AND activity.backend_type='shiba runtime'
WHERE state.singleton AND state.active
  AND activity.datname=current_database()
""",
        tuples=True,
    )
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def legacy_shiba_backend_count() -> int:
    backend_types = ",".join(
        "'" + backend_type.replace("'", "''") + "'"
        for backend_type in LEGACY_SHIBA_BACKEND_TYPES
    )
    return int(
        scalar(
            "SELECT count(*) FROM pg_stat_activity "
            f"WHERE datname=current_database() AND backend_type IN ({backend_types})"
        )
    )


def runtime_topology_ready() -> bool:
    topology = runtime_topology()
    return (
        len(topology) == 1
        and scalar(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname=current_database() AND backend_type='shiba runtime'"
        )
        == "1"
        and legacy_shiba_backend_count() == 0
    )


def record_runtime_topology() -> None:
    if not runtime_topology_ready():
        raise BenchmarkError("Single Runtime topology invariant is not satisfied")
    topology = runtime_topology()
    legacy_count = legacy_shiba_backend_count()
    observation = {
        "database": DATABASE,
        "observed_utc": datetime.now(timezone.utc).isoformat(),
        "actual_count": len(topology),
        "legacy_worker_count": legacy_count,
        "runtimes": topology,
    }
    RUNTIME_OBSERVATIONS.append(observation)
    (OUTPUT_DIR / "runtime-topology.json").write_text(
        json.dumps(RUNTIME_OBSERVATIONS, indent=2) + "\n"
    )
    environment_path = OUTPUT_DIR / "environment.json"
    environment = json.loads(environment_path.read_text())
    environment["actual_runtime_count"] = len(topology)
    environment["legacy_worker_count"] = legacy_count
    environment["runtime_pids"] = [row["owner_pid"] for row in topology]
    environment["runtime_observations"] = RUNTIME_OBSERVATIONS
    environment_path.write_text(json.dumps(environment, indent=2) + "\n")


def metric(
    repetition: int,
    scenario: str,
    phase: str,
    name: str,
    value: float | int,
    unit: str,
    notes: str = "",
) -> None:
    METRICS.append(
        {
            "repetition": repetition,
            "scenario": scenario,
            "phase": phase,
            "metric": name,
            "value": value,
            "unit": unit,
            "notes": notes.replace(",", ";"),
        }
    )


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return math.nan
    index = math.ceil(quantile * len(ordered)) - 1
    return ordered[max(index, 0)]


def wait_until(description: str, predicate, timeout: float = 120.0) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (BenchmarkError, ValueError):
            pass
        time.sleep(0.01)
    raise BenchmarkError(f"timed out waiting for {description}; last={last!r}")


def lsn() -> str:
    return scalar("SELECT pg_current_wal_lsn()")


def wal_diff(after: str, before: str) -> int:
    return int(
        scalar(
            f"SELECT pg_wal_lsn_diff('{after}'::pg_lsn,'{before}'::pg_lsn)::bigint"
        )
    )


def io_snapshot() -> dict[str, float]:
    output = scalar(
        """
SELECT json_build_object(
  'read_bytes',coalesce(sum(reads * op_bytes),0),
  'write_bytes',coalesce(sum(writes * op_bytes),0),
  'extend_bytes',coalesce(sum(extends * op_bytes),0),
  'read_ms',coalesce(sum(read_time),0),
  'write_ms',coalesce(sum(write_time),0),
  'fsyncs',coalesce(sum(fsyncs),0),
  'fsync_ms',coalesce(sum(fsync_time),0)
)::text
FROM pg_stat_io
"""
    )
    return {key: float(value) for key, value in json.loads(output).items()}


def io_delta(after: dict[str, float], before: dict[str, float]) -> dict[str, float]:
    return {key: after[key] - before.get(key, 0.0) for key in after}


def process_snapshot(runtime_pid: int | None = None) -> tuple[float, int, float, int]:
    if POSTMASTER_PID is None:
        return (0.0, 0, 0.0, 0)
    completed = run(
        ["ps", "-axo", "pid=,ppid=,%cpu=,rss="], check=False, capture=True
    )
    records: dict[int, tuple[int, float, int]] = {}
    for line in completed.stdout.splitlines():
        fields = line.split()
        if len(fields) != 4:
            continue
        try:
            pid, ppid = int(fields[0]), int(fields[1])
            records[pid] = (ppid, float(fields[2]), int(fields[3]))
        except ValueError:
            continue
    descendants = {POSTMASTER_PID}
    changed = True
    while changed:
        changed = False
        for pid, (ppid, _, _) in records.items():
            if ppid in descendants and pid not in descendants:
                descendants.add(pid)
                changed = True
    cpu = sum(records.get(pid, (0, 0.0, 0))[1] for pid in descendants)
    rss = sum(records.get(pid, (0, 0.0, 0))[2] for pid in descendants)
    runtime_record = records.get(runtime_pid) if runtime_pid is not None else None
    runtime_cpu = runtime_record[1] if runtime_record else 0.0
    runtime_rss = runtime_record[2] if runtime_record else 0
    return cpu, rss, runtime_cpu, runtime_rss


class ResourceSampler:
    def __init__(self, repetition: int, scenario: str, phase: str):
        self.repetition = repetition
        self.scenario = scenario
        self.phase = phase
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.runtime_pid: int | None = None

    def __enter__(self):
        if not runtime_topology_ready():
            raise BenchmarkError(
                f"{self.scenario}/{self.phase}: expected exactly one Runtime "
                "and no legacy Shiba workers"
            )
        self.runtime_pid = runtime_topology()[0]["owner_pid"]

        def sample() -> None:
            while not self.stop_event.is_set():
                cpu, rss, runtime_cpu, runtime_rss = process_snapshot(
                    self.runtime_pid
                )
                RESOURCE_ROWS.append(
                    {
                        "epoch_ms": time.time_ns() / 1_000_000,
                        "repetition": self.repetition,
                        "scenario": self.scenario,
                        "phase": self.phase,
                        "runtime_pid": self.runtime_pid,
                        "cpu_percent": cpu,
                        "rss_kib": rss,
                        "runtime_cpu_percent": runtime_cpu,
                        "runtime_rss_kib": runtime_rss,
                    }
                )
                self.stop_event.wait(RESOURCE_SAMPLE_MS / 1000)

        self.thread = threading.Thread(target=sample, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, *_):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)
        rows = [
            row
            for row in RESOURCE_ROWS
            if row["repetition"] == self.repetition
            and row["scenario"] == self.scenario
            and row["phase"] == self.phase
        ]
        if rows:
            metric(
                self.repetition,
                self.scenario,
                self.phase,
                "cpu_mean",
                statistics.fmean(float(row["cpu_percent"]) for row in rows),
                "percent",
                "sum across PostgreSQL process tree; 100% is one core",
            )
            metric(
                self.repetition,
                self.scenario,
                self.phase,
                "cpu_peak",
                max(float(row["cpu_percent"]) for row in rows),
                "percent",
            )
            metric(
                self.repetition,
                self.scenario,
                self.phase,
                "rss_peak",
                max(int(row["rss_kib"]) for row in rows),
                "KiB",
                "sum across PostgreSQL process tree",
            )
            metric(
                self.repetition,
                self.scenario,
                self.phase,
                "runtime_cpu_mean",
                statistics.fmean(
                    float(row["runtime_cpu_percent"]) for row in rows
                ),
                "percent",
                "single Runtime; 100% is one core",
            )
            metric(
                self.repetition,
                self.scenario,
                self.phase,
                "runtime_cpu_peak",
                max(float(row["runtime_cpu_percent"]) for row in rows),
                "percent",
                "single Runtime",
            )
            metric(
                self.repetition,
                self.scenario,
                self.phase,
                "runtime_rss_peak",
                max(int(row["runtime_rss_kib"]) for row in rows),
                "KiB",
                "single Runtime",
            )
        if exc_type is None:
            if not runtime_topology_ready():
                raise BenchmarkError(
                    f"{self.scenario}/{self.phase}: Runtime topology changed "
                    "during resource sampling"
                )
            current_pid = runtime_topology()[0]["owner_pid"]
            if current_pid != self.runtime_pid:
                raise BenchmarkError(
                    f"{self.scenario}/{self.phase}: Runtime PID changed from "
                    f"{self.runtime_pid} to {current_pid}"
                )


def restart_cluster() -> None:
    global POSTMASTER_PID
    run(
        [
            str(PG_BIN / "pg_ctl"),
            "-D",
            str(DATA_DIR),
            "-m",
            "fast",
            "-w",
            "restart",
            "-l",
            str(PG_LOG),
            "-o",
            f"-k {SOCKET_DIR} -p {PORT}",
        ]
    )
    POSTMASTER_PID = int((DATA_DIR / "postmaster.pid").read_text().splitlines()[0])


def create_database() -> None:
    run(
        [
            str(PG_BIN / "createdb"),
            "-h",
            str(SOCKET_DIR),
            "-p",
            str(PORT),
            DATABASE,
        ]
    )
    psql("CREATE EXTENSION shiba")
    psql("SELECT shiba.activate()")
    wait_until(
        "single Shiba Runtime",
        runtime_topology_ready,
    )
    record_runtime_topology()


def destroy_database() -> None:
    try:
        result_count = int(
            scalar("SELECT count(*) FROM shiba_internal.stream_views")
        )
        if result_count:
            raise BenchmarkError(
                f"cannot destroy scenario database with {result_count} active results"
            )
        psql("SELECT shiba.deactivate()")
        wait_until(
            "all Shiba processes to stop",
            lambda: scalar(
                """
SELECT count(*) FROM pg_stat_activity
WHERE datname=current_database()
  AND backend_type IN (
    'shiba runtime','shiba worker','shiba dag worker',
    'shiba router','shiba executor'
  )
"""
            )
            == "0",
        )
    finally:
        run(
            [
                str(PG_BIN / "dropdb"),
                "-h",
                str(SOCKET_DIR),
                "-p",
                str(PORT),
                "--if-exists",
                DATABASE,
            ],
            check=False,
        )


def run_transaction(sql: str) -> tuple[str, float]:
    started = time.perf_counter_ns()
    output = psql(
        f"""
BEGIN;
{sql.rstrip().rstrip(';')};
SELECT pg_current_xact_id()::text;
COMMIT;
""",
        tuples=True,
    )
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    xid_matches = re.findall(r"^\d+$", output, flags=re.MULTILINE)
    if not xid_matches:
        raise BenchmarkError(f"could not extract transaction id from output: {output}")
    return xid_matches[-1], elapsed_ms


def commit_epoch_ms(xid: str) -> float:
    value = scalar(
        f"""
SELECT extract(epoch FROM pg_xact_commit_timestamp('{xid}'::xid))*1000
"""
    )
    if not value:
        raise BenchmarkError(f"no commit timestamp for xid {xid}")
    return float(value)


def progress() -> dict[str, Any]:
    value = scalar(
        """
SELECT json_build_object(
  'applied_lsn',p.applied_lsn::text,
  'updated_epoch_ms',extract(epoch FROM p.updated_at)*1000,
  'routed_epoch_ms',extract(epoch FROM r.routed_at)*1000,
  'pending_wal_bytes',public.pending_wal_bytes
)::text
FROM shiba_internal.view_progress p
JOIN shiba.progress('shiba.perf_result') public ON true
LEFT JOIN shiba_internal.routed_transactions r ON r.commit_lsn=p.applied_lsn
WHERE p.result_oid='shiba.perf_result'::regclass
"""
    )
    return json.loads(value)


def wait_for_progress(after_epoch_ms: float) -> dict[str, Any]:
    return wait_until(
        f"result progress after {after_epoch_ms}",
        lambda: (
            current
            if (current := progress())["updated_epoch_ms"] > after_epoch_ms
            else None
        ),
    )


def max_routed_lsn() -> str:
    return scalar(
        "SELECT coalesce(max(commit_lsn),'0/0'::pg_lsn)::text "
        "FROM shiba_internal.routed_transactions"
    )


def wait_for_routed(after_lsn: str) -> dict[str, Any]:
    def routed() -> dict[str, Any] | None:
        output = scalar(
            f"""
SELECT coalesce((
  SELECT json_build_object(
    'commit_lsn',commit_lsn::text,
    'routed_epoch_ms',extract(epoch FROM routed_at)*1000
  )::text
  FROM shiba_internal.routed_transactions
  WHERE commit_lsn > '{after_lsn}'::pg_lsn
  ORDER BY commit_lsn DESC LIMIT 1
),'null')
"""
        )
        value = json.loads(output)
        return value if value else None

    return wait_until(f"a routed transaction after {after_lsn}", routed)


def wait_for_inbox_present(
    commit_lsn: str, result_regclass: str = "shiba.perf_result"
) -> None:
    wait_until(
        f"inbox row for {commit_lsn}",
        lambda: int(
            scalar(
                f"""
SELECT count(*) FROM shiba_internal.dag_inbox
WHERE result_oid='{result_regclass}'::regclass
  AND commit_lsn='{commit_lsn}'::pg_lsn
"""
            )
        )
        > 0,
    )


def wait_for_inbox_ack(
    commit_lsn: str, result_regclass: str = "shiba.perf_result"
) -> float:
    def acknowledged() -> float | None:
        output = scalar(
            f"""
SELECT CASE WHEN NOT EXISTS (
  SELECT 1 FROM shiba_internal.dag_inbox
  WHERE result_oid='{result_regclass}'::regclass
    AND commit_lsn='{commit_lsn}'::pg_lsn
) THEN extract(epoch FROM clock_timestamp())*1000 END
"""
        )
        return float(output) if output else None

    return wait_until(f"inbox acknowledgement for {commit_lsn}", acknowledged)


def correctness_difference(defining_query: str) -> int:
    return int(
        scalar(
            f"""
WITH expected AS ({defining_query}),
actual AS (SELECT * FROM shiba.perf_result),
differences AS (
  (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
  UNION ALL
  (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT count(*) FROM differences
"""
        )
    )


def assert_correct(
    repetition: int,
    scenario: Scenario,
    checkpoint: str,
    defining_query: str,
) -> None:
    difference = correctness_difference(defining_query)
    metric(
        repetition,
        scenario.name,
        checkpoint,
        "correctness_difference",
        difference,
        "rows",
        "bidirectional EXCEPT ALL",
    )
    if difference != 0:
        raise BenchmarkError(
            f"{scenario.name}/{checkpoint}: {difference} correctness differences"
        )


def state_payload_bytes() -> int:
    return int(
        scalar(
            """
WITH state_rows(bytes) AS (
  SELECT pg_column_size(s) FROM shiba_internal.aggregate_state s
    WHERE result_oid='shiba.perf_result'::regclass
  UNION ALL
  SELECT pg_column_size(s) FROM shiba_internal.distinct_state s
    WHERE result_oid='shiba.perf_result'::regclass
  UNION ALL
  SELECT pg_column_size(s) FROM shiba_internal.join_arrangements s
    WHERE result_oid='shiba.perf_result'::regclass
  UNION ALL
  SELECT pg_column_size(s) FROM shiba_internal.window_rows s
    WHERE result_oid='shiba.perf_result'::regclass
  UNION ALL
  SELECT pg_column_size(s) FROM shiba_internal.projection_state s
    WHERE result_oid='shiba.perf_result'::regclass
  UNION ALL
  SELECT pg_column_size(s) FROM shiba_internal.topn_rows s
    WHERE result_oid='shiba.perf_result'::regclass
  UNION ALL
  SELECT pg_column_size(s) FROM shiba_internal.dag_inbox s
    WHERE result_oid='shiba.perf_result'::regclass
)
SELECT coalesce(sum(bytes),0) FROM state_rows
"""
        )
    )


def all_state_payload_bytes() -> int:
    return int(
        scalar(
            """
WITH state_rows(bytes) AS (
  SELECT pg_column_size(s) FROM shiba_internal.aggregate_state s
  UNION ALL SELECT pg_column_size(s) FROM shiba_internal.distinct_state s
  UNION ALL SELECT pg_column_size(s) FROM shiba_internal.join_arrangements s
  UNION ALL SELECT pg_column_size(s) FROM shiba_internal.window_rows s
  UNION ALL SELECT pg_column_size(s) FROM shiba_internal.projection_state s
  UNION ALL SELECT pg_column_size(s) FROM shiba_internal.topn_rows s
  UNION ALL SELECT pg_column_size(s) FROM shiba_internal.dag_inbox s
)
SELECT coalesce(sum(bytes),0) FROM state_rows
"""
        )
    )


def relation_bytes(qualified_name: str) -> int:
    return int(
        scalar(f"SELECT pg_total_relation_size('{qualified_name}'::regclass)")
    )


def physical_stage_stats(result_oids_sql: str) -> dict[str, int]:
    value = scalar(
        f"""
SELECT json_build_object(
  'relation_bytes',coalesce(sum(pg_total_relation_size(stage.relation_oid)),0),
  'live_tuples',coalesce(sum(statistics.n_live_tup),0),
  'dead_tuples',coalesce(sum(statistics.n_dead_tup),0),
  'autovacuum_count',coalesce(sum(statistics.autovacuum_count),0)
)::text
FROM shiba_internal.physical_stages stage
LEFT JOIN pg_stat_user_tables statistics
  ON statistics.relid=stage.relation_oid
WHERE stage.result_oid IN ({result_oids_sql})
"""
    )
    return {name: int(number) for name, number in json.loads(value).items()}


def graph_operators() -> list[str]:
    value = scalar(
        """
SELECT coalesce(json_agg(operator ORDER BY node_id)::text,'[]')
FROM shiba_internal.operator_instances
WHERE result_oid='shiba.perf_result'::regclass
"""
    )
    return json.loads(value)


def explain(query: str, destination: Path) -> dict[str, Any]:
    raw = psql(
        f"EXPLAIN (ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON) {query}",
        tuples=True,
    )
    destination.write_text(raw + "\n")
    document = json.loads(raw)[0]
    return {
        "planning_ms": float(document.get("Planning Time", 0)),
        "execution_ms": float(document.get("Execution Time", 0)),
        "shared_hit_blocks": int(document["Plan"].get("Shared Hit Blocks", 0)),
        "shared_read_blocks": int(document["Plan"].get("Shared Read Blocks", 0)),
        "temp_read_blocks": int(document["Plan"].get("Temp Read Blocks", 0)),
        "temp_written_blocks": int(document["Plan"].get("Temp Written Blocks", 0)),
        "wal_bytes": int(document["Plan"].get("WAL Bytes", 0)),
        "actual_rows": int(document["Plan"].get("Actual Rows", 0)),
    }


def write_query_script(path: Path, query: str) -> None:
    path.write_text(query.rstrip().rstrip(";") + ";\n")


def run_pgbench(
    repetition: int,
    scenario: str,
    phase: str,
    query: str,
    destination: Path,
) -> dict[str, float]:
    script = destination.with_suffix(".sql")
    write_query_script(script, query)
    with ResourceSampler(repetition, scenario, phase):
        completed = run(
            [
                str(PG_BIN / "pgbench"),
                "-h",
                str(SOCKET_DIR),
                "-p",
                str(PORT),
                "-d",
                DATABASE,
                "-n",
                "-r",
                "-c",
                str(QUERY_CLIENTS),
                "-j",
                str(QUERY_CLIENTS),
                "-T",
                str(QUERY_SECONDS),
                "-f",
                str(script),
            ]
        )
    raw = completed.stdout + completed.stderr
    destination.write_text(raw)
    failed_match = re.search(r"number of failed transactions: (\d+)", raw)
    latency_match = re.search(r"latency average = ([0-9.]+) ms", raw)
    tps_match = re.search(r"tps = ([0-9.]+)", raw)
    if not (failed_match and latency_match and tps_match):
        raise BenchmarkError(f"could not parse pgbench output for {scenario}/{phase}")
    values = {
        "failed": float(failed_match.group(1)),
        "latency_ms": float(latency_match.group(1)),
        "tps": float(tps_match.group(1)),
    }
    for name, value, unit in (
        ("failed_transactions", values["failed"], "transactions"),
        ("latency_average", values["latency_ms"], "ms"),
        ("tps", values["tps"], "transactions_per_second"),
    ):
        metric(repetition, scenario, phase, name, value, unit)
    if values["failed"] != 0:
        raise BenchmarkError(f"pgbench failures in {scenario}/{phase}")
    return values


def run_pgbench_fixed(
    repetition: int,
    scenario: str,
    phase: str,
    script: Path,
    *,
    clients: int,
    transactions_per_client: int,
    random_seed: int,
    destination: Path,
) -> dict[str, float]:
    with ResourceSampler(repetition, scenario, phase):
        started = time.perf_counter_ns()
        completed = run(
            [
                str(PG_BIN / "pgbench"),
                "-h",
                str(SOCKET_DIR),
                "-p",
                str(PORT),
                "-d",
                DATABASE,
                "-n",
                "-r",
                "-c",
                str(clients),
                "-j",
                str(clients),
                "-t",
                str(transactions_per_client),
                "--random-seed",
                str(random_seed),
                "-f",
                str(script),
            ]
        )
        wall_ms = (time.perf_counter_ns() - started) / 1_000_000
    raw = completed.stdout + completed.stderr
    destination.write_text(raw)
    failed_match = re.search(r"number of failed transactions: (\d+)", raw)
    latency_match = re.search(r"latency average = ([0-9.]+) ms", raw)
    tps_match = re.search(r"tps = ([0-9.]+)", raw)
    if not (failed_match and latency_match and tps_match):
        raise BenchmarkError(f"could not parse fixed pgbench output {scenario}/{phase}")
    values = {
        "failed": float(failed_match.group(1)),
        "latency_ms": float(latency_match.group(1)),
        "tps": float(tps_match.group(1)),
        "wall_ms": wall_ms,
    }
    for name, value, unit in (
        ("failed_transactions", values["failed"], "transactions"),
        ("latency_average", values["latency_ms"], "ms"),
        ("tps", values["tps"], "transactions_per_second"),
        ("wall_time", wall_ms, "ms"),
    ):
        metric(repetition, scenario, phase, name, value, unit)
    if values["failed"]:
        raise BenchmarkError(f"pgbench failures in {scenario}/{phase}")
    return values


def format_action(sql: str, schema: str) -> str:
    return sql.format(schema=schema)


def unique_probe(action: Action, sample: int) -> str:
    sql = action.sql
    bases = (10_000_000, 11_000_000, 12_000_000, 13_000_000, 16_000_000)
    for base in bases:
        if str(base) in sql:
            unique = sql.replace(str(base), str(base + sample * 100_000))
            unique = unique.replace(
                "1000000 - value", f"{1_000_000 + sample * 1_000} - value"
            )
            unique = unique.replace(
                "100000 + value", f"{100_000 + sample * 1_000} + value"
            )
            unique = unique.replace(
                "-100000 - value", f"{-100_000 - sample * 1_000} - value"
            )
            return unique
    raise BenchmarkError(f"no unique id base in latency action {action.name}")


def record_action(
    repetition: int,
    scenario: Scenario,
    action: Action,
    defining_query: str,
    *,
    backlog: bool = False,
    sample_kind: str = "semantic",
    sql_override: str | None = None,
    alternate: int = 0,
) -> None:
    baseline_sql = format_action(sql_override or action.sql, "baseline")
    shiba_sql = format_action(sql_override or action.sql, "source")

    runtime_pids: set[int] | None = None
    if backlog:
        runtime_pids = {row["owner_pid"] for row in runtime_topology()}
        psql(
            """
UPDATE shiba_internal.dag_runtime_state
SET active=false WHERE result_oid='shiba.perf_result'::regclass
"""
        )
        wait_until(
            "the Runtime to remain alive while the DAG is inactive",
            runtime_topology_ready,
        )

    before_progress = progress()
    before_phase_wal = lsn()
    before_phase_io = io_snapshot()

    def observed_transaction(sql: str) -> dict[str, Any]:
        observed_before_wal = lsn()
        observed_before_io = io_snapshot()
        xid_value, wall_ms = run_transaction(sql)
        observed_after_io = io_snapshot()
        observed_after_wal = lsn()
        return {
            "xid": xid_value,
            "wall_ms": wall_ms,
            "wal_bytes": wal_diff(observed_after_wal, observed_before_wal),
            "io": io_delta(observed_after_io, observed_before_io),
        }

    def finish_source(source_observation: dict[str, Any]) -> tuple[float, dict[str, Any], float]:
        commit_epoch = commit_epoch_ms(str(source_observation["xid"]))
        routed_value = wait_for_routed(before_routed_lsn)
        if backlog:
            wait_for_inbox_present(str(routed_value["commit_lsn"]))
            psql(
                """
UPDATE shiba_internal.dag_runtime_state
SET active=true WHERE result_oid='shiba.perf_result'::regclass;
SELECT shiba.activate()
"""
            )
            wait_until(
                "the same Runtime to drain the reactivated DAG",
                lambda: {
                    row["owner_pid"] for row in runtime_topology()
                }
                == runtime_pids,
            )
        ack_epoch = wait_for_inbox_ack(str(routed_value["commit_lsn"]))
        return commit_epoch, routed_value, ack_epoch

    before_routed_lsn = max_routed_lsn()
    with ResourceSampler(repetition, scenario.name, f"dml_{action.name}"):
        if alternate % 2 == 0:
            baseline_observation = observed_transaction(baseline_sql)
            source_observation = observed_transaction(shiba_sql)
            commit_ms, routed, ack_epoch_ms = finish_source(source_observation)
        else:
            source_observation = observed_transaction(shiba_sql)
            commit_ms, routed, ack_epoch_ms = finish_source(source_observation)
            baseline_observation = observed_transaction(baseline_sql)
    after_phase_io = io_snapshot()
    after_phase_wal = lsn()
    baseline_ms = float(baseline_observation["wall_ms"])
    source_ms = float(source_observation["wall_ms"])
    after_progress = progress()
    routed_ms = float(routed["routed_epoch_ms"])
    if (
        after_progress["applied_lsn"] == routed["commit_lsn"]
        and float(after_progress["updated_epoch_ms"])
        > float(before_progress["updated_epoch_ms"])
    ):
        applied_ms = float(after_progress["updated_epoch_ms"])
        apply_timestamp_source = "view_progress"
    else:
        applied_ms = ack_epoch_ms
        apply_timestamp_source = "inbox_ack_poll_upper_bound"
    commit_to_route = routed_ms - commit_ms
    route_to_apply = applied_ms - routed_ms
    commit_to_apply = applied_ms - commit_ms
    source_row_rate = (
        action.affected_rows * 1000 / commit_to_apply
        if commit_to_apply > 0
        else math.inf
    )

    ACTION_SAMPLES.append(
        {
            "repetition": repetition,
            "scenario": scenario.name,
            "action": action.name,
            "sample_kind": sample_kind,
            "backlog": backlog,
            "affected_rows": action.affected_rows,
            "baseline_commit_wall_ms": baseline_ms,
            "shiba_commit_wall_ms": source_ms,
            "commit_to_route_ms": commit_to_route,
            "route_to_apply_ms": route_to_apply,
            "commit_to_apply_ms": commit_to_apply,
            "apply_timestamp_source": apply_timestamp_source,
            "source_rows_per_second": source_row_rate,
            "baseline_ingress_observed_wal_bytes": baseline_observation["wal_bytes"],
            "shiba_ingress_observed_wal_bytes": source_observation["wal_bytes"],
            "combined_phase_wal_bytes": wal_diff(after_phase_wal, before_phase_wal),
            **{
                f"baseline_ingress_io_{key}": value
                for key, value in baseline_observation["io"].items()
            },
            **{
                f"shiba_ingress_io_{key}": value
                for key, value in source_observation["io"].items()
            },
            **{
                f"combined_phase_io_{key}": value
                for key, value in io_delta(after_phase_io, before_phase_io).items()
            },
            "boundary": action.boundary,
        }
    )
    for name, value, unit in (
        ("baseline_commit_wall", baseline_ms, "ms"),
        ("shiba_commit_wall", source_ms, "ms"),
        ("commit_to_route", commit_to_route, "ms"),
        ("route_to_apply", route_to_apply, "ms"),
        ("commit_to_apply", commit_to_apply, "ms"),
        ("source_rows_per_second", source_row_rate, "source_rows_per_second"),
        (
            "baseline_ingress_observed_wal_bytes",
            baseline_observation["wal_bytes"],
            "bytes",
        ),
        (
            "shiba_ingress_observed_wal_bytes",
            source_observation["wal_bytes"],
            "bytes",
        ),
        (
            "combined_phase_wal_bytes",
            wal_diff(after_phase_wal, before_phase_wal),
            "bytes",
        ),
    ):
        metric(repetition, scenario.name, f"dml_{action.name}", name, value, unit)
    for prefix, values in (
        ("baseline_ingress_io", baseline_observation["io"]),
        ("shiba_ingress_io", source_observation["io"]),
        ("combined_phase_io", io_delta(after_phase_io, before_phase_io)),
    ):
        for key, value in values.items():
            metric(
                repetition,
                scenario.name,
                f"dml_{action.name}",
                f"{prefix}_{key}",
                value,
                "",
            )

    assert_correct(repetition, scenario, f"after_{action.name}", defining_query)
    inbox = int(
        scalar(
            """
SELECT count(*) FROM shiba_internal.dag_inbox
WHERE result_oid='shiba.perf_result'::regclass
"""
        )
    )
    metric(repetition, scenario.name, f"after_{action.name}", "inbox_rows", inbox, "rows")
    if inbox != 0:
        raise BenchmarkError(f"{scenario.name}/{action.name}: inbox not drained")


def run_rollback(
    repetition: int,
    scenario: Scenario,
    defining_query: str,
) -> None:
    before = progress()
    sql = format_action(scenario.actions[0].sql, "source")
    psql(f"BEGIN; {sql.rstrip().rstrip(';')}; ROLLBACK;")
    time.sleep(0.15)
    after = progress()
    unchanged = float(after["updated_epoch_ms"]) == float(before["updated_epoch_ms"])
    metric(
        repetition,
        scenario.name,
        "rollback",
        "progress_unchanged",
        int(unchanged),
        "boolean",
    )
    if not unchanged:
        raise BenchmarkError(f"{scenario.name}: rolled-back transaction advanced progress")
    assert_correct(repetition, scenario, "after_rollback", defining_query)


def record_explain_metrics(
    repetition: int,
    scenario: str,
    phase: str,
    values: dict[str, Any],
) -> None:
    units = {
        "planning_ms": "ms",
        "execution_ms": "ms",
        "shared_hit_blocks": "blocks",
        "shared_read_blocks": "blocks",
        "temp_read_blocks": "blocks",
        "temp_written_blocks": "blocks",
        "wal_bytes": "bytes",
        "actual_rows": "rows",
    }
    for name, value in values.items():
        metric(repetition, scenario, phase, name, value, units[name])


def run_scenario(repetition: int, scenario: Scenario, run_dir: Path) -> set[str]:
    print(f"[run {repetition}] {scenario.name}", flush=True)
    create_database()
    scenario_dir = run_dir / scenario.name
    scenario_dir.mkdir(parents=True)
    try:
        psql(scenario.setup_sql.format(schema="baseline"))
        psql(scenario.setup_sql.format(schema="source"))
        defining_query = scenario.defining_query.format(schema="source")
        baseline_query = scenario.defining_query.format(schema="baseline")
        view_sql = f"CREATE TABLE shiba.perf_result AS {defining_query}"

        before_lsn = lsn()
        before_io = io_snapshot()
        started = time.perf_counter_ns()
        psql(view_sql)
        backfill_ms = (time.perf_counter_ns() - started) / 1_000_000
        after_io = io_snapshot()
        after_lsn = lsn()
        metric(repetition, scenario.name, "backfill", "wall_time", backfill_ms, "ms")
        metric(
            repetition,
            scenario.name,
            "backfill",
            "source_rows_per_second",
            scenario.source_rows * 1000 / backfill_ms,
            "rows_per_second",
        )
        metric(
            repetition,
            scenario.name,
            "backfill",
            "wal_bytes",
            wal_diff(after_lsn, before_lsn),
            "bytes",
        )
        for key, value in io_delta(after_io, before_io).items():
            metric(repetition, scenario.name, "backfill", f"io_{key}", value, "")

        wait_until("single Runtime", runtime_topology_ready)
        operators = graph_operators()
        operator_set = set(operators)
        missing = set(scenario.required_operators) - operator_set
        (scenario_dir / "operators.json").write_text(
            json.dumps(
                {
                    "actual": operators,
                    "required": scenario.required_operators,
                    "missing": sorted(missing),
                },
                indent=2,
            )
            + "\n"
        )
        if missing:
            raise BenchmarkError(f"{scenario.name}: missing graph operators {sorted(missing)}")
        assert_correct(repetition, scenario, "after_backfill", defining_query)

        # PostgreSQL-buffer-cold plans. OS cache is intentionally not claimed cold.
        restart_cluster()
        source_cold = explain(
            defining_query, scenario_dir / "explain-source-buffer-cold.json"
        )
        restart_cluster()
        result_cold = explain(
            "SELECT * FROM shiba.perf_result",
            scenario_dir / "explain-result-buffer-cold.json",
        )
        record_explain_metrics(
            repetition, scenario.name, "source_buffer_cold", source_cold
        )
        record_explain_metrics(
            repetition, scenario.name, "result_buffer_cold", result_cold
        )

        # The Runtime is a dynamic BGW. A full postmaster restart intentionally
        # requires explicit activation (or the next registered-source statement)
        # before asynchronous work resumes.
        psql("SELECT shiba.activate()")
        wait_until(
            "single Runtime after buffer-cold restarts",
            runtime_topology_ready,
        )
        record_runtime_topology()

        # Warm each relation before timed concurrent reads; alternate order by run.
        psql(defining_query)
        psql("SELECT * FROM shiba.perf_result")
        if repetition % 2:
            source_pgbench = run_pgbench(
                repetition,
                scenario.name,
                "query_source_warm",
                defining_query,
                scenario_dir / "pgbench-source.txt",
            )
            result_pgbench = run_pgbench(
                repetition,
                scenario.name,
                "query_result_warm",
                "SELECT * FROM shiba.perf_result",
                scenario_dir / "pgbench-result.txt",
            )
        else:
            result_pgbench = run_pgbench(
                repetition,
                scenario.name,
                "query_result_warm",
                "SELECT * FROM shiba.perf_result",
                scenario_dir / "pgbench-result.txt",
            )
            source_pgbench = run_pgbench(
                repetition,
                scenario.name,
                "query_source_warm",
                defining_query,
                scenario_dir / "pgbench-source.txt",
            )
        metric(
            repetition,
            scenario.name,
            "query_comparison",
            "latency_speedup",
            source_pgbench["latency_ms"] / result_pgbench["latency_ms"],
            "ratio",
        )
        metric(
            repetition,
            scenario.name,
            "query_comparison",
            "throughput_speedup",
            result_pgbench["tps"] / source_pgbench["tps"],
            "ratio",
        )

        run_rollback(repetition, scenario, defining_query)
        for index, action in enumerate(scenario.actions):
            record_action(
                repetition,
                scenario,
                action,
                defining_query,
                backlog=index == 0,
                alternate=repetition + index,
            )

        latency_values: list[float] = []
        for sample in range(1, LATENCY_PROBES + 1):
            action = scenario.actions[0]
            sql = unique_probe(action, sample)
            prior_count = len(ACTION_SAMPLES)
            record_action(
                repetition,
                scenario,
                Action(
                    f"latency_probe_{sample}",
                    sql,
                    action.affected_rows,
                    action.boundary,
                ),
                defining_query,
                sample_kind="latency_probe",
                alternate=repetition + sample,
            )
            latency_values.append(
                float(ACTION_SAMPLES[prior_count]["commit_to_apply_ms"])
            )
        for name, value in (
            ("min", min(latency_values)),
            ("mean", statistics.fmean(latency_values)),
            ("p50", percentile(latency_values, 0.50)),
            ("p95", percentile(latency_values, 0.95)),
            ("p99", percentile(latency_values, 0.99)),
            ("max", max(latency_values)),
        ):
            metric(repetition, scenario.name, "visibility_latency", name, value, "ms")

        state_bytes = state_payload_bytes()
        result_bytes = relation_bytes("shiba.perf_result")
        source_bytes = sum(
            relation_bytes(f"source.{table}")
            for table in (
                ["wide_events"]
                if scenario.family == "typed_filter"
                else (
                    ["events"]
                    if scenario.family
                    in {
                        "aggregate",
                        "filter",
                        "having",
                        "distinct_aggregate",
                        "distinct",
                        "topn",
                        "window",
                    }
                    else (
                        ["facts", "dims"]
                        if scenario.family in {"join", "composed_join"}
                        else ["orders", "permits"]
                    )
                )
            )
        )
        for name, value in (
            ("operator_state_payload_bytes", state_bytes),
            ("result_relation_bytes", result_bytes),
            ("source_relation_bytes", source_bytes),
            ("database_bytes", int(scalar("SELECT pg_database_size(current_database())"))),
        ):
            metric(repetition, scenario.name, "space", name, value, "bytes")
        for name, value in physical_stage_stats(
            "'shiba.perf_result'::regclass"
        ).items():
            metric(
                repetition,
                scenario.name,
                "physical_stage",
                name,
                value,
                "bytes" if name == "relation_bytes" else "count",
            )
        final_difference = correctness_difference(defining_query)
        final_inbox = int(
            scalar(
                """
SELECT count(*) FROM shiba_internal.dag_inbox
WHERE result_oid='shiba.perf_result'::regclass
"""
            )
        )
        summary = {
            "repetition": repetition,
            "scenario": scenario.name,
            "family": scenario.family,
            "profile": scenario.profile,
            "operators": operators,
            "correctness_difference": final_difference,
            "inbox_rows": final_inbox,
            "source_rows": scenario.source_rows,
            "notes": scenario.notes,
        }
        SCENARIO_SUMMARIES.append(summary)
        (scenario_dir / "summary.json").write_text(
            json.dumps(summary, indent=2) + "\n"
        )
        if final_difference != 0 or final_inbox != 0:
            raise BenchmarkError(f"{scenario.name}: invalid final state {summary}")

        psql("DROP TABLE shiba.perf_result")
        wait_until(
            "Runtime to remain after dropping the DAG",
            runtime_topology_ready,
        )
        return operator_set
    finally:
        # A failed scenario retains its evidence, but still releases the database.
        try:
            if scalar("SELECT to_regclass('shiba.perf_result') IS NOT NULL") == "t":
                psql("DROP TABLE shiba.perf_result")
        except Exception:
            pass
        destroy_database()


def run_multidag(repetition: int, run_dir: Path) -> None:
    name = "multi_dag_fanout"
    print(f"[run {repetition}] {name}", flush=True)
    create_database()
    scenario_dir = run_dir / name
    scenario_dir.mkdir(parents=True)
    try:
        setup = (
            build_scenarios(rows=ROWS, groups=GROUPS, mutations=MUTATIONS)[0]
            .setup_sql.format(schema="source")
        )
        psql(setup)
        definitions = {
            "fanout_aggregate": """SELECT category_id,count(*) AS row_count,
                                         sum(amount) AS total_amount
                                  FROM source.events GROUP BY category_id""",
            "fanout_filter": """SELECT category_id,count(*) AS row_count,
                                      sum(amount) AS total_amount
                               FROM source.events WHERE active AND amount>=500
                               GROUP BY category_id""",
            "fanout_distinct": "SELECT DISTINCT category_id,label FROM source.events",
            "fanout_topn": """SELECT row_id,category_id,score,amount
                             FROM source.events
                             ORDER BY score DESC NULLS LAST LIMIT 50""",
        }
        for result_name, query in definitions.items():
            psql(f"CREATE TABLE shiba.{result_name} AS {query}")
        wait_until(
            "single Runtime for four DAGs",
            runtime_topology_ready,
        )
        fanout_oids = """
  'shiba.fanout_aggregate'::regclass,
  'shiba.fanout_filter'::regclass,
  'shiba.fanout_distinct'::regclass,
  'shiba.fanout_topn'::regclass
"""
        psql(
            f"""
UPDATE shiba_internal.dag_runtime_state
SET active=false
WHERE result_oid IN ({fanout_oids})
"""
        )
        wait_until(
            "all fanout DAGs to be paused",
            lambda: int(
                scalar(
                    f"""
SELECT count(*) FROM shiba_internal.dag_runtime_state
WHERE result_oid IN ({fanout_oids}) AND active
"""
                )
            )
            == 0,
        )
        record_runtime_topology()
        before_routed = max_routed_lsn()
        before_wal = lsn()
        before_io = io_snapshot()
        before = time.perf_counter_ns()
        with ResourceSampler(repetition, name, "fanout_apply"):
            xid, source_wall = run_transaction(
                f"""
INSERT INTO source.events
SELECT 19000000 + value,value % {GROUPS},value,value,
       1 + value,200000 + value,true
FROM generate_series(1,{MUTATIONS * 10}) value
"""
            )
            commit_ms = commit_epoch_ms(xid)
            routed = wait_for_routed(before_routed)
            payload_rows = int(
                scalar(
                    "SELECT count(*) FROM shiba_internal.change_log "
                    f"WHERE commit_lsn='{routed['commit_lsn']}'::pg_lsn"
                )
            )
            inbox_references = int(
                scalar(
                    "SELECT count(*) FROM shiba_internal.dag_inbox "
                    f"WHERE result_oid IN ({fanout_oids}) "
                    f"AND commit_lsn='{routed['commit_lsn']}'::pg_lsn"
                )
            )
            metric(
                repetition,
                name,
                "fanout_storage",
                "change_log_payload_rows",
                payload_rows,
                "rows",
            )
            metric(
                repetition,
                name,
                "fanout_storage",
                "dag_inbox_reference_rows",
                inbox_references,
                "rows",
            )
            metric(
                repetition,
                name,
                "fanout_storage",
                "payload_rows_per_source_delta",
                payload_rows / (MUTATIONS * 10),
                "ratio",
            )
            metric(
                repetition,
                name,
                "fanout_storage",
                "inbox_references_per_dag_transaction",
                inbox_references / len(definitions),
                "ratio",
            )
            if payload_rows != MUTATIONS * 10 or inbox_references != len(definitions):
                raise BenchmarkError(
                    "shared payload invariant failed: "
                    f"payload={payload_rows}, inbox={inbox_references}"
                )
            psql(
                f"""
UPDATE shiba_internal.dag_runtime_state
SET active=true
WHERE result_oid IN ({fanout_oids});
SELECT shiba.activate()
"""
            )
            wait_until(
                "all fanout DAGs to apply",
                lambda: int(
                    scalar(
                        """
SELECT count(*) FROM shiba_internal.dag_inbox
WHERE result_oid IN (
%s
) AND commit_lsn='%s'::pg_lsn
"""
                        % (fanout_oids, routed["commit_lsn"])
                    )
                )
                == 0,
            )
        elapsed = (time.perf_counter_ns() - before) / 1_000_000
        after_io = io_snapshot()
        after_wal = lsn()
        metric(repetition, name, "fanout_apply", "source_commit_wall", source_wall, "ms")
        metric(repetition, name, "fanout_apply", "end_to_end_wall", elapsed, "ms")
        metric(
            repetition,
            name,
            "fanout_apply",
            "commit_to_route",
            float(routed["routed_epoch_ms"]) - commit_ms,
            "ms",
        )
        metric(
            repetition,
            name,
            "fanout_apply",
            "combined_phase_wal_bytes",
            wal_diff(after_wal, before_wal),
            "bytes",
        )
        for key, value in io_delta(after_io, before_io).items():
            metric(
                repetition,
                name,
                "fanout_apply",
                f"combined_phase_io_{key}",
                value,
                "",
            )
        metric(
            repetition,
            name,
            "fanout_apply",
            "fanout_source_deliveries_per_second",
            MUTATIONS * 10 * 4 * 1000 / elapsed,
            "source_row_deliveries_per_second",
        )
        differences: dict[str, int] = {}
        for result_name, query in definitions.items():
            differences[result_name] = int(
                scalar(
                    f"""
WITH expected AS ({query}),actual AS (SELECT * FROM shiba.{result_name}),
d AS (
 (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
 UNION ALL
 (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT count(*) FROM d
"""
                )
            )
            metric(
                repetition,
                name,
                f"correctness_{result_name}",
                "correctness_difference",
                differences[result_name],
                "rows",
            )
        (scenario_dir / "correctness.json").write_text(
            json.dumps(differences, indent=2) + "\n"
        )
        if any(differences.values()):
            raise BenchmarkError(f"multi-DAG correctness failure: {differences}")
        inbox = int(scalar("SELECT count(*) FROM shiba_internal.dag_inbox"))
        metric(repetition, name, "final", "inbox_rows", inbox, "rows")
        metric(
            repetition,
            name,
            "space",
            "operator_state_payload_bytes",
            all_state_payload_bytes(),
            "bytes",
        )
        metric(
            repetition,
            name,
            "space",
            "source_relation_bytes",
            relation_bytes("source.events"),
            "bytes",
        )
        metric(
            repetition,
            name,
            "space",
            "result_relation_bytes",
            sum(relation_bytes(f"shiba.{result}") for result in definitions),
            "bytes",
        )
        for metric_name, value in physical_stage_stats(fanout_oids).items():
            metric(
                repetition,
                name,
                "physical_stage",
                metric_name,
                value,
                "bytes" if metric_name == "relation_bytes" else "count",
            )
        if inbox:
            raise BenchmarkError(f"multi-DAG inbox not empty: {inbox}")
        for result_name in reversed(list(definitions)):
            psql(f"DROP TABLE shiba.{result_name}")
        wait_until(
            "Runtime to remain after dropping fanout DAGs",
            runtime_topology_ready,
        )
    finally:
        try:
            for result_name in (
                "fanout_topn",
                "fanout_distinct",
                "fanout_filter",
                "fanout_aggregate",
            ):
                if scalar(f"SELECT to_regclass('shiba.{result_name}') IS NOT NULL") == "t":
                    psql(f"DROP TABLE shiba.{result_name}")
        except Exception:
            pass
        destroy_database()


def run_multisource_multidag(repetition: int, run_dir: Path) -> None:
    name = "multi_source_multi_dag"
    print(f"[run {repetition}] {name}", flush=True)
    create_database()
    scenario_dir = run_dir / name
    scenario_dir.mkdir(parents=True)
    try:
        join_scenario = next(
            scenario
            for scenario in build_scenarios(
                rows=ROWS, groups=GROUPS, mutations=MUTATIONS
            )
            if scenario.name == "inner_join_1to1"
        )
        psql(join_scenario.setup_sql.format(schema="source"))
        definitions = {
            "source_fact_stats": """SELECT gate AS group_key,count(*) AS row_count,
                                           sum(amount) AS total_amount
                                    FROM source.facts GROUP BY gate""",
            "source_dim_stats": """SELECT group_id AS group_key,count(*) AS row_count,
                                          sum(threshold) AS total_amount
                                   FROM source.dims GROUP BY group_id""",
            "source_join_stats": """SELECT d.group_id AS group_key,
                                           count(*) AS row_count,
                                           sum(f.amount) AS total_amount
                                    FROM source.facts f JOIN source.dims d
                                      ON f.join_key=d.join_key
                                    GROUP BY d.group_id""",
        }
        for result_name, query in definitions.items():
            psql(f"CREATE TABLE shiba.{result_name} AS {query}")
        wait_until(
            "single Runtime for three multi-source DAGs",
            runtime_topology_ready,
        )
        multisource_oids = """
  'shiba.source_fact_stats'::regclass,
  'shiba.source_dim_stats'::regclass,
  'shiba.source_join_stats'::regclass
"""
        psql(
            f"""
UPDATE shiba_internal.dag_runtime_state
SET active=false
WHERE result_oid IN ({multisource_oids})
"""
        )
        wait_until(
            "all multi-source fanout DAGs to be paused",
            lambda: int(
                scalar(
                    f"""
SELECT count(*) FROM shiba_internal.dag_runtime_state
WHERE result_oid IN ({multisource_oids}) AND active
"""
                )
            )
            == 0,
        )
        record_runtime_topology()
        before_routed = max_routed_lsn()
        before_wal = lsn()
        before_io = io_snapshot()
        started = time.perf_counter_ns()
        with ResourceSampler(repetition, name, "fanout_apply"):
            xid, source_wall = run_transaction(
                f"""
INSERT INTO source.facts
SELECT 20000000 + value,50000 + value,700 + value,value % 2
FROM generate_series(1,{MUTATIONS * 5}) value;
INSERT INTO source.dims
SELECT 21000000 + value,50000 + value,value % 31,500,value % 2
FROM generate_series(1,{MUTATIONS * 5}) value
"""
            )
            commit_ms = commit_epoch_ms(xid)
            routed = wait_for_routed(before_routed)
            payload_rows = int(
                scalar(
                    "SELECT count(*) FROM shiba_internal.change_log "
                    f"WHERE commit_lsn='{routed['commit_lsn']}'::pg_lsn"
                )
            )
            inbox_references = int(
                scalar(
                    "SELECT count(*) FROM shiba_internal.dag_inbox "
                    f"WHERE result_oid IN ({multisource_oids}) "
                    f"AND commit_lsn='{routed['commit_lsn']}'::pg_lsn"
                )
            )
            metric(
                repetition,
                name,
                "fanout_storage",
                "change_log_payload_rows",
                payload_rows,
                "rows",
            )
            metric(
                repetition,
                name,
                "fanout_storage",
                "dag_inbox_reference_rows",
                inbox_references,
                "rows",
            )
            metric(
                repetition,
                name,
                "fanout_storage",
                "payload_rows_per_source_delta",
                payload_rows / (MUTATIONS * 10),
                "ratio",
            )
            metric(
                repetition,
                name,
                "fanout_storage",
                "inbox_references_per_dag_transaction",
                inbox_references / len(definitions),
                "ratio",
            )
            if payload_rows != MUTATIONS * 10 or inbox_references != len(definitions):
                raise BenchmarkError(
                    "multi-source shared payload invariant failed: "
                    f"payload={payload_rows}, inbox={inbox_references}"
                )
            psql(
                f"""
UPDATE shiba_internal.dag_runtime_state
SET active=true
WHERE result_oid IN ({multisource_oids});
SELECT shiba.activate()
"""
            )
            wait_until(
                "multi-source fanout inbox drain",
                lambda: int(
                    scalar(
                        f"""
SELECT count(*) FROM shiba_internal.dag_inbox
WHERE result_oid IN (
{multisource_oids}
) AND commit_lsn='{routed["commit_lsn"]}'::pg_lsn
"""
                )
                )
                == 0,
            )
        elapsed = (time.perf_counter_ns() - started) / 1_000_000
        after_io = io_snapshot()
        after_wal = lsn()
        metric(repetition, name, "fanout_apply", "source_commit_wall", source_wall, "ms")
        metric(repetition, name, "fanout_apply", "end_to_end_wall", elapsed, "ms")
        metric(
            repetition,
            name,
            "fanout_apply",
            "commit_to_route",
            float(routed["routed_epoch_ms"]) - commit_ms,
            "ms",
        )
        metric(
            repetition,
            name,
            "fanout_apply",
            "combined_phase_wal_bytes",
            wal_diff(after_wal, before_wal),
            "bytes",
        )
        for key, value in io_delta(after_io, before_io).items():
            metric(
                repetition,
                name,
                "fanout_apply",
                f"combined_phase_io_{key}",
                value,
                "",
            )
        differences: dict[str, int] = {}
        for result_name, query in definitions.items():
            differences[result_name] = int(
                scalar(
                    f"""
WITH expected AS ({query}),actual AS (SELECT * FROM shiba.{result_name}),
d AS (
 (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
 UNION ALL
 (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT count(*) FROM d
"""
                )
            )
            metric(
                repetition,
                name,
                f"correctness_{result_name}",
                "correctness_difference",
                differences[result_name],
                "rows",
            )
        (scenario_dir / "correctness.json").write_text(
            json.dumps(differences, indent=2) + "\n"
        )
        if any(differences.values()):
            raise BenchmarkError(
                f"multi-source multi-DAG correctness failure: {differences}"
            )
        inbox = int(scalar("SELECT count(*) FROM shiba_internal.dag_inbox"))
        metric(repetition, name, "final", "inbox_rows", inbox, "rows")
        metric(
            repetition,
            name,
            "space",
            "operator_state_payload_bytes",
            all_state_payload_bytes(),
            "bytes",
        )
        metric(
            repetition,
            name,
            "space",
            "source_relation_bytes",
            relation_bytes("source.facts") + relation_bytes("source.dims"),
            "bytes",
        )
        metric(
            repetition,
            name,
            "space",
            "result_relation_bytes",
            sum(relation_bytes(f"shiba.{result}") for result in definitions),
            "bytes",
        )
        for metric_name, value in physical_stage_stats(multisource_oids).items():
            metric(
                repetition,
                name,
                "physical_stage",
                metric_name,
                value,
                "bytes" if metric_name == "relation_bytes" else "count",
            )
        if inbox:
            raise BenchmarkError(f"multi-source multi-DAG inbox not empty: {inbox}")
        for result_name in reversed(list(definitions)):
            psql(f"DROP TABLE shiba.{result_name}")
        wait_until(
            "Runtime to remain after dropping multi-source DAGs",
            runtime_topology_ready,
        )
    finally:
        try:
            for result_name in (
                "source_join_stats",
                "source_dim_stats",
                "source_fact_stats",
            ):
                if scalar(f"SELECT to_regclass('shiba.{result_name}') IS NOT NULL") == "t":
                    psql(f"DROP TABLE shiba.{result_name}")
        except Exception:
            pass
        destroy_database()


def run_ingress_controls(repetition: int, run_dir: Path) -> None:
    name = "ingress_concurrency_and_batching"
    print(f"[run {repetition}] {name}", flush=True)
    create_database()
    scenario_dir = run_dir / name
    scenario_dir.mkdir(parents=True)
    try:
        psql(
            f"""
CREATE SCHEMA baseline;
CREATE SCHEMA source;
CREATE TABLE baseline.ingress (
  event_id bigint NOT NULL,group_id integer NOT NULL,amount integer NOT NULL
);
CREATE TABLE source.ingress (
  event_id bigint NOT NULL,group_id integer NOT NULL,amount integer NOT NULL
);
CREATE SEQUENCE baseline.ingress_id START 1000000;
CREATE SEQUENCE source.ingress_id START 1000000;
INSERT INTO baseline.ingress
SELECT value,value % {GROUPS},1 + value % 1000
FROM generate_series(1,{ROWS}) value;
INSERT INTO source.ingress SELECT * FROM baseline.ingress;
CREATE TABLE shiba.ingress_stats AS
SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
FROM source.ingress GROUP BY group_id;
"""
        )
        wait_until(
            "single Runtime for ingress DAG",
            runtime_topology_ready,
        )
        scripts: dict[str, Path] = {}
        for schema in ("baseline", "source"):
            path = scenario_dir / f"insert-{schema}.sql"
            path.write_text(
                f"""\\set group_seed random(1,{GROUPS})
INSERT INTO {schema}.ingress(event_id,group_id,amount)
SELECT nextval('{schema}.ingress_id'),(:group_seed + value) % {GROUPS},
       1 + ((:group_seed * 31 + value) % 1000)
FROM generate_series(1,10) value;
"""
            )
            scripts[schema] = path

        profiles = (("single_client", 1, 100), ("four_clients", 4, 50))
        for profile_index, (profile, clients, tx_per_client) in enumerate(profiles):
            seed = SEED + repetition * 100 + profile_index
            before_routed = max_routed_lsn()
            phase_started = time.perf_counter_ns()
            if (repetition + profile_index) % 2:
                baseline_values = run_pgbench_fixed(
                    repetition,
                    name,
                    f"{profile}_baseline",
                    scripts["baseline"],
                    clients=clients,
                    transactions_per_client=tx_per_client,
                    random_seed=seed,
                    destination=scenario_dir / f"pgbench-{profile}-baseline.txt",
                )
                shiba_values = run_pgbench_fixed(
                    repetition,
                    name,
                    f"{profile}_shiba",
                    scripts["source"],
                    clients=clients,
                    transactions_per_client=tx_per_client,
                    random_seed=seed,
                    destination=scenario_dir / f"pgbench-{profile}-shiba.txt",
                )
            else:
                shiba_values = run_pgbench_fixed(
                    repetition,
                    name,
                    f"{profile}_shiba",
                    scripts["source"],
                    clients=clients,
                    transactions_per_client=tx_per_client,
                    random_seed=seed,
                    destination=scenario_dir / f"pgbench-{profile}-shiba.txt",
                )
                baseline_values = run_pgbench_fixed(
                    repetition,
                    name,
                    f"{profile}_baseline",
                    scripts["baseline"],
                    clients=clients,
                    transactions_per_client=tx_per_client,
                    random_seed=seed,
                    destination=scenario_dir / f"pgbench-{profile}-baseline.txt",
                )
            expected_source_rows = int(
                scalar("SELECT count(*) FROM source.ingress")
            )
            wait_until(
                f"{profile} result row-count convergence",
                lambda: int(
                    scalar(
                        "SELECT coalesce(sum(row_count),0) FROM shiba.ingress_stats"
                    )
                )
                == expected_source_rows,
            )
            e2e_ms = (time.perf_counter_ns() - phase_started) / 1_000_000
            transaction_count = clients * tx_per_client
            row_count = transaction_count * 10
            metric(
                repetition,
                name,
                profile,
                "source_tps_ratio",
                shiba_values["tps"] / baseline_values["tps"],
                "ratio",
            )
            metric(
                repetition,
                name,
                profile,
                "end_to_end_commits_per_second",
                transaction_count * 1000 / e2e_ms,
                "commits_per_second",
            )
            metric(
                repetition,
                name,
                profile,
                "end_to_end_rows_per_second",
                row_count * 1000 / e2e_ms,
                "rows_per_second",
            )
            difference = int(
                scalar(
                    """
WITH expected AS (
 SELECT group_id,count(*)::bigint AS row_count,sum(amount)::bigint AS total_amount
 FROM source.ingress GROUP BY group_id
),actual AS (
 SELECT group_id,row_count::bigint,total_amount::bigint FROM shiba.ingress_stats
),d AS (
 (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
 UNION ALL
 (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
) SELECT count(*) FROM d
"""
                )
            )
            metric(repetition, name, profile, "correctness_difference", difference, "rows")
            if difference:
                raise BenchmarkError(f"ingress control {profile} differs by {difference}")

        # Pause the logical DAG so routing storage and relation-driven apply are
        # observable while the one physical Runtime remains alive. The sampler
        # records both the complete PostgreSQL process tree and the Runtime PID.
        psql(
            """
UPDATE shiba_internal.dag_runtime_state
SET active=false
WHERE result_oid='shiba.ingress_stats'::regclass
"""
        )
        wait_until(
            "the ingress DAG to be paused",
            lambda: scalar(
                """
SELECT active::text FROM shiba_internal.dag_runtime_state
WHERE result_oid='shiba.ingress_stats'::regclass
"""
            )
            == "false",
        )
        record_runtime_topology()
        before_routed = max_routed_lsn()
        started = time.perf_counter_ns()
        with ResourceSampler(repetition, name, "large_transaction"):
            xid, source_wall = run_transaction(
                f"""
INSERT INTO source.ingress
SELECT nextval('source.ingress_id'),value % {GROUPS},1 + value % 1000
FROM generate_series(1,{LARGE_TRANSACTION_ROWS}) value
"""
            )
            commit_ms = commit_epoch_ms(xid)
            routed = wait_for_routed(before_routed)
            payload_rows = int(
                scalar(
                    "SELECT count(*) FROM shiba_internal.change_log "
                    f"WHERE commit_lsn='{routed['commit_lsn']}'::pg_lsn"
                )
            )
            inbox_references = int(
                scalar(
                    """
SELECT count(*) FROM shiba_internal.dag_inbox
WHERE result_oid='shiba.ingress_stats'::regclass
"""
                    f"AND commit_lsn='{routed['commit_lsn']}'::pg_lsn"
                )
            )
            metric(
                repetition,
                name,
                "large_transaction",
                "change_log_payload_rows",
                payload_rows,
                "rows",
            )
            metric(
                repetition,
                name,
                "large_transaction",
                "dag_inbox_reference_rows",
                inbox_references,
                "rows",
            )
            if (
                payload_rows != LARGE_TRANSACTION_ROWS
                or inbox_references != 1
            ):
                raise BenchmarkError(
                    "large transaction storage invariant failed: "
                    f"payload={payload_rows}, inbox={inbox_references}"
                )
            psql(
                """
UPDATE shiba_internal.dag_runtime_state
SET active=true
WHERE result_oid='shiba.ingress_stats'::regclass;
SELECT shiba.activate()
"""
            )
            expected_source_rows = int(
                scalar("SELECT count(*) FROM source.ingress")
            )
            ack_ms = wait_until(
                "large transaction result row-count convergence",
                lambda: (
                    time.time_ns() / 1_000_000
                    if int(
                        scalar(
                            "SELECT coalesce(sum(row_count),0) "
                            "FROM shiba.ingress_stats"
                        )
                    )
                    == expected_source_rows
                    else None
                ),
            )
            wait_for_inbox_ack(
                str(routed["commit_lsn"]), "shiba.ingress_stats"
            )
        elapsed = (time.perf_counter_ns() - started) / 1_000_000
        metric(repetition, name, "large_transaction", "source_commit_wall", source_wall, "ms")
        metric(repetition, name, "large_transaction", "end_to_end_wall", elapsed, "ms")
        metric(
            repetition,
            name,
            "large_transaction",
            "commit_to_ack",
            ack_ms - commit_ms,
            "ms",
        )
        metric(
            repetition,
            name,
            "large_transaction",
            "rows_per_second",
            LARGE_TRANSACTION_ROWS * 1000 / max(ack_ms - commit_ms, 0.001),
            "rows_per_second",
        )
        difference = int(
            scalar(
                """
WITH expected AS (
 SELECT group_id,count(*)::bigint AS row_count,sum(amount)::bigint AS total_amount
 FROM source.ingress GROUP BY group_id
),actual AS (
 SELECT group_id,row_count::bigint,total_amount::bigint FROM shiba.ingress_stats
),d AS (
 (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
 UNION ALL
 (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
) SELECT count(*) FROM d
"""
            )
        )
        metric(
            repetition,
            name,
            "large_transaction",
            "correctness_difference",
            difference,
            "rows",
        )
        if difference:
            raise BenchmarkError(f"large ingress transaction differs by {difference}")
        psql("DROP TABLE shiba.ingress_stats")
        wait_until(
            "Runtime to remain after dropping ingress DAG",
            runtime_topology_ready,
        )
    finally:
        try:
            if scalar("SELECT to_regclass('shiba.ingress_stats') IS NOT NULL") == "t":
                psql("DROP TABLE shiba.ingress_stats")
        except Exception:
            pass
        destroy_database()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def aggregate_metrics() -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[float]] = {}
    units: dict[tuple[str, str, str, str], str] = {}
    for row in METRICS:
        key = (row["scenario"], row["phase"], row["metric"], row["notes"])
        try:
            grouped.setdefault(key, []).append(float(row["value"]))
            units[key] = row["unit"]
        except (TypeError, ValueError):
            continue
    aggregated: list[dict[str, Any]] = []
    for key, values in sorted(grouped.items()):
        scenario, phase, name, notes = key
        mean = statistics.fmean(values)
        stdev = statistics.stdev(values) if len(values) > 1 else 0.0
        aggregated.append(
            {
                "scenario": scenario,
                "phase": phase,
                "metric": name,
                "unit": units[key],
                "runs": len(values),
                "median": statistics.median(values),
                "mean": mean,
                "stdev": stdev,
                "cv_percent": (stdev / mean * 100) if mean else 0.0,
                "min": min(values),
                "max": max(values),
                "notes": notes,
            }
        )
    return aggregated


def aggregate_latency_samples() -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = {}
    for row in ACTION_SAMPLES:
        if row.get("sample_kind") == "latency_probe":
            grouped.setdefault(str(row["scenario"]), []).append(
                float(row["commit_to_apply_ms"])
            )
    output: list[dict[str, Any]] = []
    for scenario, values in sorted(grouped.items()):
        output.append(
            {
                "scenario": scenario,
                "raw_samples": len(values),
                "repetitions": len(
                    {
                        row["repetition"]
                        for row in ACTION_SAMPLES
                        if row.get("sample_kind") == "latency_probe"
                        and row["scenario"] == scenario
                    }
                ),
                "min_ms": min(values),
                "mean_ms": statistics.fmean(values),
                "p50_ms": percentile(values, 0.50),
                "p95_ms": percentile(values, 0.95),
                "p99_ms": percentile(values, 0.99),
                "max_ms": max(values),
                "stdev_ms": statistics.stdev(values) if len(values) > 1 else 0.0,
            }
        )
    return output


def aggregate_latency_by_run() -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[float]] = {}
    for row in ACTION_SAMPLES:
        if row.get("sample_kind") == "latency_probe":
            key = (str(row["scenario"]), int(row["repetition"]))
            grouped.setdefault(key, []).append(float(row["commit_to_apply_ms"]))
    output: list[dict[str, Any]] = []
    for (scenario, repetition), values in sorted(grouped.items()):
        output.append(
            {
                "scenario": scenario,
                "repetition": repetition,
                "samples": len(values),
                "p50_ms": percentile(values, 0.50),
                "p95_ms": percentile(values, 0.95),
                "p99_ms": percentile(values, 0.99),
                "max_ms": max(values),
            }
        )
    return output


def snapshot_workspace() -> None:
    workload_dir = OUTPUT_DIR / "workload"
    workload_dir.mkdir(parents=True, exist_ok=True)
    for source in (
        PROJECT_ROOT / "scripts" / "performance-matrix.py",
        PROJECT_ROOT / "benchmarks" / "operator_matrix.py",
        PROJECT_ROOT / "Cargo.lock",
        PROJECT_ROOT / "Cargo.toml",
    ):
        shutil.copy2(source, workload_dir / source.name)
    diff = run(["git", "diff", "--binary", "HEAD"]).stdout
    (OUTPUT_DIR / "working-tree.patch").write_text(diff)
    untracked = [
        line
        for line in run(
            ["git", "ls-files", "--others", "--exclude-standard"]
        ).stdout.splitlines()
        if line
        and not line.startswith("performance/results/")
        and not line.startswith("performance/matrix-results/")
    ]
    with tarfile.open(OUTPUT_DIR / "untracked-files.tar.gz", "w:gz") as archive:
        for relative in untracked:
            path = PROJECT_ROOT / relative
            if path.is_file():
                archive.add(path, arcname=relative)
    hashes: list[str] = []
    for path in sorted(workload_dir.iterdir()):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        hashes.append(f"{digest}  workload/{path.name}")
    (OUTPUT_DIR / "checksums.sha256").write_text("\n".join(hashes) + "\n")


def write_environment(scenarios: list[Scenario]) -> None:
    environment = {
        "run_id": RUN_ID,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": run(["git", "rev-parse", "HEAD"]).stdout.strip(),
        "git_status": run(["git", "status", "--porcelain=v1"]).stdout.splitlines(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "macos": platform.mac_ver()[0],
        "cpu_count": os.cpu_count(),
        "memory_bytes": int(
            run(["sysctl", "-n", "hw.memsize"]).stdout.strip()
            if sys.platform == "darwin"
            else 0
        ),
        "pg_config": str(PG_CONFIG),
        "postgres_version": run([str(PG_BIN / "postgres"), "--version"]).stdout.strip(),
        "pgbench_version": run([str(PG_BIN / "pgbench"), "--version"]).stdout.strip(),
        "rust_version": run(["rustc", "--version"]).stdout.strip(),
        "cargo_version": run(["cargo", "--version"]).stdout.strip(),
        "parameters": {
            "rows": ROWS,
            "groups": GROUPS,
            "mutations": MUTATIONS,
            "repetitions": REPETITIONS,
            "query_seconds": QUERY_SECONDS,
            "query_clients": QUERY_CLIENTS,
            "latency_probes": LATENCY_PROBES,
            "large_transaction_rows": LARGE_TRANSACTION_ROWS,
            "resource_sample_ms": RESOURCE_SAMPLE_MS,
            "seed": SEED,
            "scenario_filter": sorted(SCENARIO_FILTER),
        },
        "scenario_catalog": [asdict(scenario) for scenario in scenarios],
    }
    (OUTPUT_DIR / "environment.json").write_text(
        json.dumps(environment, indent=2) + "\n"
    )


def start_cluster() -> None:
    global POSTMASTER_PID
    if not SKIP_BUILD:
        build = run(
            [
                "cargo",
                "pgrx",
                "install",
                "--release",
                "--pg-config",
                str(PG_CONFIG),
            ]
        )
        (OUTPUT_DIR / "build.txt").write_text(build.stdout + build.stderr)
    init = run(
        [
            str(PG_BIN / "initdb"),
            "-D",
            str(DATA_DIR),
            "--no-locale",
            "--encoding=UTF8",
        ]
    )
    (OUTPUT_DIR / "initdb.txt").write_text(init.stdout + init.stderr)
    config = f"""
session_preload_libraries = 'shiba'
wal_level = logical
max_replication_slots = 8
max_worker_processes = 32
listen_addresses = ''
unix_socket_directories = '{SOCKET_DIR}'
port = {PORT}
shared_buffers = '1GB'
work_mem = '64MB'
maintenance_work_mem = '256MB'
max_wal_size = '4GB'
checkpoint_timeout = '30min'
synchronous_commit = on
fsync = on
full_page_writes = on
jit = off
track_io_timing = on
track_wal_io_timing = on
track_commit_timestamp = on
log_min_messages = warning
"""
    with (DATA_DIR / "postgresql.conf").open("a") as handle:
        handle.write(config)
    shutil.copy2(DATA_DIR / "postgresql.conf", OUTPUT_DIR / "postgresql.conf")
    run(
        [
            str(PG_BIN / "pg_ctl"),
            "-D",
            str(DATA_DIR),
            "-l",
            str(PG_LOG),
            "-o",
            f"-k {SOCKET_DIR} -p {PORT}",
            "-w",
            "start",
        ]
    )
    POSTMASTER_PID = int((DATA_DIR / "postmaster.pid").read_text().splitlines()[0])


def stop_cluster() -> None:
    if KEEP_CLUSTER:
        print(f"retained cluster: {DATA_DIR}", file=sys.stderr)
        print(f"retained socket: {SOCKET_DIR}", file=sys.stderr)
        return
    run(
        [
            str(PG_BIN / "pg_ctl"),
            "-D",
            str(DATA_DIR),
            "-m",
            "immediate",
            "stop",
        ],
        check=False,
    )
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    shutil.rmtree(SOCKET_DIR, ignore_errors=True)


def check_log() -> list[str]:
    if not PG_LOG.exists():
        return []
    pattern = re.compile(r"\b(WARNING|ERROR|FATAL|PANIC)\b")
    lines = PG_LOG.read_text(errors="replace").splitlines()
    errors: list[str] = []
    for index, line in enumerate(lines):
        if not pattern.search(line):
            continue
        expected_runtime_shutdown = (
            'FATAL:  terminating background worker "shiba runtime" '
            "due to administrator command"
        ) in line and any(
            "received fast shutdown request" in prior
            # Logical-decoding DETAIL/LOG records can be emitted between the
            # postmaster request and the Runtime's expected FATAL.
            for prior in lines[max(0, index - 20) : index]
        )
        if not expected_runtime_shutdown:
            errors.append(line)
    return errors


def global_postgres_stats() -> dict[str, Any]:
    return json.loads(
        scalar(
            """
SELECT json_build_object(
  'wal',(SELECT row_to_json(w) FROM (
    SELECT wal_records,wal_fpi,wal_bytes,wal_buffers_full,wal_write,
           wal_sync,wal_write_time,wal_sync_time
    FROM pg_stat_wal
  ) w),
  'checkpointer',(SELECT row_to_json(c) FROM (
    SELECT num_timed,num_requested,write_time,sync_time,buffers_written
    FROM pg_stat_checkpointer
  ) c),
  'bgwriter',(SELECT row_to_json(b) FROM (
    SELECT buffers_clean,maxwritten_clean,buffers_alloc
    FROM pg_stat_bgwriter
  ) b),
  'io',(SELECT json_build_object(
    'read_bytes',coalesce(sum(reads*op_bytes),0),
    'write_bytes',coalesce(sum(writes*op_bytes),0),
    'extend_bytes',coalesce(sum(extends*op_bytes),0),
    'fsyncs',coalesce(sum(fsyncs),0)
  ) FROM pg_stat_io)
)::text
""",
            database="postgres",
        )
    )


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=False)
    snapshot_workspace()
    scenarios = build_scenarios(rows=ROWS, groups=GROUPS, mutations=MUTATIONS)
    if SCENARIO_FILTER:
        unknown = SCENARIO_FILTER - {scenario.name for scenario in scenarios}
        if unknown:
            raise BenchmarkError(f"unknown scenarios: {sorted(unknown)}")
        scenarios = [scenario for scenario in scenarios if scenario.name in SCENARIO_FILTER]
    write_environment(scenarios)
    (OUTPUT_DIR / "scenario-catalog.json").write_text(
        json.dumps([asdict(scenario) for scenario in scenarios], indent=2) + "\n"
    )

    start_cluster()
    (OUTPUT_DIR / "postgres-stats-start.json").write_text(
        json.dumps(global_postgres_stats(), indent=2) + "\n"
    )
    covered: set[str] = set()
    try:
        for repetition in range(1, REPETITIONS + 1):
            run_dir = OUTPUT_DIR / f"run-{repetition}"
            run_dir.mkdir()
            ordered = scenarios[:]
            random.Random(SEED + repetition).shuffle(ordered)
            (run_dir / "scenario-order.json").write_text(
                json.dumps([scenario.name for scenario in ordered], indent=2) + "\n"
            )
            for scenario in ordered:
                covered.update(run_scenario(repetition, scenario, run_dir))
                write_csv(OUTPUT_DIR / "metrics-raw.csv", METRICS)
                write_csv(OUTPUT_DIR / "action-samples.csv", ACTION_SAMPLES)
                write_csv(OUTPUT_DIR / "resources.csv", RESOURCE_ROWS)
            run_multidag(repetition, run_dir)
            run_multisource_multidag(repetition, run_dir)
            run_ingress_controls(repetition, run_dir)

        expected_coverage = (
            set().union(*(set(scenario.required_operators) for scenario in scenarios))
            if SCENARIO_FILTER
            else ALL_OPERATOR_KINDS
        )
        missing = expected_coverage - covered
        extra = covered - ALL_OPERATOR_KINDS
        coverage = {
            "expected": sorted(expected_coverage),
            "covered": sorted(covered),
            "missing": sorted(missing),
            "extra": sorted(extra),
            "complete": not missing,
        }
        (OUTPUT_DIR / "operator-coverage.json").write_text(
            json.dumps(coverage, indent=2) + "\n"
        )
        if missing:
            raise BenchmarkError(f"operator coverage incomplete: {sorted(missing)}")
        write_csv(OUTPUT_DIR / "metrics-raw.csv", METRICS)
        write_csv(OUTPUT_DIR / "metrics-summary.csv", aggregate_metrics())
        write_csv(
            OUTPUT_DIR / "latency-summary.csv", aggregate_latency_samples()
        )
        write_csv(
            OUTPUT_DIR / "latency-by-run.csv", aggregate_latency_by_run()
        )
        write_csv(OUTPUT_DIR / "action-samples.csv", ACTION_SAMPLES)
        write_csv(OUTPUT_DIR / "scenario-summaries.csv", SCENARIO_SUMMARIES)
        write_csv(OUTPUT_DIR / "resources.csv", RESOURCE_ROWS)
        (OUTPUT_DIR / "postgres-stats-end.json").write_text(
            json.dumps(global_postgres_stats(), indent=2) + "\n"
        )
        errors = check_log()
        (OUTPUT_DIR / "log-errors.json").write_text(json.dumps(errors, indent=2) + "\n")
        shutil.copy2(PG_LOG, OUTPUT_DIR / "postgresql.log")
        if errors:
            raise BenchmarkError(f"PostgreSQL log contains {len(errors)} errors")
        manifest = {
            "status": "passed",
            "finished_utc": datetime.now(timezone.utc).isoformat(),
            "scenario_runs": len(SCENARIO_SUMMARIES),
            "repetitions": REPETITIONS,
            "operator_coverage": coverage,
            "correctness_checks": sum(
                1 for row in METRICS if row["metric"] == "correctness_difference"
            ),
            "correctness_failures": sum(
                1
                for row in METRICS
                if row["metric"] == "correctness_difference"
                and float(row["value"]) != 0
            ),
            "pgbench_failures": sum(
                float(row["value"])
                for row in METRICS
                if row["metric"] == "failed_transactions"
            ),
            "log_error_count": len(errors),
            "actual_runtime_counts": sorted(
                {row["actual_count"] for row in RUNTIME_OBSERVATIONS}
            ),
            "legacy_worker_counts": sorted(
                {row["legacy_worker_count"] for row in RUNTIME_OBSERVATIONS}
            ),
            "runtime_pids": sorted(
                {
                    runtime["owner_pid"]
                    for row in RUNTIME_OBSERVATIONS
                    for runtime in row["runtimes"]
                }
            ),
        }
        (OUTPUT_DIR / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n"
        )
        print(f"matrix benchmark passed: {OUTPUT_DIR}")
        return 0
    except Exception as error:
        write_csv(OUTPUT_DIR / "metrics-raw.csv", METRICS)
        write_csv(OUTPUT_DIR / "action-samples.csv", ACTION_SAMPLES)
        write_csv(OUTPUT_DIR / "scenario-summaries.csv", SCENARIO_SUMMARIES)
        write_csv(OUTPUT_DIR / "resources.csv", RESOURCE_ROWS)
        if PG_LOG.exists():
            shutil.copy2(PG_LOG, OUTPUT_DIR / "postgresql.log")
        (OUTPUT_DIR / "manifest.json").write_text(
            json.dumps(
                {
                    "status": "failed",
                    "finished_utc": datetime.now(timezone.utc).isoformat(),
                    "error": str(error),
                },
                indent=2,
            )
            + "\n"
        )
        raise
    finally:
        stop_cluster()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        os.kill(os.getpid(), signal.SIGTERM)
