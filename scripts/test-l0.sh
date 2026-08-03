#!/usr/bin/env bash
# Static and unit-test gate. It intentionally has no PostgreSQL server
# lifecycle; use test-empty-install.sh for database installation behavior.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

pg_config="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
if [[ "$pg_config" != /* || ! -x "$pg_config" ]]; then
  echo "PG_CONFIG must be an executable absolute path (got: $pg_config)" >&2
  exit 64
fi
pg_major="$($pg_config --version | sed -E 's/.*PostgreSQL ([0-9]+)\..*/\1/')"
if [[ "$pg_major" != "17" && "$pg_major" != "18" ]]; then
  echo "only PostgreSQL 17 or 18 is supported (got: $pg_major)" >&2
  exit 64
fi
feature="pg$pg_major"

cargo fmt --all -- --check
PG_CONFIG="$pg_config" cargo check -p shiba-protocol --all-targets
PG_CONFIG="$pg_config" cargo check -p shiba-operator --all-targets
PG_CONFIG="$pg_config" cargo check -p shiba-compiler --all-targets
PG_CONFIG="$pg_config" cargo check -p shiba-catalog --no-default-features --features "$feature" --all-targets
PG_CONFIG="$pg_config" cargo check -p shiba-runtime --all-targets
PG_CONFIG="$pg_config" cargo check -p shiba-ingress --all-targets
PG_CONFIG="$pg_config" cargo test -p shiba-protocol
PG_CONFIG="$pg_config" cargo test -p shiba-operator
PG_CONFIG="$pg_config" cargo test -p shiba-compiler
PG_CONFIG="$pg_config" cargo test -p shiba-catalog --no-default-features --features "$feature"
PG_CONFIG="$pg_config" cargo test -p shiba-runtime --lib
PG_CONFIG="$pg_config" cargo test -p shiba-ingress
PG_CONFIG="$pg_config" cargo clippy -p shiba-protocol --all-targets -- -D warnings
PG_CONFIG="$pg_config" cargo clippy -p shiba-operator --all-targets -- -D warnings
PG_CONFIG="$pg_config" cargo clippy -p shiba-compiler --all-targets -- -D warnings
PG_CONFIG="$pg_config" cargo clippy -p shiba-catalog --no-default-features --features "$feature" --all-targets -- -D warnings
PG_CONFIG="$pg_config" cargo clippy -p shiba-runtime --all-targets -- -D warnings
PG_CONFIG="$pg_config" cargo clippy -p shiba-ingress --all-targets -- -D warnings
git diff --check
scripts/check-m15-contract.sh
scripts/check-m15-parser.sh
scripts/check-m15-registration.sh

# An unborn branch has no tracked diff, so check every tracked-or-untracked
# text file directly for whitespace damage and missing final newlines.
python3 - <<'PY'
import pathlib
import subprocess

names = subprocess.check_output(
    ["git", "ls-files", "-co", "--exclude-standard"], text=True
).splitlines()
errors = []
for name in names:
    path = pathlib.Path(name)
    if not path.is_file():
        continue
    data = path.read_bytes()
    if b"\0" in data:
        continue
    lines = data.splitlines()
    for number, line in enumerate(lines, 1):
        if line.rstrip(b" \t") != line:
            errors.append(f"{name}:{number}: trailing whitespace")
    if data and not data.endswith(b"\n"):
        errors.append(f"{name}: missing final newline")
if errors:
    raise SystemExit("\n".join(errors))
PY

# Central complexity thresholds: warnings report growth; hard limits fail.
# Crate totals are trend warnings, not correctness gates. M14 uses file-level
# responsibility limits and per-stage production deltas as anti-monolith gates.
python3 - <<'PY'
import pathlib
import sys

limits = {
    "runtime_soft": 1200,
    "production_file_soft": 300,
    "production_file_hard": 400,
    "test_file_soft": 300,
    "sql_file_hard": 150,
    "operator_soft": 600,
    "compiler_soft": 600,
    "ingress_soft": 1400,
}

runtime_files = sorted(pathlib.Path("crates/shiba-runtime/src").glob("*.rs"))
line_counts = {path: len(path.read_text().splitlines()) for path in runtime_files}
runtime_total = sum(line_counts.values())
component_files = {
    "protocol": sorted(pathlib.Path("crates/shiba-protocol/src").glob("*.rs")),
    "catalog": sorted(pathlib.Path("crates/shiba-catalog/src").glob("*.rs")),
    "operator": sorted(pathlib.Path("crates/shiba-operator/src").glob("*.rs")),
    "compiler": sorted(pathlib.Path("crates/shiba-compiler/src").glob("*.rs")),
    "ingress": sorted(pathlib.Path("crates/shiba-ingress/src").glob("*.rs")),
}
all_production_counts = dict(line_counts)
for files in component_files.values():
    all_production_counts.update(
        {path: len(path.read_text().splitlines()) for path in files}
    )
production_warnings = [
    f"{path}: {count}" for path, count in all_production_counts.items()
    if count > limits["production_file_soft"]
]
production_failures = [
    f"{path}: {count}" for path, count in all_production_counts.items()
    if count > limits["production_file_hard"]
]
if runtime_total > limits["runtime_soft"]:
    details = ", ".join(f"{path.name}={count}" for path, count in line_counts.items())
    print(
        f"warning: Runtime production total {runtime_total} > {limits['runtime_soft']} ({details})",
        file=sys.stderr,
    )
if production_warnings:
    print(
        f"warning: production file > {limits['production_file_soft']} lines: "
        + ", ".join(production_warnings),
        file=sys.stderr,
    )
if production_failures:
    raise SystemExit(
        f"production file exceeds {limits['production_file_hard']}-line hard limit: "
        + ", ".join(production_failures)
    )

for component in ("operator", "compiler", "ingress"):
    files = component_files[component]
    total = sum(all_production_counts[path] for path in files)
    if total > limits[f"{component}_soft"]:
        print(
            f"warning: shiba-{component} production total {total} > "
            f"{limits[f'{component}_soft']}",
            file=sys.stderr,
        )

test_counts = {
    path: len(path.read_text().splitlines())
    for root in (
        pathlib.Path("crates/shiba-runtime/tests"),
        pathlib.Path("crates/shiba-operator/tests"),
        pathlib.Path("crates/shiba-compiler/tests"),
        pathlib.Path("crates/shiba-ingress/tests"),
    )
    for path in root.rglob("*.rs")
}
test_warnings = [
    f"{path}: {count}" for path, count in test_counts.items()
    if count > limits["test_file_soft"]
]
if test_warnings:
    print(
        f"warning: integration test file > {limits['test_file_soft']} lines: "
        + ", ".join(test_warnings),
        file=sys.stderr,
    )

sql_counts = {
    path: len(path.read_text().splitlines()) for path in pathlib.Path("sql/v2").glob("*.sql")
}
too_large_sql = [
    f"{path}: {count}" for path, count in sql_counts.items()
    if count > limits["sql_file_hard"]
]
if too_large_sql:
    raise SystemExit(
        f"SQL file exceeds {limits['sql_file_hard']}-line hard limit: "
        + ", ".join(too_large_sql)
    )
PY

# Repeated pgoutput test mechanics have one test-only owner.
python3 - <<'PY'
import pathlib
import re

tests = pathlib.Path("crates/shiba-runtime/tests")
support = tests / "support" / "mod.rs"
helper_pattern = re.compile(
    r"fn (strip_recvlogical_delimiters|framed_message_count|message_end|"
    r"message_end_checked|cstring_end_checked|read_u16_checked|read_u32_checked)\b"
)
owners = [path for path in tests.rglob("*.rs") if helper_pattern.search(path.read_text())]
if owners != [support]:
    raise SystemExit(f"pgoutput framing helpers must exist only in {support}: {owners}")

scripts = [
    pathlib.Path(f"scripts/test-{name}.sh")
    for name in (
        "m3", "m4", "m4-empty", "m4-composite", "m4-update", "m4-delete",
        "m4-replica-identity", "m5-toast", "m5-incompressible-toast",
        "m5-composite-delete", "m5-replica-index", "m5-source-binding",
        "m6-stream-commit", "m6-stream-abort", "m7-ddl-invalidation",
        "m7-drop-invalidation", "m7-column-invalidation", "m7-index-invalidation",
        "m7-concurrent-ddl", "m8-multi-source", "m8-concurrent-sources",
        "m8-bounded-decode", "m8-performance", "m9-registration",
        "m9-count-sum", "m9-operator-concurrency", "m9-operator-performance",
        "m10-committed-ingress", "m10-streaming-ingress",
        "m10-catalog-ingress", "m10-governed-ingress",
        "m10-performance-ingress", "m10-shutdown-ingress",
        "m11-bootstrap-contract", "m11-bootstrap", "m11-recovery",
        "m11-bootstrap-performance", "m11-bootstrap-roles",
        "m12-rebuild-contract", "m12-rebuild-admission",
        "m12-rebuild-snapshot-live", "m12-rebuild-identity-authority",
        "m12-rebuild-recovery",
        "m12-rebuild-governance",
        "m12-rebuild-performance",
        "m13-operator-kernel",
        "m14-grouped", "m14-graph-runtime",
    )
]
for path in scripts:
    text = path.read_text()
    if "lib/pg-integration.sh" not in text:
        raise SystemExit(f"{path} does not use shared PostgreSQL integration support")
    if re.search(r"cargo pgrx package|\binitdb\b|^trap ", text, re.MULTILINE):
        raise SystemExit(f"{path} duplicates shared PostgreSQL cluster mechanics")
PY

# The M11 exported-snapshot proof remains binding after M14.6 moves lifecycle
# and compute progress from one source to one graph.
python3 - <<'PY'
import pathlib

contract_path = pathlib.Path("docs/BOOTSTRAP_CONTRACT.md")
adr_path = pathlib.Path("docs/adr/0002-m11-consistent-bootstrap.md")
if not contract_path.is_file() or not adr_path.is_file():
    raise SystemExit("M11.1 bootstrap contract and ADR must both exist")

contract = contract_path.read_text().lower()
required = (
    "export_snapshot", "consistent_point", "snapshot_name",
    "set transaction snapshot", "repeatable read", "bootstrapid",
    "bootstrapbatchid", "effectorigin", "graph_bootstrap", "source_row_state",
    "bootstrapfence", "building", "unavailable",
    "three connections", "scan_complete", "graph_continuation",
)
missing = [term for term in required if term not in contract]
if missing:
    raise SystemExit(f"M11.1 bootstrap contract is missing: {missing}")

readme = pathlib.Path("docs/README.md").read_text()
if "BOOTSTRAP_CONTRACT.md" not in readme or "0002-m11-consistent-bootstrap.md" not in readme:
    raise SystemExit("docs README must link the M11.1 contract and ADR")
manifest = pathlib.Path("docs/contracts/REUSE_MANIFEST.md").read_text()
for milestone in ("M11.1", "M11.2", "M11.3", "M11.4", "M11.5"):
    if milestone not in manifest:
        raise SystemExit(f"REUSE_MANIFEST must record {milestone} evidence")
PY

# The M12 forward-only rebuild proof remains binding after graph-wide lifecycle
# cutover. One graph_bootstrap row remains the sole graph lifecycle authority.
python3 - <<'PY'
import pathlib

contract_path = pathlib.Path("docs/REBUILD_CONTRACT.md")
adr_path = pathlib.Path("docs/adr/0003-m12-offline-rebuild.md")
if not contract_path.is_file() or not adr_path.is_file():
    raise SystemExit("M12.1 rebuild contract and ADR must both exist")

contract = contract_path.read_text().lower()
required = (
    "building authority", "active authority", "destructive prepare",
    "graph_bootstrap", "exact old", "forward-only", "building/null",
    "old continuation", "old generation", "slot-birth", "threat model",
    "`replication` credential", "same-name", "activation",
)
missing = [term for term in required if term not in contract]
if missing:
    raise SystemExit(f"M12.1 rebuild contract is missing: {missing}")

readme = pathlib.Path("docs/README.md").read_text()
if "REBUILD_CONTRACT.md" not in readme or "0003-m12-offline-rebuild.md" not in readme:
    raise SystemExit("docs README must link the M12.1 contract and ADR")
manifest = pathlib.Path("docs/contracts/REUSE_MANIFEST.md").read_text()
if "M12.1" not in manifest or "test-m12-rebuild-contract.sh" not in manifest:
    raise SystemExit("REUSE_MANIFEST must record the M12.1 failure-first evidence")
PY

# M12.2 admission now operates on the one graph lifecycle/config authority.
# Slot retirement and target-slot creation remain non-transactional operations.
python3 - <<'PY'
import pathlib

sql = "\n".join(
    pathlib.Path(path).read_text().lower()
    for path in (
        "sql/v2/014_source_rebuild.sql",
        "sql/v2/015_source_rebuild_preflight.sql",
        "sql/v2/016_source_rebuild_current.sql",
        "sql/v2/017_source_rebuild_prepare.sql",
    )
)
for required in (
    "rebuild_prepared", "retired_bootstrap_id", "retired_slot_name",
    "retired_slot_generation", "prepare_graph_rebuild",
    "insert into shiba.graph_result", "'building', null, null",
    "shiba_internal.graph_definition",
    "shiba_internal.graph_ingress_config",
    "shiba_internal.graph_bootstrap",
    "shiba_internal.graph_continuation",
):
    if required not in sql:
        raise SystemExit(f"M12.2 admission SQL is missing: {required}")
if "create table" in sql:
    raise SystemExit("M12.2 must reuse the existing lifecycle and authority tables")

manifest = pathlib.Path("docs/contracts/REUSE_MANIFEST.md").read_text()
if "M12.2" not in manifest or "test-m12-rebuild-admission.sh" not in manifest:
    raise SystemExit("REUSE_MANIFEST must record the M12.2 PG17/18 admission evidence")
PY

if rg -n -i \
  'create table[^;]*(source_rebuild|rebuild_(intent|log|state)|wal_spool|effect_log)|create table[^;]*candidate_(binding|config)|(source|graph)_continuation_v2' \
  sql/v2; then
  echo "parallel rebuild authority, spool, or second continuation found" >&2
  exit 1
fi

# No production SQL may smuggle in an old authority or dynamic workflow.
if rg -n -i \
  'source_publications|change[_ ]log|dual[-_ ]write|compatibility|fallback|alias|count_state|count_result|create table .*effect|create trigger|execute format\(' \
  sql/v2; then
  echo "forbidden implementation surface found" >&2
  exit 1
fi
if rg -n -i \
  'confirmed_flush_lsn|restart_lsn|active_pid|pg_create_logical_replication_slot|pg_drop_replication_slot' \
  sql/v2; then
  echo "dynamic slot state or automatic slot administration leaked into production SQL" >&2
  exit 1
fi

python3 - tests/fixtures/protocol/canonical-v1.json <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = path.read_bytes()
if data.endswith(b"\n"):
    data = data[:-1]
if b"\n" in data or b"\r" in data:
    raise SystemExit("canonical fixture contains non-canonical whitespace")
expected_json = (b'{"protocol_version":1,"catalog_version":1,"message":{"kind":"cause",'
                 b'"body":{"transaction":{"source_id":3,"slot_generation":7,'
                 b'"commit_lsn":"0/64","ingress_transaction_id":11},"input_sequence":2}}}')
if data != expected_json:
    raise SystemExit("canonical fixture bytes differ from the approved vector")
actual = hashlib.sha256(b"shiba.protocol.wire.v1\0" + data).hexdigest()
expected = "82b80d7d38e26756d89e9d390525b1f57189f4a002d75603892d8f0d7c382b39"
if actual != expected:
    raise SystemExit(f"canonical digest mismatch: {actual}")
PY

python3 - tests/fixtures/pg/deferred-evidence.json <<'PY'
import json
import pathlib
import sys

manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
required = {
    "PG17/18 behavior differential", "pgoutput decoding boundaries", "TOAST", "NULL",
    "empty tuple", "streaming transaction", "source identity and replica identity",
    "catalog binding field semantics", "continuation/CAS invariants",
    "DDL invalidation ObjectAddress semantics", "crash", "rollback", "concurrency",
    "performance", "verified failure cases",
}
present = {item["name"] for item in manifest["items"]}
if manifest["schema_version"] != 1 or required - present:
    raise SystemExit("deferred evidence manifest is incomplete")
if not manifest["old_repository"].startswith("/") or not manifest["old_commit"]:
    raise SystemExit("deferred evidence provenance is incomplete")
for item in manifest["items"]:
    if not item["source"].startswith("/") or not item["postgres_versions"] or not item["legacy_command"]:
        raise SystemExit(f"incomplete item provenance: {item['name']}")
    sources = item.get("sources", [item["source"]])
    if item["source"] not in sources:
        raise SystemExit(f"primary source omitted from sources: {item['name']}")
    for source in sources:
        candidate = pathlib.Path(source)
        if not candidate.is_absolute() or not candidate.is_file():
            raise SystemExit(f"missing legacy evidence source: {item['name']}: {source}")
    command = item["legacy_command"]
    if not command.startswith("n/a:"):
        for major in ("17", "18"):
            marker = f"/opt/homebrew/opt/postgresql@{major}/bin/pg_config"
            if marker not in command:
                raise SystemExit(f"legacy command lacks PostgreSQL {major} usage: {item['name']}")
PY

python3 - <<'PY'
import pathlib

manifest = pathlib.Path("docs/contracts/REUSE_MANIFEST.md").read_text()
header = "| 成果 | 来源 | 分类A/B/C | 复用方式 | 证据 | 未证明边界 |"
if header not in manifest:
    raise SystemExit("REUSE_MANIFEST.md lacks the required audit-table header")
for required in ("Protocol JSON/schema", "canonical digest", "PG17/18", "Phase 1", "M3.1", "M3.2", "M4.1", "M4.2", "M4.3", "M4.4", "M4.5", "M4.6", "M5.1", "M5.2", "M5.3", "M5.4", "M5.5", "M6.1", "M6.2", "M7.1", "M7.2", "M7.3", "M7.4", "M7.5", "M8.1", "M8.2", "M8.3", "M8.4", "M9.1", "M9.2"):
    if required not in manifest:
        raise SystemExit(f"REUSE_MANIFEST.md lacks required Phase-1 contract: {required}")
PY

python3 - <<'PY'
import pathlib

goal_gap = pathlib.Path("docs/GOAL_GAP.md").read_text()
required = (
    "# V2 goal gap after M14",
    "Active/non-pristine rebuild for the declared",
    "That green gate closes the declared active",
    "96 successful PG invocations",
)
for marker in required:
    if marker not in goal_gap:
        raise SystemExit(f"GOAL_GAP.md lacks completed M12 evidence: {marker}")

stale = (
    "M12.6 active-source rebuild performance/release evidence remain",
    "non-pristine binding rebuild",
    "Passing that gate will close",
)
for phrase in stale:
    if phrase in goal_gap:
        raise SystemExit(f"GOAL_GAP.md retains stale M12 status: {phrase}")

for path in (
    "docs/ARCHITECTURE.md",
    "docs/README.md",
    "docs/SOURCE_INGRESS_CONTRACT.md",
    "docs/TRANSACTION_RECOVERY.md",
):
    text = pathlib.Path(path).read_text()
    if "Active/non-pristine rebuild remains M12" in text or \
            "M12 active/non-pristine rebuild remain" in text or \
            "production rebuild remains\nunimplemented until M12.2--M12.6" in text:
        raise SystemExit(f"{path} retains stale pre-M12 completion status")

reuse_manifest = pathlib.Path("docs/contracts/REUSE_MANIFEST.md").read_text()
for phrase in (
    "M12.6 performance/release 尚未证明",
    "M12.6 最终性能和运维矩阵未证明",
    "M12 generation rebuild CAS 未证明",
    "active/non-pristine binding rebuild lifecycle 属 M12，尚未证明",
    "M12 rebuild 未证明",
    "non-pristine binding rebuild、sustained performance 仍未证明",
    "M12 active rebuild 未证明",
):
    if phrase in reuse_manifest:
        raise SystemExit(f"REUSE_MANIFEST.md retains stale M12 status: {phrase}")

graph_contract = pathlib.Path("docs/OPERATOR_GRAPH_CONTRACT.md").read_text()
graph_adr = pathlib.Path("docs/adr/0005-m14-operator-graph.md").read_text()
join_contract = pathlib.Path("docs/JOIN_AUTHORITY_CONTRACT.md").read_text()
join_adr = pathlib.Path("docs/adr/0006-m14-two-source-join-authority.md").read_text()
for marker in (
    "One durable graph authority",
    "Pure two-input INNER JOIN boundary",
    "graph/generation ownership mutex",
    "782.302750/787.157125 ms and M14 stop lines",
):
    if marker not in graph_contract:
        raise SystemExit(f"M14 graph contract lacks frozen marker: {marker}")
if "Status: accepted for M14.1." not in graph_adr:
    raise SystemExit("M14 ADR is not accepted")
if "M14.1 typed Operator SDK and graph contract" not in reuse_manifest:
    raise SystemExit("REUSE_MANIFEST.md lacks M14.1 evidence")
if "M14.2 typed stateless graph and Materialize" not in reuse_manifest:
    raise SystemExit("REUSE_MANIFEST.md lacks M14.2 evidence")
if "M14.3 generic grouped state and aggregates" not in reuse_manifest:
    raise SystemExit("REUSE_MANIFEST.md lacks M14.3 evidence")
for marker in (
    "One graph, transport and progress authority",
    "one publication, one logical slot and one slot generation",
    "Progress belongs only to `(graph_id, slot_generation)`",
    "non-null bigint primary-key or unique-key index `ObjectAddress`",
):
    if marker not in join_contract:
        raise SystemExit(f"M14 JOIN authority contract lacks frozen marker: {marker}")
if "Status: accepted for the M14.4 contract slice" not in join_adr:
    raise SystemExit("M14.4 JOIN authority ADR is not accepted")
if "M14.4 accepted two-source JOIN authority" not in reuse_manifest:
    raise SystemExit("REUSE_MANIFEST.md lacks M14.4 contract evidence")
if "M14.5 pure multi-input INNER JOIN kernel" not in reuse_manifest:
    raise SystemExit("REUSE_MANIFEST.md lacks M14.5 pure-kernel evidence")
if "M14.6 unified graph production cutover" not in reuse_manifest:
    raise SystemExit("REUSE_MANIFEST.md lacks M14.6 graph-cutover evidence")
if "M14.7 full graph release closure" not in reuse_manifest:
    raise SystemExit("REUSE_MANIFEST.md lacks M14.7 release evidence")

current_docs = (
    "docs/README.md",
    "docs/ARCHITECTURE.md",
    "docs/CATALOG_CONTRACT.md",
    "docs/SOURCE_INGRESS_CONTRACT.md",
    "docs/TRANSACTION_RECOVERY.md",
    "docs/TESTING.md",
    "docs/GOAL_GAP.md",
)
for path in current_docs:
    text = pathlib.Path(path).read_text()
    for stale in ("M14 is not complete", "M14.7 remains unproved"):
        if stale in text:
            raise SystemExit(f"{path} retains stale M14 status: {stale}")
PY

if rg -n \
  'PlanImplementation::ProjectRows|OperatorOperationV1::ProjectRows|project_transition|projected\(|\bProjectRows\b|project_rows' \
  crates/shiba-operator/src crates/shiba-compiler/src \
  crates/shiba-runtime/src crates/shiba-ingress/src sql/v2; then
  echo "M14.2 forbidden ProjectRows production path remains" >&2
  exit 1
fi

if rg -n \
  '\bCompiledPlan\b|\bPlanImplementation\b|\bOperatorSpecV1\b|\bOperatorOperationV1\b' \
  crates/shiba-operator/src crates/shiba-compiler/src \
  crates/shiba-runtime/src crates/shiba-ingress/src sql/v2; then
  echo "M14.6 singular operator-plan production API remains" >&2
  exit 1
fi

if rg -n 'OperatorGraph::build' crates/shiba-runtime/src crates/shiba-ingress/src sql/v2; then
  echo "M14 graph construction leaked outside pure Compiler/Operator" >&2
  exit 1
fi

if rg -n -i \
  '\b(operator_definition|operator_state|operator_node_state|operator_result|operator_result_row|source_continuation|source_ingress_config|source_ingress_invalidation|source_bootstrap)\b' \
  crates/shiba-runtime/src crates/shiba-ingress/src sql/v2; then
  echo "M14.6 superseded source/operator execution authority remains" >&2
  exit 1
fi

if rg -n \
  '\b(CountRows|SumInt8|ProjectRows|GroupedCount|GroupedSumInt8|InnerJoin|GraphOutputSpecV1)\b|operator_kind' \
  crates/shiba-runtime/src crates/shiba-ingress/src sql/v2; then
  echo "M14 concrete operator kind leaked outside Operator/Compiler" >&2
  exit 1
fi

if rg -n \
  'KeyBy|GroupedCount|GroupedSumInt8' \
  crates/shiba-runtime/src crates/shiba-ingress/src sql/v2; then
  echo "M14.3 concrete grouped operator dispatch leaked outside operator/compiler" >&2
  exit 1
fi

if rg -n \
  'InnerJoin|JoinSpecV1|compile_join|left_source_id|right_source_id' \
  crates/shiba-runtime/src crates/shiba-ingress/src sql/v2; then
  echo "M14.5 concrete JOIN dispatch leaked outside operator/compiler" >&2
  exit 1
fi

python3 - <<'PY'
import pathlib

identity = pathlib.Path("crates/shiba-protocol/src/identity.rs").read_text()
graph = "\n".join(
    path.read_text() for path in pathlib.Path("crates/shiba-operator/src").glob("graph*.rs")
)
kernel = pathlib.Path("crates/shiba-operator/src/kernel.rs").read_text()
compiler = "\n".join(
    path.read_text() for path in pathlib.Path("crates/shiba-compiler/src").glob("*.rs")
)
for marker in ("id_type!(GraphId", "pub struct GraphTransactionId"):
    if marker not in identity:
        raise SystemExit(f"M14.5 graph identity lacks marker: {marker}")
for marker in (
    "pub graph_id: GraphId",
    "pub sources: Vec<SourcePort>",
    "SourcePort(SourceId)",
    "pub struct MultiInputBatch",
    "pub enum ResultDelta",
    "pub struct GraphTransition",
):
    if marker not in graph:
        raise SystemExit(f"M14.5 operator graph lacks marker: {marker}")
for marker in ("pub fn graph_state_read_set", "pub fn apply_graph_plan"):
    if marker not in kernel:
        raise SystemExit(f"M14.6 graph kernel lacks marker: {marker}")
for marker in (
    "pub struct QuerySpecV1", "pub fn compile_query",
    "effective_replica_identity", "Result<OperatorGraph, CompilerError>",
):
    if marker not in compiler:
        raise SystemExit(f"M14 graph compiler lacks marker: {marker}")
PY

python3 - <<'PY'
import pathlib

schema = "\n".join(
    path.read_text() for path in sorted(pathlib.Path("sql/v2").glob("*.sql"))
)
required = (
    "CREATE TABLE shiba_internal.graph_definition",
    "CREATE TABLE shiba_internal.graph_source_member",
    "CONSTRAINT graph_source_member_one_graph UNIQUE (source_id)",
    "CREATE TABLE shiba_internal.graph_ingress_config",
    "CREATE TABLE shiba_internal.graph_ingress_source",
    "CREATE TABLE shiba_internal.graph_ingress_invalidation",
    "CREATE TABLE shiba_internal.graph_continuation",
    "CREATE TABLE shiba_internal.graph_bootstrap",
    "CREATE TABLE shiba_internal.graph_bootstrap_checkpoint",
    "CREATE TABLE shiba_internal.graph_node_state",
    "CREATE TABLE shiba.graph_result",
    "CREATE TABLE shiba_internal.graph_result_row",
    "CREATE VIEW shiba.graph_result_rows",
    "REVOKE ALL ON TABLE shiba_internal.graph_node_state FROM PUBLIC",
)
for marker in required:
    if marker not in schema:
        raise SystemExit(f"M14.6 graph authority lacks marker: {marker}")
PY

echo "L0 passed for PostgreSQL $pg_major ($pg_config)"
