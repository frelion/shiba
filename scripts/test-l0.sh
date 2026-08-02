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
PG_CONFIG="$pg_config" cargo test -p shiba-protocol
PG_CONFIG="$pg_config" cargo test -p shiba-operator
PG_CONFIG="$pg_config" cargo test -p shiba-compiler
PG_CONFIG="$pg_config" cargo test -p shiba-catalog --no-default-features --features "$feature"
PG_CONFIG="$pg_config" cargo test -p shiba-runtime --lib
PG_CONFIG="$pg_config" cargo clippy -p shiba-protocol --all-targets -- -D warnings
PG_CONFIG="$pg_config" cargo clippy -p shiba-operator --all-targets -- -D warnings
PG_CONFIG="$pg_config" cargo clippy -p shiba-compiler --all-targets -- -D warnings
PG_CONFIG="$pg_config" cargo clippy -p shiba-catalog --no-default-features --features "$feature" --all-targets -- -D warnings
PG_CONFIG="$pg_config" cargo clippy -p shiba-runtime --all-targets -- -D warnings
git diff --check

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
python3 - <<'PY'
import pathlib
import sys

limits = {
    "runtime_soft": 1200,
    "runtime_hard": 2000,
    "production_file_soft": 250,
    "production_file_hard": 350,
    "test_file_soft": 300,
    "sql_file_hard": 150,
    "operator_soft": 600,
    "compiler_soft": 600,
}

runtime_files = sorted(pathlib.Path("crates/shiba-runtime/src").glob("*.rs"))
line_counts = {path: len(path.read_text().splitlines()) for path in runtime_files}
runtime_total = sum(line_counts.values())
component_files = {
    "operator": sorted(pathlib.Path("crates/shiba-operator/src").glob("*.rs")),
    "compiler": sorted(pathlib.Path("crates/shiba-compiler/src").glob("*.rs")),
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
if runtime_total > limits["runtime_hard"]:
    raise SystemExit(
        f"Runtime production total {runtime_total} exceeds "
        f"{limits['runtime_hard']}-line hard limit"
    )
if production_failures:
    raise SystemExit(
        f"production file exceeds {limits['production_file_hard']}-line hard limit: "
        + ", ".join(production_failures)
    )

for component, files in component_files.items():
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
    )
]
for path in scripts:
    text = path.read_text()
    if "lib/pg-integration.sh" not in text:
        raise SystemExit(f"{path} does not use shared PostgreSQL integration support")
    if re.search(r"cargo pgrx package|\binitdb\b|^trap ", text, re.MULTILINE):
        raise SystemExit(f"{path} duplicates shared PostgreSQL cluster mechanics")
PY

# No production SQL may smuggle in an old authority or dynamic workflow.
if rg -n -i \
  'source_publications|change[_ ]log|dual[-_ ]write|compatibility|fallback|alias|count_state|count_result|create table .*effect|create trigger|execute format\(' \
  sql/v2; then
  echo "forbidden implementation surface found" >&2
  exit 1
fi
if rg -n -i 'effectstream|source ingress|publication|replication slot' \
  sql/v2; then
  echo "out-of-scope component leaked into production SQL" >&2
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

echo "L0 passed for PostgreSQL $pg_major ($pg_config)"
