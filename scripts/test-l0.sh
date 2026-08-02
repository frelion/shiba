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
PG_CONFIG="$pg_config" cargo check -p shiba-catalog --no-default-features --features "$feature" --all-targets
PG_CONFIG="$pg_config" cargo check -p shiba-runtime --all-targets
PG_CONFIG="$pg_config" cargo test -p shiba-protocol
PG_CONFIG="$pg_config" cargo test -p shiba-catalog --no-default-features --features "$feature"
PG_CONFIG="$pg_config" cargo test -p shiba-runtime --lib
PG_CONFIG="$pg_config" cargo clippy -p shiba-protocol --all-targets -- -D warnings
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

# Keep the accepted M3 production budget executable rather than aspirational.
python3 - <<'PY'
import pathlib

runtime_files = sorted(pathlib.Path("crates/shiba-runtime/src").glob("*.rs"))
line_counts = {path: len(path.read_text().splitlines()) for path in runtime_files}
too_large = [f"{path}: {count}" for path, count in line_counts.items() if count > 250]
if too_large:
    raise SystemExit("M3 production file exceeds 250 lines: " + ", ".join(too_large))
if sum(line_counts.values()) > 600:
    raise SystemExit("M3 Runtime production code exceeds its 600-line hard limit")
sql_lines = len(pathlib.Path("sql/v2/002_insert_count.sql").read_text().splitlines())
if sql_lines > 150:
    raise SystemExit("M3 SQL exceeds its 150-line hard limit")
PY

# No production SQL may smuggle in an old authority or dynamic workflow.
if rg -n -i \
  'source_publications|change[_ ]log|dual[-_ ]write|compatibility|fallback|alias|create trigger|execute format\(' \
  sql/v2; then
  echo "forbidden implementation surface found" >&2
  exit 1
fi
if rg -n -i 'effectstream|source ingress|registration|publication|replication slot' \
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
for required in ("Protocol JSON/schema", "canonical digest", "PG17/18", "Phase 1", "M3.1", "M3.2"):
    if required not in manifest:
        raise SystemExit(f"REUSE_MANIFEST.md lacks required Phase-1 contract: {required}")
PY

echo "L0 passed for PostgreSQL $pg_major ($pg_config)"
