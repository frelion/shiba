#!/usr/bin/env bash
# M15.1 failure-first static contract. It has no PostgreSQL lifecycle.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

python3 - <<'PY'
import pathlib
import re

forbidden_dependencies = {
    "sqlparser",
    "postgresql-parser",
    "pg-query",
    "pg_query",
    "tree-sitter-sql",
    "tree_sitter_sql",
    "nom-sql",
    "nom_sql",
}
for crate in ("shiba-operator", "shiba-runtime", "shiba-ingress"):
    manifest_path = pathlib.Path("crates") / crate / "Cargo.toml"
    dependencies = set()
    dependency_section = False
    for raw_line in manifest_path.read_text().splitlines():
        line = raw_line.strip()
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            dependency_section = section == "dependencies" or section.endswith(".dependencies")
            continue
        if dependency_section and "=" in line and not line.startswith("#"):
            dependencies.add(line.split("=", 1)[0].strip().strip('"'))
    denied = sorted(dependencies & forbidden_dependencies)
    if denied:
        raise SystemExit(f"{crate} must remain SQL/parser-free; found {denied}")

    source = "\n".join(
        path.read_text() for path in sorted((manifest_path.parent / "src").rglob("*.rs"))
    )
    forbidden_source = {
        "SQL frontend": r"\bSqlFrontend\b|\bparse_sql\b",
        "parser crate": r"\b(?:sqlparser|pg_query|tree_sitter_sql|nom_sql)::",
    }
    present = [name for name, pattern in forbidden_source.items() if re.search(pattern, source)]
    if present:
        raise SystemExit(
            f"{crate} must not own SQL parsing or frontend AST types: {present}"
        )

contracts = {
    "docs/SQL_FRONTEND_CONTRACT.md": (
        "QuerySpecV1 is the sole durable declaration authority",
        "OperatorGraph is the sole Runtime execution authority",
        "SQL text is non-authoritative provenance",
        "stable byte span",
    ),
    "docs/QUERY_SPEC_CONTRACT.md": (
        "QuerySpecV1 is the sole durable declaration authority",
        "OperatorGraph is the sole Runtime execution authority",
        "SQL text is non-authoritative provenance",
        "GraphOutputSpecV1",
        "M15.2",
        "deleted",
    ),
    "docs/adr/0007-m15-sql-frontend.md": (
        "QuerySpecV1 is the sole durable declaration authority",
        "OperatorGraph is the sole Runtime execution authority",
        "SQL text is non-authoritative provenance",
    ),
}
for name, markers in contracts.items():
    path = pathlib.Path(name)
    if not path.is_file():
        raise SystemExit(f"M15.1 contract is missing: {name}")
    text = path.read_text()
    missing = [marker for marker in markers if marker not in text]
    if missing:
        raise SystemExit(f"{name} is missing frozen M15.1 markers: {missing}")
    if not re.search(r"(?im)^#{2,3}\s+.*bounds", text):
        raise SystemExit(f"{name} must contain an explicit bounds heading")

matrix = pathlib.Path("scripts/release-matrix.sh").read_text()
invocations = len(re.findall(r"^scripts/check-m15-contract\.sh$", matrix, re.MULTILINE))
if invocations != 1:
    raise SystemExit(
        "release matrix must invoke check-m15-contract.sh exactly once outside PG gates"
    )
if "test-m15-contract.sh" in matrix:
    raise SystemExit("M15.1 static contract must not inflate PostgreSQL gate enrollment")
PY

echo "M15.1 SQL frontend and QuerySpec contract gate passed"
