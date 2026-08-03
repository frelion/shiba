#!/usr/bin/env bash
# M15.7 final static authority, dependency, and specialization gate.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

python3 - <<'PY'
import pathlib
import re


def production(crate):
    root = pathlib.Path("crates") / crate / "src"
    return "\n".join(path.read_text() for path in sorted(root.rglob("*.rs")))


runtime = production("shiba-runtime")
ingress = production("shiba-ingress")
operator = production("shiba-operator")
compiler = production("shiba-compiler")
frontend = production("shiba-sql-frontend")

for scope, text in (("Runtime", runtime), ("Ingress", ingress), ("Operator", operator)):
    leaks = [
        marker
        for marker in ("parse_sql", "UnboundQuery", "sqlparser", "bind_query")
        if re.search(rf"\b{re.escape(marker)}\b", text)
    ]
    if leaks:
        raise SystemExit(f"M15.7 {scope} contains SQL frontend/parser knowledge: {leaks}")

all_production = "\n".join((runtime, ingress, operator, compiler, frontend))
for forbidden in (
    r"\bGraphOutputSpecV1\b",
    r"\b(?:CountRowsSQL|GroupedQuery|FilteredAggregate|SqlJoin|JoinQuery|JoinRecipe|compile_sql_join)\b",
):
    if re.search(forbidden, all_production):
        raise SystemExit(f"M15.7 superseded query recipe remains: {forbidden}")

for path in pathlib.Path("crates").glob("*/Cargo.toml"):
    if path.parent.name in {"shiba-runtime", "shiba-ingress", "shiba-operator"}:
        text = path.read_text()
        if re.search(r"(?m)^\s*(?:shiba-sql-frontend|sqlparser)\s*=", text):
            raise SystemExit(f"M15.7 SQL frontend/parser dependency leaked into {path}")

raw_sql = re.compile(
    r"\b(?:raw_sql|sql_text|query_sql|query_text|statement_sql|statement_text)\b",
    re.IGNORECASE,
)
for path in sorted(pathlib.Path("sql/v2").glob("*.sql")):
    if raw_sql.search(path.read_text()):
        raise SystemExit(f"M15.7 Catalog contains raw SQL authority: {path}")

matrix = pathlib.Path("scripts/release-matrix.sh").read_text()
if len(re.findall(r"^\s*test-m15-sql-performance\.sh\s*$", matrix, re.MULTILINE)) != 1:
    raise SystemExit("M15.7 performance gate must be enrolled exactly once")
for marker in (
    "M15 SQL frontend performance evidence:",
    "M15 SQL frontend performance ",
    "M15 SQL registration performance ",
):
    if marker not in matrix:
        raise SystemExit(f"M15.7 release matrix lacks performance evidence marker: {marker}")
PY

echo "M15.7 final SQL frontend authority and specialization gate passed"
