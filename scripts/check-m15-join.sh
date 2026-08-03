#!/usr/bin/env bash
# M15.6 generic two-source SQL Join contract gate.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

python3 - <<'PY'
import pathlib
import re

def source(crate):
    return "\n".join(
        path.read_text() for path in (pathlib.Path("crates") / crate / "src").glob("*.rs")
    )

frontend = source("shiba-sql-frontend")
compiler = source("shiba-compiler")
runtime = source("shiba-runtime")
ingress = source("shiba-ingress")

required = {
    "two-source Binder dispatch": (frontend, r"bind_join::bind\s*\("),
    "generic InnerJoin declaration": (frontend, r"QueryOperationV1::InnerJoin"),
    "canonical source membership order": (frontend, r"source_ids\.sort_unstable\s*\("),
    "exact right identity binding": (compiler, r"exact\.key_column\s*!=\s*right_source\.columns"),
    "right source effective identity": (compiler, r"identity_for\(right_source"),
}
missing = [name for name, (text, pattern) in required.items() if not re.search(pattern, text, re.S)]
if missing:
    raise SystemExit(f"M15.6 SQL Join contract markers are missing: {missing}")

forbidden = re.compile(r"\b(?:SqlJoin|JoinQuery|JoinRecipe|compile_sql_join)\b")
for scope, text in (("frontend", frontend), ("compiler", compiler), ("runtime", runtime), ("ingress", ingress)):
    if forbidden.search(text):
        raise SystemExit(f"M15.6 {scope} contains a forbidden SQL Join recipe")

for scope, text in (("runtime", runtime), ("ingress", ingress)):
    if re.search(r"\b(?:UnboundQuery|sqlparser|parse_sql|bind_query)\b", text):
        raise SystemExit(f"M15.6 {scope} knows SQL or Binder syntax")
PY

echo "M15.6 generic two-source SQL Join contract gate passed"
