#!/usr/bin/env bash
# M15.5 generic SQL aggregate and nullable scalar contract gate.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

python3 - <<'PY'
import pathlib
import re

frontend = "\n".join(
    path.read_text() for path in pathlib.Path("crates/shiba-sql-frontend/src").glob("*.rs")
)
operator = "\n".join(
    path.read_text() for path in pathlib.Path("crates/shiba-operator/src").glob("*.rs")
)
runtime = "\n".join(
    path.read_text() for path in pathlib.Path("crates/shiba-runtime/src").glob("*.rs")
)
ingress = "\n".join(
    path.read_text() for path in pathlib.Path("crates/shiba-ingress/src").glob("*.rs")
)
compiler = "\n".join(
    path.read_text() for path in pathlib.Path("crates/shiba-compiler/src").glob("*.rs")
)
sql = pathlib.Path("sql/v2/002_source_apply.sql").read_text()

required = {
    "generic count lowering": (frontend, r"QueryOperationV1::CountRows"),
    "generic scalar sum lowering": (frontend, r"QueryOperationV1::SumInt8"),
    "generic grouped count lowering": (frontend, r"QueryOperationV1::GroupedCount"),
    "generic grouped sum lowering": (frontend, r"QueryOperationV1::GroupedSumInt8"),
    "declared result field nullability": (compiler, r"QueryResultFieldV1\s*\{[^}]*nullable:\s*bool"),
    "compiled result field nullability": (operator, r"ResultField\s*\{[^}]*nullable:\s*bool"),
    "sum non-null count state": (operator, r"fn\s+non_null_key\s*\("),
    "schema-driven result sink": (runtime, r"ResultSchemaV1::from_canonical_payload"),
    "canonical result schema header": (sql, r"schema_payload bytea NOT NULL.*schema_digest bytea NOT NULL"),
}
missing = [name for name, (text, pattern) in required.items() if not re.search(pattern, text, re.S)]
if missing:
    raise SystemExit(f"M15.5 aggregate contract markers are missing: {missing}")

forbidden_recipe = re.compile(r"\b(?:CountRowsSQL|GroupedQuery|FilteredAggregate)\b")
for scope, text in (("frontend", frontend), ("compiler", compiler), ("runtime", runtime), ("ingress", ingress)):
    if forbidden_recipe.search(text):
        raise SystemExit(f"M15.5 {scope} contains a forbidden SQL aggregate recipe")

for scope, text in (("runtime", runtime), ("ingress", ingress)):
    if re.search(r"\b(?:CountRows|SumInt8|GroupedCount|GroupedSumInt8)\b", text):
        raise SystemExit(f"M15.5 {scope} knows a concrete aggregate operator")
PY

echo "M15.5 generic SQL aggregate and nullable scalar contract gate passed"
