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
    "generic aggregate lowering": (frontend, r"QueryOperationV1::Aggregate"),
    "count function mapping": (frontend, r"AggregateFunctionV1::CountStar"),
    "sum function mapping": (frontend, r"AggregateFunctionV1::SumInt8"),
    "descriptor-driven compiler validation": (compiler, r"aggregate_function_descriptor"),
    "generic aggregate node": (operator, r"OperatorNodeKind::Aggregate"),
    "declared result field nullability": (compiler, r"QueryResultFieldV1\s*\{[^}]*nullable:\s*bool"),
    "compiled result field nullability": (operator, r"ResultField\s*\{[^}]*nullable:\s*bool"),
    "sum non-null count state": (operator, r"CallState::Sum\s*\{\s*non_null"),
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
    if re.search(r"\b(?:CountRows|SumInt8|GroupedCount|GroupedSumInt8|AggregateFunctionV1)\b", text):
        raise SystemExit(f"M15.5 {scope} knows a concrete aggregate operator")

old_nodes = re.compile(r"QueryOperationV1::(?:CountRows|SumInt8|GroupedCount|GroupedSumInt8)|OperatorNodeKind::(?:CountRows|SumInt8|GroupedCount|GroupedSumInt8)")
for scope, text in (("frontend", frontend), ("compiler", compiler), ("operator", operator)):
    if old_nodes.search(text):
        raise SystemExit(f"M16.3 {scope} retains a removed concrete aggregate node")
PY

echo "M15.5 generic SQL aggregate and nullable scalar contract gate passed"
