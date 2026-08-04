#!/usr/bin/env bash
# M16.1 database-free generic aggregate reference-contract gate.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

python3 - <<'PY'
import pathlib

test = pathlib.Path("crates/shiba-operator/tests/m16_aggregate_reference.rs")
model = pathlib.Path("crates/shiba-operator/tests/m16_aggregate_reference/model.rs")
fixtures = pathlib.Path("crates/shiba-operator/tests/m16_aggregate_reference/fixtures.rs")
grouped = pathlib.Path("crates/shiba-operator/tests/m16_aggregate_reference/grouped.rs")
grouped_cases = pathlib.Path(
    "crates/shiba-operator/tests/m16_aggregate_reference/grouped_cases.rs"
)
transition = pathlib.Path("crates/shiba-operator/tests/m16_aggregate_reference/transition.rs")
for path in (test, model, fixtures, grouped, grouped_cases, transition):
    if not path.is_file():
        raise SystemExit(f"M16.1 reference-contract file is missing: {path}")

combined = "\n".join(
    path.read_text()
    for path in (test, model, fixtures, grouped, grouped_cases, transition)
)
required = (
    "CountStar", "Count {", "Sum {", "Min {", "Max {",
    "ordinal", "Null", "before", "after", "normalize",
    "having_visibility", "UnknownVersion", "UnknownFunction",
    "RetractMissing", "MAX_CALLS", "MAX_CHANGES", "MAX_ROW_WIDTH",
    "fixed_seed_randomized_iud", "grouped_creation_deletion", "key_change",
    "decode_result", "truncated_extra_type_nullability_and_version",
    "FUNCTION_VERSION", "STATE_CODEC_VERSION", "UnknownCodec",
    "kernel_membership_namespace_supports_groups_without_count_star",
    "MAX_TOUCHED_GROUPS", "MAX_EMITTED_RESULT_IMAGES",
    "MAX_GRAPH_STATE_MUTATIONS", "MAX_GRAPH_OUTPUT_MUTATIONS",
)
missing = [marker for marker in required if marker not in combined]
if missing:
    raise SystemExit(f"M16.1 reference semantics are missing: {missing}")

for forbidden in ("postgres::", "pgrx::", "tokio_postgres", "sqlx::"):
    if forbidden in combined:
        raise SystemExit(f"M16.1 database-free reference model uses {forbidden}")

pg_gates = sorted(path.name for path in pathlib.Path("scripts").glob("test-m16*.sh"))
if pg_gates != ["test-m16-wide-results.sh"]:
    raise SystemExit(
        "M16 PostgreSQL gate enrollment must be exact: " + ", ".join(pg_gates)
    )

wide = "\n".join(
    pathlib.Path(path).read_text()
    for path in (
        "crates/shiba-runtime/tests/m16_wide_results.rs",
        "crates/shiba-runtime/tests/support/mod.rs",
    )
)
for marker in (
    "canonical_result_rows", "scalar_int8_result", "keyed_int8_results",
    "schema_payload", "schema_digest", "row_identity", "row_payload",
    "reject wide sink", "AlreadyApplied", "result_status='building'",
):
    if marker not in wide:
        raise SystemExit(f"M16.2 wide-result gate is missing: {marker}")

legacy_columns = (
    "value_bigint", "value_payload", "output_shape", "result_key_bigint",
    "result_value_bigint", "result_key_is_null", "result_value_is_null",
)
ingress_tests = "\n".join(
    path.read_text() for path in pathlib.Path("crates/shiba-ingress/tests").rglob("*.rs")
)
found = [column for column in legacy_columns if column in ingress_tests]
if found:
    raise SystemExit(f"M16.2 ingress tests retain fixed result columns: {found}")

contract = pathlib.Path("docs/AGGREGATE_FUNCTION_CONTRACT.md").read_text()
contract_markers = (
    "# M16 generic aggregate function and wide result contract",
    "## Stable Aggregate Function ABI",
    "## Canonical wide results",
    "## Bounds",
    "## Extension and residual boundaries",
    "MAX_AGGREGATE_CALLS = 16",
    "namespace `0`",
    "The M16.7 extensibility acceptance test is exact:",
)
missing_contract = [marker for marker in contract_markers if marker not in contract]
if missing_contract:
    raise SystemExit(f"M16.1 aggregate contract ABI markers are missing: {missing_contract}")
PY

echo "M16.1 database-free aggregate reference-contract gate passed"
