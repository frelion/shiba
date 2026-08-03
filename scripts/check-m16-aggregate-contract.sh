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

fake_pg_gates = sorted(pathlib.Path("scripts").glob("test-m16*.sh"))
if fake_pg_gates:
    raise SystemExit(
        "M16.1 must not claim PostgreSQL production evidence: "
        + ", ".join(map(str, fake_pg_gates))
    )

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
