#!/usr/bin/env bash
set -euo pipefail

# Static guard for the surfaces that a refactor must not change accidentally.
# Runtime behavior is covered by the PostgreSQL acceptance tests; this gate
# catches misplaced files, renamed entry points, and persistence-contract
# drift before those tests start a cluster.
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

require_file() {
  if test ! -f "$1"; then
    printf 'required contract file is missing: %s\n' "$1" >&2
    exit 1
  fi
}

require_match() {
  local pattern="$1"
  local file="$2"
  local message="$3"
  if ! rg -q --multiline -- "$pattern" "$file"; then
    printf '%s (%s)\n' "$message" "$file" >&2
    exit 1
  fi
}

require_file shiba.control
require_match "default_version = '0.1.0'" shiba.control \
  "extension default version changed"
require_match "relocatable = false" shiba.control \
  "extension relocatability changed"
require_match "schema = 'shiba'" shiba.control \
  "extension schema changed"

expected_sql_files=(
  sql/00_catalog.sql
  sql/10_runtime.sql
  sql/11_ingress.sql
  sql/12_effect_stream.sql
  sql/25_introspection.sql
  sql/30_registration.sql
  sql/40_lifecycle.sql
)
for sql_file in "${expected_sql_files[@]}"; do
  require_file "$sql_file"
done

test "$(rg -c 'pgrx::extension_sql_file!' src/lib.rs)" -eq "${#expected_sql_files[@]}" || {
  printf 'extension SQL file count changed\n' >&2
  exit 1
}
for sql_file in "${expected_sql_files[@]}"; do
  require_match "\\.\\./${sql_file}" src/lib.rs \
    "extension SQL registration is missing"
done

require_match '#\[pg_extern' src/lib.rs \
  "version entry point lost its PostgreSQL registration"
require_match "fn version\\(\\) -> &'static str" src/lib.rs \
  "version entry point changed"
require_match 'pub fn activate\(\) -> bool' src/lifecycle.rs \
  "activate entry point changed"
require_match 'pub fn deactivate\(\)' src/lifecycle.rs \
  "deactivate entry point changed"
require_match 'deny_unknown_fields' src/planner/model.rs \
  "persisted plan JSON is no longer strict"
require_match 'pub\(crate\) struct DataflowPlan' src/planner/model.rs \
  "persisted plan model moved unexpectedly"
require_match 'replace_cas' src/execution/continuation.rs \
  "continuation compare-and-set authority was removed"
require_match 'streamed_transactions: BTreeMap' src/ingress.rs \
  "concurrent streamed transaction tracking regressed to a single slot"
require_file src/replication/mod.rs
require_file src/replication/transport.rs
require_file src/replication/pgoutput.rs
require_file docs/OPERATOR_PROTOCOL.md
require_file docs/OPERATOR_SQL_AUDIT.md
require_match 'KernelPhase' docs/OPERATOR_PROTOCOL.md \
  "operator lifecycle protocol is not documented"
require_match 'TransactionResult' docs/OPERATOR_PROTOCOL.md \
  "transaction result protocol is not documented"
require_match 'OperatorProtocol trait decision' docs/OPERATOR_PROTOCOL.md \
  "operator protocol abstraction decision is not documented"
require_match 'StepContext::record_output_append' docs/OPERATOR_SQL_AUDIT.md \
  "operator SQL output boundary is not documented"
require_match 'Join / `append_inner_page`, `append_actions`' docs/OPERATOR_SQL_AUDIT.md \
  "Join retention decision is not documented"

for operator_file in \
  src/execution/linear/mod.rs \
  src/execution/linear/machine.rs \
  src/execution/linear/runtime.rs \
  src/execution/linear/storage.rs \
  src/execution/sink/mod.rs \
  src/execution/sink/machine.rs \
  src/execution/sink/runtime.rs \
  src/execution/distinct/mod.rs \
  src/execution/distinct/machine.rs \
  src/execution/distinct/provision.rs \
  src/execution/distinct/runtime.rs \
  src/execution/join/mod.rs \
  src/execution/join/planner.rs \
  src/execution/join/provision.rs \
  src/execution/join/runtime.rs \
  src/execution/aggregate/mod.rs \
  src/execution/aggregate/machine.rs \
  src/execution/aggregate/provision.rs \
  src/execution/aggregate/runtime.rs \
  src/execution/window/mod.rs \
  src/execution/window/machine.rs \
  src/execution/window/output.rs \
  src/execution/window/primitives.rs \
  src/execution/window/provision.rs \
  src/execution/window/step.rs \
  src/execution/topn/mod.rs \
  src/execution/topn/machine.rs \
  src/execution/topn/provision.rs \
  src/execution/topn/runtime.rs
do
  require_file "$operator_file"
done

operator_dirs=(linear sink distinct join aggregate window topn)
for operator_dir in "${operator_dirs[@]}"; do
  operator_root="src/execution/${operator_dir}"
  if ! rg -q 'StepContext|\.transition\(' "${operator_root}"; then
    printf 'operator does not use the shared StepContext transition path: %s\n' \
      "${operator_root}" >&2
    exit 1
  fi
  if ! rg -q 'validate_(typed_)?continuation_abi|validate_continuation_abi' \
      "${operator_root}"; then
    printf 'operator has no visible continuation ABI validation: %s\n' \
      "${operator_root}" >&2
    exit 1
  fi
done

for removed_path in \
  src/kernel \
  src/logical \
  src/query_lowering.rs \
  src/scalar_sql.rs
do
  if test -e "$removed_path"; then
    printf 'compatibility-only path still exists: %s\n' "$removed_path" >&2
    exit 1
  fi
done

printf '%s\n' "contract surface guard passed"
