#!/usr/bin/env bash
set -euo pipefail

# Complete correctness gate for changes to Shiba. Keep this list explicit so
# adding a new test layer cannot silently replace an existing one.
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

run_gate() {
  local name="$1"
  shift
  printf '\n==> %s\n' "${name}"
  "$@"
}

pg_config_path="${PG_CONFIG:-$("${project_root}/scripts/resolve-pg-config.sh")}"
pg_major="$("${pg_config_path}" --version | sed -E 's/^PostgreSQL ([0-9]+).*/\1/')"
case "${pg_major}" in
  17|18) pg_feature="pg${pg_major}" ;;
  *)
    printf 'unsupported PostgreSQL major version: %s\n' "${pg_major}" >&2
    exit 1
    ;;
esac

run_gate "Gate self-check" \
  "${project_root}/scripts/test-gate-contract.sh"
run_gate "Clean-cut architecture guard" \
  "${project_root}/scripts/test-clean-cut.sh"
run_gate "Public and persistence contract guard" \
  "${project_root}/scripts/test-contract-surface.sh"
run_gate "Rust formatting" cargo fmt --all -- --check
run_gate "Rust lints" cargo clippy --all-targets --no-default-features --features "${pg_feature}" -- -D warnings
run_gate "Rust unit and pgrx integration tests" cargo test --lib --no-default-features --features "${pg_feature}"
run_gate "Prepare PostgreSQL extension once" \
  cargo pgrx install \
    --pg-config "${pg_config_path}" \
    --no-default-features \
    --features "${pg_feature} pg_test"
export SHIBA_SKIP_EXTENSION_INSTALL=1
run_gate "Independent differential SQL oracle test" \
  "${project_root}/scripts/test-differential-oracle.sh"
run_gate "Continuous effect-stream core test" \
  "${project_root}/scripts/test-effect-stream-core.sh"
run_gate "Durable logical-replication ingress test" \
  "${project_root}/scripts/test-replication-ingress.sh"
run_gate "Stateless Scan/Filter/Project/Sink test" \
  "${project_root}/scripts/test-stateless-kernels.sh"
run_gate "Shared fanout recovery and backpressure test" \
  "${project_root}/scripts/test-fanout-recovery.sh"
run_gate "Aggregate and Distinct recovery test" \
  "${project_root}/scripts/test-aggregate-distinct-kernels.sh"
run_gate "Window and TopN recovery test" \
  "${project_root}/scripts/test-window-topn-kernels.sh"

printf '\nAll Shiba correctness gates passed.\n'
