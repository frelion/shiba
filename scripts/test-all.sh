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

run_gate "Clean-cut architecture guard" \
  "${project_root}/scripts/test-clean-cut.sh"
run_gate "Rust formatting" cargo fmt --all -- --check
run_gate "Rust lints" cargo clippy --all-targets -- -D warnings
run_gate "Rust unit and pgrx integration tests" cargo test --lib
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
