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

run_gate "Rust formatting" cargo fmt -- --check
run_gate "Rust lints" cargo clippy --all-targets -- -D warnings
run_gate "Rust unit and pgrx integration tests" cargo test --lib
run_gate "Durable logical-replication ingress test" \
  "${project_root}/scripts/test-replication-ingress.sh"
run_gate "PostgreSQL asynchronous acceptance test" \
  "${project_root}/scripts/test-e2e.sh"
run_gate "Single-source deterministic differential test" \
  "${project_root}/scripts/test-differential-single.sh"
run_gate "Join and subquery deterministic differential test" \
  "${project_root}/scripts/test-join-differential.sh"
run_gate "Concurrency, transaction, and recovery test" \
  "${project_root}/scripts/test-concurrency-recovery.sh"
run_gate "Single-Runtime architecture and shared-log test" \
  "${project_root}/scripts/test-executor-architecture.sh"
run_gate "Deterministic Runtime failpoint recovery test" \
  "${project_root}/scripts/test-failpoint-recovery.sh"
run_gate "Runtime resource-bound and low-work_mem test" \
  "${project_root}/scripts/test-memory-bounds.sh"

printf '\nAll Shiba correctness gates passed.\n'
