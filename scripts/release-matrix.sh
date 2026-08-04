#!/usr/bin/env bash
# One-click M1--current-M15 release matrix. Every PostgreSQL scenario owns an isolated cluster.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <absolute-pg17-pg_config> <absolute-pg18-pg_config>" >&2
  exit 64
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg17="$1"
pg18="$2"
for pg_config in "$pg17" "$pg18"; do
  if [[ "$pg_config" != /* || ! -x "$pg_config" ]]; then
    echo "pg_config must be an executable absolute path (got: $pg_config)" >&2
    exit 64
  fi
done

version17="$($pg17 --version)"
version18="$($pg18 --version)"
[[ "$version17" =~ PostgreSQL\ 17\. ]] || {
  echo "first pg_config must be PostgreSQL 17 (got: $version17)" >&2
  exit 64
}
[[ "$version18" =~ PostgreSQL\ 18\. ]] || {
  echo "second pg_config must be PostgreSQL 18 (got: $version18)" >&2
  exit 64
}

foundation_gates=(
  test-empty-install.sh
  test-m2.sh
  test-m3.sh
  test-m4.sh
  test-m4-empty.sh
  test-m4-composite.sh
  test-m4-update.sh
  test-m4-delete.sh
  test-m4-replica-identity.sh
  test-m5-toast.sh
  test-m5-incompressible-toast.sh
  test-m5-composite-delete.sh
  test-m5-replica-index.sh
  test-m5-source-binding.sh
  test-m6-stream-commit.sh
  test-m6-stream-abort.sh
  test-m7-ddl-invalidation.sh
  test-m7-drop-invalidation.sh
  test-m7-column-invalidation.sh
  test-m7-index-invalidation.sh
  test-m7-concurrent-ddl.sh
  test-m8-bounded-decode.sh
  test-m8-multi-source.sh
  test-m8-concurrent-sources.sh
  test-m8-performance.sh
  test-m9-registration.sh
  test-m9-count-sum.sh
  test-m9-operator-concurrency.sh
  test-m9-operator-performance.sh
  test-m10-committed-ingress.sh
  test-m10-streaming-ingress.sh
  test-m10-catalog-ingress.sh
  test-m10-governed-ingress.sh
  test-m10-performance-ingress.sh
  test-m10-shutdown-ingress.sh
  test-m11-bootstrap-contract.sh
  test-m11-bootstrap.sh
  test-m11-recovery.sh
  test-m11-bootstrap-performance.sh
  test-m11-bootstrap-roles.sh
)

m12_gates=(
  test-m12-rebuild-contract.sh
  test-m12-rebuild-admission.sh
  test-m12-rebuild-identity-authority.sh
  test-m12-rebuild-snapshot-live.sh
  test-m12-rebuild-recovery.sh
  test-m12-rebuild-governance.sh
  test-m12-rebuild-performance.sh
)

m13_gates=(
  test-m13-operator-kernel.sh
)

m14_gates=(
  test-m14-grouped.sh
  test-m14-graph-runtime.sh
  test-m14-join-lifecycle.sh
)

m15_gates=(
  test-m15-sql-vertical.sh
  test-m15-sql-aggregates.sh
  test-m15-sql-join.sh
  test-m15-sql-performance.sh
)

m16_gates=(
  test-m16-wide-results.sh
)

cd "$root"

# Enrollment is closed over every current test script: adding a gate without adding
# it to this matrix fails before any expensive PostgreSQL work begins.
expected="$(printf '%s\n' test-l0.sh "${foundation_gates[@]}" "${m12_gates[@]}" "${m13_gates[@]}" "${m14_gates[@]}" "${m15_gates[@]}" "${m16_gates[@]}" | sort)"
discovered="$(find scripts -maxdepth 1 -type f -name 'test-*.sh' -exec basename {} \; | sort)"
if [[ "$expected" != "$discovered" ]]; then
  echo "release matrix enrollment differs from current test scripts" >&2
  diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$discovered") >&2 || true
  exit 1
fi

logs="$(mktemp -d /tmp/shiba-m15-release.XXXXXX)"
cleanup() {
  local exit_code=$?
  trap - EXIT HUP INT TERM
  rm -rf -- "$logs"
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

run() {
  local label="$1"
  shift
  echo "==> $label"
  "$@" 2>&1 | tee "$logs/${label//\//_}.log"
}

run_foundation_matrix() {
  local label="$1"
  local pg_config="$2"
  run "$label/test-l0" env PG_CONFIG="$pg_config" SHIBA_L0_SKIP_RUST=1 scripts/test-l0.sh
  local gate
  for gate in "${foundation_gates[@]}"; do
    run "$label/${gate%.sh}" "scripts/$gate" "$pg_config"
  done
}

run_m12_matrix() {
  local label="$1"
  local pg_config="$2"
  local gate
  for gate in "${m12_gates[@]}"; do
    run "$label/${gate%.sh}" "scripts/$gate" "$pg_config"
  done
}

run_m13_matrix() {
  local label="$1"
  local pg_config="$2"
  local gate
  for gate in "${m13_gates[@]}"; do
    run "$label/${gate%.sh}" "scripts/$gate" "$pg_config"
  done
}

run_m14_matrix() {
  local label="$1"
  local pg_config="$2"
  local gate
  for gate in "${m14_gates[@]}"; do
    run "$label/${gate%.sh}" "scripts/$gate" "$pg_config"
  done
}

run_m15_matrix() {
  local label="$1"
  local pg_config="$2"
  local gate
  for gate in "${m15_gates[@]}"; do
    run "$label/${gate%.sh}" "scripts/$gate" "$pg_config"
  done
}

run_m16_matrix() {
  local label="$1"
  local pg_config="$2"
  local gate
  for gate in "${m16_gates[@]}"; do
    run "$label/${gate%.sh}" "scripts/$gate" "$pg_config"
  done
}

foundation_count=$((${#foundation_gates[@]} + 1))
m12_count=${#m12_gates[@]}
m13_count=${#m13_gates[@]}
m14_count=${#m14_gates[@]}
m15_count=${#m15_gates[@]}
m16_count=${#m16_gates[@]}
unique_pg_scripts=$((foundation_count + m12_count + m13_count + m14_count + m15_count + m16_count))
pg_invocations=$((2 * unique_pg_scripts))
echo "Release matrix plan: $version17; $version18"
echo "PG scripts: unique=$unique_pg_scripts foundation_per_version=$foundation_count m12_per_version=$m12_count m13_per_version=$m13_count m14_per_version=$m14_count m15_per_version=$m15_count m16_per_version=$m16_count invocations=$pg_invocations"

echo "==> phase 1/9: formatting, check, and directed pure tests"
scripts/check-m15-contract.sh
scripts/check-m15-parser.sh
scripts/check-m15-registration.sh
scripts/check-m15-aggregates.sh
scripts/check-m15-join.sh
scripts/check-m15-release.sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test -p shiba-operator -p shiba-compiler
cargo test -p shiba-runtime --lib
cargo test -p shiba-ingress --lib

echo "==> phase 2/9: workspace tests and warning-free clippy"
# Phase 1's all-target check compiles every integration target, and the
# enrolled PostgreSQL scripts execute those integration bodies. Running
# `cargo test --workspace` here only launches each ignored integration binary
# again to print "ignored". Keep library and doctest coverage without that
# redundant process fan-out; clippy still checks every target below.
cargo test --workspace --lib
cargo test --workspace --doc
cargo clippy --workspace --all-targets -- -D warnings

echo "==> phase 3/9: PostgreSQL 17 M1--M11 matrix"
run_foundation_matrix pg17 "$pg17"

echo "==> phase 4/9: PostgreSQL 18 M1--M11 matrix"
run_foundation_matrix pg18 "$pg18"

echo "==> phase 5/9: M12 differential, recovery, concurrency, roles, and performance"
run_m12_matrix pg17-m12 "$pg17"
run_m12_matrix pg18-m12 "$pg18"

echo "==> phase 6/9: M13 generic scalar/keyed operator differential"
run_m13_matrix pg17-m13 "$pg17"
run_m13_matrix pg18-m13 "$pg18"

echo "==> phase 7/9: M14 grouped-state and unified graph Runtime differential"
run_m14_matrix pg17-m14 "$pg17"
run_m14_matrix pg18-m14 "$pg18"

echo "==> phase 8/9: M15 SQL registration and production lifecycle differential"
run_m15_matrix pg17-m15 "$pg17"
run_m15_matrix pg18-m15 "$pg18"

echo "==> phase 9/9: M16 canonical wide result ABI"
run_m16_matrix pg17-m16 "$pg17"
run_m16_matrix pg18-m16 "$pg18"

echo "==> release matrix passed"
echo "PostgreSQL versions: $version17; $version18"
echo "PG scripts: unique=$unique_pg_scripts foundation_per_version=$foundation_count m12_per_version=$m12_count m13_per_version=$m13_count m14_per_version=$m14_count m15_per_version=$m15_count m16_per_version=$m16_count invocations=$pg_invocations"
echo "M12 performance evidence:"
for label in pg17-m12 pg18-m12; do
  performance_log="$logs/${label}_test-m12-rebuild-performance.log"
  performance_count="$(grep -o 'M12 performance measured .*' "$performance_log" | wc -l | tr -d ' ' || true)"
  if [[ "$performance_count" -ne 1 ]]; then
    echo "M12 performance test for $label emitted $performance_count measurement markers, expected exactly one" >&2
    exit 1
  fi
  performance_line="$(grep -o 'M12 performance measured .*' "$performance_log")"
  echo "$label: $performance_line"
done
echo "M15 SQL frontend performance evidence:"
for label in pg17-m15 pg18-m15; do
  performance_log="$logs/${label}_test-m15-sql-performance.log"
  frontend_count="$(grep -c 'M15 SQL frontend performance ' "$performance_log" || true)"
  registration_count="$(grep -c 'M15 SQL registration performance ' "$performance_log" || true)"
  if [[ "$frontend_count" -ne 1 || "$registration_count" -ne 1 ]]; then
    echo "M15 performance test for $label emitted frontend=$frontend_count registration=$registration_count markers, expected one each" >&2
    exit 1
  fi
  echo "$label: $(grep 'M15 SQL frontend performance ' "$performance_log")"
  echo "$label: $(grep 'M15 SQL registration performance ' "$performance_log")"
done
