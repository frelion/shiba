#!/usr/bin/env bash
set -euo pipefail

# Meta-gate for the correctness gate itself. It deliberately checks durable
# test obligations: a new test script must be wired into the complete gate,
# and every PostgreSQL scenario must fail fast and clean up. Keeping this
# outside the scenario scripts makes accidental test omission visible before a
# slow database run starts.
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

fail_contract() {
  printf 'test gate contract failed: %s\n' "$1" >&2
  exit 1
}

require_fixed_match() {
  local needle="$1"
  local file="$2"
  local message="$3"
  if ! rg -q --fixed-strings -- "$needle" "$file"; then
    fail_contract "${message} (${file})"
  fi
}

require_exactly_once_in_all() {
  local script="$1"
  local count
  count="$(rg -F -c -- "${script}" scripts/test-all.sh || true)"
  if test "${count}" != "1"; then
    fail_contract "${script} must be invoked exactly once by scripts/test-all.sh; found ${count}"
  fi
}

require_fixed_match 'set -euo pipefail' scripts/test-all.sh \
  'complete gate must use strict shell failure handling'
require_fixed_match '"$@"' scripts/test-all.sh \
  'complete gate must propagate the command status from run_gate'
require_fixed_match 'All Shiba correctness gates passed.' scripts/test-all.sh \
  'complete gate success marker disappeared'
require_exactly_once_in_all scripts/test-gate-contract.sh

server_scripts=(
  scripts/test-differential-oracle.sh
  scripts/test-effect-stream-core.sh
  scripts/test-replication-ingress.sh
  scripts/test-stateless-kernels.sh
  scripts/test-fanout-recovery.sh
  scripts/test-aggregate-distinct-kernels.sh
  scripts/test-window-topn-kernels.sh
)

for script in scripts/test-*.sh; do
  case "${script}" in
    scripts/test-all.sh|scripts/test-gate-contract.sh|scripts/test-lib.sh)
      continue
      ;;
  esac

  test -x "${script}" || fail_contract "test script is not executable: ${script}"
  test "$(sed -n '1p' "${script}")" = '#!/usr/bin/env bash' \
    || fail_contract "test script must use the repository Bash entrypoint: ${script}"
  require_fixed_match 'set -euo pipefail' "${script}" \
    'test script must use strict shell failure handling'
  require_exactly_once_in_all "${script}"
done

for script in "${server_scripts[@]}"; do
  require_fixed_match 'source "${project_root}/scripts/test-lib.sh"' "${script}" \
    'PostgreSQL test must use shared assertions and cleanup'
  require_fixed_match 'trap cleanup EXIT' "${script}" \
    'PostgreSQL test must clean up its temporary cluster'
  require_fixed_match 'statement_timeout=' "${script}" \
    'PostgreSQL test must bound SQL execution'
  require_fixed_match 'lock_timeout=' "${script}" \
    'PostgreSQL test must bound lock waits'
  require_fixed_match 'install_test_extension "${pg_config_path}"' "${script}" \
    'PostgreSQL test must use the shared one-install optimization'
done

require_fixed_match 'run_gate "Gate self-check"' scripts/test-all.sh \
  'complete gate must validate its own wiring before slow scenarios'
require_fixed_match 'run_gate "Prepare PostgreSQL extension once"' scripts/test-all.sh \
  'complete gate must prepare one reusable extension artifact'
require_fixed_match 'export SHIBA_SKIP_EXTENSION_INSTALL=1' scripts/test-all.sh \
  'complete gate must prevent repeated extension installation'

printf '%s\n' 'test gate contract passed'
