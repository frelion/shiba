#!/usr/bin/env bash
# Shared, deliberately small helpers for PostgreSQL integration gates. Each
# gate supplies its connection wrapper and timing knobs; its SQL scenario stays
# in the gate itself.

cleanup() {
  if test "${SHIBA_KEEP_TEST_CLUSTER:-0}" = "1"; then
    printf 'Retained test cluster: %s\n' "${pg_data_dir}" >&2
    printf 'Retained test socket: %s\n' "${pg_socket_dir}" >&2
    if test "${test_retain_log:-0}" = "1"; then
      printf 'PostgreSQL log: %s\n' "${pg_log_file}" >&2
    fi
    return
  fi
  "${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -m immediate stop \
    >/dev/null 2>&1 || true
  rm -rf "${pg_data_dir}" "${pg_socket_dir}"
}

fail() {
  printf '%s failed: %s\n' "${test_name}" "$1" >&2
  tail -n "${test_log_lines:-160}" "${pg_log_file}" >&2 || true
  exit 1
}

assert_query() {
  local expected="$1"
  local query="$2"
  local actual
  actual="$("${test_psql_command}" -Atqc "${query}")"
  if test "${actual}" != "${expected}"; then
    fail "expected [${expected}], got [${actual}] for: ${query}"
  fi
}

expect_failure() {
  local expected_message="$1"
  local query="$2"
  local output
  if output="$("${test_psql_command}" -qc "${query}" 2>&1)"; then
    fail "query unexpectedly succeeded: ${query}"
  fi
  if [[ "${output}" != *"${expected_message}"* ]]; then
    fail "expected error containing [${expected_message}], got: ${output}"
  fi
}

wait_for_query() {
  local expected="$1"
  local query="$2"
  local description="$3"
  local actual=""
  local attempt
  for ((attempt = 1; attempt <= test_wait_attempts; attempt++)); do
    if actual="$("${test_psql_command}" -Atqc "${query}" 2>/dev/null)" &&
       test "${actual}" = "${expected}"; then
      return
    fi
    sleep "${test_wait_sleep}"
  done
  fail "timed out waiting for ${description}; expected [${expected}], last value was [${actual}]"
}

wait_for_log() {
  local pattern="$1"
  local description="$2"
  local attempt
  for ((attempt = 1; attempt <= test_wait_attempts; attempt++)); do
    if grep -Fq "${pattern}" "${pg_log_file}"; then
      return
    fi
    sleep "${test_wait_sleep}"
  done
  fail "timed out waiting for ${description}"
}

runtime_pid() {
  "${test_psql_command}" -Atqc "
    SELECT pid
    FROM pg_stat_activity
    WHERE backend_type='shiba runtime'"
}

wait_for_runtime_replacement() {
  local failed_pid="$1"
  wait_for_query "1" "
    SELECT count(*)
    FROM shiba_internal.runtime_state AS state
    JOIN pg_stat_activity AS activity
      ON activity.pid=state.owner_pid
     AND activity.backend_type='shiba runtime'
    WHERE state.singleton
      AND state.active
      AND state.owner_pid<>${failed_pid}" \
    "the singleton Runtime replacement"
}

assert_bag_equal() {
  local expected_sql="$1"
  local actual_sql="$2"
  local description="$3"
  wait_for_query "0" "
    WITH expected AS (${expected_sql}),
    actual AS (${actual_sql}),
    difference AS (
      (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
      UNION ALL
      (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
    )
    SELECT count(*) FROM difference" "${description}"
}
