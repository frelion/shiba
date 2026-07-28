#!/usr/bin/env bash
set -euo pipefail

# Deterministically crash the test-only single Runtime at the two durable
# handoff boundaries that ordinary kill-based recovery tests cannot target:
# apply-before-inbox-ack and route-before-slot-advance.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-runtime-failpoint-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-runtime-failpoint-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_FAILPOINT_TEST_PORT:-$((59000 + $$ % 4000))}"
database_name="shiba_runtime_failpoint"

cleanup() {
  if test "${SHIBA_KEEP_TEST_CLUSTER:-0}" = "1"; then
    printf 'Retained test cluster: %s\n' "${pg_data_dir}" >&2
    printf 'Retained test socket: %s\n' "${pg_socket_dir}" >&2
    return
  fi
  "${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "${pg_data_dir}" "${pg_socket_dir}"
}
trap cleanup EXIT

psql_gate() {
  PGOPTIONS="-c statement_timeout=15000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

fail() {
  printf 'deterministic Runtime failpoint gate failed: %s\n' "$1" >&2
  printf 'PostgreSQL log: %s\n' "${pg_log_file}" >&2
  exit 1
}

assert_query() {
  local expected="$1"
  local query="$2"
  local actual
  actual="$(psql_gate -Atqc "${query}")"
  if test "${actual}" != "${expected}"; then
    fail "expected [${expected}], got [${actual}] for: ${query}"
  fi
}

wait_for_query() {
  local expected="$1"
  local query="$2"
  local description="$3"
  local actual=""
  local attempt
  for attempt in {1..300}; do
    if actual="$(psql_gate -Atqc "${query}" 2>/dev/null)" &&
       test "${actual}" = "${expected}"; then
      return 0
    fi
    sleep 0.1
  done
  fail "timed out waiting for ${description}; last value was [${actual}]"
}

wait_for_log() {
  local pattern="$1"
  local description="$2"
  local attempt
  for attempt in {1..300}; do
    if grep -Fq "${pattern}" "${pg_log_file}"; then
      return 0
    fi
    sleep 0.1
  done
  fail "timed out waiting for ${description}"
}

assert_log_count() {
  local expected="$1"
  local pattern="$2"
  local description="$3"
  local actual
  actual="$(grep -Fc "${pattern}" "${pg_log_file}" || true)"
  if test "${actual}" != "${expected}"; then
    fail "expected ${expected} log record(s) for ${description}, got ${actual}: ${pattern}"
  fi
}

runtime_pid() {
  psql_gate -Atqc "
    SELECT pid FROM pg_stat_activity
    WHERE backend_type='shiba runtime'"
}

wait_for_replacement_runtime() {
  local failed_pid="$1"
  wait_for_query "1" "
    SELECT count(*)
    FROM shiba_internal.runtime_state state
    JOIN pg_stat_activity activity
      ON activity.pid=state.owner_pid
     AND activity.backend_type='shiba runtime'
    WHERE state.singleton
      AND state.active
      AND state.owner_pid<>${failed_pid}" \
    "PostgreSQL to restart one replacement Runtime"
  assert_query "1|0|0" "
    SELECT
      count(*) FILTER (WHERE backend_type='shiba runtime')
      || '|' ||
      count(*) FILTER (WHERE backend_type='shiba router')
      || '|' ||
      count(*) FILTER (WHERE backend_type='shiba executor')
    FROM pg_stat_activity"
}

set_dag_active() {
  local result_oid="$1"
  local active="$2"
  psql_gate -qc "
    UPDATE shiba_internal.dag_runtime_state
    SET active=${active}
    WHERE result_oid=${result_oid}::oid"
}

baseline_diff="
WITH expected AS (
  SELECT group_id,count(*)::bigint AS row_count,sum(amount)::bigint AS total
  FROM public.failpoint_source GROUP BY group_id
),
actual AS (
  SELECT group_id,row_count::bigint,total::bigint
  FROM shiba.failpoint_result
),
difference AS (
  (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
  UNION ALL
  (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT count(*) FROM difference"

cd "${project_root}"
cargo pgrx install --pg-config "${pg_config_path}" --features pg_test

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\n"
  printf "wal_level = logical\n"
  printf "max_replication_slots = 4\n"
  printf "max_worker_processes = 16\n"
  printf "listen_addresses = ''\n"
  printf "unix_socket_directories = '%s'\n" "${pg_socket_dir}"
  printf "port = %s\n" "${pg_port}"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_gate -qc "CREATE EXTENSION shiba"
psql_gate -qc "SELECT shiba.activate()"
psql_gate -qc "
  CREATE TABLE public.shiba_runtime_failpoints (
    kind text PRIMARY KEY,
    runtime_pid integer,
    result_oid oid,
    commit_lsn pg_lsn,
    pause_ms integer NOT NULL DEFAULT 0 CHECK (pause_ms>=0),
    fired boolean NOT NULL DEFAULT false
  );
  CREATE TABLE public.failpoint_source (
    event_id integer PRIMARY KEY,
    group_id integer NOT NULL,
    amount integer NOT NULL
  );
  INSERT INTO public.failpoint_source VALUES (1,1,10);
  CREATE TABLE shiba.failpoint_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total
  FROM public.failpoint_source GROUP BY group_id"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'" \
  "the single Runtime"
assert_query "0|0" "
  SELECT
    count(*) FILTER (WHERE backend_type='shiba router')
    || '|' ||
    count(*) FILTER (WHERE backend_type='shiba executor')
  FROM pg_stat_activity"
wait_for_query "0" "${baseline_diff}" "the initial result"

result_oid="$(psql_gate -Atqc "
  SELECT 'shiba.failpoint_result'::regclass::oid::integer")"

printf '\n==> Runtime apply-before-ack rollback\n'
set_dag_active "${result_oid}" false
psql_gate -qc "INSERT INTO public.failpoint_source VALUES (2,2,20)"
wait_for_query "1" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid" \
  "the apply failpoint commit to reach the durable inbox"
apply_lsn="$(psql_gate -Atqc "
  SELECT commit_lsn FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid")"
apply_runtime_pid="$(runtime_pid)"
progress_before="$(psql_gate -Atqc "
  SELECT coalesce(applied_lsn::text,'NULL')
  FROM shiba_internal.view_progress
  WHERE result_oid=${result_oid}::oid")"
psql_gate -qc "
  INSERT INTO public.shiba_runtime_failpoints
    (kind,runtime_pid,result_oid,commit_lsn,pause_ms)
  VALUES
    ('runtime_apply_before_ack',${apply_runtime_pid},${result_oid}::oid,
     '${apply_lsn}'::pg_lsn,2000);
  UPDATE shiba_internal.dag_runtime_state
  SET active=true
  WHERE result_oid=${result_oid}::oid"

wait_for_log \
  "test failpoint reached: runtime_apply_before_ack result ${result_oid} commit ${apply_lsn}" \
  "the Runtime to reach the apply-before-ack boundary"
# Keep the replacement from draining retained input before rollback assertions.
set_dag_active "${result_oid}" false
wait_for_log \
  "runtime exited after applying commit ${apply_lsn} and before acknowledgement" \
  "the apply-before-ack failpoint"
wait_for_query "0" "
  SELECT count(*) FROM pg_stat_activity
  WHERE backend_type='shiba runtime' AND pid=${apply_runtime_pid}" \
  "the failed Runtime to exit"
wait_for_replacement_runtime "${apply_runtime_pid}"

assert_query "1|1|0|${progress_before}" "
  SELECT
    (SELECT count(*) FROM shiba_internal.dag_inbox
     WHERE result_oid=${result_oid}::oid
       AND commit_lsn='${apply_lsn}'::pg_lsn)
    || '|' ||
    (SELECT count(*) FROM shiba_internal.change_log
     WHERE commit_lsn='${apply_lsn}'::pg_lsn)
    || '|' ||
    (SELECT count(*) FROM shiba.failpoint_result WHERE group_id=2)
    || '|' ||
    (SELECT coalesce(applied_lsn::text,'NULL')
     FROM shiba_internal.view_progress
     WHERE result_oid=${result_oid}::oid)"
# The claim happens in the failed apply transaction and must roll back too.
assert_query "f" "
  SELECT fired FROM public.shiba_runtime_failpoints
  WHERE kind='runtime_apply_before_ack'"

psql_gate -qc "
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='runtime_apply_before_ack';
  UPDATE shiba_internal.dag_runtime_state
  SET active=true
  WHERE result_oid=${result_oid}::oid"
wait_for_query "0" "${baseline_diff}" "the replacement Runtime to replay retained input"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid" \
  "the replayed inbox reference to be acknowledged"

printf '\n==> Runtime route-before-slot-advance replay idempotence\n'
set_dag_active "${result_oid}" false
route_runtime_pid="$(runtime_pid)"
slot_lsn_before="$(psql_gate -Atqc "
  SELECT confirmed_flush_lsn FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text")"
psql_gate -qc "
  INSERT INTO public.shiba_runtime_failpoints(kind,runtime_pid,pause_ms)
  VALUES ('runtime_route_before_slot_advance',${route_runtime_pid},100);
  INSERT INTO public.failpoint_source VALUES (3,3,30)"

wait_for_log \
  "runtime exited after routing and before slot advancement" \
  "the route-before-slot-advance failpoint"
route_lsn="$(psql_gate -Atqc "
  SELECT commit_lsn FROM shiba_internal.change_log
  WHERE source_oid='public.failpoint_source'::regclass
    AND row_data->>'event_id'='3'")"
wait_for_query "0" "
  SELECT count(*) FROM pg_stat_activity
  WHERE backend_type='shiba runtime' AND pid=${route_runtime_pid}" \
  "the routing Runtime to exit"
wait_for_replacement_runtime "${route_runtime_pid}"

assert_query "true|${route_runtime_pid}|${route_lsn}" "
  SELECT fired || '|' || runtime_pid || '|' || commit_lsn
  FROM public.shiba_runtime_failpoints
  WHERE kind='runtime_route_before_slot_advance'"
wait_for_query "t" "
  SELECT confirmed_flush_lsn>'${slot_lsn_before}'::pg_lsn
  FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text" \
  "the replacement Runtime to advance the logical slot"
# GC may concurrently remove older, fully acknowledged transactions. Assert
# the replayed commit's checkpoint directly instead of relying on a global
# catalog count that is intentionally not stable.
assert_query "1" "
  SELECT count(*) FROM shiba_internal.routed_transactions
  WHERE commit_lsn='${route_lsn}'::pg_lsn"
assert_query "1|1" "
  SELECT
    (SELECT count(*) FROM shiba_internal.change_log
     WHERE commit_lsn='${route_lsn}'::pg_lsn
       AND source_oid='public.failpoint_source'::regclass
       AND row_data->>'event_id'='3')
    || '|' ||
    (SELECT count(*) FROM shiba_internal.dag_inbox
     WHERE result_oid=${result_oid}::oid
       AND commit_lsn='${route_lsn}'::pg_lsn)"

psql_gate -qc "
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='runtime_route_before_slot_advance';
  UPDATE shiba_internal.dag_runtime_state
  SET active=true
  WHERE result_oid=${result_oid}::oid"
wait_for_query "0" "${baseline_diff}" "the replay-idempotent final result"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid" \
  "the final inbox acknowledgement"

apply_panic="Shiba test failpoint: runtime exited after applying commit ${apply_lsn} and before acknowledgement"
route_panic="Shiba test failpoint: runtime exited after routing and before slot advancement"
apply_exit="background worker \"shiba runtime\" (PID ${apply_runtime_pid}) exited with exit code 1"
route_exit="background worker \"shiba runtime\" (PID ${route_runtime_pid}) exited with exit code 1"

assert_log_count 1 \
  "Shiba test failpoint reached: runtime_apply_before_ack result ${result_oid} commit ${apply_lsn}" \
  "the apply boundary arrival with exact result OID and commit LSN"
assert_log_count 1 "${apply_panic}" "the expected apply panic"
assert_log_count 1 "${route_panic}" "the expected route panic"
assert_log_count 1 "${apply_exit}" "the failed apply Runtime PID exit"
assert_log_count 1 "${route_exit}" "the failed route Runtime PID exit"

runtime_exit_log="$(mktemp /tmp/shiba-runtime-failpoint-exits.XXXXXX)"
rg 'background worker "shiba runtime"' \
  "${pg_log_file}" >"${runtime_exit_log}" || true
if test "$(wc -l <"${runtime_exit_log}" | tr -d ' ')" != "2" ||
   ! grep -Fq "${apply_exit}" "${runtime_exit_log}" ||
   ! grep -Fq "${route_exit}" "${runtime_exit_log}"; then
  sed -n '1,120p' "${runtime_exit_log}" >&2
  rm -f "${runtime_exit_log}"
  fail "expected exactly the two deliberately failed Runtime exit records"
fi
rm -f "${runtime_exit_log}"

unexpected_log="$(mktemp /tmp/shiba-runtime-failpoint-unexpected.XXXXXX)"
rg -n 'WARNING|ERROR|FATAL|PANIC' "${pg_log_file}" |
  grep -Fv -e "${apply_panic}" -e "${route_panic}" \
  >"${unexpected_log}" || true
if test -s "${unexpected_log}"; then
  sed -n '1,120p' "${unexpected_log}" >&2
  rm -f "${unexpected_log}"
  fail "PostgreSQL log contains warning-or-higher messages beyond expected Runtime crashes"
fi
rm -f "${unexpected_log}"

printf '\nDeterministic single-Runtime failpoint recovery gate passed.\n'
