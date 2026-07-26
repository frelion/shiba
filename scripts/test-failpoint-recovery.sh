#!/usr/bin/env bash
set -euo pipefail

# Deterministically crash the test-only workers at the two durable handoff
# boundaries that ordinary kill-based recovery tests cannot target exactly.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-failpoint-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-failpoint-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_FAILPOINT_TEST_PORT:-$((59000 + $$ % 4000))}"
database_name="shiba_failpoint"

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
  printf 'deterministic failpoint gate failed: %s\n' "$1" >&2
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

restart_dag() {
  local result_oid="$1"
  local worker_count
  local attempt
  psql_gate -qc "
    UPDATE shiba_internal.dag_worker_state
    SET active=true,last_heartbeat=NULL,last_requested_at=NULL
    WHERE result_oid=${result_oid}::oid;
    SELECT shiba._ensure_dag_worker(${result_oid}::oid)"
  for attempt in {1..300}; do
    worker_count="$(psql_gate -Atqc "
      SELECT count(*) FROM pg_stat_activity
      WHERE backend_type='shiba dag worker'")"
    if test "${worker_count}" = "1"; then
      return 0
    fi
    if test "${worker_count}" -gt 1; then
      fail "DAG restart created ${worker_count} concurrent executors"
    fi
    sleep 0.1
  done
  fail "timed out restarting the DAG executor"
}

restart_router() {
  local worker_count
  local attempt
  psql_gate -qc "
    UPDATE shiba_internal.worker_state
    SET active=true,last_heartbeat=NULL,last_requested_at=NULL
    WHERE singleton;
    SELECT shiba._ensure_worker()"
  for attempt in {1..300}; do
    worker_count="$(psql_gate -Atqc "
      SELECT count(*) FROM pg_stat_activity
      WHERE backend_type='shiba worker'")"
    if test "${worker_count}" = "1"; then
      return 0
    fi
    if test "${worker_count}" -gt 1; then
      fail "router restart created ${worker_count} concurrent workers"
    fi
    sleep 0.1
  done
  fail "timed out restarting the WAL router"
}

baseline_diff="
WITH expected AS (
  SELECT group_id,count(*)::bigint AS row_count,sum(amount)::bigint AS total
  FROM failpoint_source GROUP BY group_id
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
} >> "${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_gate -qc "CREATE EXTENSION shiba"
psql_gate -qc "
  CREATE TABLE public.shiba_worker_failpoints (
    kind text PRIMARY KEY,
    worker_pid integer,
    result_oid oid,
    commit_lsn pg_lsn,
    pause_ms integer NOT NULL DEFAULT 0 CHECK (pause_ms>=0),
    fired boolean NOT NULL DEFAULT false
  )"
psql_gate -Atqc "SELECT shiba.activate()" >/dev/null
psql_gate -qc "
  CREATE TABLE failpoint_source (
    event_id integer PRIMARY KEY,
    group_id integer NOT NULL,
    amount integer NOT NULL
  );
  INSERT INTO failpoint_source VALUES (1,1,10);
  CREATE TABLE shiba.failpoint_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total
  FROM failpoint_source GROUP BY group_id"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba worker'" \
  "the WAL router"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the DAG executor"
wait_for_query "0" "${baseline_diff}" "the initial result"

result_oid="$(psql_gate -Atqc "SELECT 'shiba.failpoint_result'::regclass::oid::integer")"
executor_pid="$(psql_gate -Atqc "
  SELECT pid FROM pg_stat_activity WHERE backend_type='shiba dag worker'")"

printf '\n==> Executor apply-before-ack rollback\n'
# Hold the same advisory lock used by the executor so the exact inbox LSN and
# worker PID can be armed without racing the commit.
psql_gate -qc "
  SELECT pg_advisory_lock(${result_oid}::bigint);
  SELECT pg_sleep(3)" >/dev/null &
lock_holder_pid=$!
wait_for_query "1" "
  SELECT count(*) FROM pg_locks
  WHERE locktype='advisory' AND granted AND objid=${result_oid}" \
  "the executor advisory lock holder"

psql_gate -qc "INSERT INTO failpoint_source VALUES (2,2,20)"
wait_for_query "1" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid" \
  "the executor test commit in the durable inbox"
executor_lsn="$(psql_gate -Atqc "
  SELECT commit_lsn FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid")"
psql_gate -qc "
  INSERT INTO public.shiba_worker_failpoints
    (kind,worker_pid,result_oid,commit_lsn,pause_ms)
  VALUES
    ('executor_before_ack',${executor_pid},${result_oid}::oid,
     '${executor_lsn}'::pg_lsn,2000)"

wait "${lock_holder_pid}"
wait_for_log \
  "test failpoint reached: executor_before_ack result ${result_oid} commit ${executor_lsn}" \
  "the executor to reach the apply-before-ack boundary"
# The arrival log is emitted after the old worker has claimed the failpoint
# and before it sleeps. Prevent a replacement from draining the inbox before
# rollback assertions are observed.
psql_gate -qc "
  UPDATE shiba_internal.dag_worker_state
  SET active=false
  WHERE result_oid=${result_oid}::oid"
wait_for_log \
  "executor exited after applying commit ${executor_lsn} and before acknowledgement" \
  "the executor failpoint"
wait_for_query "0" "
  SELECT count(*) FROM pg_stat_activity
  WHERE backend_type='shiba dag worker' AND pid=${executor_pid}" \
  "the failed executor to exit"

assert_query "1" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid AND commit_lsn='${executor_lsn}'::pg_lsn"
assert_query "0" "SELECT count(*) FROM shiba.failpoint_result WHERE group_id=2"
# This UPDATE occurred inside the same transaction as apply_batch, so it too
# rolls back. The old PID binding prevents the replacement from firing again.
assert_query "f" "
  SELECT fired FROM public.shiba_worker_failpoints
  WHERE kind='executor_before_ack'"

psql_gate -qc "
  DELETE FROM public.shiba_worker_failpoints
  WHERE kind='executor_before_ack'"
restart_dag "${result_oid}"
wait_for_query "0" "${baseline_diff}" "the replacement executor to drain the retained inbox"
assert_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid"

printf '\n==> Router route-before-slot-advance replay idempotence\n'
psql_gate -qc "
  UPDATE shiba_internal.dag_worker_state
  SET active=false
  WHERE result_oid=${result_oid}::oid;
  UPDATE shiba_internal.worker_state SET active=false WHERE singleton"
wait_for_query "0" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba worker'" \
  "the WAL router to stop"
wait_for_query "0" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the DAG executor to stop"

slot_lsn_before="$(psql_gate -Atqc "
  SELECT confirmed_flush_lsn FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text")"
routed_before="$(psql_gate -Atqc "SELECT count(*) FROM shiba_internal.routed_transactions")"
psql_gate -qc "
  INSERT INTO failpoint_source VALUES (3,3,30);
  INSERT INTO public.shiba_worker_failpoints(kind,pause_ms)
  VALUES ('router_before_slot_advance',100)"
psql_gate -qc "
  UPDATE shiba_internal.worker_state
  SET active=true,last_heartbeat=NULL,last_requested_at=NULL
  WHERE singleton"
psql_gate -Atqc "SELECT shiba.start_worker()" >/dev/null
wait_for_log \
  "router exited after routing and before slot advancement" \
  "the router failpoint"
wait_for_query "0" "
  SELECT count(*) FROM pg_stat_activity
  WHERE backend_type='shiba worker'
    AND pid=(SELECT worker_pid FROM public.shiba_worker_failpoints
             WHERE kind='router_before_slot_advance')" \
  "the failed router to exit"

assert_query "t" "
  SELECT fired AND worker_pid IS NOT NULL
  FROM public.shiba_worker_failpoints
  WHERE kind='router_before_slot_advance'"
assert_query "${slot_lsn_before}" "
  SELECT confirmed_flush_lsn FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text"
assert_query "$((routed_before + 1))" \
  "SELECT count(*) FROM shiba_internal.routed_transactions"
assert_query "1" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid AND row_data->>'event_id'='3'"

psql_gate -qc "
  DELETE FROM public.shiba_worker_failpoints
  WHERE kind='router_before_slot_advance'"
restart_router
wait_for_query "t" "
  SELECT confirmed_flush_lsn>'${slot_lsn_before}'::pg_lsn
  FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text" \
  "the replacement router to advance the slot"
assert_query "$((routed_before + 1))" \
  "SELECT count(*) FROM shiba_internal.routed_transactions"
assert_query "1" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid AND row_data->>'event_id'='3'"

restart_dag "${result_oid}"
wait_for_query "0" "${baseline_diff}" "the final replayed result"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid=${result_oid}::oid" \
  "the final inbox acknowledgement"

printf '\nDeterministic executor and router failpoint recovery gate passed.\n'
