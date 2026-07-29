#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-ingress-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-ingress-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_INGRESS_TEST_PORT:-$((58000 + $$ % 3000))}"
database_name="shiba_ingress"
database_user="$(id -un)"

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

psql_ingress() {
  PGOPTIONS="-c statement_timeout=30000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

fail() {
  printf 'replication ingress test failed: %s\n' "$1" >&2
  printf 'PostgreSQL log: %s\n' "${pg_log_file}" >&2
  tail -n 120 "${pg_log_file}" >&2 || true
  exit 1
}

assert_query() {
  local expected="$1"
  local query="$2"
  local actual
  actual="$(psql_ingress -Atqc "${query}")"
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
  for attempt in {1..600}; do
    if actual="$(psql_ingress -Atqc "${query}" 2>/dev/null)" &&
       test "${actual}" = "${expected}"; then
      return
    fi
    sleep 0.1
  done
  fail "timed out waiting for ${description}; last value was [${actual}]"
}

cd "${project_root}"
PG_CONFIG="${pg_config_path}" \
  cargo pgrx install --pg-config "${pg_config_path}" --features pg_test

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\n"
  printf "wal_level = logical\n"
  printf "max_replication_slots = 8\n"
  printf "max_wal_senders = 8\n"
  printf "max_worker_processes = 16\n"
  printf "logical_decoding_work_mem = '64kB'\n"
  printf "fsync = on\n"
  printf "synchronous_commit = on\n"
  printf "listen_addresses = ''\n"
  printf "unix_socket_directories = '%s'\n" "${pg_socket_dir}"
  printf "port = %s\n" "${pg_port}"
  printf "shiba.ingress_batch_rows = 4\n"
  printf "shiba.ingress_batch_bytes = '32kB'\n"
  printf "shiba.ingress_retention = '10min'\n"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s connect_timeout=5'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "${database_user}"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_ingress -qc "CREATE EXTENSION shiba"
psql_ingress -qc "SELECT shiba.activate()"

wait_for_query "1|1" "
  SELECT
    count(*) FILTER (WHERE backend_type='shiba runtime'),
    count(*) FILTER (
      WHERE backend_type='walsender' AND application_name='shiba'
    )
  FROM pg_stat_activity
" "the Runtime and its PostgreSQL walsender"

psql_ingress -qc "
  CREATE TABLE public.ingress_source (
    id bigint PRIMARY KEY,
    payload text NOT NULL
  );
  ALTER TABLE public.ingress_source REPLICA IDENTITY FULL;
  ALTER PUBLICATION shiba_publication ADD TABLE public.ingress_source;
  INSERT INTO public.ingress_source VALUES (1, 'small');
"

wait_for_query "1|1" "
  SELECT count(*), coalesce(sum(event_count),0)
  FROM shiba_internal.ingress_transactions
  WHERE status='committed'
" "the first committed ingress transaction"
assert_query "1" "
  SELECT count(*)
  FROM shiba_internal.routing_tasks
  WHERE status='complete'
"

psql_ingress -qc "
  CREATE TABLE public.ingress_dag_source (
    id bigint PRIMARY KEY,
    group_id bigint NOT NULL,
    amount bigint NOT NULL
  );
  CREATE TABLE shiba.ingress_dag_totals AS
  SELECT group_id, count(*) AS row_count, sum(amount) AS total_amount
  FROM public.ingress_dag_source
  GROUP BY group_id;
  INSERT INTO public.ingress_dag_source
  VALUES (1, 7, 10), (2, 7, 20), (3, 8, 5);
"
wait_for_query "2|30" "
  SELECT row_count, total_amount
  FROM shiba.ingress_dag_totals
  WHERE group_id=7
" "the DAG consuming the unified change-log view"
psql_ingress -qc "
  UPDATE public.ingress_dag_source SET amount=15 WHERE id=1;
  DELETE FROM public.ingress_dag_source WHERE id=2;
"
wait_for_query "1|15" "
  SELECT row_count, total_amount
  FROM shiba.ingress_dag_totals
  WHERE group_id=7
" "ingress update/delete DAG application"

psql_ingress -qc "
  CREATE TABLE public.shiba_runtime_failpoints (
    kind text PRIMARY KEY,
    runtime_pid integer,
    result_oid oid,
    commit_lsn pg_lsn,
    pause_ms integer NOT NULL DEFAULT 0 CHECK (pause_ms >= 0),
    fired boolean NOT NULL DEFAULT false
  );
  INSERT INTO public.shiba_runtime_failpoints(kind)
  VALUES ('runtime_ingress_before_feedback');
"
runtime_pid_before="$(psql_ingress -Atqc "
  SELECT pid FROM pg_stat_activity WHERE backend_type='shiba runtime'
")"

psql_ingress -qc "
  INSERT INTO public.ingress_source
  SELECT source_id,
         (
           SELECT string_agg(md5(source_id::text || ':' || chunk_id::text), '')
           FROM generate_series(1, 128) AS chunk_id
         )
  FROM generate_series(2, 101) AS source_id;
"

wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='runtime_ingress_before_feedback'
" "the post-ingress/pre-feedback crash failpoint"
wait_for_query "1" "
  SELECT count(*)
  FROM pg_stat_activity
  WHERE backend_type='shiba runtime'
    AND pid <> ${runtime_pid_before}
" "the replacement Runtime"
wait_for_query "1" "
  SELECT count(*)
  FROM shiba_internal.ingress_transactions
  WHERE status='committed' AND event_count=100
" "the bounded large source transaction"

assert_query "100" "
  SELECT event_count
  FROM shiba_internal.ingress_transactions
  WHERE status='committed'
  ORDER BY event_count DESC
  LIMIT 1
"
assert_query "t" "
  SELECT streamed AND identity_lsn < commit_lsn
  FROM shiba_internal.ingress_transactions
  WHERE event_count=100
"
assert_query "t" "
  SELECT stream_txns > 0
  FROM pg_stat_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text
"
assert_query "t" "
  SELECT count(*) > 10
  FROM shiba_internal.ingress_decode_batches
"
assert_query "t|t" "
  SELECT
    persisted_lsn IS NOT NULL,
    confirmed_lsn IS NOT NULL
  FROM shiba_internal.ingress_replay_state
  WHERE state='active'
"
assert_query "1" "
  SELECT count(*)
  FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text
    AND active_pid IS NOT NULL
"

psql_ingress -qc "
  BEGIN;
  INSERT INTO public.ingress_source VALUES (102, 'parent-kept');
  SAVEPOINT doomed;
  INSERT INTO public.ingress_source
  SELECT source_id,
         (
           SELECT string_agg(md5(source_id::text || ':rollback:' || chunk_id::text), '')
           FROM generate_series(1, 128) AS chunk_id
         )
  FROM generate_series(103, 202) AS source_id;
  ROLLBACK TO SAVEPOINT doomed;
  INSERT INTO public.ingress_source VALUES (203, 'after-rollback-kept');
  COMMIT;
"

wait_for_query "t|2|102,203" "
  SELECT raw.event_count > 2,
         count(effective.sequence),
         string_agg(effective.row_data ->> 'id', ',' ORDER BY effective.sequence)
  FROM shiba_internal.ingress_transactions AS raw
  JOIN shiba_internal.ingress_rollbacks AS rollback
    ON rollback.ingress_txn_id = raw.ingress_txn_id
  JOIN shiba_internal.effective_change_log AS effective
    ON effective.ingress_txn_id = raw.ingress_txn_id
  GROUP BY raw.ingress_txn_id, raw.event_count
" "the streamed savepoint rollback"
assert_query "1" "
  SELECT count(*) FROM shiba_internal.ingress_rollbacks
"

old_generation="$(psql_ingress -Atqc "
  SELECT slot_generation
  FROM shiba_internal.ingress_replay_state
  WHERE state='active'
")"
psql_ingress -qc "
  DROP TABLE shiba.ingress_dag_totals;
  DROP TABLE public.ingress_dag_source;
"
psql_ingress -qc "SELECT shiba.deactivate()"
assert_query "0|1" "
  SELECT
    count(*) FILTER (WHERE state='active'),
    count(*) FILTER (
      WHERE slot_generation=${old_generation} AND state='retired'
    )
  FROM shiba_internal.ingress_replay_state
"
assert_query "0" "
  SELECT count(*)
  FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text
"

psql_ingress -qc "SELECT shiba.activate()"
wait_for_query "1" "
  SELECT count(*)
  FROM shiba_internal.ingress_replay_state
  WHERE state='active'
    AND slot_generation > ${old_generation}
" "a fresh generation for the recreated slot"

printf 'replication ingress test passed: bounded streaming, crash replay, subxact rollback, and slot rotation.\n'
