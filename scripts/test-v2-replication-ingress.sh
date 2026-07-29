#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-v2-ingress-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-v2-ingress-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_V2_INGRESS_TEST_PORT:-$((58000 + $$ % 3000))}"
database_name="shiba_v2_ingress"
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

psql_v2() {
  PGOPTIONS="-c statement_timeout=30000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

fail() {
  printf 'v2 replication ingress test failed: %s\n' "$1" >&2
  printf 'PostgreSQL log: %s\n' "${pg_log_file}" >&2
  tail -n 120 "${pg_log_file}" >&2 || true
  exit 1
}

assert_query() {
  local expected="$1"
  local query="$2"
  local actual
  actual="$(psql_v2 -Atqc "${query}")"
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
    if actual="$(psql_v2 -Atqc "${query}" 2>/dev/null)" &&
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
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s connect_timeout=5'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "${database_user}"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_v2 -qc "CREATE EXTENSION shiba"
psql_v2 -qc "SELECT shiba.activate()"

wait_for_query "1|1" "
  SELECT
    count(*) FILTER (WHERE backend_type='shiba runtime'),
    count(*) FILTER (
      WHERE backend_type='walsender' AND application_name='shiba'
    )
  FROM pg_stat_activity
" "the Runtime and its PostgreSQL walsender"

psql_v2 -qc "
  CREATE TABLE public.v2_source (
    id bigint PRIMARY KEY,
    payload text NOT NULL
  );
  ALTER TABLE public.v2_source REPLICA IDENTITY FULL;
  ALTER PUBLICATION shiba_publication ADD TABLE public.v2_source;
  INSERT INTO public.v2_source VALUES (1, 'small');
"

wait_for_query "1|1" "
  SELECT count(*), coalesce(sum(event_count),0)
  FROM shiba_internal.v2_ingress_transactions
  WHERE status='committed'
" "the first committed v2 ingress transaction"
assert_query "0" "SELECT count(*) FROM shiba_internal.routed_transactions"

psql_v2 -qc "
  CREATE TABLE public.shiba_runtime_failpoints (
    kind text PRIMARY KEY,
    runtime_pid integer,
    result_oid oid,
    commit_lsn pg_lsn,
    pause_ms integer NOT NULL DEFAULT 0 CHECK (pause_ms >= 0),
    fired boolean NOT NULL DEFAULT false
  );
  INSERT INTO public.shiba_runtime_failpoints(kind)
  VALUES ('runtime_v2_ingress_before_feedback');
"
runtime_pid_before="$(psql_v2 -Atqc "
  SELECT pid FROM pg_stat_activity WHERE backend_type='shiba runtime'
")"

psql_v2 -qc "
  INSERT INTO public.v2_source
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
  WHERE kind='runtime_v2_ingress_before_feedback'
" "the post-ingress/pre-feedback crash failpoint"
wait_for_query "1" "
  SELECT count(*)
  FROM pg_stat_activity
  WHERE backend_type='shiba runtime'
    AND pid <> ${runtime_pid_before}
" "the replacement Runtime"
wait_for_query "2|101" "
  SELECT count(*), coalesce(sum(event_count),0)
  FROM shiba_internal.v2_ingress_transactions
  WHERE status='committed'
" "the bounded large source transaction"

assert_query "100" "
  SELECT event_count
  FROM shiba_internal.v2_ingress_transactions
  WHERE status='committed'
  ORDER BY event_count DESC
  LIMIT 1
"
assert_query "t" "
  SELECT streamed AND identity_lsn < commit_lsn
  FROM shiba_internal.v2_ingress_transactions
  WHERE event_count=100
"
assert_query "t" "
  SELECT stream_txns > 0
  FROM pg_stat_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text
"
assert_query "t" "
  SELECT count(*) > 10
  FROM shiba_internal.v2_ingress_decode_batches
"
assert_query "t|t" "
  SELECT
    persisted_lsn IS NOT NULL,
    confirmed_lsn IS NOT NULL
  FROM shiba_internal.v2_ingress_replay_state
  WHERE state='active'
"
assert_query "1" "
  SELECT count(*)
  FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text
    AND active_pid IS NOT NULL
"

printf 'v2 replication ingress test passed: walsender + one Runtime persisted bounded batches.\n'
