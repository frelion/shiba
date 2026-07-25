#!/usr/bin/env bash
set -euo pipefail

# A focused correctness gate for changes to Shiba's routing, transaction,
# worker-lifecycle, and recovery paths.  Every assertion runs against an
# isolated PostgreSQL cluster and every potentially blocking SQL statement has
# a server-side timeout, so a lock-order regression fails instead of hanging.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-concurrency-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-concurrency-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_CONCURRENCY_TEST_PORT:-55433}"
database_name="shiba_concurrency"

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
  printf 'concurrency/recovery gate failed: %s\n' "$1" >&2
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

# Compare as bags in both directions.  This is deliberately independent of
# Shiba's internal state: PostgreSQL recomputes the same SELECT from the source.
result_diff_query="
WITH expected AS (
  SELECT group_id, count(*)::bigint AS row_count, sum(amount)::bigint AS total_amount
  FROM concurrency_source
  GROUP BY group_id
),
actual AS (
  SELECT group_id, row_count::bigint, total_amount::bigint
  FROM shiba.concurrency_result
),
differences AS (
  (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
  UNION ALL
  (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT count(*) FROM differences"

wait_for_baseline() {
  wait_for_query "0" "${result_diff_query}" "$1 to equal a fresh PostgreSQL recomputation"
}

cd "${project_root}"
cargo pgrx install --pg-config "${pg_config_path}"

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
psql_gate -Atqc "SELECT shiba.activate()" >/dev/null
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba worker'" \
  "the WAL router"

psql_gate -qc "
  CREATE TABLE concurrency_source (
    event_id integer NOT NULL,
    group_id integer NOT NULL,
    amount integer NOT NULL
  );
  INSERT INTO concurrency_source
  SELECT value, value % 7, value * 2 FROM generate_series(1,40) AS value;
  CREATE TABLE shiba.concurrency_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM concurrency_source
  GROUP BY group_id"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the result DAG worker"
wait_for_baseline "initial backfill"

# Four independent sessions commit many overlapping transactions.  Each
# transaction mixes INSERT/UPDATE/DELETE so router ordering and old/new tuple
# handling are exercised under concurrent wakeups.
writer_pids=()
for writer in 1 2 3 4; do
  (
    for batch in {1..8}; do
      first_id=$((writer * 100000 + batch * 1000))
      psql_gate -qc "
        BEGIN;
        INSERT INTO concurrency_source
        SELECT ${first_id}+value,
               (${writer}+${batch}+value) % 13,
               ${writer}*100+${batch}*10+value
        FROM generate_series(1,12) AS value;
        UPDATE concurrency_source
        SET amount=amount+7
        WHERE event_id BETWEEN ${first_id}+1 AND ${first_id}+12
          AND event_id % 4=0;
        DELETE FROM concurrency_source
        WHERE event_id BETWEEN ${first_id}+1 AND ${first_id}+12
          AND event_id % 7=0;
        SELECT pg_sleep(0.01);
        COMMIT" >/dev/null
    done
  ) &
  writer_pids+=("$!")
done
for writer_pid in "${writer_pids[@]}"; do
  wait "${writer_pid}"
done
wait_for_baseline "concurrent writers"

# One large transaction must become visible in full.  The following rolled
# back transaction must produce neither routed deltas nor result changes.
psql_gate -qc "
  BEGIN;
  INSERT INTO concurrency_source
  SELECT 800000+value, value % 17, value*3
  FROM generate_series(1,500) AS value;
  UPDATE concurrency_source
  SET group_id=(group_id+3) % 17,amount=amount-5
  WHERE event_id BETWEEN 800001 AND 800500 AND event_id % 5=0;
  DELETE FROM concurrency_source
  WHERE event_id BETWEEN 800001 AND 800500 AND event_id % 11=0;
  COMMIT"
wait_for_baseline "the committed bulk transaction"

source_fingerprint_before_rollback="$(psql_gate -Atqc "
  SELECT count(*) || ':' || sum(event_id)::text || ':' || sum(amount)::text
  FROM concurrency_source")"
psql_gate -qc "
  BEGIN;
  INSERT INTO concurrency_source
  SELECT 900000+value, value % 19, -value
  FROM generate_series(1,250) AS value;
  UPDATE concurrency_source SET amount=amount+100000 WHERE event_id<=20;
  DELETE FROM concurrency_source WHERE event_id BETWEEN 800001 AND 800100;
  ROLLBACK"
assert_query "${source_fingerprint_before_rollback}" "
  SELECT count(*) || ':' || sum(event_id)::text || ':' || sum(amount)::text
  FROM concurrency_source"
assert_query "0" "SELECT count(*) FROM concurrency_source WHERE event_id>900000"
wait_for_baseline "the rolled-back bulk transaction"

# Quiesce only the DAG executor while the router keeps consuming WAL.  Its
# durable inbox must retain the complete transaction until activate() starts a
# replacement executor.
psql_gate -qc "
  UPDATE shiba_internal.dag_worker_state
  SET active=false
  WHERE result_oid='shiba.concurrency_result'::regclass"
wait_for_query "0" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the DAG worker to stop"
psql_gate -qc "
  INSERT INTO concurrency_source
  SELECT 950000+value,value % 19,value*4
  FROM generate_series(1,120) AS value"
wait_for_query "t" "
  SELECT EXISTS (
    SELECT 1 FROM shiba_internal.dag_inbox
    WHERE result_oid='shiba.concurrency_result'::regclass
  )" "the stopped DAG worker's durable inbox"
psql_gate -Atqc "SELECT shiba.activate()" >/dev/null
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the replacement DAG worker"
wait_for_baseline "durable-inbox recovery after DAG worker restart"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.concurrency_result'::regclass" \
  "the replacement DAG worker to acknowledge its inbox"

# Stop the router cleanly, commit WAL while it is absent, and prove that the
# persistent slot has not advanced.  Restart PostgreSQL before reactivation;
# the replacement workers must drain precisely that retained WAL.
psql_gate -qc "UPDATE shiba_internal.worker_state SET active=false WHERE singleton"
wait_for_query "0" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba worker'" \
  "the WAL router to stop"
slot_name_before_restart="$(psql_gate -Atqc "SELECT shiba_internal.slot_name()")"
slot_lsn_before_restart="$(psql_gate -Atqc "
  SELECT confirmed_flush_lsn
  FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text")"
test -n "${slot_lsn_before_restart}" || fail "logical slot has no confirmed LSN before restart"

psql_gate -qc "
  BEGIN;
  INSERT INTO concurrency_source
  SELECT 1000000+value, value % 23, value*5
  FROM generate_series(1,300) AS value;
  UPDATE concurrency_source
  SET amount=amount+13
  WHERE event_id BETWEEN 1000001 AND 1000300 AND event_id % 3=0;
  DELETE FROM concurrency_source
  WHERE event_id BETWEEN 1000001 AND 1000300 AND event_id % 10=0;
  COMMIT"
assert_query "${slot_lsn_before_restart}" "
  SELECT confirmed_flush_lsn
  FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text"
assert_query "t" "
  SELECT pg_current_wal_lsn() >
         (SELECT confirmed_flush_lsn FROM pg_replication_slots
          WHERE slot_name=shiba_internal.slot_name()::text)"

"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -m immediate -t 30 -w stop >/dev/null
"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
assert_query "${slot_name_before_restart}" "SELECT shiba_internal.slot_name()"
assert_query "1" "
  SELECT count(*) FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text"
psql_gate -Atqc "SELECT shiba.activate()" >/dev/null
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba worker'" \
  "the replacement WAL router"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the replacement DAG worker"
wait_for_baseline "persistent-slot recovery after PostgreSQL restart"
assert_query "t" "
  SELECT confirmed_flush_lsn > '${slot_lsn_before_restart}'::pg_lsn
  FROM pg_replication_slots
  WHERE slot_name=shiba_internal.slot_name()::text"

# A post-recovery transaction detects a replacement worker that merely drained
# old WAL and then stalled.
psql_gate -qc "
  INSERT INTO concurrency_source
  VALUES (1100001,31,17),(1100002,31,19),(1100003,32,23)"
wait_for_baseline "post-recovery writes"
assert_query "t" "
  SELECT last_heartbeat >= pg_postmaster_start_time()
  FROM shiba_internal.worker_state WHERE singleton"

# Force an actual lock overlap: one writer holds its source lock while a second
# writer keeps committing and DROP quiesces the result DAG.  SQL timeouts make
# any source/result lock inversion a bounded failure.
psql_gate -qc "
  CREATE TABLE drop_race_source (
    event_id integer NOT NULL,
    group_id integer NOT NULL,
    amount integer NOT NULL
  );
  CREATE TABLE shiba.drop_race_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM drop_race_source GROUP BY group_id"
wait_for_query "2" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the DROP-race DAG worker"

psql_gate -qc "
  BEGIN;
  INSERT INTO drop_race_source
  SELECT value,value % 5,value FROM generate_series(1,50) AS value;
  SELECT pg_sleep(0.5);
  INSERT INTO drop_race_source VALUES (51,1,51);
  COMMIT" >/dev/null &
holding_writer_pid=$!

(
  for value in {101..130}; do
    psql_gate -qc "
      INSERT INTO drop_race_source VALUES (${value},${value}%5,${value})"
  done
) &
streaming_writer_pid=$!

sleep 0.1
psql_gate -qc "DROP TABLE shiba.drop_race_result" &
drop_pid=$!

wait "${holding_writer_pid}"
wait "${streaming_writer_pid}"
wait "${drop_pid}"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the dropped DAG worker to exit"
assert_query "t" "SELECT to_regclass('shiba.drop_race_result') IS NULL"
assert_query "0" "
  SELECT count(*) FROM shiba_internal.stream_views
  WHERE source_oid='drop_race_source'::regclass"
assert_query "81" "SELECT count(*) FROM drop_race_source"
wait_for_query "0" \
  "SELECT count(*) FROM pg_publication_tables
   WHERE pubname='shiba_publication' AND tablename='drop_race_source'" \
  "DROP-race publication cleanup"
wait_for_baseline "the surviving result after concurrent DROP"

printf 'Shiba concurrency, transaction, and persistent-slot recovery gate passed.\n'
