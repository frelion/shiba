#!/usr/bin/env bash
set -euo pipefail

# Acceptance gate for executor scheduling changes. This test deliberately
# observes the durable inbox while its DAG worker is stopped, so transaction
# boundaries and event ordering are checked independently of final results.
#
# Tunables:
#   SHIBA_ARCH_BACKLOG_COMMITS                 number of one-row commits (default 160)
#   SHIBA_ARCH_MIN_BACKLOG_COMMITS_PER_SECOND required drain rate (default 60)
#   SHIBA_ARCH_TEST_PORT                       isolated PostgreSQL port
#   SHIBA_KEEP_TEST_CLUSTER=1                  retain cluster after the run

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-architecture-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-architecture-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_ARCH_TEST_PORT:-$((57000 + $$ % 5000))}"
database_name="shiba_architecture"
backlog_commits="${SHIBA_ARCH_BACKLOG_COMMITS:-160}"
minimum_drain_rate="${SHIBA_ARCH_MIN_BACKLOG_COMMITS_PER_SECOND:-60}"

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

psql_arch() {
  PGOPTIONS="-c statement_timeout=30000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

fail() {
  printf 'executor architecture gate failed: %s\n' "$1" >&2
  printf 'PostgreSQL log: %s\n' "${pg_log_file}" >&2
  exit 1
}

assert_query() {
  local expected="$1"
  local query="$2"
  local actual
  actual="$(psql_arch -Atqc "${query}")"
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
    if actual="$(psql_arch -Atqc "${query}" 2>/dev/null)" &&
       test "${actual}" = "${expected}"; then
      return 0
    fi
    sleep 0.05
  done
  fail "timed out waiting for ${description}; last value was [${actual}]"
}

pause_dag() {
  psql_arch -qc "
    UPDATE shiba_internal.dag_worker_state
    SET active=false
    WHERE result_oid='shiba.arch_result'::regclass"
  wait_for_query "0" \
    "SELECT count(*) FROM pg_stat_activity
     WHERE backend_type='shiba dag worker'" \
    "the DAG worker to stop"
}

activate_dag() {
  psql_arch -Atqc "SELECT shiba.activate()" >/dev/null
  wait_for_query "1" \
    "SELECT count(*) FROM pg_stat_activity
     WHERE backend_type='shiba dag worker'" \
    "the DAG worker to start"
}

wait_for_correct_result() {
  wait_for_query "0" "
    WITH expected AS (
      SELECT group_id,count(*)::bigint AS row_count,sum(amount)::bigint AS total_amount
      FROM public.arch_source GROUP BY group_id
    ),
    actual AS (
      SELECT group_id,row_count::bigint,total_amount::bigint
      FROM shiba.arch_result
    ),
    difference AS (
      (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
      UNION ALL
      (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
    )
    SELECT count(*) FROM difference" \
    "the incrementally maintained result to equal PostgreSQL recomputation"
}

if ! test "${backlog_commits}" -ge 100 2>/dev/null; then
  fail "SHIBA_ARCH_BACKLOG_COMMITS must be an integer >= 100"
fi
if ! awk -v value="${minimum_drain_rate}" 'BEGIN { exit !(value > 0) }'; then
  fail "SHIBA_ARCH_MIN_BACKLOG_COMMITS_PER_SECOND must be positive"
fi

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

psql_arch -qc "CREATE EXTENSION shiba"
psql_arch -Atqc "SELECT shiba.activate()" >/dev/null
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba worker'" \
  "the WAL router"

psql_arch -qc "
  CREATE TABLE public.arch_source (
    event_id integer PRIMARY KEY,
    group_id integer NOT NULL,
    amount integer NOT NULL
  );
  INSERT INTO public.arch_source VALUES (1,0,10),(2,0,20);
  CREATE TABLE shiba.arch_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.arch_source GROUP BY group_id"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the result DAG worker"
wait_for_correct_result

printf '\n==> Durable transaction boundaries and UPDATE ordering\n'
pause_dag

# Three source commits with an UPDATE in the middle. WAL decoding must keep the
# old-row -1 immediately before the new-row +1 in one commit.
psql_arch -qc "INSERT INTO public.arch_source VALUES (10,1,100),(11,1,110)"
psql_arch -qc "UPDATE public.arch_source SET group_id=2,amount=125 WHERE event_id=10"
psql_arch -qc "DELETE FROM public.arch_source WHERE event_id=11"

wait_for_query "3" "
  SELECT count(DISTINCT commit_lsn)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_result'::regclass" \
  "three source commits to be durably routed"

# A DAG sees only events routed to that result. Sequence values are global
# within the source transaction, so a multi-source DAG may legitimately see
# gaps; uniqueness and positive monotonic ordering are the required invariant.
assert_query "t" "
  SELECT bool_and(event_count=distinct_sequences AND min_sequence>0)
  FROM (
    SELECT commit_lsn,min(sequence) AS min_sequence,
           count(*) AS event_count,
           count(DISTINCT sequence) AS distinct_sequences
    FROM shiba_internal.dag_inbox
    WHERE result_oid='shiba.arch_result'::regclass
    GROUP BY commit_lsn
  ) commits"

# The UPDATE is exactly old-row -1 followed by new-row +1, with no event from
# another source transaction interleaved between the two.
assert_query "t" "
  SELECT count(*)=2
     AND array_agg(delta ORDER BY sequence)=ARRAY[-1,1]
     AND max(sequence)-min(sequence)=1
     AND count(DISTINCT commit_lsn)=1
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_result'::regclass
    AND row_data->>'event_id'='10'
    AND delta IN (-1,1)
    AND commit_lsn IN (
      SELECT commit_lsn
      FROM shiba_internal.dag_inbox
      WHERE result_oid='shiba.arch_result'::regclass
        AND row_data->>'event_id'='10'
      GROUP BY commit_lsn
      HAVING count(*)=2
    )"

# Before execution, none of the three commits may be partially visible.
assert_query "0|30" "
  SELECT coalesce(sum(row_count) FILTER (WHERE group_id IN (1,2)),0)
         || '|' ||
         coalesce(sum(total_amount) FILTER (WHERE group_id=0),0)
  FROM shiba.arch_result"

activate_dag
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_result'::regclass" \
  "the ordered commits to drain"
wait_for_correct_result

printf '\n==> Cross-source WAL order with legal sequence gaps\n'
psql_arch -qc "
  CREATE TABLE public.arch_left (
    row_id integer PRIMARY KEY,
    join_key integer,
    amount integer NOT NULL
  );
  CREATE TABLE public.arch_right (
    row_id integer PRIMARY KEY,
    join_key integer,
    group_id integer NOT NULL
  );
  CREATE TABLE public.arch_noise (
    row_id integer PRIMARY KEY,
    group_id integer NOT NULL,
    amount integer NOT NULL
  );
  INSERT INTO public.arch_left VALUES (1,1,10);
  INSERT INTO public.arch_right VALUES (1,1,1);
  INSERT INTO public.arch_noise VALUES (1,1,1);
  CREATE TABLE shiba.arch_join_result AS
  SELECT r.group_id,count(*) AS row_count,sum(l.amount) AS total_amount
  FROM public.arch_left l
  JOIN public.arch_right r ON l.join_key=r.join_key
  GROUP BY r.group_id;
  CREATE TABLE shiba.arch_noise_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.arch_noise GROUP BY group_id"
wait_for_query "3" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the main, join, and unrelated DAG workers"

psql_arch -qc "
  UPDATE shiba_internal.dag_worker_state
  SET active=false
  WHERE result_oid='shiba.arch_join_result'::regclass"
wait_for_query "2" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the join DAG worker to stop"

# The unrelated source deliberately consumes global event positions 2,5,6.
# The join DAG must retain its relevant subsequence 1,3,4,7,8 in WAL order
# rather than requiring a per-DAG sequence beginning at 1 without gaps.
psql_arch -qc "
  BEGIN;
  INSERT INTO public.arch_left VALUES (2,1,20);
  INSERT INTO public.arch_noise VALUES (2,2,2);
  UPDATE public.arch_right SET group_id=2 WHERE row_id=1;
  UPDATE public.arch_noise SET amount=3 WHERE row_id=1;
  UPDATE public.arch_left SET amount=15 WHERE row_id=1;
  COMMIT"
wait_for_query "5" "
  SELECT count(*)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_join_result'::regclass" \
  "the interleaved join transaction to route"
assert_query "1,3,4,7,8" "
  SELECT array_to_string(array_agg(sequence ORDER BY sequence),',')
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_join_result'::regclass"
assert_query "1" "
  SELECT count(DISTINCT commit_lsn)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_join_result'::regclass"

psql_arch -Atqc "SELECT shiba.activate()" >/dev/null
wait_for_query "3" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "the interleaved join DAG worker to restart"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_join_result'::regclass" \
  "the interleaved join transaction to apply"
assert_query "2|2|35" "
  SELECT group_id || '|' || row_count || '|' || total_amount
  FROM shiba.arch_join_result"

psql_arch -qc "
  DROP TABLE shiba.arch_join_result;
  DROP TABLE shiba.arch_noise_result;
  DROP TABLE public.arch_left;
  DROP TABLE public.arch_right;
  DROP TABLE public.arch_noise"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'" \
  "only the main DAG worker to remain"

printf '\n==> Backlog busy-drain throughput and atomic acknowledgement\n'
pause_dag

backlog_sql="$(mktemp /tmp/shiba-architecture-backlog.XXXXXX.sql)"
for ((commit=1; commit<=backlog_commits; commit++)); do
  printf 'INSERT INTO public.arch_source VALUES (%d,%d,%d);\n' \
    "$((100000 + commit))" "$((commit % 17))" "$commit" >> "${backlog_sql}"
done
psql_arch -qf "${backlog_sql}"
rm -f "${backlog_sql}"

wait_for_query "${backlog_commits}" "
  SELECT count(DISTINCT commit_lsn)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_result'::regclass" \
  "the one-transaction-per-statement backlog to be routed"

psql_arch -qc "
  CREATE TABLE public.arch_progress_audit (
    applied_lsn pg_lsn NOT NULL,
    executor_xid bigint NOT NULL
  );
  CREATE FUNCTION public.capture_arch_progress()
  RETURNS trigger LANGUAGE plpgsql AS \$trigger\$
  BEGIN
    IF NEW.result_oid='shiba.arch_result'::regclass THEN
      INSERT INTO public.arch_progress_audit
      VALUES (NEW.applied_lsn,txid_current());
    END IF;
    RETURN NEW;
  END
  \$trigger\$;
  CREATE TRIGGER capture_arch_progress
  AFTER UPDATE ON shiba_internal.view_progress
  FOR EACH ROW EXECUTE FUNCTION public.capture_arch_progress()"

start_ns="$(python3 -c 'import time; print(time.time_ns())')"
psql_arch -Atqc "SELECT shiba.activate()" >/dev/null

# Repeated snapshots enforce the externally visible atomic invariant:
# acknowledging LSN X and advancing progress through X happen together.
while :; do
  snapshot="$(psql_arch -Atqc "
    WITH progress AS (
      SELECT applied_lsn FROM shiba_internal.view_progress
      WHERE result_oid='shiba.arch_result'::regclass
    )
    SELECT count(*) || '|' ||
           NOT EXISTS (
             SELECT 1
             FROM shiba_internal.dag_inbox inbox,progress
             WHERE inbox.result_oid='shiba.arch_result'::regclass
               AND inbox.commit_lsn <= progress.applied_lsn
           )
    FROM shiba_internal.dag_inbox
    WHERE result_oid='shiba.arch_result'::regclass")"
  pending="${snapshot%%|*}"
  invariant="${snapshot##*|}"
  # Concatenation coerces PostgreSQL boolean to the text "true"/"false";
  # psql's standalone boolean display ("t"/"f") does not apply here.
  if test "${invariant}" != "true"; then
    psql_arch -P pager=off -c "
      SELECT progress.applied_lsn,inbox.commit_lsn,inbox.sequence,
             inbox.delta,inbox.row_data
      FROM shiba_internal.view_progress progress
      JOIN shiba_internal.dag_inbox inbox
        ON inbox.result_oid=progress.result_oid
       AND inbox.commit_lsn <= progress.applied_lsn
      WHERE progress.result_oid='shiba.arch_result'::regclass
      ORDER BY inbox.commit_lsn,inbox.sequence
      LIMIT 20" >&2 || true
    fail "inbox acknowledgement became visible without matching progress"
  fi
  if test "${pending}" = "0"; then
    break
  fi
  sleep 0.005
done
end_ns="$(python3 -c 'import time; print(time.time_ns())')"

elapsed_seconds="$(python3 -c \
  'import sys; print(f"{(int(sys.argv[2])-int(sys.argv[1]))/1_000_000_000:.6f}")' \
  "${start_ns}" "${end_ns}")"
drain_rate="$(awk -v commits="${backlog_commits}" -v elapsed="${elapsed_seconds}" \
  'BEGIN { printf "%.2f", commits/elapsed }')"
printf 'Backlog: %s commits in %ss = %s commits/s (legacy 25ms wait ceiling: 40 commits/s)\n' \
  "${backlog_commits}" "${elapsed_seconds}" "${drain_rate}"

if ! awk -v actual="${drain_rate}" -v minimum="${minimum_drain_rate}" \
  'BEGIN { exit !(actual >= minimum) }'; then
  fail "backlog drain ${drain_rate} commits/s is below required ${minimum_drain_rate}"
fi

# A future implementation that wraps several source commits in one outer
# database transaction could still satisfy the snapshot invariant above.
# Persisted txids make the intended one-source-commit/one-executor-tx contract
# explicit and independently testable.
assert_query "${backlog_commits}|${backlog_commits}" "
  SELECT count(*) || '|' || count(DISTINCT executor_xid)
  FROM public.arch_progress_audit"

wait_for_correct_result
assert_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.arch_result'::regclass"

if rg -n 'WARNING|ERROR|FATAL|PANIC' "${pg_log_file}" >/tmp/shiba-architecture-log-errors.$$; then
  sed -n '1,120p' /tmp/shiba-architecture-log-errors.$$ >&2
  rm -f /tmp/shiba-architecture-log-errors.$$
  fail "PostgreSQL log contains warning-or-higher messages"
fi
rm -f /tmp/shiba-architecture-log-errors.$$

printf '\nExecutor architecture gate passed.\n'
