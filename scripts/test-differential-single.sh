#!/usr/bin/env bash
set -euo pipefail

# Deterministic, single-source differential test for Shiba's stateful
# operators. Every committed mutation changes at least one fully projected
# row, then the test waits for every DAG inbox to drain and performs a
# bag-semantics comparison against PostgreSQL recomputation.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-diff-pg17-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-diff-pg17-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_DIFF_PORT:-$((56000 + $$ % 5000))}"
seed="${SHIBA_DIFF_SEED:-20260725}"
rounds="${SHIBA_DIFF_ROUNDS:-120}"
replay_log="${SHIBA_DIFF_REPLAY_LOG:-/tmp/shiba-differential-single-seed-${seed}.sql}"

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

psql_diff() {
  "${pg_bin_dir}/psql" -X -v ON_ERROR_STOP=1 \
    -h "${pg_socket_dir}" -p "${pg_port}" -d shiba_diff "$@"
}

wait_for_value() {
  local expected="$1"
  local query="$2"
  local attempt
  for attempt in {1..200}; do
    if test "$(psql_diff -Atqc "${query}")" = "${expected}"; then
      return 0
    fi
    sleep 0.05
  done
  psql_diff -c "${query}" >&2
  return 1
}

dump_failure() {
  local round="$1"
  local operation="$2"
  printf '\nDifferential failure: seed=%s round=%s operation=%s\n' \
    "${seed}" "${round}" "${operation}" >&2
  printf 'Replay with: SHIBA_DIFF_SEED=%s SHIBA_DIFF_ROUNDS=%s %s\n' \
    "${seed}" "${round}" "$0" >&2
  printf 'Executed SQL log: %s\n' "${replay_log}" >&2
  psql_diff -P null=NULL -c \
    "SELECT * FROM public.diff_failures() WHERE mismatch_count <> 0 ORDER BY view_name" >&2 || true
  psql_diff -P null=NULL -c \
    "SELECT * FROM public.diff_events ORDER BY row_id" >&2 || true
  psql_diff -P null=NULL -c \
    "SELECT * FROM public.diff_extremes ORDER BY row_id" >&2 || true
  psql_diff -P null=NULL -c \
    "SELECT c.relname AS view_name,p.applied_lsn
       FROM shiba_internal.view_progress p
       JOIN pg_class c ON c.oid=p.result_oid
      ORDER BY c.relname" >&2 || true
  tail -100 "${pg_log_file}" >&2 || true
}

# POSIX-shell-friendly deterministic LCG. Keeping the state below 2^31 also
# avoids signed-overflow differences between bash builds.
rng_state="$((seed & 0x7fffffff))"
rand_n() {
  local modulus="$1"
  rng_state="$(((1103515245 * rng_state + 12345) & 0x7fffffff))"
  REPLY="$((rng_state % modulus))"
}

random_value_tuple() {
  local row_id="$1"
  local category customer label amount score_rank
  rand_n 7
  category="${REPLY}"
  if test "${category}" -eq 0; then
    category="NULL"
  else
    category="$((category - 3))"
  fi
  rand_n 8
  customer="${REPLY}"
  if test "${customer}" -lt 2; then
    customer="NULL"
  else
    customer="$((customer - 4))"
  fi
  rand_n 5
  label="$((REPLY - 2))"
  rand_n 11
  amount="$((REPLY - 5))"
  rand_n 31
  score_rank="$((REPLY - 15))"
  # row_id is a deterministic tiebreaker without adding a second ORDER BY key.
  score="$((score_rank * 100000 + row_id))"
  CATEGORY_VALUE="${category}"
  CUSTOMER_VALUE="${customer}"
  LABEL_VALUE="${label}"
  AMOUNT_VALUE="${amount}"
  SCORE_VALUE="${score}"
  VALUE_TUPLE="${category},${customer},${label},${amount},${score}"
}

committed_mutation() {
  local sql="$1"
  local attempt status
  printf '%s;\n' "${sql}" >> "${replay_log}"
  psql_diff -q <<SQL
BEGIN;
${sql};
COMMIT;
SQL

  for attempt in {1..300}; do
    status="$(psql_diff -Atq <<'SQL'
SELECT CASE
  WHEN NOT EXISTS (SELECT 1 FROM shiba_internal.dag_inbox)
   AND NOT EXISTS (
     SELECT 1 FROM public.diff_failures() WHERE mismatch_count <> 0
   )
  THEN 'ok'
  ELSE 'wait'
END;
SQL
    )"
    if test "${status}" = "ok"; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

rolled_back_mutation() {
  local sql="$1"
  {
    printf 'BEGIN;\n'
    printf '%s;\n' "${sql}"
    printf 'ROLLBACK;\n'
  } >> "${replay_log}"
  psql_diff -q <<SQL
BEGIN;
${sql};
ROLLBACK;
SQL
  test "$(psql_diff -Atqc \
    "SELECT count(*) FROM public.diff_failures() WHERE mismatch_count <> 0")" = "0"
}

cd "${project_root}"
printf -- '-- seed=%s rounds=%s\n' "${seed}" "${rounds}" > "${replay_log}"

if ! test "${rounds}" -ge 1 2>/dev/null; then
  printf 'SHIBA_DIFF_ROUNDS must be a positive integer\n' >&2
  exit 2
fi

cargo pgrx install --pg-config "${pg_config_path}"

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\\n"
  printf "wal_level = logical\\n"
  printf "max_replication_slots = 4\\n"
  printf "max_worker_processes = 32\\n"
  printf "unix_socket_directories = '%s'\\n" "${pg_socket_dir}"
  printf "port = %s\\n" "${pg_port}"
} >> "${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -w start
"${pg_bin_dir}/createdb" -h "${pg_socket_dir}" -p "${pg_port}" shiba_diff

psql_diff -q <<'SQL'
CREATE EXTENSION shiba;
SELECT shiba.activate();

CREATE TABLE public.diff_events (
  row_id integer NOT NULL,
  category_id integer,
  customer_id integer,
  label integer NOT NULL,
  amount integer NOT NULL,
  score integer NOT NULL
);

CREATE TABLE public.diff_extremes (
  row_id integer NOT NULL,
  group_id bigint NOT NULL,
  amount bigint NOT NULL,
  enabled boolean NOT NULL
);

CREATE TABLE shiba.diff_aggregate AS
SELECT category_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.diff_events GROUP BY category_id;

CREATE TABLE shiba.diff_having AS
SELECT category_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.diff_events GROUP BY category_id HAVING count(*) >= 3;

CREATE TABLE shiba.diff_count_distinct AS
SELECT category_id,count(DISTINCT customer_id) AS customer_count,
       sum(amount) AS total_amount
  FROM public.diff_events GROUP BY category_id;

CREATE TABLE shiba.diff_distinct AS
SELECT DISTINCT category_id,label FROM public.diff_events;

CREATE TABLE shiba.diff_topn AS
SELECT row_id,category_id,score,amount
  FROM public.diff_events
 ORDER BY score DESC NULLS LAST OFFSET 2 LIMIT 7;

CREATE TABLE shiba.diff_topn_ascending AS
SELECT row_id,category_id,score,amount
  FROM public.diff_events
 ORDER BY score ASC NULLS FIRST LIMIT 5;

CREATE TABLE shiba.diff_window_rows AS
SELECT row_id,category_id,score,
       row_number() OVER w AS row_number_value,
       rank() OVER w AS rank_value,
       dense_rank() OVER w AS dense_rank_value,
       count(*) OVER w AS running_count,
       sum(amount) OVER w AS running_sum,
       avg(amount) OVER w AS running_avg,
       min(amount) OVER w AS running_min,
       max(amount) OVER w AS running_max
  FROM public.diff_events
 WINDOW w AS (PARTITION BY category_id ORDER BY score);

CREATE TABLE shiba.diff_window_peers AS
SELECT row_id,category_id,amount,
       rank() OVER w AS rank_value,
       dense_rank() OVER w AS dense_rank_value,
       count(*) OVER w AS running_count,
       sum(amount) OVER w AS running_sum
  FROM public.diff_events
 WINDOW w AS (PARTITION BY category_id ORDER BY amount);

CREATE TABLE shiba.diff_window_rows_frame AS
SELECT row_id,category_id,score,
       sum(amount) OVER (
         PARTITION BY category_id ORDER BY score DESC NULLS FIRST
         ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
       ) AS framed_sum
  FROM public.diff_events;

CREATE TABLE shiba.diff_window_range_frame AS
SELECT row_id,category_id,score,
       sum(amount) OVER (
         PARTITION BY category_id ORDER BY score
         RANGE BETWEEN 100000 PRECEDING AND 100000 FOLLOWING
       ) AS framed_sum
  FROM public.diff_events;

CREATE TABLE shiba.diff_window_groups_frame AS
SELECT row_id,category_id,amount,
       sum(amount) OVER (
         PARTITION BY category_id ORDER BY amount
         GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
       ) AS framed_sum
  FROM public.diff_events;

CREATE TABLE shiba.diff_bigint AS
SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.diff_extremes GROUP BY group_id;

CREATE TABLE shiba.diff_bigint_filtered AS
SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.diff_extremes
 WHERE enabled = true AND amount <> 0
 GROUP BY group_id;

CREATE TABLE public.diff_specs (
  view_name regclass PRIMARY KEY,
  expected_sql text NOT NULL
);

INSERT INTO public.diff_specs VALUES
('shiba.diff_aggregate',
 $$SELECT category_id,count(*) AS row_count,sum(amount) AS total_amount
     FROM public.diff_events GROUP BY category_id$$),
('shiba.diff_having',
 $$SELECT category_id,count(*) AS row_count,sum(amount) AS total_amount
     FROM public.diff_events GROUP BY category_id HAVING count(*) >= 3$$),
('shiba.diff_count_distinct',
 $$SELECT category_id,count(DISTINCT customer_id) AS customer_count,
          sum(amount) AS total_amount
     FROM public.diff_events GROUP BY category_id$$),
('shiba.diff_distinct',
 $$SELECT DISTINCT category_id,label FROM public.diff_events$$),
('shiba.diff_topn',
 $$SELECT row_id,category_id,score,amount FROM public.diff_events
     ORDER BY score DESC NULLS LAST OFFSET 2 LIMIT 7$$),
('shiba.diff_topn_ascending',
 $$SELECT row_id,category_id,score,amount FROM public.diff_events
     ORDER BY score ASC NULLS FIRST LIMIT 5$$),
('shiba.diff_window_rows',
 $$SELECT row_id,category_id,score,
          row_number() OVER w AS row_number_value,
          rank() OVER w AS rank_value,
          dense_rank() OVER w AS dense_rank_value,
          count(*) OVER w AS running_count,
          sum(amount) OVER w AS running_sum,
          avg(amount) OVER w AS running_avg,
          min(amount) OVER w AS running_min,
          max(amount) OVER w AS running_max
     FROM public.diff_events
     WINDOW w AS (PARTITION BY category_id ORDER BY score)$$),
('shiba.diff_window_peers',
 $$SELECT row_id,category_id,amount,
          rank() OVER w AS rank_value,
          dense_rank() OVER w AS dense_rank_value,
          count(*) OVER w AS running_count,
          sum(amount) OVER w AS running_sum
     FROM public.diff_events
     WINDOW w AS (PARTITION BY category_id ORDER BY amount)$$),
('shiba.diff_window_rows_frame',
 $$SELECT row_id,category_id,score,
          sum(amount) OVER (
            PARTITION BY category_id ORDER BY score DESC NULLS FIRST
            ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
          ) AS framed_sum
     FROM public.diff_events$$),
('shiba.diff_window_range_frame',
 $$SELECT row_id,category_id,score,
          sum(amount) OVER (
            PARTITION BY category_id ORDER BY score
            RANGE BETWEEN 100000 PRECEDING AND 100000 FOLLOWING
          ) AS framed_sum
     FROM public.diff_events$$),
('shiba.diff_window_groups_frame',
 $$SELECT row_id,category_id,amount,
          sum(amount) OVER (
            PARTITION BY category_id ORDER BY amount
            GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
          ) AS framed_sum
     FROM public.diff_events$$),
('shiba.diff_bigint',
 $$SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
     FROM public.diff_extremes GROUP BY group_id$$),
('shiba.diff_bigint_filtered',
 $$SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
     FROM public.diff_extremes
    WHERE enabled = true AND amount <> 0
    GROUP BY group_id$$);

CREATE FUNCTION public.diff_failures()
RETURNS TABLE(view_name text,mismatch_count bigint)
LANGUAGE plpgsql
AS $function$
DECLARE
  spec record;
BEGIN
  FOR spec IN
    SELECT s.view_name,s.expected_sql
      FROM public.diff_specs AS s
     ORDER BY s.view_name::text
  LOOP
    view_name := spec.view_name::text;
    EXECUTE format(
      'SELECT count(*) FROM (
         (SELECT * FROM %s
          EXCEPT ALL
          SELECT * FROM (%s) AS expected)
         UNION ALL
         (SELECT * FROM (%s) AS expected
          EXCEPT ALL
          SELECT * FROM %s)
       ) AS mismatch',
      spec.view_name, spec.expected_sql, spec.expected_sql, spec.view_name
    ) INTO mismatch_count;
    RETURN NEXT;
  END LOOP;
END
$function$;
SQL

wait_for_value "13" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba dag worker'"
wait_for_value "0" \
  "SELECT count(*) FROM public.diff_failures() WHERE mismatch_count <> 0"

next_id=1

# Fixed-width type and accumulator boundaries. Each source value fits bigint,
# while the first group SUM exceeds i64 and therefore proves that state is not
# accidentally narrowed during incremental maintenance.
committed_mutation \
  "INSERT INTO public.diff_extremes VALUES
     (1,9223372036854770000,9000000000000000000,true),
     (2,9223372036854770000,9000000000000000000,false),
     (3,-9223372036854770000,-9000000000000000000,true),
     (4,0,0,true)"
committed_mutation \
  "UPDATE public.diff_extremes
      SET enabled=true,amount=-9000000000000000000
    WHERE row_id=2"
committed_mutation \
  "UPDATE public.diff_extremes
      SET group_id=-9223372036854770000
    WHERE row_id=1"
committed_mutation \
  "DELETE FROM public.diff_extremes WHERE row_id=4"

for ((round=1; round<=rounds; round++)); do
  row_count="$(psql_diff -Atqc "SELECT count(*) FROM public.diff_events")"
  rand_n 100
  operation_roll="${REPLY}"

  if test "${row_count}" -eq 0 || test "${operation_roll}" -lt 35; then
    random_value_tuple "${next_id}"
    sql="INSERT INTO public.diff_events VALUES (${next_id},${VALUE_TUPLE})"
    operation="insert"
    next_id="$((next_id + 1))"
    if ! committed_mutation "${sql}"; then
      dump_failure "${round}" "${operation}"
      exit 1
    fi
  elif test "${operation_roll}" -lt 60; then
    rand_n "${row_count}"
    target_id="$(psql_diff -Atqc \
      "SELECT row_id FROM public.diff_events ORDER BY row_id OFFSET ${REPLY} LIMIT 1")"
    random_value_tuple "${target_id}"
    sql="UPDATE public.diff_events
            SET category_id=${CATEGORY_VALUE},
                customer_id=${CUSTOMER_VALUE},
                label=${LABEL_VALUE},
                amount=CASE
                  WHEN amount=${AMOUNT_VALUE}
                  THEN amount+1
                  ELSE ${AMOUNT_VALUE}
                END,
                score=CASE
                  WHEN score=${SCORE_VALUE}
                  THEN score+100000
                  ELSE ${SCORE_VALUE}
                END
          WHERE row_id=${target_id}"
    operation="update"
    if ! committed_mutation "${sql}"; then
      dump_failure "${round}" "${operation}"
      exit 1
    fi
  elif test "${operation_roll}" -lt 80; then
    rand_n "${row_count}"
    target_id="$(psql_diff -Atqc \
      "SELECT row_id FROM public.diff_events ORDER BY row_id OFFSET ${REPLY} LIMIT 1")"
    sql="DELETE FROM public.diff_events WHERE row_id=${target_id}"
    operation="delete"
    if ! committed_mutation "${sql}"; then
      dump_failure "${round}" "${operation}"
      exit 1
    fi
  elif test "${operation_roll}" -lt 90; then
    random_value_tuple "${next_id}"
    sql="INSERT INTO public.diff_events VALUES (${next_id},${VALUE_TUPLE})"
    operation="rollback_insert"
    next_id="$((next_id + 1))"
    if ! rolled_back_mutation "${sql}"; then
      dump_failure "${round}" "${operation}"
      exit 1
    fi
  else
    rand_n "${row_count}"
    target_id="$(psql_diff -Atqc \
      "SELECT row_id FROM public.diff_events ORDER BY row_id OFFSET ${REPLY} LIMIT 1")"
    sql="UPDATE public.diff_events SET amount=amount+1000 WHERE row_id=${target_id}"
    operation="rollback_update"
    if ! rolled_back_mutation "${sql}"; then
      dump_failure "${round}" "${operation}"
      exit 1
    fi
  fi

  if ((round % 20 == 0 || round == rounds)); then
    printf 'seed=%s round=%s/%s rows=%s\n' \
      "${seed}" "${round}" "${rounds}" \
      "$(psql_diff -Atqc "SELECT count(*) FROM public.diff_events")"
  fi
done

test "$(psql_diff -Atqc \
  "SELECT count(*) FROM public.diff_failures() WHERE mismatch_count <> 0")" = "0"
printf 'single-source differential test passed: seed=%s rounds=%s\n' \
  "${seed}" "${rounds}"
