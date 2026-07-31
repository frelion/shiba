#!/usr/bin/env bash
set -euo pipefail

# Independent user-visible correctness oracle. The result tables are produced
# by Shiba, while each expected relation below is re-evaluated by PostgreSQL's
# ordinary executor from the current source tables. A deterministic pseudo-
# random DML sequence makes this a compact differential test instead of one
# more hand-picked fixture.
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-differential-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-differential-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_DIFFERENTIAL_TEST_PORT:-$((63000 + $$ % 1000))}"
database_name="shiba_differential"

psql_differential() {
  PGOPTIONS="-c statement_timeout=30000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

test_name="independent differential oracle gate"
test_psql_command=psql_differential
test_log_lines=180
test_wait_attempts="${SHIBA_DIFFERENTIAL_WAIT_ATTEMPTS:-600}"
test_wait_sleep=0.05
test_retain_log=1
source "${project_root}/scripts/test-lib.sh"
trap cleanup EXIT

cd "${project_root}"
install_test_extension "${pg_config_path}"

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" \
  --no-locale --encoding=UTF8 >/dev/null
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
  printf "shiba.batch_rows = 3\n"
  printf "shiba.batch_bytes = '16kB'\n"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s connect_timeout=5'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "$(id -un)"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_differential -qc "CREATE EXTENSION shiba"
psql_differential -qc "SELECT shiba.activate()"
wait_for_query "1|1" "
  SELECT
    count(*) FILTER (WHERE backend_type='shiba runtime'),
    count(*) FILTER (
      WHERE backend_type='walsender' AND application_name='shiba'
    )
  FROM pg_stat_activity
" "the differential oracle Runtime and walsender"

psql_differential -qc "
  CREATE TABLE public.oracle_left (
    id integer PRIMARY KEY,
    group_id integer,
    amount integer,
    label text
  );
  CREATE TABLE public.oracle_right (
    id integer PRIMARY KEY,
    group_id integer,
    amount integer
  );
  INSERT INTO public.oracle_left VALUES
    (1,1,10,'a'), (2,1,NULL,NULL), (3,2,-3,'c'),
    (4,3,7,'d'), (5,1,10,'a');
  INSERT INTO public.oracle_right VALUES
    (101,1,10), (102,1,NULL), (103,2,-3),
    (104,4,7), (105,1,10);

  CREATE TABLE shiba.oracle_filter AS
    SELECT id, group_id, amount * 2 AS doubled, coalesce(label,'<null>') AS label
    FROM public.oracle_left
    WHERE amount IS NULL OR amount >= 0;
  CREATE TABLE shiba.oracle_join AS
    SELECT left_side.id AS left_id,
           right_side.id AS right_id,
           left_side.group_id,
           right_side.amount AS right_amount
    FROM public.oracle_left AS left_side
    JOIN public.oracle_right AS right_side
      ON left_side.group_id = right_side.group_id
     AND left_side.amount = right_side.amount;
  CREATE TABLE shiba.oracle_aggregate AS
    SELECT group_id,
           count(*)::bigint AS row_count,
           coalesce(sum(amount),0::bigint) AS total_amount
    FROM public.oracle_left
    GROUP BY group_id;
  CREATE TABLE shiba.oracle_distinct AS
    SELECT DISTINCT group_id, label
    FROM public.oracle_left;
  CREATE TABLE shiba.oracle_bag AS
    SELECT group_id, label
    FROM public.oracle_left;
  CREATE TABLE shiba.oracle_window AS
    SELECT id, group_id, amount,
           row_number() OVER (
             PARTITION BY group_id
             ORDER BY amount DESC NULLS LAST, id
           ) AS row_number_value
    FROM public.oracle_left;
  CREATE TABLE shiba.oracle_topn AS
    SELECT id, group_id, amount
    FROM public.oracle_left
    ORDER BY amount DESC NULLS LAST, id
    FETCH FIRST 3 ROWS ONLY;
"

assert_oracles() {
  local description="$1"
  wait_for_query "0" "
    WITH
    filter_expected AS (
      SELECT id, group_id, amount * 2 AS doubled,
             coalesce(label,'<null>') AS label
      FROM public.oracle_left
      WHERE amount IS NULL OR amount >= 0
    ),
    filter_actual AS (
      SELECT id, group_id, doubled, label FROM shiba.oracle_filter
    ),
    filter_diff AS (
      (SELECT * FROM filter_expected EXCEPT ALL SELECT * FROM filter_actual)
      UNION ALL
      (SELECT * FROM filter_actual EXCEPT ALL SELECT * FROM filter_expected)
    ),
    join_expected AS (
      SELECT left_side.id AS left_id,
             right_side.id AS right_id,
             left_side.group_id,
             right_side.amount AS right_amount
      FROM public.oracle_left AS left_side
      JOIN public.oracle_right AS right_side
        ON left_side.group_id = right_side.group_id
       AND left_side.amount = right_side.amount
    ),
    join_actual AS (
      SELECT left_id, right_id, group_id, right_amount
      FROM shiba.oracle_join
    ),
    join_diff AS (
      (SELECT * FROM join_expected EXCEPT ALL SELECT * FROM join_actual)
      UNION ALL
      (SELECT * FROM join_actual EXCEPT ALL SELECT * FROM join_expected)
    ),
    aggregate_expected AS (
      SELECT group_id, count(*)::bigint AS row_count,
             coalesce(sum(amount),0::bigint) AS total_amount
      FROM public.oracle_left
      GROUP BY group_id
    ),
    aggregate_actual AS (
      SELECT group_id, row_count, total_amount
      FROM shiba.oracle_aggregate
    ),
    aggregate_diff AS (
      (SELECT * FROM aggregate_expected EXCEPT ALL SELECT * FROM aggregate_actual)
      UNION ALL
      (SELECT * FROM aggregate_actual EXCEPT ALL SELECT * FROM aggregate_expected)
    ),
    distinct_expected AS (
      SELECT DISTINCT group_id, label FROM public.oracle_left
    ),
    distinct_actual AS (
      SELECT group_id, label FROM shiba.oracle_distinct
    ),
    distinct_diff AS (
      (SELECT * FROM distinct_expected EXCEPT ALL SELECT * FROM distinct_actual)
      UNION ALL
      (SELECT * FROM distinct_actual EXCEPT ALL SELECT * FROM distinct_expected)
    ),
    bag_expected AS (
      SELECT group_id, label FROM public.oracle_left
    ),
    bag_actual AS (
      SELECT group_id, label FROM shiba.oracle_bag
    ),
    bag_diff AS (
      (SELECT * FROM bag_expected EXCEPT ALL SELECT * FROM bag_actual)
      UNION ALL
      (SELECT * FROM bag_actual EXCEPT ALL SELECT * FROM bag_expected)
    ),
    window_expected AS (
      SELECT id, group_id, amount,
             row_number() OVER (
               PARTITION BY group_id
               ORDER BY amount DESC NULLS LAST, id
             ) AS row_number_value
      FROM public.oracle_left
    ),
    window_actual AS (
      SELECT id, group_id, amount, row_number_value
      FROM shiba.oracle_window
    ),
    window_diff AS (
      (SELECT * FROM window_expected EXCEPT ALL SELECT * FROM window_actual)
      UNION ALL
      (SELECT * FROM window_actual EXCEPT ALL SELECT * FROM window_expected)
    ),
    topn_expected AS (
      SELECT id, group_id, amount
      FROM public.oracle_left
      ORDER BY amount DESC NULLS LAST, id
      FETCH FIRST 3 ROWS ONLY
    ),
    topn_actual AS (
      SELECT id, group_id, amount FROM shiba.oracle_topn
    ),
    topn_diff AS (
      (SELECT * FROM topn_expected EXCEPT ALL SELECT * FROM topn_actual)
      UNION ALL
      (SELECT * FROM topn_actual EXCEPT ALL SELECT * FROM topn_expected)
    )
    SELECT
      (SELECT count(*) FROM filter_diff)
      + (SELECT count(*) FROM join_diff)
      + (SELECT count(*) FROM aggregate_diff)
      + (SELECT count(*) FROM distinct_diff)
      + (SELECT count(*) FROM bag_diff)
      + (SELECT count(*) FROM window_diff)
      + (SELECT count(*) FROM topn_diff)
  " "${description}"
}

assert_oracles "initial independent SQL oracle"

# Multi-statement transactions and savepoints are deliberately kept separate
# from the one-mutation rounds below. A rolled-back subtransaction must leave
# no WAL-visible effect, while the committed statements must be applied as one
# atomic source transaction.
psql_differential -qc "
  BEGIN;
  INSERT INTO public.oracle_left(id,group_id,amount,label)
  VALUES (20,NULL,0,'txn_before');
  UPDATE public.oracle_left
  SET amount=amount+1
  WHERE group_id=1 AND amount IS NOT NULL;
  DELETE FROM public.oracle_right WHERE id=102;
  SAVEPOINT before_reverted_update;
  UPDATE public.oracle_left
  SET group_id=2, amount=NULL, label='must_not_survive'
  WHERE id=20;
  ROLLBACK TO before_reverted_update;
  UPDATE public.oracle_left
  SET group_id=3, amount=99, label='txn_after'
  WHERE id=20;
  INSERT INTO public.oracle_right(id,group_id,amount)
  VALUES (206,NULL,0);
  SAVEPOINT before_reverted_delete;
  DELETE FROM public.oracle_left WHERE id=5;
  ROLLBACK TO before_reverted_delete;
  COMMIT;
"
assert_oracles "multi-statement transaction and savepoint oracle"

# A deterministic LCG gives varied, reproducible values without introducing a
# dependency or a flaky source of randomness. UPSERT/UPDATE/DELETE are allowed
# to target absent rows so each round remains a valid committed transaction.
seed=7919
next_random() {
  seed=$(( (seed * 1103515245 + 12345) % 2147483647 ))
}

for round in $(seq 1 "${SHIBA_DIFFERENTIAL_ROUNDS:-36}"); do
  next_random
  operation=$((seed % 6))
  next_random
  row_id=$((1 + seed % 12))
  next_random
  group_id=$((seed % 4))
  if test $((seed % 7)) -eq 0; then
    group_id_sql="NULL"
  else
    group_id_sql="${group_id}"
  fi
  next_random
  if test $((seed % 5)) -eq 0; then
    amount_sql="NULL"
  else
    amount_sql="$((seed % 25 - 12))"
  fi
  label="'r${round}_id${row_id}'"

  case "${operation}" in
    0)
      mutation="
        INSERT INTO public.oracle_left(id,group_id,amount,label)
        VALUES (${row_id},${group_id_sql},${amount_sql},${label})
        ON CONFLICT (id) DO UPDATE
        SET group_id=EXCLUDED.group_id,
            amount=EXCLUDED.amount,
            label=EXCLUDED.label;
      "
      ;;
    1)
      mutation="DELETE FROM public.oracle_left WHERE id=${row_id};"
      ;;
    2)
      mutation="
        UPDATE public.oracle_left
        SET group_id=${group_id_sql}, amount=${amount_sql}, label=${label}
        WHERE id=${row_id};
      "
      ;;
    3)
      mutation="
        INSERT INTO public.oracle_right(id,group_id,amount)
        VALUES (100+${row_id},${group_id_sql},${amount_sql})
        ON CONFLICT (id) DO UPDATE
        SET group_id=EXCLUDED.group_id, amount=EXCLUDED.amount;
      "
      ;;
    4)
      mutation="DELETE FROM public.oracle_right WHERE id=100+${row_id};"
      ;;
    5)
      mutation="
        UPDATE public.oracle_right
        SET group_id=${group_id_sql}, amount=${amount_sql}
        WHERE id=100+${row_id};
      "
      ;;
  esac

  psql_differential -qc "BEGIN; ${mutation} COMMIT"
  assert_oracles "independent SQL oracle round ${round}"
done

# Attack lifecycle boundaries while the source is still live. These must fail
# atomically and leave the same independent oracle result intact.
expect_failure "cannot ALTER TABLE" \
  "ALTER TABLE public.oracle_left ADD COLUMN forbidden integer"
expect_failure "cannot ALTER TABLE" \
  "ALTER TABLE public.oracle_left DISABLE TRIGGER shiba_wakeup"
expect_failure "cannot drop Shiba source protection trigger" \
  "DROP TRIGGER shiba_wakeup ON public.oracle_left"
expect_failure "cannot drop Shiba source protection trigger" \
  "DROP TRIGGER shiba_no_truncate ON public.oracle_left"
expect_failure "cannot ALTER Shiba protection trigger" \
  "ALTER TRIGGER shiba_wakeup ON public.oracle_left RENAME TO user_wakeup"
expect_failure "TRUNCATE is not supported" \
  "TRUNCATE public.oracle_left"
expect_failure "cannot DROP TABLE with OID" \
  "DROP TABLE public.oracle_left"
assert_oracles "oracle after rejected source DDL"

expect_failure "cannot drop Shiba result protection trigger" \
  "DROP TRIGGER shiba_result_guard ON shiba.oracle_filter"
expect_failure "cannot ALTER Shiba protection trigger" \
  "ALTER TRIGGER shiba_result_guard ON shiba.oracle_filter RENAME TO forged_guard"
expect_failure "cannot ALTER Shiba result table" \
  "ALTER TABLE shiba.oracle_filter DISABLE TRIGGER shiba_result_guard"

# Result writes must never bypass the Sink transaction boundary.
psql_differential -qc "
  CREATE ROLE oracle_writer;
  GRANT USAGE ON SCHEMA shiba TO oracle_writer;
  GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON shiba.oracle_filter TO oracle_writer;
"
expect_failure "cannot modify Shiba result table" "
  SET ROLE oracle_writer;
  INSERT INTO shiba.oracle_filter(id,group_id,doubled,label)
  VALUES (999,999,999,'forged')
"
expect_failure "cannot modify Shiba result table" "
  SET ROLE oracle_writer;
  UPDATE shiba.oracle_filter SET doubled=999 WHERE id=1
"
expect_failure "cannot modify Shiba result table" "
  SET ROLE oracle_writer;
  DELETE FROM shiba.oracle_filter WHERE id=1
"
expect_failure "cannot modify Shiba result table" "
  SET ROLE oracle_writer;
  TRUNCATE shiba.oracle_filter
"
expect_failure "cannot modify Shiba result table" "
  SET ROLE oracle_writer;
  MERGE INTO shiba.oracle_filter AS target
  USING (VALUES (1,1,999,'forged')) AS incoming(id,group_id,doubled,label)
    ON target.id=incoming.id
  WHEN MATCHED THEN UPDATE SET doubled=incoming.doubled
  WHEN NOT MATCHED THEN INSERT (id,group_id,doubled,label)
    VALUES (incoming.id,incoming.group_id,incoming.doubled,incoming.label)
"

# Managed indexes are part of the result lifecycle and must be registered and
# removed through the public API without changing the result oracle.
psql_differential -qc "
  SELECT shiba.create_index(
    'shiba.oracle_filter', 'oracle_filter_id_idx', ARRAY['id']
  )
"
assert_query "1" "
  SELECT count(*)
  FROM shiba_internal.managed_indexes
  WHERE index_name='oracle_filter_id_idx'
"
psql_differential -qc "SELECT shiba.drop_index('shiba.oracle_filter_id_idx')"
assert_query "0" "
  SELECT count(*)
  FROM shiba_internal.managed_indexes
  WHERE index_name='oracle_filter_id_idx'
"
assert_oracles "oracle after result lifecycle checks"

printf '%s\n' 'independent differential oracle gate passed'
