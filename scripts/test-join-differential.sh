#!/usr/bin/env bash
set -euo pipefail

# Fixed-seed differential coverage for Shiba's supported join surface.
# Every source mutation is committed independently, then each incrementally
# maintained result is compared with PostgreSQL recomputation using EXCEPT ALL.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-join-diff-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-join-diff-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="56432"
seed=1592594996

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
  "${pg_bin_dir}/psql" \
    -v ON_ERROR_STOP=1 \
    -h "${pg_socket_dir}" \
    -p "${pg_port}" \
    -d shiba_join_diff \
    "$@"
}

wait_for_value() {
  local expected="$1"
  local query="$2"
  local attempt
  for attempt in {1..500}; do
    if test "$(psql_diff -Atqc "${query}")" = "${expected}"; then
      return 0
    fi
    sleep 0.02
  done
  psql_diff -c "${query}"
  return 1
}

assert_join_results() {
  local step="$1"
  psql_diff -qAtc "
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_inner',
      \$q\$SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
          FROM join_diff.facts f
          JOIN join_diff.dims d ON f.join_key = d.join_key
          GROUP BY d.group_id\$q\$,
      'seed=${seed} step=${step} inner'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_left',
      \$q\$SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
          FROM join_diff.facts f
          LEFT JOIN join_diff.dims d ON f.join_key = d.join_key
          GROUP BY d.group_id\$q\$,
      'seed=${seed} step=${step} left'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_right',
      \$q\$SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
          FROM join_diff.facts f
          RIGHT JOIN join_diff.dims d ON f.join_key = d.join_key
          GROUP BY d.group_id\$q\$,
      'seed=${seed} step=${step} right'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_full',
      \$q\$SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
          FROM join_diff.facts f
          FULL JOIN join_diff.dims d ON f.join_key = d.join_key
          GROUP BY d.group_id\$q\$,
      'seed=${seed} step=${step} full'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_cross_predicate',
      \$q\$SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
          FROM join_diff.facts f
          JOIN join_diff.dims d ON f.join_key = d.join_key
          WHERE f.amount >= d.threshold AND f.gate <> d.gate
          GROUP BY d.group_id\$q\$,
      'seed=${seed} step=${step} cross-input-predicate'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_join_having',
      \$q\$SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
          FROM join_diff.facts f
          JOIN join_diff.dims d ON f.join_key = d.join_key
          GROUP BY d.group_id
          HAVING count(*) >= 3 AND sum(f.amount) <> 0\$q\$,
      'seed=${seed} step=${step} join-having'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_join_distinct',
      \$q\$SELECT d.group_id, count(DISTINCT f.row_id) AS row_count,
                   sum(f.amount) AS total_amount
          FROM join_diff.facts f
          JOIN join_diff.dims d ON f.join_key = d.join_key
          GROUP BY d.group_id\$q\$,
      'seed=${seed} step=${step} join-count-distinct'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_group_from_left',
      \$q\$SELECT f.gate, count(*) AS row_count, sum(f.amount) AS total_amount
          FROM join_diff.facts f
          JOIN join_diff.dims d ON f.join_key = d.join_key
          GROUP BY f.gate\$q\$,
      'seed=${seed} step=${step} join-group-from-left'
    );
  " >/dev/null
}

assert_sublink_results() {
  local step="$1"
  psql_diff -qAtc "
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_exists',
      \$q\$SELECT o.join_key, count(*) AS row_count, sum(o.amount) AS total_amount
          FROM join_diff.orders o
          WHERE EXISTS (
            SELECT 1 FROM join_diff.permits p
            WHERE p.join_key = o.join_key
          )
          GROUP BY o.join_key\$q\$,
      'seed=${seed} step=${step} exists'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_not_exists',
      \$q\$SELECT o.join_key, count(*) AS row_count, sum(o.amount) AS total_amount
          FROM join_diff.orders o
          WHERE NOT EXISTS (
            SELECT 1 FROM join_diff.permits p
            WHERE p.join_key = o.join_key
          )
          GROUP BY o.join_key\$q\$,
      'seed=${seed} step=${step} not-exists'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_in',
      \$q\$SELECT o.join_key, count(*) AS row_count, sum(o.amount) AS total_amount
          FROM join_diff.orders o
          WHERE o.join_key IN (SELECT p.join_key FROM join_diff.permits p)
          GROUP BY o.join_key\$q\$,
      'seed=${seed} step=${step} in'
    );
    SELECT join_diff.assert_eventually_equal(
      'shiba.diff_not_in',
      \$q\$SELECT o.join_key, count(*) AS row_count, sum(o.amount) AS total_amount
          FROM join_diff.orders o
          WHERE o.join_key NOT IN (SELECT p.join_key FROM join_diff.permits p)
          GROUP BY o.join_key\$q\$,
      'seed=${seed} step=${step} not-in'
    );
  " >/dev/null
}

cd "${project_root}"
cargo pgrx install --pg-config "${pg_config_path}"

"${pg_bin_dir}/initdb" \
  -D "${pg_data_dir}" \
  --no-locale \
  --encoding=UTF8 \
  >/dev/null
{
  printf "session_preload_libraries = 'shiba'\n"
  printf "wal_level = logical\n"
  printf "max_replication_slots = 4\n"
  printf "max_worker_processes = 12\n"
  printf "listen_addresses = ''\n"
  printf "unix_socket_directories = '%s'\n" "${pg_socket_dir}"
  printf "port = %s\n" "${pg_port}"
  printf "shiba.ingress_batch_rows = 4\n"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=shiba_join_diff user=%s'\n" \
    "${pg_socket_dir}" "${pg_port}" "$(id -un)"
} >> "${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" \
  -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" \
  -w start
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" \
  -p "${pg_port}" \
  shiba_join_diff

psql_diff -qc "CREATE EXTENSION shiba"
psql_diff -qc "SELECT shiba.activate()"
wait_for_value \
  "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

psql_diff -qc "
  CREATE SCHEMA join_diff;

  CREATE FUNCTION join_diff.assert_eventually_equal(
    actual regclass,
    expected_sql text,
    test_label text
  )
  RETURNS void
  LANGUAGE plpgsql
  AS \$function\$
  DECLARE
    attempt integer;
    differs boolean;
    actual_rows jsonb;
    expected_rows jsonb;
  BEGIN
    FOR attempt IN 1..300 LOOP
      EXECUTE format(
        'SELECT EXISTS (
           (SELECT * FROM %s EXCEPT ALL SELECT * FROM (%s) expected_rows)
           UNION ALL
           (SELECT * FROM (%s) expected_rows EXCEPT ALL SELECT * FROM %s)
         )',
        actual,
        expected_sql,
        expected_sql,
        actual
      )
      INTO differs;

      IF NOT differs THEN
        RETURN;
      END IF;
      PERFORM pg_sleep(0.02);
    END LOOP;

    EXECUTE format(
      'SELECT coalesce(jsonb_agg(to_jsonb(actual_row)
                                 ORDER BY to_jsonb(actual_row)::text),
                       ''[]''::jsonb)
         FROM %s actual_row',
      actual
    )
    INTO actual_rows;
    EXECUTE format(
      'SELECT coalesce(jsonb_agg(to_jsonb(expected_row)
                                 ORDER BY to_jsonb(expected_row)::text),
                       ''[]''::jsonb)
         FROM (%s) expected_row',
      expected_sql
    )
    INTO expected_rows;

    RAISE EXCEPTION
      'differential mismatch: %, actual=%, expected=%',
      test_label,
      actual_rows,
      expected_rows;
  END
  \$function\$;
"

# Phase 1: ordinary joins. Duplicate keys and NULL keys are present in both
# initial state and later deltas. The curated prefix forces every zero/one
# match boundary before the fixed-seed sequence explores mixed mutations.
psql_diff -qc "
  CREATE TABLE join_diff.facts (
    row_id integer NOT NULL,
    join_key integer,
    amount integer NOT NULL,
    gate integer NOT NULL
  );
  CREATE TABLE join_diff.dims (
    row_id integer NOT NULL,
    join_key integer,
    group_id integer,
    threshold integer NOT NULL,
    gate integer NOT NULL
  );

  INSERT INTO join_diff.facts VALUES
    (1, 1, 5, 0),
    (2, 1, 5, 1),
    (3, 2, 20, 0),
    (4, 9, 7, 1),
    (5, NULL, 11, 0);
  INSERT INTO join_diff.dims VALUES
    (1, 1, 10, 5, 1),
    (2, 1, 10, 6, 0),
    (3, 3, 30, 1, 1),
    (4, NULL, 40, 1, 1);

  CREATE TABLE shiba.diff_inner AS
    SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
    FROM join_diff.facts f
    JOIN join_diff.dims d ON f.join_key = d.join_key
    GROUP BY d.group_id;
  CREATE TABLE shiba.diff_left AS
    SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
    FROM join_diff.facts f
    LEFT JOIN join_diff.dims d ON f.join_key = d.join_key
    GROUP BY d.group_id;
  CREATE TABLE shiba.diff_right AS
    SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
    FROM join_diff.facts f
    RIGHT JOIN join_diff.dims d ON f.join_key = d.join_key
    GROUP BY d.group_id;
  CREATE TABLE shiba.diff_full AS
    SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
    FROM join_diff.facts f
    FULL JOIN join_diff.dims d ON f.join_key = d.join_key
    GROUP BY d.group_id;
  CREATE TABLE shiba.diff_cross_predicate AS
    SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
    FROM join_diff.facts f
    JOIN join_diff.dims d ON f.join_key = d.join_key
    WHERE f.amount >= d.threshold AND f.gate <> d.gate
    GROUP BY d.group_id;
  CREATE TABLE shiba.diff_join_having AS
    SELECT d.group_id, count(*) AS row_count, sum(f.amount) AS total_amount
    FROM join_diff.facts f
    JOIN join_diff.dims d ON f.join_key = d.join_key
    GROUP BY d.group_id
    HAVING count(*) >= 3 AND sum(f.amount) <> 0;
  CREATE TABLE shiba.diff_join_distinct AS
    SELECT d.group_id, count(DISTINCT f.row_id) AS row_count,
           sum(f.amount) AS total_amount
    FROM join_diff.facts f
    JOIN join_diff.dims d ON f.join_key = d.join_key
    GROUP BY d.group_id;
  CREATE TABLE shiba.diff_group_from_left AS
    SELECT f.gate, count(*) AS row_count, sum(f.amount) AS total_amount
    FROM join_diff.facts f
    JOIN join_diff.dims d ON f.join_key = d.join_key
    GROUP BY f.gate;
"
wait_for_value \
  "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
assert_join_results "initial"

join_step=0
for statement in \
  "INSERT INTO join_diff.dims VALUES (5,9,90,5,0)" \
  "INSERT INTO join_diff.dims VALUES (6,9,91,10,1)" \
  "DELETE FROM join_diff.dims WHERE row_id=5" \
  "UPDATE join_diff.dims SET join_key=2,group_id=20 WHERE row_id=3" \
  "UPDATE join_diff.facts SET join_key=3,amount=2 WHERE row_id=3" \
  "DELETE FROM join_diff.dims WHERE row_id IN (1,2)" \
  "INSERT INTO join_diff.dims VALUES (7,1,NULL,4,0)" \
  "UPDATE join_diff.facts SET join_key=NULL WHERE row_id=1" \
  "UPDATE join_diff.facts SET join_key=1,amount=8,gate=1 WHERE row_id=1" \
  "BEGIN; INSERT INTO join_diff.facts VALUES (100,77,13,0); INSERT INTO join_diff.dims VALUES (100,77,770,1,1); COMMIT" \
  "BEGIN; DELETE FROM join_diff.facts WHERE row_id=100; DELETE FROM join_diff.dims WHERE row_id=100; COMMIT"
do
  join_step=$((join_step + 1))
  psql_diff -qc "${statement}"
  assert_join_results "curated-${join_step}"
done

state="${seed}"
for join_step in {1..24}; do
  state=$(((1103515245 * state + 12345) & 2147483647))
  operation=$((state % 8))
  row_id=$((20 + join_step))
  target_id=$((1 + (state / 97) % (19 + join_step)))
  join_key=$(((state / 17) % 6))
  if test "$((state % 11))" = "0"; then
    join_key_sql="NULL"
  else
    join_key_sql="${join_key}"
  fi
  amount=$((1 + (state / 31) % 40))
  group_id=$(((state / 43) % 5 * 10))
  threshold=$((1 + (state / 59) % 30))
  gate=$(((state / 71) % 3))

  case "${operation}" in
    0)
      mutation="INSERT INTO join_diff.facts VALUES
                (${row_id},${join_key_sql},${amount},${gate})"
      ;;
    1)
      mutation="INSERT INTO join_diff.dims VALUES
                (${row_id},${join_key_sql},${group_id},${threshold},${gate})"
      ;;
    2)
      mutation="UPDATE join_diff.facts
                SET join_key=${join_key_sql},amount=${amount},gate=${gate}
                WHERE row_id=${target_id}"
      ;;
    3)
      mutation="UPDATE join_diff.dims
                SET join_key=${join_key_sql},group_id=${group_id},
                    threshold=${threshold},gate=${gate}
                WHERE row_id=${target_id}"
      ;;
    4)
      mutation="DELETE FROM join_diff.facts WHERE row_id=${target_id}"
      ;;
    5)
      mutation="DELETE FROM join_diff.dims WHERE row_id=${target_id}"
      ;;
    6)
      mutation="UPDATE join_diff.facts
                SET amount=amount+${amount},gate=${gate}
                WHERE join_key IS NOT DISTINCT FROM ${join_key_sql}"
      ;;
    7)
      mutation="UPDATE join_diff.dims
                SET threshold=${threshold},gate=${gate}
                WHERE join_key IS NOT DISTINCT FROM ${join_key_sql}"
      ;;
  esac

  psql_diff -qc "${mutation}"
  assert_join_results "random-${join_step}"
done

psql_diff -qc "
  DROP TABLE shiba.diff_cross_predicate;
  DROP TABLE shiba.diff_join_having;
  DROP TABLE shiba.diff_join_distinct;
  DROP TABLE shiba.diff_group_from_left;
  DROP TABLE shiba.diff_full;
  DROP TABLE shiba.diff_right;
  DROP TABLE shiba.diff_left;
  DROP TABLE shiba.diff_inner;
"
wait_for_value \
  "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

# Phase 2: thresholded semi/anti joins. Right duplicates exercise the 0/1 and
# 1/2 multiplicity boundaries; NULL on either side exercises PostgreSQL's
# three-valued IN/NOT IN rules.
psql_diff -qc "
  CREATE TABLE join_diff.orders (
    row_id integer NOT NULL,
    join_key integer,
    amount integer NOT NULL
  );
  CREATE TABLE join_diff.permits (
    row_id integer NOT NULL,
    join_key integer
  );

  INSERT INTO join_diff.orders VALUES
    (1,1,5),
    (2,1,7),
    (3,2,11),
    (4,4,13),
    (5,NULL,17);
  INSERT INTO join_diff.permits VALUES
    (1,1),
    (2,1),
    (3,3),
    (4,NULL);

  CREATE TABLE shiba.diff_exists AS
    SELECT o.join_key, count(*) AS row_count, sum(o.amount) AS total_amount
    FROM join_diff.orders o
    WHERE EXISTS (
      SELECT 1 FROM join_diff.permits p WHERE p.join_key=o.join_key
    )
    GROUP BY o.join_key;
  CREATE TABLE shiba.diff_not_exists AS
    SELECT o.join_key, count(*) AS row_count, sum(o.amount) AS total_amount
    FROM join_diff.orders o
    WHERE NOT EXISTS (
      SELECT 1 FROM join_diff.permits p WHERE p.join_key=o.join_key
    )
    GROUP BY o.join_key;
  CREATE TABLE shiba.diff_in AS
    SELECT o.join_key, count(*) AS row_count, sum(o.amount) AS total_amount
    FROM join_diff.orders o
    WHERE o.join_key IN (SELECT p.join_key FROM join_diff.permits p)
    GROUP BY o.join_key;
  CREATE TABLE shiba.diff_not_in AS
    SELECT o.join_key, count(*) AS row_count, sum(o.amount) AS total_amount
    FROM join_diff.orders o
    WHERE o.join_key NOT IN (SELECT p.join_key FROM join_diff.permits p)
    GROUP BY o.join_key;
"
wait_for_value \
  "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
assert_sublink_results "initial"

sublink_step=0
for statement in \
  "DELETE FROM join_diff.permits WHERE row_id=1" \
  "DELETE FROM join_diff.permits WHERE row_id=2" \
  "INSERT INTO join_diff.permits VALUES (5,2)" \
  "DELETE FROM join_diff.permits WHERE row_id=4" \
  "INSERT INTO join_diff.permits VALUES (6,NULL)" \
  "UPDATE join_diff.permits SET join_key=4 WHERE row_id=6" \
  "UPDATE join_diff.orders SET join_key=3 WHERE row_id=4" \
  "DELETE FROM join_diff.permits WHERE join_key=3" \
  "DELETE FROM join_diff.permits"
do
  sublink_step=$((sublink_step + 1))
  psql_diff -qc "${statement}"
  assert_sublink_results "curated-${sublink_step}"
done

state="$((seed ^ 610839776))"
for sublink_step in {1..24}; do
  state=$(((1664525 * state + 1013904223) & 2147483647))
  operation=$((state % 7))
  row_id=$((30 + sublink_step))
  target_id=$((1 + (state / 89) % (29 + sublink_step)))
  join_key=$(((state / 13) % 6))
  if test "$((state % 9))" = "0"; then
    join_key_sql="NULL"
  else
    join_key_sql="${join_key}"
  fi
  amount=$((1 + (state / 37) % 50))

  case "${operation}" in
    0)
      mutation="INSERT INTO join_diff.orders VALUES
                (${row_id},${join_key_sql},${amount})"
      ;;
    1)
      mutation="INSERT INTO join_diff.permits VALUES
                (${row_id},${join_key_sql})"
      ;;
    2)
      mutation="UPDATE join_diff.orders
                SET join_key=${join_key_sql},amount=${amount}
                WHERE row_id=${target_id}"
      ;;
    3)
      mutation="UPDATE join_diff.permits
                SET join_key=${join_key_sql}
                WHERE row_id=${target_id}"
      ;;
    4)
      mutation="DELETE FROM join_diff.orders WHERE row_id=${target_id}"
      ;;
    5)
      mutation="DELETE FROM join_diff.permits WHERE row_id=${target_id}"
      ;;
    6)
      mutation="DELETE FROM join_diff.permits
                WHERE join_key IS NOT DISTINCT FROM ${join_key_sql}"
      ;;
  esac

  psql_diff -qc "${mutation}"
  assert_sublink_results "random-${sublink_step}"
done

# A single source transaction whose left and right deltas land in different
# ingress batches must converge when each batch is published directly.
psql_diff -qc "
  CREATE TABLE join_diff.batch_left (
    row_id integer NOT NULL,
    join_key integer NOT NULL,
    amount integer NOT NULL
  );
  CREATE TABLE join_diff.batch_right (
    join_key integer NOT NULL,
    group_id integer NOT NULL
  );
  CREATE TABLE shiba.diff_multibatch_join AS
  SELECT r.group_id,count(*) AS row_count,sum(l.amount) AS total_amount
  FROM join_diff.batch_left l
  JOIN join_diff.batch_right r USING(join_key)
  GROUP BY r.group_id;
  UPDATE shiba_internal.dag_runtime_state
  SET active=false
  WHERE result_oid='shiba.diff_multibatch_join'::regclass"
psql_diff -qc "
  BEGIN;
  INSERT INTO join_diff.batch_left
  SELECT id,1,1 FROM generate_series(1,64) AS id;
  INSERT INTO join_diff.batch_right VALUES (1,7);
  COMMIT"
wait_for_value "1" "
  SELECT count(*)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.diff_multibatch_join'::regclass"
multibatch_join_lsn="$(psql_diff -Atqc "
  SELECT commit_lsn
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.diff_multibatch_join'::regclass")"
test "$(psql_diff -Atqc "
  SELECT count(*)>1
  FROM shiba_internal.ingress_apply_batches batch
  JOIN shiba_internal.dag_inbox inbox
    ON inbox.ingress_txn_id=batch.ingress_txn_id
  WHERE inbox.result_oid='shiba.diff_multibatch_join'::regclass
    AND inbox.commit_lsn='${multibatch_join_lsn}'::pg_lsn")" = "t"
test "$(psql_diff -Atqc "
  WITH event_batches AS (
    SELECT event.source_oid,batch.batch_ordinal
    FROM shiba_internal.effective_change_log event
    JOIN shiba_internal.ingress_apply_batches batch
      ON batch.ingress_txn_id=event.ingress_txn_id
     AND event.sequence BETWEEN batch.first_input_seq AND batch.last_input_seq
    WHERE event.commit_lsn='${multibatch_join_lsn}'::pg_lsn
  )
  SELECT
    (SELECT min(batch_ordinal) FROM event_batches
     WHERE source_oid='join_diff.batch_right'::regclass)
    >
    (SELECT min(batch_ordinal) FROM event_batches
     WHERE source_oid='join_diff.batch_left'::regclass)")" = "t"
psql_diff -qc "
  UPDATE shiba_internal.dag_runtime_state
  SET active=true
  WHERE result_oid='shiba.diff_multibatch_join'::regclass"
wait_for_value "1|64|64|0" "
  SELECT count(*) || '|' ||
         coalesce(max(row_count),0) || '|' ||
         coalesce(max(total_amount),0) || '|' ||
         (
           SELECT count(*)
           FROM shiba_internal.dag_inbox
           WHERE result_oid='shiba.diff_multibatch_join'::regclass
         )
  FROM shiba.diff_multibatch_join
  WHERE group_id=7"

printf 'join differential tests passed (seed=%s, 69 committed mutations, 424 comparisons)\n' \
  "${seed}"
