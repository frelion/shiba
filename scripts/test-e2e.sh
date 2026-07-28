#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$(${pg_config_path} --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-pg17-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-pg17-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="55432"

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

psql_e2e() {
  "${pg_bin_dir}/psql" -v ON_ERROR_STOP=1 -h "${pg_socket_dir}" -p "${pg_port}" -d shiba_e2e "$@"
}

wait_for_value() {
  local expected="$1"
  local query="$2"
  local attempt
  for attempt in {1..100}; do
    if test "$(psql_e2e -Atqc "${query}")" = "${expected}"; then
      return 0
    fi
    sleep 0.1
  done
  psql_e2e -c "${query}"
  return 1
}

cd "${project_root}"
cargo pgrx install --pg-config "${pg_config_path}"

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\\n"
  printf "wal_level = logical\\n"
  printf "max_replication_slots = 4\\n"
  printf "unix_socket_directories = '%s'\\n" "${pg_socket_dir}"
  printf "port = %s\\n" "${pg_port}"
} >> "${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -l "${pg_log_file}" -o "-k ${pg_socket_dir} -p ${pg_port}" -w start
"${pg_bin_dir}/createdb" -h "${pg_socket_dir}" -p "${pg_port}" shiba_e2e

psql_e2e -qc "CREATE EXTENSION shiba"
psql_e2e -qc "SELECT shiba.activate()"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
psql_e2e -qc "CREATE TABLE orders (product_id integer NOT NULL, amount integer NOT NULL)"
psql_e2e -qc "INSERT INTO orders VALUES (1, 10), (1, 20), (2, 5)"
psql_e2e -qc "CREATE TABLE shiba.order_stats AS SELECT product_id, count(*) AS order_count, sum(amount) AS total_amount FROM orders GROUP BY product_id"
# One DAG is scheduled by the single database-wide Runtime.
wait_for_value "1" "SELECT count(*) FROM shiba_internal.dag_runtime_state WHERE active"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid = 'shiba.order_stats'::regclass")" = "scan,aggregate,project,sink"
test "$(psql_e2e -Atqc "SELECT count(*) || ':' || count(*) FILTER (WHERE stateful) FROM shiba_internal.operator_instances WHERE result_oid = 'shiba.order_stats'::regclass")" = "4:1"
test "$(psql_e2e -Atqc "SELECT row_count || ':' || sum_value FROM shiba_internal.aggregate_state WHERE result_oid = 'shiba.order_stats'::regclass AND group_key = '1'::jsonb")" = "2:30"
test "$(psql_e2e -Atqc "SELECT (analyzed_query->>'has_aggregates') || ':' || jsonb_array_length(analyzed_query->'sources') || ':' || jsonb_array_length(analyzed_query->'targets') FROM shiba_internal.stream_graphs WHERE result_oid = 'shiba.order_stats'::regclass")" = "true:1:3"
test "$(psql_e2e -Atqc "SELECT (analyzed_query->'targets'->0->>'expression') || ':' || (analyzed_query->'targets'->1->>'aggregate') || ':' || (analyzed_query->'targets'->2->>'aggregate') || ':' || (analyzed_query->'targets'->2->>'input_column') FROM shiba_internal.stream_graphs WHERE result_oid = 'shiba.order_stats'::regclass")" = "column:count:sum:2"
# The internal JSON ABI accepts integer text as well as JSON numbers. A
# string "-1" must select the ordered-prefix path, never the insertion-only
# fast path. Sixty-four events force batch specialization.
if psql_e2e -qc "
  BEGIN;
  WITH event_rows AS (
    SELECT 1 AS sequence,
           jsonb_build_object('product_id',999,'amount',1) AS row_data,
           to_jsonb('-1'::text) AS delta
    UNION ALL
    SELECT 2,jsonb_build_object('product_id',999,'amount',1),to_jsonb(1)
    UNION ALL
    SELECT 2+n*2,
           jsonb_build_object('product_id',1000+n,'amount',1),to_jsonb(1)
    FROM generate_series(1,31) n
    UNION ALL
    SELECT 3+n*2,
           jsonb_build_object('product_id',1000+n,'amount',1),to_jsonb(-1)
    FROM generate_series(1,31) n
  ),
  insert_header AS (
    INSERT INTO shiba_internal.routed_transactions(commit_lsn)
    VALUES ('0/100001') RETURNING commit_lsn
  ),
  insert_events AS (
    INSERT INTO shiba_internal.change_log(
      commit_lsn,sequence,source_oid,delta,row_data
    )
    SELECT insert_header.commit_lsn,event_rows.sequence,
           'orders'::regclass,(event_rows.delta #>> '{}')::integer,
           event_rows.row_data
    FROM event_rows CROSS JOIN insert_header
  )
  INSERT INTO shiba_internal.dag_inbox(result_oid,commit_lsn)
  VALUES ('shiba.order_stats'::regclass,'0/100001');
  SELECT shiba._apply_dag_commit(
    'shiba.order_stats'::regclass,
    shiba._logical_execution_descriptor('shiba.order_stats'::regclass),
    '0/100001'
  );
  COMMIT
" >/dev/null 2>&1; then
  printf 'aggregate batch accepted a string retraction before insertion\n' >&2
  exit 1
fi
test "$(psql_e2e -Atqc "SELECT row_count || ':' || sum_value FROM shiba_internal.aggregate_state WHERE result_oid = 'shiba.order_stats'::regclass AND group_key = '1'::jsonb")" = "2:30"
psql_e2e -qc "CREATE MATERIALIZED VIEW public.native_snapshot AS SELECT count(*) AS order_count FROM orders"
if psql_e2e -qc "CREATE TABLE shiba.unsupported AS SELECT product_id, count(*) AS order_count, sum(amount) AS total_amount, row_number() OVER (ORDER BY product_id) AS position FROM orders GROUP BY product_id" >/dev/null 2>&1; then
  printf 'unsupported Shiba query unexpectedly succeeded\n' >&2
  exit 1
fi
if psql_e2e -qc "CREATE TABLE shiba.filtered_aggregate AS SELECT product_id,count(*) FILTER (WHERE amount>0) AS order_count,sum(amount) AS total_amount FROM orders GROUP BY product_id" >/dev/null 2>&1; then
  printf 'an aggregate FILTER clause unexpectedly succeeded\n' >&2
  exit 1
fi
if psql_e2e -qc "CREATE TABLE shiba.mismatched_sum_having AS SELECT product_id,count(*) AS order_count,sum(amount) AS total_amount FROM orders GROUP BY product_id HAVING sum(product_id)>0" >/dev/null 2>&1; then
  printf 'a mismatched HAVING SUM input unexpectedly succeeded\n' >&2
  exit 1
fi
if psql_e2e -qc "CREATE TABLE shiba.self_join_stats AS SELECT l.product_id,count(*) AS order_count,sum(l.amount) AS total_amount FROM orders l JOIN orders r ON l.product_id=r.product_id GROUP BY l.product_id" >/dev/null 2>&1; then
  printf 'a self-join without input-port identity unexpectedly succeeded\n' >&2
  exit 1
fi

# HAVING is a visibility operator over durable aggregate state. Hidden groups
# retain their accumulator and can cross the boundary in either direction.
psql_e2e -qc "CREATE TABLE shiba.popular_orders AS SELECT product_id, count(*) AS order_count, sum(amount) AS total_amount FROM orders GROUP BY product_id HAVING count(*) >= 3"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid = 'shiba.popular_orders'::regclass")" = "scan,aggregate,having,project,sink"
test "$(psql_e2e -Atqc "SELECT count(*) FROM shiba.popular_orders")" = "0"
test "$(psql_e2e -Atqc "SELECT row_count FROM shiba_internal.aggregate_state WHERE result_oid = 'shiba.popular_orders'::regclass AND group_key = '1'::jsonb")" = "2"
psql_e2e -qc "INSERT INTO orders VALUES (8, 1), (8, 2), (8, 3)"
wait_for_value "1" "SELECT count(*) FROM shiba.popular_orders WHERE product_id = 8 AND order_count = 3 AND total_amount = 6"
psql_e2e -qc "DELETE FROM orders WHERE product_id = 8 AND amount = 1"
wait_for_value "0" "SELECT count(*) FROM shiba.popular_orders WHERE product_id = 8"
wait_for_value "1" "SELECT count(*) FROM shiba_internal.aggregate_state WHERE result_oid = 'shiba.popular_orders'::regclass AND group_key = '8'::jsonb AND row_count = 2 AND sum_value = 5"
psql_e2e -qc "DELETE FROM orders WHERE product_id = 8"
psql_e2e -qc "DROP TABLE shiba.popular_orders"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

# COUNT(DISTINCT column) owns a per-group multiplicity arrangement. Duplicate
# values and NULLs must not change the public count, while SUM still receives
# every row delta.
psql_e2e -qc "CREATE TABLE distinct_orders (row_id integer NOT NULL, product_id integer NOT NULL, customer_id integer, amount integer NOT NULL)"
psql_e2e -qc "INSERT INTO distinct_orders VALUES (1,1,10,5),(2,1,10,7),(3,1,20,3),(4,1,NULL,2)"
psql_e2e -qc "CREATE TABLE shiba.distinct_customers AS SELECT product_id, count(DISTINCT customer_id) AS customer_count, sum(amount) AS total_amount FROM distinct_orders GROUP BY product_id"
wait_for_value "1" "SELECT count(*) FROM shiba.distinct_customers WHERE product_id=1 AND customer_count=2 AND total_amount=17"
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid='shiba.distinct_customers'::regclass")" = "scan,distinct,aggregate,project,sink"
test "$(psql_e2e -Atqc "SELECT count(*) || ':' || sum(multiplicity) FROM shiba_internal.distinct_state WHERE result_oid='shiba.distinct_customers'::regclass")" = "2:3"
test "$(psql_e2e -Atqc "SELECT count(*) || ':' || count(*) FILTER (WHERE stateful) FROM shiba_internal.operator_instances WHERE result_oid='shiba.distinct_customers'::regclass")" = "5:2"
psql_e2e -qc "INSERT INTO distinct_orders VALUES (5,1,10,4)"
wait_for_value "1" "SELECT count(*) FROM shiba.distinct_customers WHERE product_id=1 AND customer_count=2 AND total_amount=21"
wait_for_value "3" "SELECT multiplicity FROM shiba_internal.distinct_state WHERE result_oid='shiba.distinct_customers'::regclass AND group_key='1'::jsonb AND value_key='10'::jsonb"
psql_e2e -qc "DELETE FROM distinct_orders WHERE row_id=1"
wait_for_value "1" "SELECT count(*) FROM shiba.distinct_customers WHERE product_id=1 AND customer_count=2 AND total_amount=16"
psql_e2e -qc "DELETE FROM distinct_orders WHERE row_id IN (2,5)"
wait_for_value "1" "SELECT count(*) FROM shiba.distinct_customers WHERE product_id=1 AND customer_count=1 AND total_amount=5"
psql_e2e -qc "UPDATE distinct_orders SET customer_id=30 WHERE row_id IN (3,4)"
wait_for_value "1" "SELECT count(*) FROM shiba.distinct_customers WHERE product_id=1 AND customer_count=1 AND total_amount=5"
wait_for_value "2" "SELECT multiplicity FROM shiba_internal.distinct_state WHERE result_oid='shiba.distinct_customers'::regclass AND group_key='1'::jsonb AND value_key='30'::jsonb"
psql_e2e -qc "DELETE FROM distinct_orders"
wait_for_value "0" "SELECT count(*) FROM shiba.distinct_customers"
wait_for_value "0" "SELECT count(*) FROM shiba_internal.distinct_state WHERE result_oid='shiba.distinct_customers'::regclass"
psql_e2e -qc "DROP TABLE shiba.distinct_customers"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

# HAVING COUNT(DISTINCT ...) reads the same durable distinct arrangement as
# the projected aggregate. Groups remain accumulated while hidden and can
# cross the visibility threshold in either direction.
psql_e2e -qc "TRUNCATE distinct_orders"
psql_e2e -qc "INSERT INTO distinct_orders VALUES (11,2,100,4),(12,2,100,5)"
psql_e2e -qc "CREATE TABLE shiba.multi_customer_orders AS SELECT product_id, count(DISTINCT customer_id) AS customer_count, sum(amount) AS total_amount FROM distinct_orders GROUP BY product_id HAVING count(DISTINCT customer_id) >= 2"
wait_for_value "0" "SELECT count(*) FROM shiba.multi_customer_orders"
psql_e2e -qc "INSERT INTO distinct_orders VALUES (13,2,200,6)"
wait_for_value "1" "SELECT count(*) FROM shiba.multi_customer_orders WHERE product_id=2 AND customer_count=2 AND total_amount=15"
psql_e2e -qc "DELETE FROM distinct_orders WHERE row_id=13"
wait_for_value "0" "SELECT count(*) FROM shiba.multi_customer_orders"
wait_for_value "1" "SELECT count(*) FROM shiba_internal.aggregate_state WHERE result_oid='shiba.multi_customer_orders'::regclass AND group_key='2'::jsonb AND count_value=1 AND row_count=2"
if psql_e2e -qc "CREATE TABLE shiba.mismatched_distinct_having AS SELECT product_id, count(DISTINCT customer_id) AS customer_count, sum(amount) AS total_amount FROM distinct_orders GROUP BY product_id HAVING count(DISTINCT amount) >= 2" >/dev/null 2>&1; then
  printf 'mismatched HAVING COUNT(DISTINCT) unexpectedly succeeded\n' >&2
  exit 1
fi
psql_e2e -qc "DROP TABLE shiba.multi_customer_orders"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

# PostgreSQL SubLink nodes are decorrelated to thresholded arrangements:
# EXISTS/IN are semi joins and NOT EXISTS is an anti join.
psql_e2e -qc "CREATE TABLE sub_orders (row_id integer NOT NULL, product_id integer, amount integer NOT NULL)"
psql_e2e -qc "CREATE TABLE allowed_products (permit_id integer NOT NULL, product_id integer)"
psql_e2e -qc "INSERT INTO sub_orders VALUES (1,1,5),(2,1,7),(3,2,3),(4,NULL,4)"
psql_e2e -qc "INSERT INTO allowed_products VALUES (10,1),(11,1)"
psql_e2e -qc "CREATE TABLE shiba.allowed_order_stats AS SELECT o.product_id, count(*) AS order_count, sum(o.amount) AS total_amount FROM sub_orders o WHERE EXISTS (SELECT 1 FROM allowed_products a WHERE a.product_id=o.product_id) GROUP BY o.product_id"
wait_for_value "1" "SELECT count(*) FROM shiba.allowed_order_stats WHERE product_id=1 AND order_count=2 AND total_amount=12"
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid='shiba.allowed_order_stats'::regclass")" = "scan,scan,semi_join,aggregate,project,sink"
# Registration persists the versioned physical plan and creates only the
# cross-statement Join output Stage. Join input deltas remain statement-local
# MATERIALIZED CTEs and therefore must not allocate unused relations.
test "$(psql_e2e -Atqc "SELECT count(*) FROM shiba_internal.physical_plans WHERE result_oid='shiba.allowed_order_stats'::regclass")" = "1"
test "$(psql_e2e -Atqc "SELECT count(*) || ':' || min(stage_name) || ':' || min(storage) FROM shiba_internal.physical_stages WHERE result_oid='shiba.allowed_order_stats'::regclass")" = "1:join_delta:unlogged"
test "$(psql_e2e -Atqc "SELECT count(*) FROM shiba_internal.physical_stages stage JOIN pg_class relation ON relation.oid=stage.relation_oid JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace WHERE stage.result_oid='shiba.allowed_order_stats'::regclass AND relation.relpersistence='u' AND namespace.nspname='shiba_internal'")" = "1"
test "$(psql_e2e -Atqc "SELECT jsonb_array_length(shiba.explain_physical('shiba.allowed_order_stats'::regclass)->'stages')")" = "1"
allowed_stage_oid="$(psql_e2e -Atqc "SELECT relation_oid FROM shiba_internal.physical_stages WHERE result_oid='shiba.allowed_order_stats'::regclass AND stage_name='join_delta'")"
allowed_stage_relation="$(psql_e2e -Atqc "SELECT format('%I.%I',namespace.nspname,relation.relname) FROM shiba_internal.physical_stages stage JOIN pg_class relation ON relation.oid=stage.relation_oid JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace WHERE stage.result_oid='shiba.allowed_order_stats'::regclass AND stage.stage_name='join_delta'")"
test "$(psql_e2e -Atqc "
  BEGIN;
  ALTER TABLE ${allowed_stage_relation} ADD COLUMN invalid_shape integer;
  SELECT outcome
  FROM shiba_internal._load_dag_runtime_safely(
    'shiba.allowed_order_stats'::regclass
  );
  ROLLBACK
")" = "quarantined"
test "$(psql_e2e -Atqc "SELECT count(*) FROM pg_attribute WHERE attrelid=${allowed_stage_oid}::oid AND attname='invalid_shape' AND NOT attisdropped")" = "0"
test "$(psql_e2e -Atqc "SELECT active FROM shiba_internal.dag_runtime_state WHERE result_oid='shiba.allowed_order_stats'::regclass")" = "t"
# A long-lived Runtime explicitly releases both prepared Join statements when
# a cached DAG generation disappears. Exercise the session-local helper
# directly because pg_prepared_statements cannot observe another backend.
test "$(psql_e2e -Atqc "
  PREPARE shiba_join_stage_r42_p7 AS SELECT 1;
  PREPARE shiba_join_consume_r42_p7 AS SELECT 1;
  SELECT shiba_internal._deallocate_join_physical_plans(42::oid,7)
")" = "2"
# The compatibility begin/finish entry points are intentionally stateless.
# Exercise two cycles and verify that they do not create a catalog-backed
# scratch relation in the caller session.
psql_e2e -qc "
  BEGIN;
  SELECT shiba._begin_join_batch('shiba.allowed_order_stats'::regclass);
  SELECT shiba._finish_join_batch('shiba.allowed_order_stats'::regclass);
  COMMIT;
  BEGIN;
  SELECT shiba._begin_join_batch('shiba.allowed_order_stats'::regclass);
  SELECT shiba._finish_join_batch('shiba.allowed_order_stats'::regclass);
  DO \$block\$
  BEGIN
    IF EXISTS (
      SELECT 1
      FROM pg_class relation
      WHERE relation.relpersistence='t'
        AND relation.relname='shiba_join_batch_groups'
    ) THEN
      RAISE EXCEPTION 'join execution created a temp scratch relation';
    END IF;
  END
  \$block\$;
  COMMIT;
"
# Compare the public ordered single-delta compatibility path with one
# commit-level batch on identical starting state. Both executions run in
# rolled-back subtransactions, so the registered semi join remains unchanged.
psql_e2e -qc "
  DO \$oracle\$
  DECLARE
    reference_state jsonb;
    batch_state jsonb;
    states_differ boolean := false;
  BEGIN
    BEGIN
      INSERT INTO shiba_internal.routed_transactions(commit_lsn)
      VALUES ('0/200001'),('0/200002');
      INSERT INTO shiba_internal.change_log
        (commit_lsn,sequence,source_oid,delta,row_data)
      VALUES
        ('0/200001',1,'allowed_products'::regclass,1,
         jsonb_build_object('permit_id',901,'product_id',2)),
        ('0/200002',1,'allowed_products'::regclass,1,
         jsonb_build_object('permit_id',902,'product_id',2));
      INSERT INTO shiba_internal.dag_inbox(result_oid,commit_lsn)
      VALUES
        ('shiba.allowed_order_stats'::regclass,'0/200001'),
        ('shiba.allowed_order_stats'::regclass,'0/200002');
      PERFORM shiba._apply_dag_commit(
        'shiba.allowed_order_stats'::regclass,
        shiba._logical_execution_descriptor(
          'shiba.allowed_order_stats'::regclass
        ),
        '0/200001'
      );
      DELETE FROM shiba_internal.dag_inbox
      WHERE result_oid='shiba.allowed_order_stats'::regclass
        AND commit_lsn='0/200001';
      PERFORM shiba._apply_dag_commit(
        'shiba.allowed_order_stats'::regclass,
        shiba._logical_execution_descriptor(
          'shiba.allowed_order_stats'::regclass
        ),
        '0/200002'
      );
      DELETE FROM shiba_internal.dag_inbox
      WHERE result_oid='shiba.allowed_order_stats'::regclass
        AND commit_lsn='0/200002';
      SELECT jsonb_build_object(
        'sink',(SELECT jsonb_agg(to_jsonb(s) ORDER BY product_id::text)
                FROM shiba.allowed_order_stats s),
        'aggregate',(SELECT jsonb_agg(to_jsonb(a) ORDER BY group_key::text)
                     FROM shiba_internal.aggregate_state a
                     WHERE result_oid='shiba.allowed_order_stats'::regclass),
        'arrangements',(SELECT jsonb_agg(to_jsonb(a)
                           ORDER BY input_side,join_key,row_data::text)
                        FROM shiba_internal.join_arrangements a
                        WHERE result_oid='shiba.allowed_order_stats'::regclass)
      ) INTO reference_state;
      RAISE EXCEPTION 'rollback ordered oracle';
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM<>'rollback ordered oracle' THEN RAISE; END IF;
    END;

    BEGIN
      INSERT INTO shiba_internal.routed_transactions(commit_lsn)
      VALUES ('0/200003');
      INSERT INTO shiba_internal.change_log
        (commit_lsn,sequence,source_oid,delta,row_data)
      VALUES
        ('0/200003',1,'allowed_products'::regclass,1,
         jsonb_build_object('permit_id',901,'product_id',2)),
        ('0/200003',2,'allowed_products'::regclass,1,
         jsonb_build_object('permit_id',902,'product_id',2));
      INSERT INTO shiba_internal.dag_inbox(result_oid,commit_lsn)
      VALUES ('shiba.allowed_order_stats'::regclass,'0/200003');
      PERFORM shiba._apply_dag_commit(
        'shiba.allowed_order_stats'::regclass,
        shiba._logical_execution_descriptor(
          'shiba.allowed_order_stats'::regclass
        ),
        '0/200003'
      );
      DELETE FROM shiba_internal.dag_inbox
      WHERE result_oid='shiba.allowed_order_stats'::regclass
        AND commit_lsn='0/200003';
      SELECT jsonb_build_object(
        'sink',(SELECT jsonb_agg(to_jsonb(s) ORDER BY product_id::text)
                FROM shiba.allowed_order_stats s),
        'aggregate',(SELECT jsonb_agg(to_jsonb(a) ORDER BY group_key::text)
                     FROM shiba_internal.aggregate_state a
                     WHERE result_oid='shiba.allowed_order_stats'::regclass),
        'arrangements',(SELECT jsonb_agg(to_jsonb(a)
                           ORDER BY input_side,join_key,row_data::text)
                        FROM shiba_internal.join_arrangements a
                        WHERE result_oid='shiba.allowed_order_stats'::regclass)
      ) INTO batch_state;
      states_differ := batch_state IS DISTINCT FROM reference_state;
      RAISE EXCEPTION 'rollback batch oracle';
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM<>'rollback batch oracle' THEN RAISE; END IF;
    END;
    IF states_differ THEN
      RAISE EXCEPTION 'join batch state differs from ordered reference';
    END IF;
  END
  \$oracle\$;
"
test "$(psql_e2e -Atqc "SELECT count(*) FROM ${allowed_stage_relation}")" = "0"
psql_e2e -qc "DELETE FROM allowed_products WHERE permit_id=10"
wait_for_value "0" "SELECT count(*) FROM shiba_internal.join_arrangements WHERE result_oid='shiba.allowed_order_stats'::regclass AND input_side='right' AND row_data->>'permit_id'='10'"
allowed_stage_relfilenode="$(psql_e2e -Atqc "SELECT relfilenode FROM pg_class WHERE oid=${allowed_stage_oid}::oid")"
psql_e2e -qc "DELETE FROM allowed_products WHERE permit_id=11"
wait_for_value "0" "SELECT count(*) FROM shiba.allowed_order_stats"
psql_e2e -qc "INSERT INTO allowed_products VALUES (12,2)"
wait_for_value "1" "SELECT count(*) FROM shiba.allowed_order_stats WHERE product_id=2 AND order_count=1 AND total_amount=3"
psql_e2e -qc "INSERT INTO sub_orders VALUES (5,2,8)"
wait_for_value "1" "SELECT count(*) FROM shiba.allowed_order_stats WHERE product_id=2 AND order_count=2 AND total_amount=11"
test "$(psql_e2e -Atqc "SELECT count(*) FROM ${allowed_stage_relation}")" = "0"
# A cached DagRuntime is keyed by physical plan_id. Repeated commits must not
# reload the same generation and therefore must not re-TRUNCATE the Stage.
test "$(psql_e2e -Atqc "SELECT relfilenode FROM pg_class WHERE oid=${allowed_stage_oid}::oid")" = "${allowed_stage_relfilenode}"
# DELETE RETURNING leaves reusable dead space. Verify threshold compaction
# truncates only the already-empty Stage; production uses a 64 MiB threshold.
test "$(psql_e2e -Atqc "SELECT shiba_internal._compact_physical_stages('shiba.allowed_order_stats'::regclass,1)")" = "1"
test "$(psql_e2e -Atqc "SELECT count(*) FROM ${allowed_stage_relation}")" = "0"
test "$(psql_e2e -Atqc "SELECT relfilenode FROM pg_class WHERE oid=${allowed_stage_oid}::oid")" != "${allowed_stage_relfilenode}"

psql_e2e -qc "CREATE TABLE shiba.blocked_order_stats AS SELECT o.product_id, count(*) AS order_count, sum(o.amount) AS total_amount FROM sub_orders o WHERE NOT EXISTS (SELECT 1 FROM allowed_products a WHERE a.product_id=o.product_id) GROUP BY o.product_id"
wait_for_value "1" "SELECT count(*) FROM shiba.blocked_order_stats WHERE product_id=1 AND order_count=2 AND total_amount=12"
wait_for_value "1" "SELECT count(*) FROM shiba.blocked_order_stats WHERE product_id IS NULL AND order_count=1 AND total_amount=4"
test "$(psql_e2e -Atqc "SELECT join_type FROM shiba_internal.inner_join_views WHERE result_oid='shiba.blocked_order_stats'::regclass")" = "anti"
psql_e2e -qc "INSERT INTO allowed_products VALUES (13,1)"
wait_for_value "0" "SELECT count(*) FROM shiba.blocked_order_stats WHERE product_id=1"
psql_e2e -qc "DELETE FROM allowed_products WHERE permit_id=12"
wait_for_value "1" "SELECT count(*) FROM shiba.blocked_order_stats WHERE product_id=2 AND order_count=2 AND total_amount=11"

psql_e2e -qc "CREATE TABLE shiba.in_order_stats AS SELECT o.product_id, count(*) AS order_count, sum(o.amount) AS total_amount FROM sub_orders o WHERE o.product_id IN (SELECT a.product_id FROM allowed_products a) GROUP BY o.product_id"
wait_for_value "1" "SELECT count(*) FROM shiba.in_order_stats WHERE product_id=1 AND order_count=2 AND total_amount=12"
wait_for_value "0" "SELECT count(*) FROM shiba.in_order_stats WHERE product_id IS NULL"
psql_e2e -qc "INSERT INTO allowed_products VALUES (14,NULL)"
psql_e2e -qc "CREATE TABLE shiba.not_in_order_stats AS SELECT o.product_id, count(*) AS order_count, sum(o.amount) AS total_amount FROM sub_orders o WHERE o.product_id NOT IN (SELECT a.product_id FROM allowed_products a) GROUP BY o.product_id"
wait_for_value "0" "SELECT count(*) FROM shiba.not_in_order_stats"
test "$(psql_e2e -Atqc "SELECT join_type FROM shiba_internal.inner_join_views WHERE result_oid='shiba.not_in_order_stats'::regclass")" = "null_anti"
psql_e2e -qc "DELETE FROM allowed_products WHERE permit_id=14"
wait_for_value "1" "SELECT count(*) FROM shiba.not_in_order_stats WHERE product_id=2 AND order_count=2 AND total_amount=11"
wait_for_value "0" "SELECT count(*) FROM shiba.not_in_order_stats WHERE product_id IS NULL"
psql_e2e -qc "DELETE FROM allowed_products WHERE permit_id=13"
wait_for_value "1" "SELECT count(*) FROM shiba.not_in_order_stats WHERE product_id=1 AND order_count=2 AND total_amount=12"
wait_for_value "1" "SELECT count(*) FROM shiba.not_in_order_stats WHERE product_id IS NULL AND order_count=1 AND total_amount=4"
if psql_e2e -qc "CREATE TABLE shiba.filtered_in_stats AS SELECT o.product_id,count(*) AS order_count,sum(o.amount) AS total_amount FROM sub_orders o WHERE o.product_id IN (SELECT a.product_id FROM allowed_products a WHERE a.permit_id>0) GROUP BY o.product_id" >/dev/null 2>&1; then
  printf 'an IN subquery predicate that is not in the DAG unexpectedly succeeded\n' >&2
  exit 1
fi

# SQL NULL and the text empty string are distinct JOIN keys. Empty strings
# match each other; NULL never does.
psql_e2e -qc "CREATE TABLE empty_key_facts (row_id integer NOT NULL, join_key name, amount integer NOT NULL)"
psql_e2e -qc "CREATE TABLE empty_key_dims (row_id integer NOT NULL, join_key name, group_id integer NOT NULL)"
psql_e2e -qc "INSERT INTO empty_key_facts VALUES (1,'',5),(2,NULL,7)"
psql_e2e -qc "INSERT INTO empty_key_dims VALUES (1,'',9),(2,NULL,10)"
psql_e2e -qc "CREATE TABLE shiba.empty_key_stats AS SELECT d.group_id,count(*) AS row_count,sum(f.amount) AS total_amount FROM empty_key_facts f JOIN empty_key_dims d ON f.join_key=d.join_key GROUP BY d.group_id"
wait_for_value "1" "SELECT count(*) FROM shiba.empty_key_stats WHERE group_id=9 AND row_count=1 AND total_amount=5"
wait_for_value "0" "SELECT count(*) FROM shiba.empty_key_stats WHERE group_id=10"
psql_e2e -qc "INSERT INTO empty_key_facts VALUES (3,'',11)"
wait_for_value "1" "SELECT count(*) FROM shiba.empty_key_stats WHERE group_id=9 AND row_count=2 AND total_amount=16"
psql_e2e -qc "DELETE FROM empty_key_dims WHERE row_id=1"
wait_for_value "0" "SELECT count(*) FROM shiba.empty_key_stats"
psql_e2e -qc "DROP TABLE shiba.empty_key_stats"

psql_e2e -qc "DROP TABLE shiba.not_in_order_stats"
psql_e2e -qc "DROP TABLE shiba.in_order_stats"
psql_e2e -qc "DROP TABLE shiba.blocked_order_stats"
psql_e2e -qc "DROP TABLE shiba.allowed_order_stats"
test "$(psql_e2e -Atqc "SELECT count(*) FROM pg_class WHERE oid=${allowed_stage_oid}::oid")" = "0"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

# Window state is an ordered multiset per partition. A delta rebuilds only its
# old/new partition from durable Shiba state, including peer-aware ranks and
# default-frame running aggregates.
psql_e2e -qc "CREATE TABLE window_events (event_id integer NOT NULL, category_id integer, score integer NOT NULL)"
psql_e2e -qc "INSERT INTO window_events VALUES (1,1,10),(2,1,20),(3,1,20)"
psql_e2e -qc "CREATE TABLE shiba.event_windows AS SELECT event_id,category_id,score,row_number() OVER w AS position,rank() OVER w AS ranking,dense_rank() OVER w AS dense_ranking,count(*) OVER w AS running_count,sum(score) OVER w AS running_sum,avg(score) OVER w AS running_avg,min(score) OVER w AS running_min,max(score) OVER w AS running_max FROM window_events WINDOW w AS (PARTITION BY category_id ORDER BY score)"
wait_for_value "1" "SELECT count(*) FROM shiba.event_windows WHERE event_id=1 AND position=1 AND ranking=1 AND dense_ranking=1 AND running_count=1 AND running_sum=10 AND running_avg=10 AND running_min=10 AND running_max=10"
wait_for_value "2" "SELECT count(*) FROM shiba.event_windows WHERE score=20 AND ranking=2 AND dense_ranking=2 AND running_count=3 AND running_sum=50"
if psql_e2e -qc "
  BEGIN;
  INSERT INTO shiba_internal.routed_transactions(commit_lsn)
  VALUES ('0/100002');
  INSERT INTO shiba_internal.change_log
    (commit_lsn,sequence,source_oid,delta,row_data)
  VALUES
    ('0/100002',1,'window_events'::regclass,-1,
     jsonb_build_object('event_id',999,'category_id',9,'score',999)),
    ('0/100002',2,'window_events'::regclass,1,
     jsonb_build_object('event_id',999,'category_id',9,'score',999));
  INSERT INTO shiba_internal.dag_inbox(result_oid,commit_lsn)
  VALUES ('shiba.event_windows'::regclass,'0/100002');
  SELECT shiba._apply_dag_commit(
    'shiba.event_windows'::regclass,
    shiba._logical_execution_descriptor('shiba.event_windows'::regclass),
    '0/100002'
  );
  COMMIT
" >/dev/null 2>&1; then
  printf 'window batch accepted a retraction-before-insertion from zero state\n' >&2
  exit 1
fi
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid='shiba.event_windows'::regclass")" = "scan,window,project,sink"
test "$(psql_e2e -Atqc "SELECT count(*) || ':' || count(*) FILTER (WHERE stateful) FROM shiba_internal.operator_instances WHERE result_oid='shiba.event_windows'::regclass")" = "4:1"
psql_e2e -qc "INSERT INTO window_events VALUES (4,1,15)"
wait_for_value "1" "SELECT count(*) FROM shiba.event_windows WHERE event_id=4 AND position=2 AND ranking=2 AND dense_ranking=2 AND running_count=2 AND running_sum=25"
wait_for_value "2" "SELECT count(*) FROM shiba.event_windows WHERE score=20 AND ranking=3 AND dense_ranking=3 AND running_count=4 AND running_sum=65"
psql_e2e -qc "UPDATE window_events SET category_id=2,score=5 WHERE event_id=1"
wait_for_value "1" "SELECT count(*) FROM shiba.event_windows WHERE event_id=1 AND category_id=2 AND position=1 AND running_sum=5"
wait_for_value "1" "SELECT count(*) FROM shiba.event_windows WHERE event_id=4 AND category_id=1 AND position=1 AND running_sum=15"
psql_e2e -qc "DELETE FROM window_events WHERE event_id=4"
wait_for_value "2" "SELECT count(*) FROM shiba.event_windows WHERE category_id=1 AND score=20 AND ranking=1 AND dense_ranking=1 AND running_count=2 AND running_sum=40"
psql_e2e -qc "DROP TABLE shiba.event_windows"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

psql_e2e -qc "CREATE TABLE frame_events (event_id integer NOT NULL, category_id integer NOT NULL, score integer NOT NULL)"
psql_e2e -qc "INSERT INTO frame_events VALUES (1,1,10),(2,1,20),(3,1,30)"
psql_e2e -qc "CREATE TABLE shiba.frame_windows AS SELECT event_id,category_id,score,sum(score) OVER (PARTITION BY category_id ORDER BY score ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS nearby_sum FROM frame_events"
wait_for_value "30,60,50" "SELECT string_agg(nearby_sum::text,',' ORDER BY score) FROM shiba.frame_windows"
psql_e2e -qc "INSERT INTO frame_events VALUES (4,1,25)"
wait_for_value "30,55,75,55" "SELECT string_agg(nearby_sum::text,',' ORDER BY score) FROM shiba.frame_windows"
psql_e2e -qc "DELETE FROM frame_events WHERE event_id=2"
wait_for_value "35,65,55" "SELECT string_agg(nearby_sum::text,',' ORDER BY score) FROM shiba.frame_windows"
# One source commit moves two rows into another partition. The old and new
# partitions must each be rebuilt once from their final commit state.
psql_e2e -qc "UPDATE frame_events SET category_id=2,score=score+100 WHERE event_id IN (1,4)"
wait_for_value "30" "SELECT nearby_sum FROM shiba.frame_windows WHERE category_id=1"
wait_for_value "235,235" "SELECT string_agg(nearby_sum::text,',' ORDER BY score) FROM shiba.frame_windows WHERE category_id=2"
psql_e2e -qc "DROP TABLE shiba.frame_windows"
psql_e2e -qc "CREATE TABLE filtered_window_events (event_id integer NOT NULL,category_id integer NOT NULL,score integer NOT NULL,active boolean NOT NULL)"
if psql_e2e -qc "CREATE TABLE shiba.filtered_windows AS SELECT event_id,category_id,score,sum(score) FILTER (WHERE active) OVER (PARTITION BY category_id ORDER BY score) AS running_sum FROM filtered_window_events" >/dev/null 2>&1; then
  printf 'a window FILTER clause unexpectedly succeeded\n' >&2
  exit 1
fi

# Top-level SELECT DISTINCT is a threshold operator over projected row keys.
psql_e2e -qc "CREATE TABLE distinct_rows (row_id integer NOT NULL, category_id integer, label integer)"
psql_e2e -qc "INSERT INTO distinct_rows VALUES (1,1,10),(2,1,10),(3,1,20),(4,NULL,30)"
psql_e2e -qc "CREATE TABLE shiba.unique_labels AS SELECT DISTINCT category_id,label FROM distinct_rows"
wait_for_value "3" "SELECT count(*) FROM shiba.unique_labels"
if psql_e2e -qc "
  BEGIN;
  INSERT INTO shiba_internal.routed_transactions(commit_lsn)
  VALUES ('0/100003');
  INSERT INTO shiba_internal.change_log
    (commit_lsn,sequence,source_oid,delta,row_data)
  VALUES
    ('0/100003',1,'distinct_rows'::regclass,-1,
     jsonb_build_object('row_id',999,'category_id',9,'label',999)),
    ('0/100003',2,'distinct_rows'::regclass,1,
     jsonb_build_object('row_id',999,'category_id',9,'label',999));
  INSERT INTO shiba_internal.dag_inbox(result_oid,commit_lsn)
  VALUES ('shiba.unique_labels'::regclass,'0/100003');
  SELECT shiba._apply_dag_commit(
    'shiba.unique_labels'::regclass,
    shiba._logical_execution_descriptor('shiba.unique_labels'::regclass),
    '0/100003'
  );
  COMMIT
" >/dev/null 2>&1; then
  printf 'DISTINCT batch accepted a retraction-before-insertion from zero state\n' >&2
  exit 1
fi
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid='shiba.unique_labels'::regclass")" = "scan,distinct,project,sink"
test "$(psql_e2e -Atqc "SELECT multiplicity FROM shiba_internal.projection_state WHERE result_oid='shiba.unique_labels'::regclass AND row_key=jsonb_build_object('category_id',1,'label',10)")" = "2"
psql_e2e -qc "INSERT INTO distinct_rows VALUES (5,1,10)"
wait_for_value "3" "SELECT count(*) FROM shiba.unique_labels"
wait_for_value "3" "SELECT multiplicity FROM shiba_internal.projection_state WHERE result_oid='shiba.unique_labels'::regclass AND row_key=jsonb_build_object('category_id',1,'label',10)"
psql_e2e -qc "DELETE FROM distinct_rows WHERE row_id IN (1,2)"
wait_for_value "1" "SELECT count(*) FROM shiba.unique_labels WHERE category_id=1 AND label=10"
psql_e2e -qc "DELETE FROM distinct_rows WHERE row_id=5"
wait_for_value "0" "SELECT count(*) FROM shiba.unique_labels WHERE category_id=1 AND label=10"
psql_e2e -qc "UPDATE distinct_rows SET category_id=2,label=40 WHERE row_id=3"
wait_for_value "1" "SELECT count(*) FROM shiba.unique_labels WHERE category_id=2 AND label=40"
wait_for_value "1" "SELECT count(*) FROM shiba.unique_labels WHERE category_id IS NULL AND label=30"
# Multiple rows in one commit collide on existing projected keys, then one row
# migrates between keys. Net multiplicity and zero-boundary sink changes must
# be computed per projected key.
psql_e2e -qc "INSERT INTO distinct_rows VALUES (6,2,40),(7,NULL,30),(8,NULL,99)"
wait_for_value "2" "SELECT multiplicity FROM shiba_internal.projection_state WHERE result_oid='shiba.unique_labels'::regclass AND row_key=jsonb_build_object('category_id',2,'label',40)"
wait_for_value "2" "SELECT multiplicity FROM shiba_internal.projection_state WHERE result_oid='shiba.unique_labels'::regclass AND row_key=jsonb_build_object('category_id',NULL,'label',30)"
psql_e2e -qc "UPDATE distinct_rows SET label=99 WHERE row_id=6"
wait_for_value "4" "SELECT count(*) FROM shiba.unique_labels"
wait_for_value "1" "SELECT multiplicity FROM shiba_internal.projection_state WHERE result_oid='shiba.unique_labels'::regclass AND row_key=jsonb_build_object('category_id',2,'label',99)"
psql_e2e -qc "DROP TABLE shiba.unique_labels"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

# Global TopN keeps the full ordered multiset in operator state and rewrites
# only the bounded sink after each committed delta.
psql_e2e -qc "CREATE TABLE scored_rows (row_id integer NOT NULL, score integer)"
psql_e2e -qc "INSERT INTO scored_rows VALUES (1,10),(2,20),(3,15),(4,NULL)"
psql_e2e -qc "CREATE TABLE shiba.top_scores AS SELECT row_id,score FROM scored_rows ORDER BY score DESC NULLS LAST LIMIT 3"
wait_for_value "2,3,1" "SELECT string_agg(row_id::text,',' ORDER BY score DESC NULLS LAST) FROM shiba.top_scores"
if psql_e2e -qc "
  BEGIN;
  INSERT INTO shiba_internal.routed_transactions(commit_lsn)
  VALUES ('0/100004');
  INSERT INTO shiba_internal.change_log
    (commit_lsn,sequence,source_oid,delta,row_data)
  VALUES
    ('0/100004',1,'scored_rows'::regclass,-1,
     jsonb_build_object('row_id',999,'score',999)),
    ('0/100004',2,'scored_rows'::regclass,1,
     jsonb_build_object('row_id',999,'score',999));
  INSERT INTO shiba_internal.dag_inbox(result_oid,commit_lsn)
  VALUES ('shiba.top_scores'::regclass,'0/100004');
  SELECT shiba._apply_dag_commit(
    'shiba.top_scores'::regclass,
    shiba._logical_execution_descriptor('shiba.top_scores'::regclass),
    '0/100004'
  );
  COMMIT
" >/dev/null 2>&1; then
  printf 'TopN batch accepted a retraction-before-insertion from zero state\n' >&2
  exit 1
fi
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid='shiba.top_scores'::regclass")" = "scan,top_n,project,sink"
psql_e2e -qc "INSERT INTO scored_rows VALUES (5,25)"
wait_for_value "5,2,3" "SELECT string_agg(row_id::text,',' ORDER BY score DESC NULLS LAST) FROM shiba.top_scores"
psql_e2e -qc "DELETE FROM scored_rows WHERE row_id=2"
wait_for_value "5,3,1" "SELECT string_agg(row_id::text,',' ORDER BY score DESC NULLS LAST) FROM shiba.top_scores"
psql_e2e -qc "UPDATE scored_rows SET score=30 WHERE row_id=1"
wait_for_value "1,5,3" "SELECT string_agg(row_id::text,',' ORDER BY score DESC NULLS LAST) FROM shiba.top_scores"
psql_e2e -qc "CREATE TABLE shiba.offset_scores AS SELECT row_id,score FROM scored_rows ORDER BY score DESC NULLS LAST OFFSET 1 LIMIT 2"
wait_for_value "5,3" "SELECT string_agg(row_id::text,',' ORDER BY score DESC NULLS LAST) FROM shiba.offset_scores"
# A single commit crosses both TopN boundaries and moves a row to NULL. Each
# DAG rewrites its bounded sink only after all net state changes are applied.
psql_e2e -qc "UPDATE scored_rows SET score=CASE row_id WHEN 3 THEN 40 WHEN 4 THEN 35 WHEN 5 THEN NULL END WHERE row_id IN (3,4,5)"
wait_for_value "3,4,1" "SELECT string_agg(row_id::text,',' ORDER BY score DESC NULLS LAST) FROM shiba.top_scores"
wait_for_value "4,1" "SELECT string_agg(row_id::text,',' ORDER BY score DESC NULLS LAST) FROM shiba.offset_scores"
psql_e2e -qc "DROP TABLE shiba.offset_scores"
psql_e2e -qc "DROP TABLE shiba.top_scores"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
if psql_e2e -qc "CREATE TABLE shiba.top_scores_with_ties AS SELECT row_id,score FROM scored_rows ORDER BY score DESC FETCH FIRST 2 ROWS WITH TIES" >/dev/null 2>&1; then
  printf 'FETCH WITH TIES unexpectedly succeeded\n' >&2
  exit 1
fi

# Initial backfill and pgoutput deltas pass through the same canonical typed
# row encoding. PostgreSQL's boolean text forms used to differ on these paths.
psql_e2e -qc "CREATE TABLE boolean_topn_rows (row_id integer NOT NULL,enabled boolean NOT NULL,score integer NOT NULL)"
psql_e2e -qc "INSERT INTO boolean_topn_rows VALUES (1,true,20),(2,false,10)"
psql_e2e -qc "CREATE TABLE shiba.boolean_topn AS SELECT row_id,enabled,score FROM boolean_topn_rows ORDER BY score DESC LIMIT 2"
psql_e2e -qc "DELETE FROM boolean_topn_rows WHERE row_id=1"
wait_for_value "2" "SELECT row_id FROM shiba.boolean_topn"
wait_for_value "1" "SELECT count(*) FROM shiba_internal.topn_rows WHERE result_oid='shiba.boolean_topn'::regclass"
psql_e2e -qc "DROP TABLE shiba.boolean_topn"

# Row identity also preserves the registration session's type-output GUCs.
# Runtime applies in a different backend but must encode timestamptz state with
# the captured TimeZone so DELETE/UPDATE can find the backfilled row.
psql_e2e -qc "CREATE TABLE timezone_topn_rows (row_id integer NOT NULL, occurred_at timestamptz NOT NULL)"
psql_e2e -qc "INSERT INTO timezone_topn_rows VALUES (1,'2025-01-01 00:00:00+00'),(2,'2025-01-02 00:00:00+00')"
psql_e2e -qc "SET TIME ZONE 'America/New_York'; CREATE TABLE shiba.timezone_topn AS SELECT row_id,occurred_at FROM timezone_topn_rows ORDER BY occurred_at LIMIT 2"
test "$(psql_e2e -Atqc "SELECT execution_settings->>'TimeZone' FROM shiba_internal.stream_views WHERE result_oid='shiba.timezone_topn'::regclass")" = "America/New_York"
psql_e2e -qc "DELETE FROM timezone_topn_rows WHERE row_id=1"
wait_for_value "2" "SELECT row_id FROM shiba.timezone_topn"
wait_for_value "1" "SELECT count(*) FROM shiba_internal.topn_rows WHERE result_oid='shiba.timezone_topn'::regclass"
test "$(psql_e2e -Atqc "SELECT active FROM shiba_internal.dag_runtime_state WHERE result_oid='shiba.timezone_topn'::regclass")" = "t"
psql_e2e -qc "DROP TABLE shiba.timezone_topn"

if psql_e2e -qc "CREATE TABLE shiba.not_a_stream (id integer)" >/dev/null 2>&1; then
  printf 'ordinary table creation in the shiba schema unexpectedly succeeded\n' >&2
  exit 1
fi

# Source writers do not need privileges on Shiba's internal process controls.
# The statement-level wakeup trigger executes with the extension owner's
# privileges while row data continues to flow exclusively through WAL.
psql_e2e -qc "CREATE ROLE shiba_writer"
psql_e2e -qc "GRANT SELECT,INSERT,UPDATE,DELETE ON orders TO shiba_writer"
psql_e2e -qc "SET SESSION AUTHORIZATION shiba_writer; INSERT INTO orders VALUES (77,5)"
wait_for_value "1" "SELECT count(*) FROM shiba.order_stats WHERE product_id=77 AND order_count=1 AND total_amount=5"
psql_e2e -qc "SET SESSION AUTHORIZATION shiba_writer; UPDATE orders SET amount=6 WHERE product_id=77"
wait_for_value "1" "SELECT count(*) FROM shiba.order_stats WHERE product_id=77 AND order_count=1 AND total_amount=6"
psql_e2e -qc "SET SESSION AUTHORIZATION shiba_writer; DELETE FROM orders WHERE product_id=77"
wait_for_value "0" "SELECT count(*) FROM shiba.order_stats WHERE product_id=77"
if psql_e2e -qc "SET SESSION AUTHORIZATION shiba_writer; SELECT shiba._ensure_runtime()" >/dev/null 2>&1; then
  printf 'source writer unexpectedly executed an internal Shiba Runtime function\n' >&2
  exit 1
fi

psql_e2e -qc "CREATE ROLE shiba_untrusted"
psql_e2e -qc "GRANT USAGE ON SCHEMA shiba TO shiba_untrusted"
if psql_e2e -qc "SET SESSION AUTHORIZATION shiba_untrusted; SET shiba.internal_apply = 'on'; UPDATE shiba.order_stats SET total_amount = 0" >/dev/null 2>&1; then
  printf 'untrusted direct writes to a Shiba result table unexpectedly succeeded\n' >&2
  exit 1
fi

psql_e2e -qc "INSERT INTO orders VALUES (1, 7)"
wait_for_value "1" "SELECT count(*) FROM shiba.order_stats WHERE product_id = 1 AND order_count = 3 AND total_amount = 37"
wait_for_value "1" "SELECT count(*) FROM shiba_internal.aggregate_state WHERE result_oid = 'shiba.order_stats'::regclass AND group_key = '1'::jsonb AND row_count = 3 AND sum_value = 37"
if psql_e2e -qc "TRUNCATE orders" >/dev/null 2>&1; then
  printf 'truncating a Shiba source unexpectedly succeeded\n' >&2
  exit 1
fi

# A view created while the Runtime is deliberately paused must not replay WAL
# which its CTAS snapshot already contains.
psql_e2e -qc "UPDATE shiba_internal.runtime_state SET active = false WHERE singleton"
wait_for_value "0" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
psql_e2e -qc "INSERT INTO orders VALUES (9, 90)"
psql_e2e -qc "CREATE TABLE shiba.paused_order_stats AS SELECT product_id, count(*) AS order_count, sum(amount) AS total_amount FROM orders GROUP BY product_id"
psql_e2e -qc "SELECT shiba.activate()"
wait_for_value "1" "SELECT count(*) FROM shiba.paused_order_stats WHERE product_id = 9 AND order_count = 1 AND total_amount = 90"
wait_for_value "1" "SELECT count(*) FROM shiba.order_stats WHERE product_id = 9 AND order_count = 1 AND total_amount = 90"
psql_e2e -qc "DROP TABLE shiba.paused_order_stats"

result="$(psql_e2e -Atq <<'SQL'
SELECT shiba.version();
SELECT relkind FROM pg_class WHERE oid = 'shiba.order_stats'::regclass;
SELECT string_agg(product_id || ':' || order_count || ':' || total_amount, ',' ORDER BY product_id)
FROM shiba.order_stats;
SELECT order_count FROM public.native_snapshot;
UPDATE orders SET amount = 9 WHERE product_id = 2;
DELETE FROM orders WHERE product_id = 1 AND amount = 10;
SQL
)"
wait_for_value "1" "SELECT count(*) FROM shiba.order_stats WHERE product_id = 1 AND order_count = 2 AND total_amount = 27"
result+=$'\n'"$(psql_e2e -Atq <<'SQL'
SELECT string_agg(product_id || ':' || order_count || ':' || total_amount, ',' ORDER BY product_id)
FROM shiba.order_stats;
BEGIN;
INSERT INTO orders VALUES (3, 12);
ROLLBACK;
SELECT string_agg(product_id || ':' || order_count || ':' || total_amount, ',' ORDER BY product_id)
FROM shiba.order_stats;
SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'shiba_capture_%' AND NOT tgisinternal;
SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'shiba_wakeup_%' AND NOT tgisinternal;
SELECT count(*) FROM shiba_internal.stream_views;
SELECT string_agg(operator, ',' ORDER BY node_id)
FROM shiba_internal.stream_graph_nodes
WHERE result_oid = 'shiba.order_stats'::regclass;
SELECT count(*) FROM shiba_internal.stream_graph_edges WHERE result_oid = 'shiba.order_stats'::regclass;
SELECT count(*) FROM shiba_internal.view_progress WHERE applied_lsn IS NOT NULL;
SELECT applied_lsn IS NOT NULL FROM shiba.progress('shiba.order_stats');
SELECT pending_wal_bytes >= 0 FROM shiba.progress('shiba.order_stats');
SELECT to_regprocedure('shiba._apply_delta()') IS NULL;
SELECT to_regclass('shiba.unsupported') IS NULL;
SELECT relkind FROM pg_class WHERE oid = 'public.native_snapshot'::regclass;
SQL
)"
test "${result}" = $'0.1.0\nr\n1:3:37,2:1:5,9:1:90\n3\n1:2:27,2:1:9,9:1:90\n1:2:27,2:1:9,9:1:90\n0\n1\n1\nAggregate,Seq Scan,Shiba Sink\n2\n1\nt\nt\nt\nt\nm'

psql_e2e -qc "CREATE TABLE unsupported_money_source (row_id integer NOT NULL,amount money NOT NULL)"
if psql_e2e -qc "CREATE TABLE shiba.unsupported_money AS SELECT row_id,amount FROM unsupported_money_source ORDER BY row_id LIMIT 1" >/dev/null 2>&1; then
  printf 'registering a locale-sensitive money identity unexpectedly succeeded\n' >&2
  exit 1
fi
test "$(psql_e2e -Atqc "SELECT to_regclass('shiba.unsupported_money') IS NULL")" = "t"
psql_e2e -qc "DROP TABLE unsupported_money_source"

if psql_e2e -qc "ALTER TABLE orders ADD COLUMN ignored integer" >/dev/null 2>&1; then
  printf 'altering a Shiba source unexpectedly succeeded\n' >&2
  exit 1
fi
if psql_e2e -qc "DROP TABLE orders" >/dev/null 2>&1; then
  printf 'dropping a live Shiba source unexpectedly succeeded\n' >&2
  exit 1
fi
psql_e2e -qc "CREATE SCHEMA cascade_source"
psql_e2e -qc "CREATE TABLE cascade_source.events (group_id integer NOT NULL,amount integer NOT NULL)"
psql_e2e -qc "CREATE TABLE shiba.cascade_guard AS SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount FROM cascade_source.events GROUP BY group_id"
if psql_e2e -qc "DROP SCHEMA cascade_source CASCADE" >/dev/null 2>&1; then
  printf 'indirectly dropping a live Shiba source unexpectedly succeeded\n' >&2
  exit 1
fi
test "$(psql_e2e -Atqc "SELECT to_regclass('cascade_source.events') IS NOT NULL")" = "t"
psql_e2e -qc "DROP TABLE shiba.cascade_guard"
psql_e2e -qc "DROP SCHEMA cascade_source CASCADE"

# Filter and Project are maintained incrementally. An UPDATE crossing the
# predicate boundary must become a retraction followed by an insertion.
psql_e2e -qc "CREATE TABLE shiba.large_order_stats AS SELECT product_id AS product, count(*) AS order_count, sum(amount) AS total_amount FROM orders WHERE amount >= 20 AND (product_id <> 8 OR amount >= 100) GROUP BY product_id"
wait_for_value "1" "SELECT count(*) FROM shiba.large_order_stats WHERE product = 1 AND order_count = 1 AND total_amount = 20"
wait_for_value "1" "SELECT count(*) FROM shiba.large_order_stats WHERE product = 9 AND order_count = 1 AND total_amount = 90"
# 100 >= 20 is false under lexical comparison, so this also proves that the
# predicate is evaluated with the source column's PostgreSQL type.
psql_e2e -qc "INSERT INTO orders VALUES (7, 100)"
wait_for_value "1" "SELECT count(*) FROM shiba.large_order_stats WHERE product = 7 AND order_count = 1 AND total_amount = 100"
psql_e2e -qc "INSERT INTO orders VALUES (6, 19)"
wait_for_value "0" "SELECT count(*) FROM shiba.large_order_stats WHERE product = 6"
psql_e2e -qc "UPDATE orders SET amount = 21 WHERE product_id = 6 AND amount = 19"
wait_for_value "1" "SELECT count(*) FROM shiba.large_order_stats WHERE product = 6 AND order_count = 1 AND total_amount = 21"
psql_e2e -qc "UPDATE orders SET amount = 18 WHERE product_id = 1 AND amount = 20"
wait_for_value "0" "SELECT count(*) FROM shiba.large_order_stats WHERE product = 1"
test "$(psql_e2e -Atqc "SELECT result_group_column || ':' || (predicate_sql LIKE '% AND %') || ':' || (predicate_sql LIKE '% OR %') FROM shiba_internal.stream_views v JOIN shiba_internal.stream_filters f USING (result_oid) WHERE result_oid = 'shiba.large_order_stats'::regclass")" = "product:true:true"
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid = 'shiba.large_order_stats'::regclass")" = "scan,filter,aggregate,project,sink"
psql_e2e -qc "DROP TABLE shiba.large_order_stats"

psql_e2e -qc "CREATE TABLE items (id integer NOT NULL, category_id integer NOT NULL)"
psql_e2e -qc "CREATE TABLE sales (line_id integer NOT NULL, item_id integer NOT NULL, amount integer NOT NULL)"
psql_e2e -qc "INSERT INTO items VALUES (1, 7), (2, 8)"
psql_e2e -qc "INSERT INTO sales VALUES (1, 1, 10), (2, 2, 20)"
psql_e2e -qc "CREATE TABLE shiba.category_sales AS SELECT i.category_id, count(*) AS sale_count, sum(s.amount) AS total_amount FROM sales s JOIN items i ON s.item_id = i.id GROUP BY i.category_id"
# Two simultaneously active DAGs must not grow the Runtime process count.
wait_for_value "2" "SELECT count(*) FROM shiba_internal.dag_runtime_state WHERE active"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid = 'shiba.category_sales'::regclass")" = "scan,scan,inner_join,aggregate,project,sink"
test "$(psql_e2e -Atqc "SELECT count(*) || ':' || count(*) FILTER (WHERE stateful) FROM shiba_internal.operator_instances WHERE result_oid = 'shiba.category_sales'::regclass")" = "6:2"
test "$(psql_e2e -Atqc "SELECT row_count || ':' || sum_value FROM shiba_internal.aggregate_state WHERE result_oid = 'shiba.category_sales'::regclass AND group_key = '7'::jsonb")" = "1:10"
test "$(psql_e2e -Atqc "SELECT jsonb_array_length(analyzed_query->'sources') || ':' || (analyzed_query->'joins'->0->>'kind') FROM shiba_internal.stream_graphs WHERE result_oid = 'shiba.category_sales'::regclass")" = "2:inner"
test "$(psql_e2e -Atqc "SELECT (analyzed_query->'joins'->0->>'operator') || ':' || (analyzed_query->'joins'->0->>'left_column') || ':' || (analyzed_query->'joins'->0->>'right_column') FROM shiba_internal.stream_graphs WHERE result_oid = 'shiba.category_sales'::regclass")" = "=:2:1"
psql_e2e -qc "CREATE TABLE nullable_items (id integer NOT NULL, category_id integer NOT NULL)"
psql_e2e -qc "CREATE TABLE nullable_sales (item_id integer NOT NULL, amount integer)"
if psql_e2e -qc "CREATE TABLE shiba.nullable_category_sales AS SELECT i.category_id, count(*) AS sale_count, sum(s.amount) AS total_amount FROM nullable_sales s JOIN nullable_items i ON s.item_id = i.id GROUP BY i.category_id" >/dev/null 2>&1; then
  printf 'a Shiba JOIN with nullable SUM input unexpectedly succeeded\n' >&2
  exit 1
fi
psql_e2e -qc "CREATE TABLE mixed_key_facts (item_id numeric NOT NULL, amount integer NOT NULL)"
psql_e2e -qc "CREATE TABLE mixed_key_items (id integer NOT NULL, category_id integer NOT NULL)"
if psql_e2e -qc "CREATE TABLE shiba.mixed_key_join AS SELECT i.category_id, count(*) AS sale_count, sum(f.amount) AS total_amount FROM mixed_key_facts f JOIN mixed_key_items i ON f.item_id=i.id GROUP BY i.category_id" >/dev/null 2>&1; then
  printf 'a Shiba JOIN with differently typed arrangement keys unexpectedly succeeded\n' >&2
  exit 1
fi
if psql_e2e -qc "ALTER TABLE items ADD COLUMN forbidden integer" >/dev/null 2>&1; then
  printf 'altering a Shiba JOIN right source unexpectedly succeeded\n' >&2
  exit 1
fi
psql_e2e -qc "CREATE TABLE shiba.item_stats AS SELECT category_id, count(*) AS item_count, sum(id) AS total_id FROM items GROUP BY category_id"
psql_e2e -qc "DROP TABLE shiba.item_stats"
wait_for_value "1" "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'shiba_publication' AND tablename = 'items'"
psql_e2e -qc "INSERT INTO sales VALUES (3, 1, 5)"
wait_for_value "1" "SELECT count(*) FROM shiba.category_sales WHERE category_id = 7 AND sale_count = 2 AND total_amount = 15"
psql_e2e -qc "UPDATE items SET category_id = 9 WHERE id = 1"
wait_for_value "1" "SELECT count(*) FROM shiba.category_sales WHERE category_id = 9 AND sale_count = 2 AND total_amount = 15"
wait_for_value "0" "SELECT count(*) FROM shiba.category_sales WHERE category_id = 7"
psql_e2e -qc "DELETE FROM sales WHERE line_id = 2"
wait_for_value "0" "SELECT count(*) FROM shiba.category_sales WHERE category_id = 8"

# A filter on the fact side of a JOIN controls arrangement membership. Project
# aliases the dimension group key without changing its typed delta semantics.
psql_e2e -qc "CREATE TABLE shiba.large_category_sales AS SELECT i.category_id AS category, count(*) AS sale_count, sum(s.amount) AS total_amount FROM sales s JOIN items i ON s.item_id = i.id WHERE s.amount >= 10 AND NOT (s.item_id = 999) GROUP BY i.category_id"
wait_for_value "1" "SELECT count(*) FROM shiba.large_category_sales WHERE category = 9 AND sale_count = 1 AND total_amount = 10"
psql_e2e -qc "INSERT INTO sales VALUES (4, 1, 8)"
wait_for_value "1" "SELECT count(*) FROM shiba.large_category_sales WHERE category = 9 AND sale_count = 1 AND total_amount = 10"
psql_e2e -qc "UPDATE sales SET amount = 12 WHERE line_id = 4"
wait_for_value "1" "SELECT count(*) FROM shiba.large_category_sales WHERE category = 9 AND sale_count = 2 AND total_amount = 22"
psql_e2e -qc "UPDATE sales SET amount = 9 WHERE line_id = 1"
wait_for_value "1" "SELECT count(*) FROM shiba.large_category_sales WHERE category = 9 AND sale_count = 1 AND total_amount = 12"
psql_e2e -qc "UPDATE items SET category_id = 11 WHERE id = 1"
wait_for_value "1" "SELECT count(*) FROM shiba.large_category_sales WHERE category = 11 AND sale_count = 1 AND total_amount = 12"
wait_for_value "0" "SELECT count(*) FROM shiba.large_category_sales WHERE category = 9"
psql_e2e -qc "DROP TABLE shiba.large_category_sales"

# DISTINCT also works when its value comes from the right JOIN arrangement.
psql_e2e -qc "CREATE TABLE distinct_items (id integer NOT NULL, category_id integer NOT NULL)"
psql_e2e -qc "CREATE TABLE distinct_sales (row_id integer NOT NULL, item_id integer NOT NULL, amount integer NOT NULL)"
psql_e2e -qc "INSERT INTO distinct_items VALUES (1,7),(2,7)"
psql_e2e -qc "INSERT INTO distinct_sales VALUES (1,1,5),(2,1,6),(3,2,7)"
psql_e2e -qc "CREATE TABLE shiba.distinct_join_items AS SELECT i.category_id, count(DISTINCT i.id) AS item_count, sum(s.amount) AS total_amount FROM distinct_sales s JOIN distinct_items i ON s.item_id=i.id GROUP BY i.category_id"
wait_for_value "1" "SELECT count(*) FROM shiba.distinct_join_items WHERE category_id=7 AND item_count=2 AND total_amount=18"
test "$(psql_e2e -Atqc "SELECT count_input_source || ':' || count_input_column FROM shiba_internal.stream_views WHERE result_oid='shiba.distinct_join_items'::regclass")" = "right:id"
psql_e2e -qc "INSERT INTO distinct_sales VALUES (4,1,4)"
wait_for_value "1" "SELECT count(*) FROM shiba.distinct_join_items WHERE category_id=7 AND item_count=2 AND total_amount=22"
psql_e2e -qc "DELETE FROM distinct_sales WHERE item_id=2"
wait_for_value "1" "SELECT count(*) FROM shiba.distinct_join_items WHERE category_id=7 AND item_count=1 AND total_amount=15"
psql_e2e -qc "INSERT INTO distinct_sales VALUES (5,2,8)"
wait_for_value "1" "SELECT count(*) FROM shiba.distinct_join_items WHERE category_id=7 AND item_count=2 AND total_amount=23"
psql_e2e -qc "DROP TABLE shiba.distinct_join_items"

# Predicates that reference both JOIN inputs are evaluated after the
# arrangement probe with both typed rows in scope.
psql_e2e -qc "CREATE TABLE cross_dims (id integer NOT NULL, threshold integer NOT NULL, group_id integer NOT NULL)"
psql_e2e -qc "CREATE TABLE cross_facts (dim_id integer NOT NULL, amount integer NOT NULL)"
psql_e2e -qc "INSERT INTO cross_dims VALUES (1,10,7)"
psql_e2e -qc "INSERT INTO cross_facts VALUES (1,5),(1,15)"
psql_e2e -qc "CREATE TABLE shiba.cross_filtered AS SELECT d.group_id,count(*) AS matched_count,sum(f.amount) AS total_amount FROM cross_facts f JOIN cross_dims d ON f.dim_id=d.id WHERE f.amount>=d.threshold AND d.group_id<>999 GROUP BY d.group_id"
wait_for_value "1" "SELECT count(*) FROM shiba.cross_filtered WHERE group_id=7 AND matched_count=1 AND total_amount=15"
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid='shiba.cross_filtered'::regclass")" = "scan,scan,inner_join,filter,aggregate,project,sink"
psql_e2e -qc "UPDATE cross_dims SET threshold=20 WHERE id=1"
wait_for_value "0" "SELECT count(*) FROM shiba.cross_filtered"
psql_e2e -qc "INSERT INTO cross_facts VALUES (1,25)"
wait_for_value "1" "SELECT count(*) FROM shiba.cross_filtered WHERE group_id=7 AND matched_count=1 AND total_amount=25"
psql_e2e -qc "UPDATE cross_dims SET threshold=10 WHERE id=1"
wait_for_value "1" "SELECT count(*) FROM shiba.cross_filtered WHERE group_id=7 AND matched_count=2 AND total_amount=40"
psql_e2e -qc "DROP TABLE shiba.cross_filtered"

# LEFT JOIN: first/last matches retract and restore the preserved left row.
psql_e2e -qc "CREATE TABLE outer_items (id integer NOT NULL, category_id integer NOT NULL)"
psql_e2e -qc "CREATE TABLE outer_sales (item_id integer NOT NULL, amount integer NOT NULL)"
psql_e2e -qc "INSERT INTO outer_items VALUES (1, 7)"
psql_e2e -qc "INSERT INTO outer_sales VALUES (1, 10), (99, 5)"
psql_e2e -qc "CREATE TABLE shiba.left_outer_sales AS SELECT i.category_id, count(*) AS sale_count, sum(s.amount) AS total_amount FROM outer_sales s LEFT JOIN outer_items i ON s.item_id = i.id GROUP BY i.category_id"
# Three simultaneously active DAGs still share the same Runtime.
wait_for_value "3" "SELECT count(*) FROM shiba_internal.dag_runtime_state WHERE active"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid = 'shiba.left_outer_sales'::regclass")" = "scan,scan,left_join,aggregate,project,sink"
test "$(psql_e2e -Atqc "SELECT sale_count || ':' || total_amount FROM shiba.left_outer_sales WHERE category_id IS NULL")" = "1:5"
psql_e2e -qc "INSERT INTO outer_items VALUES (99, 8)"
wait_for_value "1" "SELECT count(*) FROM shiba.left_outer_sales WHERE category_id = 8 AND sale_count = 1 AND total_amount = 5"
wait_for_value "0" "SELECT count(*) FROM shiba.left_outer_sales WHERE category_id IS NULL"
psql_e2e -qc "DELETE FROM outer_items WHERE id = 1"
wait_for_value "1" "SELECT count(*) FROM shiba.left_outer_sales WHERE category_id IS NULL AND sale_count = 1 AND total_amount = 10"
wait_for_value "0" "SELECT count(*) FROM shiba.left_outer_sales WHERE category_id = 7"
psql_e2e -qc "DROP TABLE shiba.left_outer_sales"
psql_e2e -qc "CREATE TABLE shiba.filtered_left_sales AS SELECT i.category_id, count(*) AS sale_count, sum(s.amount) AS total_amount FROM outer_sales s LEFT JOIN outer_items i ON s.item_id = i.id WHERE i.category_id >= 8 GROUP BY i.category_id"
test "$(psql_e2e -Atqc "SELECT string_agg(node->>'operator', ',' ORDER BY ordinality) FROM shiba_internal.stream_graphs, jsonb_array_elements(logical_plan->'nodes') WITH ORDINALITY AS n(node, ordinality) WHERE result_oid = 'shiba.filtered_left_sales'::regclass")" = "scan,scan,left_join,filter,aggregate,project,sink"
test "$(psql_e2e -Atqc "SELECT phase FROM shiba_internal.stream_filters WHERE result_oid = 'shiba.filtered_left_sales'::regclass")" = "post"
wait_for_value "1" "SELECT count(*) FROM shiba.filtered_left_sales WHERE category_id = 8 AND sale_count = 1 AND total_amount = 5"
psql_e2e -qc "DELETE FROM outer_items WHERE id = 99"
wait_for_value "0" "SELECT count(*) FROM shiba.filtered_left_sales"
psql_e2e -qc "INSERT INTO outer_items VALUES (99, 8)"
wait_for_value "1" "SELECT count(*) FROM shiba.filtered_left_sales WHERE category_id = 8 AND sale_count = 1 AND total_amount = 5"
psql_e2e -qc "DROP TABLE shiba.filtered_left_sales"

# RIGHT JOIN: an unmatched dimension has COUNT(*)=1 and SUM(left)=NULL.
psql_e2e -qc "CREATE TABLE right_items (id integer NOT NULL, category_id integer NOT NULL)"
psql_e2e -qc "CREATE TABLE right_sales (item_id integer NOT NULL, amount integer NOT NULL)"
psql_e2e -qc "INSERT INTO right_items VALUES (1, 7), (2, 8)"
psql_e2e -qc "INSERT INTO right_sales VALUES (1, 10)"
psql_e2e -qc "CREATE TABLE shiba.right_outer_sales AS SELECT i.category_id, count(*) AS sale_count, sum(s.amount) AS total_amount FROM right_sales s RIGHT JOIN right_items i ON s.item_id = i.id GROUP BY i.category_id"
wait_for_value "1" "SELECT count(*) FROM shiba.right_outer_sales WHERE category_id = 8 AND sale_count = 1 AND total_amount IS NULL"
psql_e2e -qc "INSERT INTO right_sales VALUES (2, 20)"
wait_for_value "1" "SELECT count(*) FROM shiba.right_outer_sales WHERE category_id = 8 AND sale_count = 1 AND total_amount = 20"
psql_e2e -qc "DELETE FROM right_sales WHERE item_id = 2"
wait_for_value "1" "SELECT count(*) FROM shiba.right_outer_sales WHERE category_id = 8 AND sale_count = 1 AND total_amount IS NULL"
psql_e2e -qc "DROP TABLE shiba.right_outer_sales"

# FULL JOIN preserves unmatched rows from both inputs.
psql_e2e -qc "CREATE TABLE full_items (id integer NOT NULL, category_id integer NOT NULL)"
psql_e2e -qc "CREATE TABLE full_sales (item_id integer NOT NULL, amount integer NOT NULL)"
psql_e2e -qc "INSERT INTO full_items VALUES (1, 7), (2, 8)"
psql_e2e -qc "INSERT INTO full_sales VALUES (1, 10), (99, 5)"
psql_e2e -qc "CREATE TABLE shiba.full_outer_sales AS SELECT i.category_id, count(*) AS sale_count, sum(s.amount) AS total_amount FROM full_sales s FULL JOIN full_items i ON s.item_id = i.id GROUP BY i.category_id"
wait_for_value "1" "SELECT count(*) FROM shiba.full_outer_sales WHERE category_id IS NULL AND sale_count = 1 AND total_amount = 5"
wait_for_value "1" "SELECT count(*) FROM shiba.full_outer_sales WHERE category_id = 8 AND sale_count = 1 AND total_amount IS NULL"
psql_e2e -qc "INSERT INTO full_items VALUES (99, 9)"
wait_for_value "1" "SELECT count(*) FROM shiba.full_outer_sales WHERE category_id = 9 AND sale_count = 1 AND total_amount = 5"
wait_for_value "0" "SELECT count(*) FROM shiba.full_outer_sales WHERE category_id IS NULL"
psql_e2e -qc "INSERT INTO full_sales VALUES (2, 20)"
wait_for_value "1" "SELECT count(*) FROM shiba.full_outer_sales WHERE category_id = 8 AND sale_count = 1 AND total_amount = 20"
psql_e2e -qc "DROP TABLE shiba.full_outer_sales"

# Exercise the large-Stage planner path and then a small commit on the same
# prepared program. The first commit crosses the 1,024-row ANALYZE threshold;
# the second verifies that resetting the emptied Stage statistics preserves
# the ordinary small-batch path.
psql_e2e -qc "CREATE TABLE large_stage_facts (dim_id integer NOT NULL,amount integer NOT NULL)"
psql_e2e -qc "CREATE TABLE large_stage_dims (id integer NOT NULL,group_id integer NOT NULL)"
psql_e2e -qc "INSERT INTO large_stage_dims VALUES (1,7)"
psql_e2e -qc "CREATE TABLE shiba.large_stage_stats AS SELECT d.group_id,count(*) AS matched_count,sum(f.amount) AS total_amount FROM large_stage_facts f JOIN large_stage_dims d ON f.dim_id=d.id GROUP BY d.group_id"
psql_e2e -qc "INSERT INTO large_stage_facts SELECT 1,1 FROM generate_series(1,1100)"
wait_for_value "1" "SELECT count(*) FROM shiba.large_stage_stats WHERE group_id=7 AND matched_count=1100 AND total_amount=1100"
psql_e2e -qc "INSERT INTO large_stage_facts VALUES (1,2)"
wait_for_value "1" "SELECT count(*) FROM shiba.large_stage_stats WHERE group_id=7 AND matched_count=1101 AND total_amount=1102"
test "$(psql_e2e -Atqc "SELECT count(*) FROM shiba_internal.physical_stages stage WHERE stage.result_oid='shiba.large_stage_stats'::regclass AND stage.storage='unlogged' AND (SELECT count(*) FROM pg_class relation WHERE relation.oid=stage.relation_oid AND relation.relpersistence='u')=1")" = "1"
large_stage_relation="$(psql_e2e -Atqc "SELECT format('%I.%I',namespace.nspname,relation.relname) FROM shiba_internal.physical_stages stage JOIN pg_class relation ON relation.oid=stage.relation_oid JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace WHERE stage.result_oid='shiba.large_stage_stats'::regclass")"
test "$(psql_e2e -Atqc "SELECT count(*) FROM ${large_stage_relation}")" = "0"
psql_e2e -qc "DROP TABLE shiba.large_stage_stats"

psql_e2e -qc "DROP TABLE shiba.category_sales"
wait_for_value "0" "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'shiba_publication' AND tablename IN ('sales', 'items')"

# DROP and apply share the result advisory lock, and both acquire it before any
# source relation lock. Repeated committed writes exercise that lock order
# while the result DAG is being quiesced and removed.
psql_e2e -qc "CREATE TABLE drop_race_source (group_id integer NOT NULL, amount integer NOT NULL)"
psql_e2e -qc "CREATE TABLE shiba.drop_race AS SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount FROM drop_race_source GROUP BY group_id"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
(
  for attempt in {1..40}; do
    psql_e2e -qc "INSERT INTO drop_race_source VALUES (1,${attempt})"
  done
) &
drop_race_writer_pid=$!
sleep 0.05
psql_e2e -qc "DROP TABLE shiba.drop_race"
wait "${drop_race_writer_pid}"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"
test "$(psql_e2e -Atqc "SELECT to_regclass('shiba.drop_race') IS NULL")" = "t"

psql_e2e -qc "CREATE TABLE shiba.unqualified_drop AS SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount FROM drop_race_source GROUP BY group_id"
psql_e2e -qc "SET search_path=shiba,public; DROP TABLE unqualified_drop"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

# Batch DROP takes every result advisory lock first, then every source lock in
# one global OID order. The crossed source ownership below exercised a
# deterministic deadlock with per-result locking.
psql_e2e -qc "CREATE TABLE batch_source_x (group_id integer NOT NULL,amount integer NOT NULL)"
psql_e2e -qc "CREATE TABLE batch_source_y (group_id integer NOT NULL,amount integer NOT NULL)"
psql_e2e -qc "CREATE TABLE shiba.batch_ax AS SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount FROM batch_source_x GROUP BY group_id"
psql_e2e -qc "CREATE TABLE shiba.batch_by AS SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount FROM batch_source_y GROUP BY group_id"
psql_e2e -qc "CREATE TABLE shiba.batch_ay AS SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount FROM batch_source_y GROUP BY group_id"
psql_e2e -qc "CREATE TABLE shiba.batch_bx AS SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount FROM batch_source_x GROUP BY group_id"
psql_e2e -qc "DROP TABLE shiba.batch_ax,shiba.batch_ay" &
batch_drop_a_pid=$!
psql_e2e -qc "DROP TABLE shiba.batch_by,shiba.batch_bx" &
batch_drop_b_pid=$!
wait "${batch_drop_a_pid}"
wait "${batch_drop_b_pid}"
wait_for_value "1" "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'shiba runtime'"

"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -m immediate stop >/dev/null
"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -l "${pg_log_file}" -o "-k ${pg_socket_dir} -p ${pg_port}" -w start >/dev/null
psql_e2e -qc "INSERT INTO orders VALUES (5, 25)"
wait_for_value "1" "SELECT count(*) FROM shiba.order_stats WHERE product_id = 5 AND order_count = 1 AND total_amount = 25"
wait_for_value "1" "SELECT count(*) FROM shiba_internal.runtime_state WHERE last_heartbeat >= pg_postmaster_start_time()"
psql_e2e -qc "DROP TABLE shiba.order_stats"
wait_for_value "0" "SELECT count(*) FROM shiba_internal.stream_views"
wait_for_value "0" "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'shiba_wakeup_%' AND NOT tgisinternal"
psql_e2e -qc "INSERT INTO orders VALUES (4, 20)"
wait_for_value "0" "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'shiba_publication' AND tablename = 'orders'"
if psql_e2e -qc "DROP EXTENSION/**/shiba" >/dev/null 2>&1; then
  printf 'dropping Shiba with an active logical slot unexpectedly succeeded\n' >&2
  exit 1
fi
if psql_e2e -qc "DROP OWNED BY CURRENT_USER" >/dev/null 2>&1; then
  printf 'DROP OWNED bypassed Shiba extension-owner lifecycle protection\n' >&2
  exit 1
fi
psql_e2e -qc "UPDATE shiba_internal.runtime_state SET active=false WHERE singleton"
wait_for_value "0" "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'"
psql_e2e -qc "SELECT pg_drop_replication_slot(shiba_internal.slot_name())"
if psql_e2e -qc "DROP EXTENSION shiba" >/dev/null 2>&1; then
  printf 'dropping Shiba without a completed deactivation unexpectedly succeeded\n' >&2
  exit 1
fi
psql_e2e -qc "SELECT shiba.deactivate()"
wait_for_value "0" "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'shiba_%'"
wait_for_value "0" "SELECT count(*) FROM pg_publication WHERE pubname = 'shiba_publication'"
psql_e2e -qc "DROP EXTENSION shiba"

printf 'Shiba asynchronous extension flow verified on PostgreSQL 17.\n'
