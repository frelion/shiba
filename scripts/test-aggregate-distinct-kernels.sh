#!/usr/bin/env bash
set -euo pipefail

# End-to-end gate for the generic typed Aggregate and Distinct kernels.
# Correctness is always compared with a fresh PostgreSQL recomputation.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-aggregate-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-aggregate-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_AGGREGATE_TEST_PORT:-$((61000 + $$ % 2000))}"
database_name="shiba_aggregate"
wait_attempts="${SHIBA_AGGREGATE_WAIT_ATTEMPTS:-1200}"
aggregate_rows="${SHIBA_AGGREGATE_TEST_ROWS:-80}"

psql_stateful() {
  PGOPTIONS="-c statement_timeout=60000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

test_name="aggregate/distinct kernel gate"
test_psql_command=psql_stateful
test_log_lines=200
test_wait_attempts="${wait_attempts}"
test_wait_sleep=0.05
test_retain_log=1
source "${project_root}/scripts/test-lib.sh"
trap cleanup EXIT

cd "${project_root}"
cargo pgrx install \
  --pg-config "${pg_config_path}" \
  --features pg_test

"${pg_bin_dir}/initdb" \
  -D "${pg_data_dir}" --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\n"
  printf "wal_level = logical\n"
  printf "max_replication_slots = 4\n"
  printf "max_worker_processes = 16\n"
  printf "listen_addresses = ''\n"
  printf "unix_socket_directories = '%s'\n" "${pg_socket_dir}"
  printf "port = %s\n" "${pg_port}"
  printf "shiba.batch_rows = 4\n"
  printf "shiba.batch_bytes = 4096\n"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "$(id -un)"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_stateful -qc "CREATE EXTENSION shiba"
psql_stateful -qc "SELECT shiba.activate()"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'" \
  "the singleton Runtime"

psql_stateful -qc "
  CREATE TABLE public.shiba_runtime_failpoints (
    kind text PRIMARY KEY,
    runtime_pid integer,
    result_oid oid,
    stage_id integer,
    commit_lsn pg_lsn,
    pause_ms integer NOT NULL DEFAULT 0 CHECK (pause_ms>=0),
    fired boolean NOT NULL DEFAULT false
  );
  CREATE TABLE public.metric_source (
    id integer PRIMARY KEY,
    group_a integer,
    group_b text,
    value integer,
    ordering integer NOT NULL,
    enabled boolean NOT NULL
  );
  CREATE TABLE public.aggregate_hot_dimension (
    id integer PRIMARY KEY,
    bucket integer NOT NULL
  );
  INSERT INTO public.aggregate_hot_dimension
  SELECT id,1 FROM generate_series(1,64) AS id;
  CREATE TABLE public.aggregate_hot_fact (
    id integer PRIMARY KEY,
    group_id integer NOT NULL,
    bucket integer NOT NULL
  );
  CREATE TABLE public.aggregate_batch_source (
    id integer PRIMARY KEY
  );
  CREATE TABLE public.aggregate_group_identity_source (
    id integer PRIMARY KEY,
    group_key text COLLATE pg_catalog.\"C\"
  );
  CREATE TABLE public.aggregate_representation_source (
    id integer PRIMARY KEY,
    value numeric
  );
  CREATE TABLE public.aggregate_order_scan_source (
    id integer PRIMARY KEY,
    group_id integer NOT NULL,
    ordering integer NOT NULL,
    value integer NOT NULL
  );
  INSERT INTO public.aggregate_batch_source
  SELECT id FROM generate_series(1,16) AS id;
  INSERT INTO public.aggregate_order_scan_source
  SELECT id,1,id,id FROM generate_series(1,512) AS id;

  CREATE TABLE shiba.aggregate_result AS
  SELECT group_a,
         group_b,
         count(*) FILTER (WHERE enabled) AS enabled_rows,
         sum(value) FILTER (WHERE enabled) AS enabled_sum,
         regr_count(
           DISTINCT value::double precision,
                    ordering::double precision
         ) FILTER (WHERE enabled) AS distinct_pairs,
         max(value ORDER BY ordering,id) FILTER (WHERE enabled) AS ordered_max
  FROM public.metric_source
  GROUP BY group_a,group_b;

  CREATE TABLE shiba.aggregate_distinct_chain AS
  SELECT DISTINCT group_a,group_b,enabled_sum
  FROM (
    SELECT group_a,
           group_b,
           sum(value) FILTER (WHERE enabled) AS enabled_sum
    FROM public.metric_source
    GROUP BY group_a,group_b
  ) AS grouped;

  CREATE TABLE shiba.aggregate_hot_group AS
  SELECT group_id,
         count(*) AS joined_rows,
         sum(dimension_id) AS dimension_sum
  FROM (
    SELECT fact.group_id,dimension.id AS dimension_id
    FROM public.aggregate_hot_fact AS fact
    JOIN public.aggregate_hot_dimension AS dimension
      ON dimension.bucket=fact.bucket
  ) AS joined
  GROUP BY group_id;

  CREATE TABLE shiba.aggregate_group_identity AS
  SELECT group_key,count(*) AS row_count
  FROM public.aggregate_group_identity_source
  GROUP BY group_key;

  CREATE TABLE shiba.aggregate_min_scale AS
  SELECT scale(min(value)) AS value_scale
  FROM public.aggregate_representation_source;

  CREATE TABLE shiba.aggregate_group_scale AS
  SELECT scale(value) AS value_scale,count(*) AS row_count
  FROM public.aggregate_representation_source
  GROUP BY value;

  CREATE TABLE shiba.aggregate_order_scan AS
  SELECT group_id,max(value ORDER BY ordering,id) AS maximum
  FROM public.aggregate_order_scan_source
  GROUP BY group_id"

# Distinct publishes a physical representative, so SQL-equal numeric values
# with different scales must replace the downstream row even while occupancy
# remains positive. Register this chain with a one-row output target: the
# replacement must durably Drain -old and +new in separate bounded steps.
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_rows='1'"
psql_stateful -qc "SELECT pg_reload_conf()"
wait_for_query "1" \
  "SELECT current_setting('shiba.batch_rows')" \
  "the one-row Distinct replacement budget"
psql_stateful -qc "
  CREATE TABLE public.distinct_numeric_rep_source (
    id integer PRIMARY KEY,
    value numeric NOT NULL
  );
  INSERT INTO public.distinct_numeric_rep_source VALUES(1,1.0);
  CREATE TABLE shiba.distinct_numeric_rep_scale AS
  SELECT scale(value) AS value_scale
  FROM (
    SELECT DISTINCT value
    FROM public.distinct_numeric_rep_source
  ) AS distinct_values"
wait_for_query "1" \
  "SELECT value_scale FROM shiba.distinct_numeric_rep_scale" \
  "the initial numeric Distinct representative"
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_rows='4'"
psql_stateful -qc "SELECT pg_reload_conf()"
wait_for_query "4" \
  "SELECT current_setting('shiba.batch_rows')" \
  "the restored Aggregate/Distinct row budget"

# UPDATE arrives as a same-page delete+insert of SQL-equal physical values.
# The outer scale() makes a stale representative directly observable. Crash
# after Apply commits but before either one-row Drain leg: recovery must retain
# the exact queue order and publish the replacement once.
numeric_result_oid="$(psql_stateful -Atqc "
  SELECT 'shiba.distinct_numeric_rep_scale'::regclass::oid::integer")"
numeric_distinct_stage="$(psql_stateful -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${numeric_result_oid}::oid
    AND stage.value->'spec'->>'operator'='distinct'")"
numeric_distinct_queue="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${numeric_result_oid}::oid
    AND stage_id=${numeric_distinct_stage}
    AND state_slot=2")"
numeric_distinct_continuation="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_continuation_relations
  WHERE result_oid=${numeric_result_oid}::oid
    AND stage_id=${numeric_distinct_stage}")"
# Registration gave every stream a one-row target. Widen the source and
# upstream/downstream operator streams so the UPDATE's -old/+new reach
# Distinct in one page; keep the Distinct output at one row per Drain chunk.
psql_stateful -qc "
  UPDATE shiba_internal.effect_streams
  SET target_chunk_rows=4
  WHERE stream_id IN (
    SELECT stream_id
    FROM shiba_internal.effect_stream_consumers
    WHERE result_oid=${numeric_result_oid}::oid
  )
    AND NOT (
      producer_kind='operator'
      AND producer_result_oid=${numeric_result_oid}::oid
      AND producer_stage_id=${numeric_distinct_stage}
    )"
runtime_before="$(runtime_pid)"
psql_stateful -qc "
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_after_commit',
    ${runtime_before},
    ${numeric_result_oid}::oid,
    ${numeric_distinct_stage},
    1200
  );
  UPDATE public.distinct_numeric_rep_source SET value=1.00 WHERE id=1"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the committed Distinct Apply"
assert_query "2|2|-1,1" "
  SELECT continuation.phase || '|' || count(queue.queue_id) || '|' ||
         string_agg(queue.weight::text,',' ORDER BY queue.queue_id)
  FROM ${numeric_distinct_continuation} AS continuation
  CROSS JOIN ${numeric_distinct_queue} AS queue
  GROUP BY continuation.phase"
wait_for_runtime_replacement "${runtime_before}"
wait_for_query "2" \
  "SELECT value_scale FROM shiba.distinct_numeric_rep_scale" \
  "the recovered 1.0 to 1.00 Distinct representative replacement"
psql_stateful -qc "
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'"

# Multiple SQL-equal physical representations share one SQL group but retain
# separate bag multiplicities. Once one value disappears, the representative
# must come from the remaining live inventory.
psql_stateful -qc "
  INSERT INTO public.distinct_numeric_rep_source VALUES(2,1.000);
  DELETE FROM public.distinct_numeric_rep_source WHERE id=1"
wait_for_query "3" \
  "SELECT value_scale FROM shiba.distinct_numeric_rep_scale" \
  "the remaining live SQL-equal Distinct representation"
psql_stateful -qc "
  UPDATE public.distinct_numeric_rep_source SET value=1.0000 WHERE id=2"
wait_for_query "4" \
  "SELECT value_scale FROM shiba.distinct_numeric_rep_scale" \
  "the second same-page Distinct representative replacement"
psql_stateful -qc "
  DELETE FROM public.distinct_numeric_rep_source WHERE id=2"
wait_for_query "0" \
  "SELECT count(*) FROM shiba.distinct_numeric_rep_scale" \
  "the live Distinct one-to-zero transition"
psql_stateful -qc "
  INSERT INTO public.distinct_numeric_rep_source VALUES(3,1.00000)"
wait_for_query "5" \
  "SELECT value_scale FROM shiba.distinct_numeric_rep_scale" \
  "the live Distinct zero-to-one transition"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.operator_checkpoints AS checkpoint
  WHERE checkpoint.result_oid='shiba.distinct_numeric_rep_scale'::regclass
    AND checkpoint.has_continuation" \
  "the Distinct replacement queue to drain completely"

# Build one actual Distinct state with thousands of SQL groups and one SQL
# group containing many SQL-equal numeric representations. The mapping gate
# below exercises the exact unique-index conflict arbiter; the representative
# gate exercises the ordered bag probe. Neither gate disables sequential scans.
psql_stateful -qc "
  CREATE TABLE public.distinct_plan_source (
    id bigint PRIMARY KEY,
    bucket integer,
    value numeric
  );
  INSERT INTO public.distinct_plan_source
  SELECT id,id,id::numeric
  FROM generate_series(1,4096) AS id;
  INSERT INTO public.distinct_plan_source
  SELECT 4096+scale,0,
         ('1.' || pg_catalog.repeat('0',scale))::numeric
  FROM generate_series(1,1024) AS scale;
  INSERT INTO public.distinct_plan_source VALUES(100000,NULL,NULL)"
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_rows='256'"
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_bytes='65536'"
psql_stateful -qc "SELECT pg_reload_conf()"
wait_for_query "256|64kB" \
  "SELECT current_setting('shiba.batch_rows') || '|' ||
          current_setting('shiba.batch_bytes')" \
  "the Distinct planner fixture budget"
psql_stateful -qc "
  CREATE TABLE shiba.distinct_plan_probe AS
  SELECT DISTINCT bucket,value
  FROM public.distinct_plan_source"
wait_for_query "4098" \
  "SELECT count(*) FROM shiba.distinct_plan_probe" \
  "the populated Distinct planner fixture"

distinct_plan_result_oid="$(psql_stateful -Atqc "
  SELECT 'shiba.distinct_plan_probe'::regclass::oid::integer")"
distinct_plan_stage="$(psql_stateful -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${distinct_plan_result_oid}::oid
    AND stage.value->'spec'->>'operator'='distinct'")"
distinct_plan_state="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${distinct_plan_result_oid}::oid
    AND stage_id=${distinct_plan_stage}
    AND state_slot=0")"
distinct_plan_bag="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${distinct_plan_result_oid}::oid
    AND stage_id=${distinct_plan_stage}
    AND state_slot=1")"
distinct_plan_state_index="$(psql_stateful -Atqc "
  SELECT index_relation.relname
  FROM pg_catalog.pg_index AS index_catalog
  JOIN pg_catalog.pg_class AS index_relation
    ON index_relation.oid=index_catalog.indexrelid
  WHERE index_catalog.indrelid='${distinct_plan_state}'::regclass
    AND index_catalog.indisunique
    AND index_catalog.indnullsnotdistinct")"
distinct_plan_state_pk="$(psql_stateful -Atqc "
  SELECT index_relation.relname
  FROM pg_catalog.pg_constraint AS constraint_catalog
  JOIN pg_catalog.pg_class AS index_relation
    ON index_relation.oid=constraint_catalog.conindid
  WHERE constraint_catalog.conrelid='${distinct_plan_state}'::regclass
    AND constraint_catalog.contype='p'")"
distinct_plan_bag_index="$(psql_stateful -Atqc "
  SELECT index_relation.relname
  FROM pg_catalog.pg_constraint AS constraint_catalog
  JOIN pg_catalog.pg_class AS index_relation
    ON index_relation.oid=constraint_catalog.conindid
  WHERE constraint_catalog.conrelid='${distinct_plan_bag}'::regclass
    AND constraint_catalog.contype='u'")"
distinct_plan_group="$(psql_stateful -Atqc "
  SELECT group_state_id
  FROM ${distinct_plan_state}
  WHERE key_1=0 AND key_2=1::numeric")"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.operator_checkpoints AS checkpoint
  WHERE checkpoint.result_oid=${distinct_plan_result_oid}::oid
    AND checkpoint.has_continuation" \
  "the Distinct planner fixture to quiesce"
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_rows='4'"
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_bytes='4096'"
psql_stateful -qc "SELECT pg_reload_conf()"
wait_for_query "4|4kB" \
  "SELECT current_setting('shiba.batch_rows') || '|' ||
          current_setting('shiba.batch_bytes')" \
  "the restored Aggregate/Distinct budget after the planner fixture"
assert_query "1024" "
  SELECT count(*)
  FROM ${distinct_plan_bag}
  WHERE group_state_id=${distinct_plan_group}"
psql_stateful -qc "
  ANALYZE ${distinct_plan_state};
  ANALYZE ${distinct_plan_bag}"

distinct_exact_plan="$(psql_stateful -Atqc "
  BEGIN;
  EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)
  INSERT INTO ${distinct_plan_state} AS groups(key_1,key_2)
  VALUES(4096,4096::numeric)
  ON CONFLICT(
    \"key_1\" pg_catalog.int4_ops,
    \"key_2\" pg_catalog.numeric_ops
  ) DO UPDATE SET multiplicity=groups.multiplicity
  RETURNING group_state_id;
  ROLLBACK")"
distinct_null_plan="$(psql_stateful -Atqc "
  BEGIN;
  EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)
  INSERT INTO ${distinct_plan_state} AS groups(key_1,key_2)
  VALUES(NULL::integer,NULL::numeric)
  ON CONFLICT(
    \"key_1\" pg_catalog.int4_ops,
    \"key_2\" pg_catalog.numeric_ops
  ) DO UPDATE SET multiplicity=groups.multiplicity
  RETURNING group_state_id;
  ROLLBACK")"
distinct_mapping_gate="$(EXACT_PLAN="${distinct_exact_plan}" \
  NULL_PLAN="${distinct_null_plan}" \
  DISTINCT_STATE_INDEX="${distinct_plan_state_index}" \
  python3 -c '
import json
import os

index_name = os.environ["DISTINCT_STATE_INDEX"]

def bounded_mapping(raw):
    root = json.loads(raw)[0]["Plan"]
    nodes = []
    stack = [root]
    while stack:
        node = stack.pop()
        nodes.append(node)
        stack.extend(node.get("Plans", []))
    scans = [
        node for node in nodes
        if node.get("Node Type") in ("Seq Scan", "Bitmap Heap Scan")
        and node.get("Relation Name", "").startswith("distinct_groups_")
    ]
    blocks = root.get("Shared Hit Blocks", 0) + root.get("Shared Read Blocks", 0)
    bounded = (
        root.get("Node Type") == "ModifyTable"
        and root.get("Operation") == "Insert"
        and root.get("Conflict Resolution") == "UPDATE"
        and index_name in root.get("Conflict Arbiter Indexes", [])
        and root.get("Actual Rows") == 1
        and root.get("Tuples Inserted") == 0
        and root.get("Conflicting Tuples") == 1
        and not scans
        and blocks <= 64
    )
    return bounded, blocks

exact_ok, exact_blocks = bounded_mapping(os.environ["EXACT_PLAN"])
null_ok, null_blocks = bounded_mapping(os.environ["NULL_PLAN"])
print(
    f"{str(exact_ok).lower()}|{exact_blocks}|"
    f"{str(null_ok).lower()}|{null_blocks}"
)
')"
IFS='|' read -r distinct_exact_ok distinct_exact_blocks \
  distinct_null_ok distinct_null_blocks <<<"${distinct_mapping_gate}"
if test "${distinct_exact_ok}|${distinct_null_ok}" != "true|true"; then
  fail "Distinct exact-key mapping was not conflict-index bounded: ${distinct_mapping_gate}; exact=${distinct_exact_plan}; null=${distinct_null_plan}"
fi
printf 'Distinct exact-key plan: index=%s exact_blocks=%s null_blocks=%s\n' \
  "${distinct_plan_state_index}" "${distinct_exact_blocks}" "${distinct_null_blocks}"

distinct_representative_plan="$(psql_stateful -Atqc "
  EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)
  SELECT bag.output_key,bag.output_row
  FROM ${distinct_plan_bag} AS bag
  WHERE bag.group_state_id=${distinct_plan_group}
  ORDER BY bag.output_key
  LIMIT 1")"
distinct_representative_gate="$(PLAN="${distinct_representative_plan}" \
  DISTINCT_BAG_INDEX="${distinct_plan_bag_index}" \
  python3 -c '
import json
import os

root = json.loads(os.environ["PLAN"])[0]["Plan"]
nodes = []
stack = [root]
while stack:
    node = stack.pop()
    nodes.append(node)
    stack.extend(node.get("Plans", []))
index_name = os.environ["DISTINCT_BAG_INDEX"]
scans = [
    node for node in nodes
    if node.get("Node Type") in ("Index Scan", "Index Only Scan")
    and node.get("Index Name") == index_name
]
seq = [
    node for node in nodes
    if node.get("Node Type") == "Seq Scan"
    and node.get("Relation Name", "").startswith("distinct_bag_")
]
bounded = (
    root.get("Node Type") == "Limit"
    and root.get("Actual Rows") == 1
    and len(scans) == 1
    and scans[0].get("Actual Rows") == 1
    and scans[0].get("Actual Loops") == 1
    and "group_state_id" in scans[0].get("Index Cond", "")
    and scans[0].get("Rows Removed by Filter", 0) == 0
    and not seq
    and scans[0].get("Shared Hit Blocks", 0)
        + scans[0].get("Shared Read Blocks", 0) <= 32
)
blocks = scans[0].get("Shared Hit Blocks", 0) + scans[0].get("Shared Read Blocks", 0) \
    if scans else -1
print(f"{str(bounded).lower()}|{blocks}")
')"
IFS='|' read -r distinct_representative_ok distinct_representative_blocks \
  <<<"${distinct_representative_gate}"
if test "${distinct_representative_ok}" != "true"; then
  fail "Distinct representative lookup was not physically bounded: ${distinct_representative_gate}; ${distinct_representative_plan}"
fi
printf 'Distinct representative plan: index=%s rows=1 blocks=%s\n' \
  "${distinct_plan_bag_index}" "${distinct_representative_blocks}"

distinct_probe_rows=32
distinct_state_probe_values="$(psql_stateful -Atqc "
  SELECT string_agg(
           pg_catalog.format('(%s::bigint)',group_state_id),
           ',' ORDER BY group_state_id
         )
  FROM (
    SELECT group_state_id
    FROM ${distinct_plan_state}
    WHERE key_1 BETWEEN 1 AND ${distinct_probe_rows}
    ORDER BY key_1
  ) AS selected")"
distinct_bag_probe_values="$(psql_stateful -Atqc "
  SELECT string_agg(
           pg_catalog.format(
             '(%s::bigint,%L::bytea)',group_state_id,output_key::text
           ),
           ',' ORDER BY output_key
         )
  FROM (
    SELECT group_state_id,output_key
    FROM ${distinct_plan_bag}
    WHERE group_state_id=${distinct_plan_group}
    ORDER BY output_key
    LIMIT ${distinct_probe_rows}
  ) AS selected")"
distinct_state_probe_plan="$(psql_stateful -Atqc "
  EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)
  SELECT locked_group.group_state_id
  FROM (
    VALUES ${distinct_state_probe_values}
  ) AS touched_page(group_state_id)
  JOIN LATERAL (
    SELECT groups.*
    FROM ${distinct_plan_state} AS groups
    WHERE groups.group_state_id=touched_page.group_state_id
    LIMIT 1
    FOR UPDATE
  ) AS locked_group ON true")"
distinct_bag_probe_plan="$(psql_stateful -Atqc "
  EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)
  SELECT locked_bag.bag_id
  FROM (
    VALUES ${distinct_bag_probe_values}
  ) AS physical_collapsed(group_state_id,output_key)
  JOIN LATERAL (
    SELECT bag.*
    FROM ${distinct_plan_bag} AS bag
    WHERE bag.group_state_id=physical_collapsed.group_state_id
      AND bag.output_key=physical_collapsed.output_key
    LIMIT 1
    FOR UPDATE
  ) AS locked_bag ON true")"
distinct_point_probe_gate="$(STATE_PLAN="${distinct_state_probe_plan}" \
  BAG_PLAN="${distinct_bag_probe_plan}" \
  STATE_INDEX="${distinct_plan_state_pk}" \
  BAG_INDEX="${distinct_plan_bag_index}" \
  PROBE_ROWS="${distinct_probe_rows}" \
  python3 -c '
import json
import os

expected = int(os.environ["PROBE_ROWS"])

def bounded_probe(raw, index_name, relation_prefix, condition_columns):
    root = json.loads(raw)[0]["Plan"]
    nodes = []
    stack = [root]
    while stack:
        node = stack.pop()
        nodes.append(node)
        stack.extend(node.get("Plans", []))
    scans = [
        node for node in nodes
        if node.get("Node Type") in ("Index Scan", "Index Only Scan")
        and node.get("Index Name") == index_name
    ]
    seq = [
        node for node in nodes
        if node.get("Node Type") == "Seq Scan"
        and node.get("Relation Name", "").startswith(relation_prefix)
    ]
    condition = scans[0].get("Index Cond", "") if scans else ""
    blocks = root.get("Shared Hit Blocks", 0) + root.get("Shared Read Blocks", 0)
    bounded = (
        root.get("Actual Rows") == expected
        and len(scans) == 1
        and scans[0].get("Actual Loops") == expected
        and scans[0].get("Actual Rows", 0) <= 1
        and scans[0].get("Rows Removed by Filter", 0) == 0
        and all(column in condition for column in condition_columns)
        and not seq
    )
    return bounded, blocks

state_ok, state_blocks = bounded_probe(
    os.environ["STATE_PLAN"],
    os.environ["STATE_INDEX"],
    "distinct_groups_",
    ("group_state_id",),
)
bag_ok, bag_blocks = bounded_probe(
    os.environ["BAG_PLAN"],
    os.environ["BAG_INDEX"],
    "distinct_bag_",
    ("group_state_id", "output_key"),
)
print(
    f"{str(state_ok).lower()}|{state_blocks}|"
    f"{str(bag_ok).lower()}|{bag_blocks}"
)
')"
IFS='|' read -r distinct_state_probe_ok distinct_state_probe_blocks \
  distinct_bag_probe_ok distinct_bag_probe_blocks \
  <<<"${distinct_point_probe_gate}"
if test "${distinct_state_probe_ok}|${distinct_bag_probe_ok}" != "true|true"; then
  fail "Distinct point probes were not physically bounded: ${distinct_point_probe_gate}; state=${distinct_state_probe_plan}; bag=${distinct_bag_probe_plan}"
fi
printf 'Distinct point probes: rows=%s state_index=%s state_blocks=%s bag_index=%s bag_blocks=%s\n' \
  "${distinct_probe_rows}" "${distinct_plan_state_pk}" \
  "${distinct_state_probe_blocks}" "${distinct_plan_bag_index}" \
  "${distinct_bag_probe_blocks}"
if test "${SHIBA_DISTINCT_PLAN_GATE_ONLY:-0}" = "1"; then
  exit 0
fi

# Aggregate bag identity sees bootstrap datums and later pgoutput rows through
# the same canonical-row helper. Real typed cases cover both kinds of
# representation detail that motivated the named-composite text roundtrip.
psql_stateful -qc "
  CREATE TABLE public.aggregate_nan_identity_source (
    value double precision NOT NULL
  );
  CREATE TABLE public.aggregate_array_identity_source (
    value integer[] NOT NULL
  );
  INSERT INTO public.aggregate_array_identity_source
  VALUES ('[0:1]={10,20}'::integer[])"
python3 - <<'PY' | psql_stateful -qc \
  "COPY public.aggregate_nan_identity_source(value) FROM STDIN WITH (FORMAT binary)"
import struct
import sys

stream = bytearray(b"PGCOPY\n\xff\r\n\x00")
stream.extend(struct.pack("!II", 0, 0))
for bits in (0x7FF8000000000001, 0x7FF8000000000002):
    stream.extend(struct.pack("!hI", 1, 8))
    stream.extend(struct.pack("!Q", bits))
stream.extend(struct.pack("!h", -1))
sys.stdout.buffer.write(stream)
PY
psql_stateful -qc "
  CREATE TABLE shiba.aggregate_nan_identity AS
  SELECT value,count(*) AS row_count
  FROM public.aggregate_nan_identity_source
  GROUP BY value;
  CREATE TABLE shiba.aggregate_array_identity AS
  SELECT value,count(*) AS row_count
  FROM public.aggregate_array_identity_source
  GROUP BY value"
wait_for_query "1|2|true" "
  SELECT count(*)||'|'||min(row_count)||'|'||
         bool_and(value='NaN'::double precision)
  FROM shiba.aggregate_nan_identity" \
  "canonical NaN Aggregate bootstrap"
psql_stateful -qc "
  DELETE FROM public.aggregate_nan_identity_source
  WHERE ctid=(
    SELECT ctid
    FROM public.aggregate_nan_identity_source
    ORDER BY pg_catalog.encode(pg_catalog.float8send(value),'hex')
    LIMIT 1
  )"
wait_for_query "1|1|true" "
  SELECT count(*)||'|'||min(row_count)||'|'||
         bool_and(value='NaN'::double precision)
  FROM shiba.aggregate_nan_identity" \
  "canonical NaN Aggregate delete"
wait_for_query "1|1|0|1" "
  SELECT count(*)||'|'||sum(row_count)||'|'||
         min(array_lower(value,1))||'|'||max(array_upper(value,1))
  FROM shiba.aggregate_array_identity" \
  "the non-1 array lower bound Aggregate bootstrap"
psql_stateful -qc "
  INSERT INTO public.aggregate_array_identity_source
  VALUES ('[0:1]={10,20}'::integer[])"
wait_for_query "1|2|0|1" "
  SELECT count(*)||'|'||sum(row_count)||'|'||
         min(array_lower(value,1))||'|'||max(array_upper(value,1))
  FROM shiba.aggregate_array_identity" \
  "the non-1 array lower bound Aggregate live insert"
psql_stateful -qc "
  DELETE FROM public.aggregate_array_identity_source
  WHERE ctid=(
    SELECT ctid
    FROM public.aggregate_array_identity_source
    ORDER BY ctid
    LIMIT 1
  )"
wait_for_query "1|1|0|1" "
  SELECT count(*)||'|'||sum(row_count)||'|'||
         min(array_lower(value,1))||'|'||max(array_upper(value,1))
  FROM shiba.aggregate_array_identity" \
  "the non-1 array lower bound Aggregate live delete"

aggregate_expected="
  SELECT group_a,
         group_b,
         count(*) FILTER (WHERE enabled) AS enabled_rows,
         sum(value) FILTER (WHERE enabled) AS enabled_sum,
         regr_count(
           DISTINCT value::double precision,
                    ordering::double precision
         ) FILTER (WHERE enabled) AS distinct_pairs,
         max(value ORDER BY ordering,id) FILTER (WHERE enabled) AS ordered_max
  FROM public.metric_source
  GROUP BY group_a,group_b"
chain_expected="
  SELECT DISTINCT group_a,group_b,enabled_sum
  FROM (
    SELECT group_a,
           group_b,
           sum(value) FILTER (WHERE enabled) AS enabled_sum
    FROM public.metric_source
    GROUP BY group_a,group_b
  ) AS grouped"

aggregate_result_oid="$(psql_stateful -Atqc "
  SELECT 'shiba.aggregate_result'::regclass::oid::integer")"
aggregate_stage="$(psql_stateful -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${aggregate_result_oid}::oid
    AND stage.value->'spec'->>'operator'='aggregate'")"
aggregate_groups_state="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${aggregate_result_oid}::oid
    AND stage_id=${aggregate_stage}
    AND state_slot=1")"
hot_result_oid="$(psql_stateful -Atqc "
  SELECT 'shiba.aggregate_hot_group'::regclass::oid::integer")"
hot_aggregate_stage="$(psql_stateful -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${hot_result_oid}::oid
    AND stage.value->'spec'->>'operator'='aggregate'")"
hot_output_stream="$(psql_stateful -Atqc "
  SELECT stream_id
  FROM shiba_internal.effect_streams
  WHERE producer_kind='operator'
    AND producer_result_oid=${hot_result_oid}::oid
    AND producer_stage_id=${hot_aggregate_stage}")"
hot_groups_state="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${hot_result_oid}::oid
    AND stage_id=${hot_aggregate_stage}
    AND state_slot=1")"
hot_first_work_state="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${hot_result_oid}::oid
    AND stage_id=${hot_aggregate_stage}
    AND state_slot=2")"
hot_second_work_state="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${hot_result_oid}::oid
    AND stage_id=${hot_aggregate_stage}
    AND state_slot=3")"
order_scan_result_oid="$(psql_stateful -Atqc "
  SELECT 'shiba.aggregate_order_scan'::regclass::oid::integer")"
order_scan_stage="$(psql_stateful -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${order_scan_result_oid}::oid
    AND stage.value->'spec'->>'operator'='aggregate'")"
order_scan_bag="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${order_scan_result_oid}::oid
    AND stage_id=${order_scan_stage}
    AND state_slot=0")"

# Aggregate has one typed-key authority (`groups`).  Every other relation uses
# its bigint group_state_id, and DISTINCT no longer provisions an unbounded
# per-value seen set.
assert_query "0" "
  SELECT count(*)
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${aggregate_result_oid}::oid
    AND stage_id=${aggregate_stage}
    AND state_slot BETWEEN 1000 AND 1999"
assert_query "0" "
  SELECT count(*)
  FROM shiba_internal.operator_state_relations AS storage
  JOIN pg_catalog.pg_attribute AS attribute
    ON attribute.attrelid=storage.relation_oid
   AND attribute.attnum>0
   AND NOT attribute.attisdropped
  WHERE storage.result_oid=${aggregate_result_oid}::oid
    AND storage.stage_id=${aggregate_stage}
    AND storage.state_slot IN (0,2000)
    AND attribute.attname ~ '^group_[0-9]+$'"
assert_query "published_key|bytea|pending_key|bytea" "
  SELECT string_agg(
           attribute.attname || '|' || attribute.atttypid::regtype::text,
           '|' ORDER BY attribute.attnum
         )
  FROM pg_catalog.pg_attribute AS attribute
  WHERE attribute.attrelid='${aggregate_groups_state}'::regclass
    AND attribute.attname IN ('published_key','pending_key')
    AND attribute.attnum>0
    AND NOT attribute.attisdropped"
assert_query "5" "
  SELECT count(*)
  FROM pg_catalog.pg_constraint AS constraint_catalog
  WHERE constraint_catalog.conrelid='${aggregate_groups_state}'::regclass
    AND constraint_catalog.contype='c'
    AND (
      pg_catalog.pg_get_constraintdef(constraint_catalog.oid)
        LIKE '%published_present%'
      OR pg_catalog.pg_get_constraintdef(constraint_catalog.oid)
        LIKE '%pending_present%'
    )"
assert_query "2" "
  WITH bag AS (
    SELECT relation_oid
    FROM shiba_internal.operator_state_relations
    WHERE result_oid=${aggregate_result_oid}::oid
      AND stage_id=${aggregate_stage}
      AND state_slot=0
  )
  SELECT count(*)
  FROM bag
  JOIN pg_catalog.pg_index AS index_catalog
    ON index_catalog.indrelid=bag.relation_oid
  JOIN pg_catalog.pg_class AS index_relation
    ON index_relation.oid=index_catalog.indexrelid
   AND index_relation.relname LIKE 'aggregate_bag_order_%'
  WHERE pg_catalog.pg_get_indexdef(index_catalog.indexrelid)
          LIKE '%(group_state_id%row_id%'"
assert_query "2" "
  WITH group_plan AS (
    SELECT key.ordinality,
           (key.value->'key'->>'equality_operator_oid')::oid AS equality_operator,
           (key.value->'key'->>'sort_operator_oid')::oid AS sort_operator
    FROM shiba_internal.dataflows AS dataflow
    CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
      AS stage(value)
    CROSS JOIN LATERAL jsonb_array_elements(
      stage.value->'spec'->'config'->'groups'
    ) WITH ORDINALITY AS key(value,ordinality)
    WHERE dataflow.result_oid=${aggregate_result_oid}::oid
      AND stage.value->'spec'->>'operator'='aggregate'
  ),
  group_state AS (
    SELECT relation_oid
    FROM shiba_internal.operator_state_relations
    WHERE result_oid=${aggregate_result_oid}::oid
      AND stage_id=${aggregate_stage}
      AND state_slot=1
  ),
  key_opclass AS (
    SELECT class.value AS opclass_oid,class.ordinality
    FROM group_state
    JOIN pg_catalog.pg_index AS index_catalog
      ON index_catalog.indrelid=group_state.relation_oid
     AND index_catalog.indisunique
     AND index_catalog.indnullsnotdistinct
    CROSS JOIN LATERAL unnest(index_catalog.indclass::oid[])
      WITH ORDINALITY AS class(value,ordinality)
  )
  SELECT count(*)
  FROM group_plan AS plan
  JOIN key_opclass AS key_class USING(ordinality)
  JOIN pg_catalog.pg_opclass AS opclass
    ON opclass.oid=key_class.opclass_oid
  WHERE EXISTS (
          SELECT 1 FROM pg_catalog.pg_amop AS member
          WHERE member.amopfamily=opclass.opcfamily
            AND member.amopopr=plan.equality_operator
            AND member.amopstrategy=3
        )
    AND EXISTS (
          SELECT 1 FROM pg_catalog.pg_amop AS member
          WHERE member.amopfamily=opclass.opcfamily
            AND member.amopopr=plan.sort_operator
            AND member.amopstrategy IN (1,5)
        )"

# One bounded input page contains repeated NULL and collated keys. Conflict
# handling must use the plan's unique B-tree semantics without attempting to
# update the same group row twice.
psql_stateful -qc "
  INSERT INTO public.aggregate_group_identity_source VALUES
    (1,NULL),(2,NULL),(3,'same'),(4,'same'),(5,'other'),(6,'same')"
assert_bag_equal "
  SELECT group_key,count(*) AS row_count
  FROM public.aggregate_group_identity_source
  GROUP BY group_key" "
  SELECT group_key,row_count
  FROM shiba.aggregate_group_identity" \
  "same-page repeated NULL/collated Aggregate groups"

# SQL equality groups numeric 1.0 and 1.00 together, but their observable
# representation differs. Both aggregate output and grouped representative
# output must therefore publish a canonical-key replacement.
psql_stateful -qc "
  INSERT INTO public.aggregate_representation_source VALUES(1,1.0)"
wait_for_query "1" "
  SELECT value_scale FROM shiba.aggregate_min_scale" \
  "the initial aggregate output representation"
wait_for_query "1|1" "
  SELECT value_scale || '|' || row_count
  FROM shiba.aggregate_group_scale" \
  "the initial grouped representative"
psql_stateful -qc "
  UPDATE public.aggregate_representation_source
  SET value=1.00::numeric
  WHERE id=1"
wait_for_query "2" "
  SELECT value_scale FROM shiba.aggregate_min_scale" \
  "canonical replacement for scale(min(value))"
wait_for_query "2|1" "
  SELECT value_scale || '|' || row_count
  FROM shiba.aggregate_group_scale" \
  "canonical replacement from the current grouped bag representative"

# The Runtime builds one disjoint index range per lexicographic key position.
# Force PostgreSQL to expose the physical late-page plan and reject any plan
# that filters hundreds of prefix rows before returning one bounded page.
wait_for_query "1|512" "
  SELECT group_id || '|' || maximum
  FROM shiba.aggregate_order_scan" \
  "the ordered Aggregate planner fixture"
late_cursor="$(psql_stateful -Atqc "
  SELECT group_state_id,agg_1_order_1,agg_1_order_2,row_id
  FROM ${order_scan_bag}
  ORDER BY agg_1_order_1,agg_1_order_2,row_id
  OFFSET 480 LIMIT 1")"
IFS='|' read -r late_group late_order_one late_order_two late_row_id \
  <<<"${late_cursor}"
late_plan="$(psql_stateful -Atqc "
  SET enable_seqscan=off;
  SET enable_bitmapscan=off;
  EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)
  SELECT *
  FROM (
    (
      SELECT row_id,agg_1_order_1,agg_1_order_2
      FROM ${order_scan_bag} AS bag
      WHERE bag.group_state_id=${late_group}
        AND ${late_order_one} OPERATOR(pg_catalog.<) bag.agg_1_order_1
      ORDER BY agg_1_order_1,agg_1_order_2,row_id
      LIMIT 5
    )
    UNION ALL
    (
      SELECT row_id,agg_1_order_1,agg_1_order_2
      FROM ${order_scan_bag} AS bag
      WHERE bag.group_state_id=${late_group}
        AND bag.agg_1_order_1 OPERATOR(pg_catalog.=) ${late_order_one}
        AND ${late_order_two} OPERATOR(pg_catalog.<) bag.agg_1_order_2
      ORDER BY agg_1_order_1,agg_1_order_2,row_id
      LIMIT 5
    )
    UNION ALL
    (
      SELECT row_id,agg_1_order_1,agg_1_order_2
      FROM ${order_scan_bag} AS bag
      WHERE bag.group_state_id=${late_group}
        AND bag.agg_1_order_1 OPERATOR(pg_catalog.=) ${late_order_one}
        AND bag.agg_1_order_2 OPERATOR(pg_catalog.=) ${late_order_two}
        AND bag.row_id>${late_row_id}
      ORDER BY agg_1_order_1,agg_1_order_2,row_id
      LIMIT 5
    )
  ) AS ranges
  ORDER BY agg_1_order_1,agg_1_order_2,row_id
  LIMIT 5")"
late_plan_gate="$(python3 -c '
import json
import sys

root = json.load(sys.stdin)[0]["Plan"]
nodes = []
stack = [root]
while stack:
    node = stack.pop()
    nodes.append(node)
    stack.extend(node.get("Plans", []))
scans = [
    node for node in nodes
    if node.get("Node Type") in ("Index Scan", "Index Only Scan")
]
bounded = bool(scans) and all(node.get("Actual Rows", 0) <= 5 for node in scans)
bounded = bounded and any(
    "group_state_id" in node.get("Index Cond", "")
    and "agg_1_order_1" in node.get("Index Cond", "")
    for node in scans
)
removed = sum(node.get("Rows Removed by Filter", 0) for node in scans)
blocks = sum(
    node.get("Shared Hit Blocks", 0) + node.get("Shared Read Blocks", 0)
    for node in scans
)
print(f"{str(bounded).lower()}|{removed}|{str(blocks <= 32).lower()}")
' <<<"${late_plan}")"
if test "${late_plan_gate}" != "true|0|true"; then
  fail "late Aggregate keyset scan was not physically bounded: ${late_plan_gate}; ${late_plan}"
fi

# One fact fans out to 64 joined rows in four-row chunks. Aggregate admits the
# whole 64-row quantum before rebuilding either accumulator. Rebuilding after
# every chunk would take more than 270 Aggregate steps for these two calls.
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_rows='64'"
psql_stateful -qc "SELECT pg_reload_conf()"
wait_for_query "64" \
  "SELECT current_setting('shiba.batch_rows')" \
  "the hot-group admission budget"
assert_query "0" "
  SELECT count(*) FROM shiba.aggregate_hot_group"
hot_revision_before="$(psql_stateful -Atqc "
  SELECT revision
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${hot_result_oid}::oid
    AND stage_id=${hot_aggregate_stage}")"
hot_output_seq_before="$(psql_stateful -Atqc "
  SELECT next_chunk_seq
  FROM shiba_internal.effect_streams
  WHERE stream_id=${hot_output_stream}")"
psql_stateful -qc "
  INSERT INTO public.aggregate_hot_fact VALUES(1,7,1)"
wait_for_query "7|64|2080" "
  SELECT group_id || '|' || joined_rows || '|' || dimension_sum
  FROM shiba.aggregate_hot_group" \
  "the 64-row Join -> Aggregate hot group"
wait_for_query "f" "
  SELECT has_continuation
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${hot_result_oid}::oid
    AND stage_id=${hot_aggregate_stage}" \
  "the hot Aggregate to become idle"
assert_query "t" "
  SELECT revision-${hot_revision_before} BETWEEN 1 AND 95
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${hot_result_oid}::oid
    AND stage_id=${hot_aggregate_stage}"
assert_query "t" "
  SELECT next_chunk_seq-${hot_output_seq_before} BETWEEN 1 AND 3
  FROM shiba_internal.effect_streams
  WHERE stream_id=${hot_output_stream}"

psql_stateful -qc "
  DELETE FROM public.aggregate_hot_fact WHERE id=1"
wait_for_query "0" "
  SELECT count(*) FROM shiba.aggregate_hot_group" \
  "the 64-row hot-group retraction"
assert_query "0|0|0" "
  SELECT (SELECT count(*) FROM ${hot_groups_state})
         || '|' || (SELECT count(*) FROM ${hot_first_work_state})
         || '|' || (SELECT count(*) FROM ${hot_second_work_state})"
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_rows='4'"
psql_stateful -qc "SELECT pg_reload_conf()"
wait_for_query "4" \
  "SELECT current_setting('shiba.batch_rows')" \
  "the restored Aggregate/Distinct row budget"

# The one persisted Aggregate contract carries typed DISTINCT keys, typed
# ordering keys, and typed outputs.  DISTINCT and ORDER BY may belong to
# different aggregate calls.
assert_query "object|object|array" "
  SELECT
    (
      SELECT jsonb_typeof(aggregate.value->'distinct'->0->'type_')
      FROM jsonb_array_elements(
        stage.value->'spec'->'config'->'aggregates'
      ) AS aggregate(value)
      WHERE jsonb_array_length(aggregate.value->'distinct')>0
      LIMIT 1
    ) || '|' ||
    (
      SELECT jsonb_typeof(aggregate.value->'order_by'->0->'type_')
      FROM jsonb_array_elements(
        stage.value->'spec'->'config'->'aggregates'
      ) AS aggregate(value)
      WHERE jsonb_array_length(aggregate.value->'order_by')>0
      LIMIT 1
    ) || '|' ||
    jsonb_typeof(stage.value->'schema'->'outputs')
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    AS stage(value)
  WHERE dataflow.result_oid=${aggregate_result_oid}::oid
    AND stage.value->'spec'->>'operator'='aggregate'"

# Capability is catalog-driven. Concrete states work; internal states are
# rejected precisely at registration.
assert_query "1|true" "
  SELECT count(DISTINCT family.oid)
         || '|' || (count(DISTINCT opclass.oid)>1)
  FROM pg_catalog.pg_amop AS sort_member
  JOIN pg_catalog.pg_opfamily AS family
    ON family.oid=sort_member.amopfamily
  JOIN pg_catalog.pg_opclass AS opclass
    ON opclass.opcfamily=family.oid
   AND opclass.opcintype='text'::regtype
  JOIN pg_catalog.pg_am AS access_method
    ON access_method.oid=opclass.opcmethod
   AND access_method.oid=family.opfmethod
   AND access_method.amname='btree'
  JOIN pg_catalog.pg_amop AS equality_member
    ON equality_member.amopfamily=family.oid
   AND equality_member.amopopr=98::oid
   AND equality_member.amoplefttype='text'::regtype
   AND equality_member.amoprighttype='text'::regtype
   AND equality_member.amopstrategy=3
  WHERE sort_member.amopopr=664::oid
    AND sort_member.amoplefttype='text'::regtype
    AND sort_member.amoprighttype='text'::regtype
    AND sort_member.amopstrategy IN (1,5)"
assert_query "bigint" "
  SELECT aggregate.aggtranstype::regtype
  FROM pg_catalog.pg_aggregate AS aggregate
  WHERE aggregate.aggfnoid='pg_catalog.count()'::regprocedure"
expect_failure "no unique durable capability" "
  CREATE TABLE shiba.unsupported_sum AS
  SELECT sum(value::bigint)
  FROM public.metric_source"

psql_stateful -qc "
  INSERT INTO public.metric_source
  SELECT id,
         CASE WHEN id%7=0 THEN NULL ELSE id%4 END,
         CASE WHEN id%11=0 THEN NULL ELSE 'g-'||(id%3) END,
         CASE WHEN id%13=0 THEN NULL ELSE id%17 END,
         1000-id,
         id%5<>0
  FROM generate_series(1,${aggregate_rows}) AS id"
assert_bag_equal "${aggregate_expected}" \
  "SELECT group_a,group_b,enabled_rows,enabled_sum,
          distinct_pairs,ordered_max
   FROM shiba.aggregate_result" \
  "multi-group FILTER/DISTINCT/ORDER Aggregate"
assert_bag_equal "${chain_expected}" \
  "SELECT group_a,group_b,enabled_sum
   FROM shiba.aggregate_distinct_chain" \
  "Aggregate -> Distinct -> Sink"

# The multi-batch source with small chunk targets must require many durable keyset
# pages; the assertion deliberately does not require one transaction per row.
wait_for_query "t" "
  SELECT checkpoint.revision>20
  FROM shiba_internal.operator_checkpoints AS checkpoint
  WHERE checkpoint.result_oid=${aggregate_result_oid}::oid
    AND checkpoint.stage_id=${aggregate_stage}" \
  "the large group to cross many bounded keyset steps"
assert_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks
  WHERE row_count>8
     OR (payload_bytes>4096 AND row_count<>1)"

# Deletes rebuild from the authoritative typed input multiset, including NULL
# groups and duplicate DISTINCT keys.
psql_stateful -qc "
  DELETE FROM public.metric_source
  WHERE id%3=0 OR id IN (1,2,4,8);
  UPDATE public.metric_source
  SET enabled=NOT enabled,
      value=value+100,
      ordering=-ordering
  WHERE id%17=0"
assert_bag_equal "${aggregate_expected}" \
  "SELECT group_a,group_b,enabled_rows,enabled_sum,
          distinct_pairs,ordered_max
   FROM shiba.aggregate_result" \
  "delete/update rebuild against fresh PostgreSQL"
assert_bag_equal "${chain_expected}" \
  "SELECT group_a,group_b,enabled_sum
   FROM shiba.aggregate_distinct_chain" \
  "chained delete/update rebuild"

# Crash after a committed continuation: replacement Runtime resumes its exact
# keyset cursor and produces the same fresh result once.
runtime_before="$(runtime_pid)"
psql_stateful -qc "
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_after_commit',
    ${runtime_before},
    ${aggregate_result_oid}::oid,
    ${aggregate_stage},
    1200
  );
  INSERT INTO public.metric_source VALUES(1001,1,'g-1',9,1,true)"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the committed Aggregate continuation"
wait_for_runtime_replacement "${runtime_before}"
assert_bag_equal "${aggregate_expected}" \
  "SELECT group_a,group_b,enabled_rows,enabled_sum,
          distinct_pairs,ordered_max
   FROM shiba.aggregate_result" \
  "post-commit Aggregate recovery"

# Crash before commit rolls state, cursor, output payload, and checkpoint back
# together. Retry must not duplicate either side of an old->new result delta.
runtime_before="$(runtime_pid)"
revision_before="$(psql_stateful -Atqc "
  SELECT revision
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${aggregate_result_oid}::oid
    AND stage_id=${aggregate_stage}")"
psql_stateful -qc "
  UPDATE public.shiba_runtime_failpoints
  SET kind='operator_step_before_commit',
      runtime_pid=${runtime_before},
      fired=false,
      pause_ms=1200
  WHERE kind='operator_step_after_commit';
  UPDATE public.metric_source SET value=value+1 WHERE id=1001"
wait_for_log \
  "operator_step_before_commit result ${aggregate_result_oid} stage ${aggregate_stage}" \
  "the pre-commit Aggregate crash"
wait_for_runtime_replacement "${runtime_before}"
assert_query "t" "
  SELECT revision>=${revision_before}
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${aggregate_result_oid}::oid
    AND stage_id=${aggregate_stage}"
assert_bag_equal "${aggregate_expected}" \
  "SELECT group_a,group_b,enabled_rows,enabled_sum,
          distinct_pairs,ordered_max
   FROM shiba.aggregate_result" \
  "pre-commit rollback and retry"

# A fresh Aggregate sees four-row input chunks but has a 64-row admission
# quantum. Stop after its first committed Apply: the input chunk is already
# released and GC-able even though the dirty global group has not been rebuilt.
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_rows='64'"
psql_stateful -qc "SELECT pg_reload_conf()"
wait_for_query "64" \
  "SELECT current_setting('shiba.batch_rows')" \
  "the Aggregate Apply admission budget"
psql_stateful -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  BEGIN;
  CREATE TABLE shiba.aggregate_batch_result AS
  SELECT count(*) AS row_count,
         max(id ORDER BY id) AS max_id
  FROM public.aggregate_batch_source;
  UPDATE shiba_internal.effect_streams
  SET target_chunk_rows=4
  WHERE stream_id IN (
    SELECT stream_id
    FROM shiba_internal.effect_stream_consumers
    WHERE result_oid='shiba.aggregate_batch_result'::regclass
  );
  INSERT INTO public.shiba_runtime_failpoints(
    kind,result_oid,stage_id,pause_ms
  )
  SELECT 'operator_step_after_commit',
         'shiba.aggregate_batch_result'::regclass,
         stage.ordinality-1,
         3000
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid='shiba.aggregate_batch_result'::regclass
    AND stage.value->'spec'->>'operator'='aggregate';
  COMMIT"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the first committed Aggregate Apply"
batch_runtime_pid="$(psql_stateful -Atqc "
  SELECT runtime_pid
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'")"
batch_result_oid="$(psql_stateful -Atqc "
  SELECT 'shiba.aggregate_batch_result'::regclass::oid::integer")"
batch_aggregate_stage="$(psql_stateful -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${batch_result_oid}::oid
    AND stage.value->'spec'->>'operator'='aggregate'")"
batch_continuation="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_continuation_relations
  WHERE result_oid=${batch_result_oid}::oid
    AND stage_id=${batch_aggregate_stage}")"
batch_groups_state="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${batch_result_oid}::oid
    AND stage_id=${batch_aggregate_stage}
    AND state_slot=1")"
batch_first_work_state="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${batch_result_oid}::oid
    AND stage_id=${batch_aggregate_stage}
    AND state_slot=2")"
batch_second_work_state="$(psql_stateful -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${batch_result_oid}::oid
    AND stage_id=${batch_aggregate_stage}
    AND state_slot=3")"
batch_input_stream="$(psql_stateful -Atqc "
  SELECT stream_id
  FROM shiba_internal.effect_stream_consumers
  WHERE result_oid=${batch_result_oid}::oid
    AND consumer_stage_id=${batch_aggregate_stage}
    AND input_port=0")"
psql_stateful -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${batch_result_oid}::oid"
wait_for_runtime_replacement "${batch_runtime_pid}"
assert_query "4|false|0|2" "
  SELECT checkpoint.admitted_rows
         || '|' || checkpoint.has_continuation
         || '|' || (SELECT count(*) FROM ${batch_continuation})
         || '|' || consumer.next_chunk_seq
  FROM shiba_internal.operator_checkpoints AS checkpoint
  JOIN shiba_internal.effect_stream_consumers AS consumer
    ON consumer.result_oid=checkpoint.result_oid
   AND consumer.consumer_stage_id=checkpoint.stage_id
   AND consumer.input_port=0
  WHERE checkpoint.result_oid=${batch_result_oid}::oid
    AND checkpoint.stage_id=${batch_aggregate_stage}"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks
  WHERE stream_id=${batch_input_stream}
    AND chunk_seq=1" \
  "GC of the first Aggregate input chunk after Apply"

# The next full input chunk crosses the lowered shared batch budget. Its Apply
# commit advances the consumer and persists a phase-2 continuation with no
# input chunk reference. Killing that Runtime cannot make Drain depend on
# GC'd input.
psql_stateful -qc "ALTER SYSTEM SET shiba.batch_rows='5'"
psql_stateful -qc "SELECT pg_reload_conf()"
wait_for_query "5" "
  SELECT setting::integer
  FROM pg_settings
  WHERE name='shiba.batch_rows'" \
  "the temporary Aggregate batch budget"
drain_runtime_pid="$(runtime_pid)"
psql_stateful -qc "
  UPDATE public.shiba_runtime_failpoints
  SET runtime_pid=${drain_runtime_pid},
      fired=false,
      pause_ms=3000
  WHERE kind='operator_step_after_commit';
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${batch_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_query "t" "
  SELECT fired AND runtime_pid=${drain_runtime_pid}
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the Aggregate Apply-to-Drain cutover"
psql_stateful -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${batch_result_oid}::oid"
wait_for_runtime_replacement "${drain_runtime_pid}"
assert_query "2|true|true|8|3" "
  SELECT continuation.phase
         || '|' || (continuation.input_chunk_seq IS NULL)
         || '|' || (continuation.input_row_ordinal IS NULL)
         || '|' || checkpoint.admitted_rows
         || '|' || consumer.next_chunk_seq
  FROM ${batch_continuation} AS continuation
  JOIN shiba_internal.operator_checkpoints AS checkpoint
    ON checkpoint.result_oid=${batch_result_oid}::oid
   AND checkpoint.stage_id=${batch_aggregate_stage}
  JOIN shiba_internal.effect_stream_consumers AS consumer
    ON consumer.result_oid=checkpoint.result_oid
   AND consumer.consumer_stage_id=checkpoint.stage_id
   AND consumer.input_port=0"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks
  WHERE stream_id=${batch_input_stream}
    AND chunk_seq<3" \
  "GC of every Aggregate input chunk consumed before pure Drain"

psql_stateful -qc "ALTER SYSTEM SET shiba.batch_rows='4'"
psql_stateful -qc "SELECT pg_reload_conf()"
psql_stateful -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${batch_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_query "16|16" "
  SELECT row_count || '|' || max_id
  FROM shiba.aggregate_batch_result" \
  "Aggregate pure-Drain crash recovery"
assert_query "1|0|0" "
  SELECT (SELECT count(*) FROM ${batch_groups_state})
         || '|' || (SELECT count(*) FROM ${batch_first_work_state})
         || '|' || (SELECT count(*) FROM ${batch_second_work_state})"
wait_for_query "0|0|false" "
  SELECT admitted_rows || '|' || admitted_bytes || '|' || has_continuation
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${batch_result_oid}::oid
    AND stage_id=${batch_aggregate_stage}" \
  "clean Aggregate admission state"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks AS chunk
  JOIN shiba_internal.effect_stream_consumers AS consumer
    ON consumer.stream_id=chunk.stream_id
  WHERE consumer.result_oid=${batch_result_oid}::oid
    AND consumer.consumer_stage_id=${batch_aggregate_stage}
    AND consumer.input_port=0
    AND chunk.chunk_seq<consumer.next_chunk_seq" \
  "GC of all consumed Aggregate input chunks"

# Force one output chunk to cross the high watermark. The Aggregate must keep
# its typed continuation pinned until the downstream Distinct/Sink drains it.
psql_stateful -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  UPDATE shiba_internal.effect_streams
  SET high_chunks=1,
      high_rows=target_chunk_rows,
      high_bytes=target_chunk_bytes,
      low_chunks=0,
      low_rows=0,
      low_bytes=0
  WHERE producer_kind='operator'
    AND producer_result_oid='shiba.aggregate_distinct_chain'::regclass
    AND producer_stage_id=(
      SELECT stage.ordinality-1
      FROM shiba_internal.dataflows AS dataflow
      CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
        WITH ORDINALITY AS stage(value,ordinality)
      WHERE dataflow.result_oid='shiba.aggregate_distinct_chain'::regclass
        AND stage.value->'spec'->>'operator'='aggregate'
    );
  INSERT INTO public.metric_source VALUES(1002,2,'g-2',33,2,true)"
assert_bag_equal "${chain_expected}" \
  "SELECT group_a,group_b,enabled_sum
   FROM shiba.aggregate_distinct_chain" \
  "backpressured Aggregate -> Distinct -> Sink"
wait_for_query "f" "
  SELECT bool_or(backpressured)
  FROM shiba_internal.effect_streams
  WHERE producer_kind='operator'
    AND producer_result_oid='shiba.aggregate_distinct_chain'::regclass" \
  "the chained streams to drain below their low watermark"

printf '%s\n' \
  "generic Aggregate/Distinct bounded recovery gate passed"
