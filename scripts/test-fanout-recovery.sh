#!/usr/bin/env bash
set -euo pipefail

# End-to-end gate for the generic resumable Join kernel.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! grep -Fq 'FILTER (WHERE ({condition}) IS TRUE)' \
  "${project_root}/src/execution/join/runtime.rs" ||
   ! grep -Fq 'FILTER (WHERE ({condition}) IS NULL)' \
  "${project_root}/src/execution/join/runtime.rs"; then
  printf '%s\n' \
    'Join static gate failed: generic theta own-state counts do not share filtered scan' >&2
  exit 1
fi

pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-fanout-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-fanout-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_FANOUT_TEST_PORT:-$((62000 + $$ % 2000))}"
database_name="shiba_fanout"
fanout_width="${SHIBA_FANOUT_WIDTH:-16}"
wait_attempts="${SHIBA_FANOUT_WAIT_ATTEMPTS:-1200}"

psql_fanout() {
  PGOPTIONS="-c statement_timeout=60000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

test_name="fanout/recovery gate"
test_psql_command=psql_fanout
test_log_lines=200
test_wait_attempts="${wait_attempts}"
test_wait_sleep=0.05
test_retain_log=1
source "${project_root}/scripts/test-lib.sh"
trap cleanup EXIT

if ! [[ "${fanout_width}" =~ ^[1-9][0-9]*$ ]] ||
   test "${fanout_width}" -lt 8; then
  fail "SHIBA_FANOUT_WIDTH must be an integer >= 8"
fi
if ! [[ "${wait_attempts}" =~ ^[1-9][0-9]*$ ]]; then
  fail "SHIBA_FANOUT_WAIT_ATTEMPTS must be a positive integer"
fi

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
  printf "shiba.batch_bytes = 512\n"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "$(id -un)"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_fanout -qc "CREATE EXTENSION shiba"
psql_fanout -qc "SELECT shiba.activate()"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'" \
  "the singleton Runtime"

psql_fanout -qc "
  CREATE TABLE public.shiba_runtime_failpoints (
    kind text PRIMARY KEY,
    runtime_pid integer,
    result_oid oid,
    stage_id integer,
    commit_lsn pg_lsn,
    pause_ms integer NOT NULL DEFAULT 0 CHECK (pause_ms>=0),
    fired boolean NOT NULL DEFAULT false
  );

  CREATE TABLE public.chain_fact (
    fact_id integer PRIMARY KEY,
    first_key integer NOT NULL,
    payload integer NOT NULL
  );
  CREATE TABLE public.chain_first (
    first_id integer PRIMARY KEY,
    first_key integer NOT NULL,
    second_key integer NOT NULL
  );
  CREATE TABLE public.chain_second (
    second_id integer PRIMARY KEY,
    second_key integer NOT NULL,
    label integer NOT NULL
  );
  INSERT INTO public.chain_fact VALUES (0,1,3);
  INSERT INTO public.chain_first
  SELECT value,1,1
  FROM generate_series(1,${fanout_width}) AS value;
  INSERT INTO public.chain_second
  SELECT value,1,value*10
  FROM generate_series(1,${fanout_width}) AS value"

# Registration snapshots all three nonempty inputs into typed Scan spools.
# Those rows must traverse the same bounded streams and build both Join
# arrangements before later WAL effects can be correct.
psql_fanout -qc "
  CREATE TABLE shiba.chain_result AS
  SELECT fact.fact_id,
         first_side.first_id,
         second_side.second_id,
         fact.payload,
         second_side.label
  FROM public.chain_fact AS fact
  JOIN public.chain_first AS first_side
    ON first_side.first_key=fact.first_key
  JOIN public.chain_second AS second_side
    ON second_side.second_key=first_side.second_key"

chain_result_oid="$(psql_fanout -Atqc "
  SELECT 'shiba.chain_result'::regclass::oid::integer")"
first_join_stage="$(psql_fanout -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${chain_result_oid}::oid
    AND stage.value->'spec'->>'operator'='join'
  ORDER BY stage.ordinality
  LIMIT 1")"
second_join_stage="$(psql_fanout -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${chain_result_oid}::oid
    AND stage.value->'spec'->>'operator'='join'
  ORDER BY stage.ordinality DESC
  LIMIT 1")"
first_join_right_state="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.operator_state_relations AS catalog
  JOIN pg_class AS relation ON relation.oid=catalog.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE catalog.result_oid=${chain_result_oid}::oid
    AND catalog.stage_id=${first_join_stage}
    AND catalog.state_slot=1")"
first_join_left_state="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.operator_state_relations AS catalog
  JOIN pg_class AS relation ON relation.oid=catalog.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE catalog.result_oid=${chain_result_oid}::oid
    AND catalog.stage_id=${first_join_stage}
    AND catalog.state_slot=0")"
second_join_right_state="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.operator_state_relations AS catalog
  JOIN pg_class AS relation ON relation.oid=catalog.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE catalog.result_oid=${chain_result_oid}::oid
    AND catalog.stage_id=${second_join_stage}
    AND catalog.state_slot=1")"
wait_for_query "${fanout_width}" \
  "SELECT count(*) FROM ${first_join_right_state}" \
  "the first Join's streamed right arrangement"
wait_for_query "${fanout_width}" \
  "SELECT count(*) FROM ${second_join_right_state}" \
  "the second Join's streamed right arrangement"
wait_for_query "$((fanout_width * fanout_width))" \
  "SELECT count(*) FROM shiba.chain_result WHERE fact_id=0" \
  "the nonempty three-source bootstrap through both Joins"
assert_query "2" "
  SELECT count(*)
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${chain_result_oid}::oid
    AND stage_id=${first_join_stage}"
assert_query "2" "
  SELECT count(*)
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${chain_result_oid}::oid
    AND stage_id=${second_join_stage}"
assert_query "0" "
  WITH keyed_join AS (
    SELECT dataflow.result_oid,stage.ordinality-1 AS stage_id
    FROM shiba_internal.dataflows AS dataflow
    CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
      WITH ORDINALITY AS stage(value,ordinality)
    WHERE stage.value->'spec'->>'operator'='join'
      AND jsonb_array_length(stage.value->'spec'->'config'->'equi_keys') > 0
  ), keyed_state AS (
    SELECT catalog.relation_oid
    FROM keyed_join
    JOIN shiba_internal.operator_state_relations AS catalog
      ON catalog.result_oid=keyed_join.result_oid
     AND catalog.stage_id=keyed_join.stage_id
     AND catalog.state_slot IN (0,1)
  )
  SELECT count(*)
  FROM keyed_state
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_index AS lookup_index
    WHERE lookup_index.indrelid=keyed_state.relation_oid
      AND lookup_index.indnkeyatts=2
      AND lookup_index.indnatts=2
      AND lookup_index.indkey[0]=4
      AND lookup_index.indkey[1]=1
      AND lookup_index.indisvalid
      AND lookup_index.indisready
      AND lookup_index.indislive
      AND lookup_index.indexprs IS NULL
      AND lookup_index.indpred IS NULL
  )"
assert_query "0" "
  WITH join_state AS (
    SELECT catalog.relation_oid
    FROM shiba_internal.operator_state_relations AS catalog
    JOIN shiba_internal.dataflows AS dataflow
      ON dataflow.result_oid=catalog.result_oid
    CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
      WITH ORDINALITY AS stage(value,ordinality)
    WHERE catalog.result_oid=${chain_result_oid}::oid
      AND stage.ordinality-1=catalog.stage_id
      AND catalog.state_slot IN (0,1)
      AND stage.value->'spec'->>'operator'='join'
  ),
  identity_column AS (
    SELECT state.relation_oid,attribute.attnum
    FROM join_state AS state
    LEFT JOIN pg_catalog.pg_attribute AS attribute
      ON attribute.attrelid=state.relation_oid
     AND attribute.attname='row_key'
     AND attribute.atttypid='bytea'::regtype
     AND attribute.attnotnull
     AND NOT attribute.attisdropped
  )
  SELECT count(*)
  FROM identity_column AS identity
  WHERE identity.attnum IS NULL
     OR NOT EXISTS (
       SELECT 1
       FROM pg_catalog.pg_index AS identity_index
       WHERE identity_index.indrelid=identity.relation_oid
         AND identity_index.indisunique
         AND identity_index.indisvalid
         AND identity_index.indisready
         AND identity_index.indislive
         AND identity_index.indnkeyatts=1
         AND identity_index.indnatts=1
         AND identity_index.indkey[0]=identity.attnum
         AND identity_index.indexprs IS NULL
         AND identity_index.indpred IS NULL
     )"
assert_query "2|2" "
  SELECT
    count(*) FILTER (
      WHERE stage.value->'spec'->>'operator'='join'
    )
    || '|' ||
    max(jsonb_array_length(stage.value->'inputs')) FILTER (
      WHERE stage.value->'spec'->>'operator'='join'
    )
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    AS stage(value)
  WHERE dataflow.result_oid=${chain_result_oid}::oid"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.operator_checkpoints AS checkpoint
  WHERE checkpoint.result_oid=${chain_result_oid}::oid
    AND (
      checkpoint.has_continuation
      OR EXISTS (
        SELECT 1
        FROM shiba_internal.effect_stream_consumers AS consumer
        JOIN shiba_internal.effect_streams AS input
          ON input.stream_id=consumer.stream_id
        WHERE consumer.result_oid=checkpoint.result_oid
          AND consumer.consumer_stage_id=checkpoint.stage_id
          AND (
            consumer.next_chunk_seq<input.next_chunk_seq
            OR (
              input.producer_kind='source'
              AND EXISTS (
                SELECT 1
                FROM shiba_internal.ingress_replay_state AS replay
                WHERE replay.slot_generation=input.slot_generation
                  AND replay.published_lsn IS NOT NULL
                  AND consumer.consumed_frontier_lsn<replay.published_lsn
              )
            )
          )
      )
    )" \
  "the bootstrapped Join chain to become durably idle"

first_join_stream="$(psql_fanout -Atqc "
  SELECT stream_id
  FROM shiba_internal.effect_streams
  WHERE producer_kind='operator'
    AND producer_result_oid=${chain_result_oid}::oid
    AND producer_stage_id=${first_join_stage}")"
first_join_payload="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.effect_streams AS stream
  JOIN pg_class AS relation ON relation.oid=stream.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE stream.stream_id=${first_join_stream}")"

# One appended row is enough to cross the first Join's high watermark. The
# shared stream keeps one payload regardless of how many downstream consumers.
psql_fanout -qc "
  UPDATE shiba_internal.effect_streams
  SET high_chunks=1,
      high_rows=target_chunk_rows,
      high_bytes=target_chunk_bytes,
      low_chunks=0,
      low_rows=0,
      low_bytes=0
  WHERE producer_kind='operator'
    AND producer_result_oid=${chain_result_oid}::oid
    AND producer_stage_id=${first_join_stage}"

# Crash after the input-row owner commits. While that Runtime is paused, stop
# this DAG so its replacement cannot race the continuation assertions.
first_runtime_pid="$(runtime_pid)"
psql_fanout -qc "
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_after_commit',
    ${first_runtime_pid},
    ${chain_result_oid}::oid,
    ${first_join_stage},
    1200
  );
  INSERT INTO public.chain_fact VALUES (1,1,7)"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the durable input continuation"
psql_fanout -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${chain_result_oid}::oid"
wait_for_runtime_replacement "${first_runtime_pid}"

# Read the typed continuation by its cataloged live name. This test resolves
# the name from the OID just as lifecycle code does; it never guesses it.
continuation_relation="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.operator_continuation_relations AS catalog
  JOIN pg_class AS relation ON relation.oid=catalog.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE catalog.result_oid=${chain_result_oid}::oid
    AND catalog.stage_id=${first_join_stage}")"
# One quantum crosses open, preflight, and the first bounded candidate page.
# The durable phase is therefore the resume point selected by the row/byte
# budget, not an implementation phase forced into its own transaction.
assert_query "2|true" "
  SELECT phase || '|' || (candidate_after>0)
  FROM ${continuation_relation}"

# The next step builds a four-row action chunk and advances four candidates,
# but Runtime dies before commit. Payload, chunk metadata, arrangement state,
# continuation cursor, and checkpoint must all roll back.
before_batch_chunk_seq="$(psql_fanout -Atqc "
  SELECT next_chunk_seq
  FROM shiba_internal.effect_streams
  WHERE stream_id=${first_join_stream}")"
before_batch_revision="$(psql_fanout -Atqc "
  SELECT revision
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${chain_result_oid}::oid
    AND stage_id=${first_join_stage}")"
before_batch_state="$(psql_fanout -Atqc "
  SELECT next_chunk_seq || '|' ||
         (SELECT revision
          FROM shiba_internal.operator_checkpoints
          WHERE result_oid=${chain_result_oid}::oid
            AND stage_id=${first_join_stage}) || '|' ||
         coalesce((SELECT candidate_after::text
                   FROM ${continuation_relation}),'')
  FROM shiba_internal.effect_streams
  WHERE stream_id=${first_join_stream}")"
second_runtime_pid="$(runtime_pid)"
psql_fanout -qc "
  UPDATE public.shiba_runtime_failpoints
  SET kind='operator_step_before_commit',
      runtime_pid=${second_runtime_pid},
      fired=false,
      pause_ms=1200
  WHERE kind='operator_step_after_commit';
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${chain_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_log \
  "operator_step_before_commit result ${chain_result_oid} stage ${first_join_stage}" \
  "the pre-commit Join action-batch crash"
psql_fanout -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${chain_result_oid}::oid"
wait_for_runtime_replacement "${second_runtime_pid}"
assert_query "2" "SELECT phase FROM ${continuation_relation}"
assert_query "${before_batch_state}" "
  SELECT next_chunk_seq || '|' ||
         (SELECT revision
          FROM shiba_internal.operator_checkpoints
          WHERE result_oid=${chain_result_oid}::oid
            AND stage_id=${first_join_stage}) || '|' ||
         coalesce((SELECT candidate_after::text
                   FROM ${continuation_relation}),'')
  FROM shiba_internal.effect_streams
  WHERE stream_id=${first_join_stream}"

# Retry the identical bounded prefix and crash after commit. Exactly one
# four-row chunk is now durable and the continuation cursor advanced once.
third_runtime_pid="$(runtime_pid)"
psql_fanout -qc "
  UPDATE public.shiba_runtime_failpoints
  SET kind='operator_step_after_commit',
      runtime_pid=${third_runtime_pid},
      fired=false,
      pause_ms=1200
  WHERE kind='operator_step_before_commit';
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${chain_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_query "t" "
  SELECT fired AND runtime_pid=${third_runtime_pid}
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the committed Join action batch"
assert_query "2|true" "
  SELECT phase || '|' || (candidate_after>0)
  FROM ${continuation_relation}"
assert_query "4" "
  SELECT row_count
  FROM shiba_internal.effect_stream_chunks
  WHERE stream_id=${first_join_stream}
    AND chunk_seq=${before_batch_chunk_seq}"
assert_query "$((before_batch_revision + 1))" "
  SELECT revision
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${chain_result_oid}::oid
    AND stage_id=${first_join_stage}"
assert_query "t" "
  SELECT backpressured
  FROM shiba_internal.effect_streams
  WHERE stream_id=${first_join_stream}"
psql_fanout -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${chain_result_oid}::oid"
wait_for_runtime_replacement "${third_runtime_pid}"
psql_fanout -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${chain_result_oid}::oid"

chain_expected="
  SELECT fact.fact_id,
         first_side.first_id,
         second_side.second_id,
         fact.payload,
         second_side.label
  FROM public.chain_fact AS fact
  JOIN public.chain_first AS first_side
    ON first_side.first_key=fact.first_key
  JOIN public.chain_second AS second_side
    ON second_side.second_key=first_side.second_key"
assert_bag_equal "${chain_expected}" \
  "SELECT fact_id,first_id,second_id,payload,label
   FROM shiba.chain_result" \
  "the recovered Join -> Join -> Sink fanout"
assert_query "$((fanout_width * fanout_width))" \
  "SELECT count(*) FROM shiba.chain_result WHERE fact_id=1"
wait_for_query "f" "
  SELECT backpressured
  FROM shiba_internal.effect_streams
  WHERE stream_id=${first_join_stream}" \
  "the low watermark to release backpressure"
assert_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks
  WHERE row_count>4
     OR (payload_bytes>512 AND row_count<>1)"

# Deletion and reinsertion must retract and recreate the entire chained
# fanout, including weights that traverse a second Join.
psql_fanout -qc "DELETE FROM public.chain_fact WHERE fact_id=1"
assert_bag_equal "${chain_expected}" \
  "SELECT fact_id,first_id,second_id,payload,label
   FROM shiba.chain_result" \
  "the chained fanout deletion"
psql_fanout -qc "INSERT INTO public.chain_fact VALUES (1,1,9)"
assert_bag_equal "${chain_expected}" \
  "SELECT fact_id,first_id,second_id,payload,label
   FROM shiba.chain_result" \
  "the chained fanout reinsertion"

# Backpressure is a no-op boundary, not a partial operator step. Let a real
# source effect reach the first Join, pin its output as backpressured, then
# call the dispatcher while holding the DAG lock in this session. Every
# Join-owned durable object must remain byte-for-byte at the same snapshot.
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.operator_checkpoints AS checkpoint
  WHERE checkpoint.result_oid=${chain_result_oid}::oid
    AND (
      checkpoint.has_continuation
      OR EXISTS (
        SELECT 1
        FROM shiba_internal.effect_stream_consumers AS consumer
        JOIN shiba_internal.effect_streams AS input
          ON input.stream_id=consumer.stream_id
        WHERE consumer.result_oid=checkpoint.result_oid
          AND consumer.consumer_stage_id=checkpoint.stage_id
          AND (
            consumer.next_chunk_seq<input.next_chunk_seq
            OR (
              input.producer_kind='source'
              AND EXISTS (
                SELECT 1
                FROM shiba_internal.ingress_replay_state AS replay
                WHERE replay.slot_generation=input.slot_generation
                  AND replay.published_lsn IS NOT NULL
                  AND consumer.consumed_frontier_lsn<replay.published_lsn
              )
            )
          )
      )
    )" \
  "the Join chain to become idle before forced backpressure"
wait_for_query "0|0|0" "
  SELECT buffered_chunks || '|' || buffered_rows || '|' || buffered_bytes
  FROM shiba_internal.effect_streams
  WHERE stream_id=${first_join_stream}" \
  "the first Join output to drain before forced backpressure"
psql_fanout -qc "
  UPDATE shiba_internal.effect_streams
  SET backpressured=true
  WHERE stream_id=${first_join_stream};
  INSERT INTO public.chain_fact VALUES (2,1,11)"
wait_for_query "t" "
  SELECT EXISTS (
    SELECT 1
    FROM shiba_internal.effect_stream_consumers AS consumer
    JOIN shiba_internal.effect_streams AS input
      ON input.stream_id=consumer.stream_id
    WHERE consumer.result_oid=${chain_result_oid}::oid
      AND consumer.consumer_stage_id=${first_join_stage}
      AND consumer.next_chunk_seq<input.next_chunk_seq
  )" \
  "a production effect to reach the backpressured Join input"

join_backpressure_snapshot() {
  psql_fanout -Atqc "
    SELECT concat_ws(
      '#',
      output.next_chunk_seq::text,
      coalesce(output.latest_data_lsn::text,''),
      coalesce(output.published_frontier_lsn::text,''),
      output.buffered_chunks::text,
      output.buffered_rows::text,
      output.buffered_bytes::text,
      output.backpressured::text,
      checkpoint.revision::text,
      checkpoint.has_continuation::text,
      (
        SELECT md5(coalesce(string_agg(
          to_jsonb(state_row)::text,',' ORDER BY state_row.row_id
        ),''))
        FROM ${first_join_left_state} AS state_row
      ),
      (
        SELECT md5(coalesce(string_agg(
          to_jsonb(state_row)::text,',' ORDER BY state_row.row_id
        ),''))
        FROM ${first_join_right_state} AS state_row
      ),
      (
        SELECT md5(coalesce(string_agg(
          to_jsonb(continuation)::text,',' ORDER BY continuation.singleton
        ),''))
        FROM ${continuation_relation} AS continuation
      ),
      (
        SELECT md5(coalesce(string_agg(
          to_jsonb(consumer)::text,','
          ORDER BY consumer.stream_id,consumer.input_port
        ),''))
        FROM shiba_internal.effect_stream_consumers AS consumer
        WHERE consumer.result_oid=${chain_result_oid}::oid
          AND consumer.consumer_stage_id=${first_join_stage}
      ),
      (
        SELECT md5(coalesce(string_agg(
          to_jsonb(chunk)::text,',' ORDER BY chunk.chunk_seq
        ),''))
        FROM shiba_internal.effect_stream_chunks AS chunk
        WHERE chunk.stream_id=${first_join_stream}
      ),
      (
        SELECT md5(coalesce(string_agg(
          to_jsonb(payload_row)::text,','
          ORDER BY payload_row.chunk_seq,payload_row.row_ordinal
        ),''))
        FROM ${first_join_payload} AS payload_row
        WHERE payload_row.stream_id=${first_join_stream}
      )
    )
    FROM shiba_internal.effect_streams AS output
    JOIN shiba_internal.operator_checkpoints AS checkpoint
      ON checkpoint.result_oid=${chain_result_oid}::oid
     AND checkpoint.stage_id=${first_join_stage}
    WHERE output.stream_id=${first_join_stream}"
}

before_forced_backpressure="$(join_backpressure_snapshot)"
sleep 1
after_forced_backpressure="$(join_backpressure_snapshot)"
if test "${after_forced_backpressure}" != "${before_forced_backpressure}"; then
  fail "backpressured Join step changed durable effect/state/cursor/checkpoint"
fi
psql_fanout -qc "
  UPDATE shiba_internal.effect_streams
  SET backpressured=false
  WHERE stream_id=${first_join_stream}"
assert_bag_equal "${chain_expected}" \
  "SELECT fact_id,first_id,second_id,payload,label
   FROM shiba.chain_result" \
  "the Join to resume after forced backpressure"
psql_fanout -qc "DELETE FROM public.chain_fact WHERE fact_id=2"
assert_bag_equal "${chain_expected}" \
  "SELECT fact_id,first_id,second_id,payload,label
   FROM shiba.chain_result" \
  "the forced-backpressure effect cleanup"

# A large opposite arrangement with no matching row exercises the bounded
# keyset scan even though it emits no output.
psql_fanout -qc "
  CREATE TABLE public.theta_left (
    id integer PRIMARY KEY,
    value integer NOT NULL
  );
  CREATE TABLE public.theta_right (
    id integer PRIMARY KEY,
    low integer NOT NULL,
    high integer NOT NULL
  );
  CREATE TABLE shiba.theta_result AS
  SELECT left_side.id AS left_id,right_side.id AS right_id
  FROM public.theta_left AS left_side
  JOIN public.theta_right AS right_side
    ON left_side.value>right_side.low
   AND left_side.value<right_side.high"
psql_fanout -qc "
  INSERT INTO public.theta_right
  SELECT value,value*10,value*10+2
  FROM generate_series(1,80) AS value;
  INSERT INTO public.theta_left VALUES (1,-100)"
assert_bag_equal \
  "SELECT left_side.id AS left_id,right_side.id AS right_id
   FROM public.theta_left AS left_side
   JOIN public.theta_right AS right_side
     ON left_side.value>right_side.low
    AND left_side.value<right_side.high" \
  "SELECT left_id,right_id FROM shiba.theta_result" \
  "the all-FALSE bounded theta scan"
wait_for_query "t" "
  SELECT checkpoint.revision>20
  FROM shiba_internal.operator_checkpoints AS checkpoint
  JOIN shiba_internal.dataflows AS dataflow
    ON dataflow.result_oid=checkpoint.result_oid
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid='shiba.theta_result'::regclass
    AND stage.ordinality-1=checkpoint.stage_id
    AND stage.value->'spec'->>'operator'='join'" \
  "the bounded no-match keyset scan to require many checkpoints"

# Binary COPY preserves float NaN payload bits, while pgoutput sends both
# values as the text "NaN". Join must canonicalize its bootstrap and WAL keys
# through the same named-composite text roundtrip. Both the inner and anti
# arrangements therefore keep one bag entry with multiplicity two, and one
# physical-row delete must decrement that entry exactly once.
psql_fanout -qc "
  CREATE TABLE public.nan_identity_left (
    value double precision NOT NULL
  );
  CREATE TABLE public.nan_identity_right (
    value double precision NOT NULL,
    kind integer NOT NULL
  )"
python3 - <<'PY' | psql_fanout -qc \
  "COPY public.nan_identity_left(value) FROM STDIN WITH (FORMAT binary)"
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
assert_query "2|2|true" "
  SELECT count(*) || '|' ||
         count(DISTINCT pg_catalog.encode(
           pg_catalog.float8send(value),'hex'
         )) || '|' ||
         bool_and(value='NaN'::double precision)
  FROM public.nan_identity_left"
psql_fanout -qc "
  INSERT INTO public.nan_identity_right VALUES ('NaN',0);
  CREATE TABLE shiba.nan_inner_result AS
  SELECT left_side.value AS left_value,
         right_side.value AS right_value
  FROM public.nan_identity_left AS left_side
  JOIN public.nan_identity_right AS right_side
    ON left_side.value=right_side.value;
  CREATE TABLE shiba.nan_anti_result AS
  SELECT left_side.value AS left_value
  FROM public.nan_identity_left AS left_side
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.nan_identity_right AS right_side
    WHERE right_side.value=left_side.value
      AND right_side.kind=1
  )"
nan_inner_oid="$(psql_fanout -Atqc "
  SELECT 'shiba.nan_inner_result'::regclass::oid::integer")"
nan_anti_oid="$(psql_fanout -Atqc "
  SELECT 'shiba.nan_anti_result'::regclass::oid::integer")"
nan_inner_join_stage="$(psql_fanout -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${nan_inner_oid}::oid
    AND stage.value->'spec'->>'operator'='join'")"
nan_anti_join_stage="$(psql_fanout -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${nan_anti_oid}::oid
    AND stage.value->'spec'->>'operator'='join'")"
nan_inner_left_state="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.operator_state_relations AS catalog
  JOIN pg_class AS relation ON relation.oid=catalog.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE catalog.result_oid=${nan_inner_oid}::oid
    AND catalog.stage_id=${nan_inner_join_stage}
    AND catalog.state_slot=0")"
nan_anti_left_state="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.operator_state_relations AS catalog
  JOIN pg_class AS relation ON relation.oid=catalog.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE catalog.result_oid=${nan_anti_oid}::oid
    AND catalog.stage_id=${nan_anti_join_stage}
    AND catalog.state_slot=0")"
nan_inner_left_type="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,type_catalog.typname)
  FROM pg_attribute AS attribute
  JOIN pg_type AS type_catalog ON type_catalog.oid=attribute.atttypid
  JOIN pg_namespace AS namespace ON namespace.oid=type_catalog.typnamespace
  WHERE attribute.attrelid='${nan_inner_left_state}'::regclass
    AND attribute.attname='row_value'")"
nan_anti_left_type="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,type_catalog.typname)
  FROM pg_attribute AS attribute
  JOIN pg_type AS type_catalog ON type_catalog.oid=attribute.atttypid
  JOIN pg_namespace AS namespace ON namespace.oid=type_catalog.typnamespace
  WHERE attribute.attrelid='${nan_anti_left_state}'::regclass
    AND attribute.attname='row_value'")"
assert_bag_equal "
  SELECT left_side.value AS left_value,
         right_side.value AS right_value
  FROM public.nan_identity_left AS left_side
  JOIN public.nan_identity_right AS right_side
    ON left_side.value=right_side.value" "
  SELECT left_value,right_value
  FROM shiba.nan_inner_result" \
  "canonical NaN inner-Join bootstrap"
assert_bag_equal "
  SELECT left_side.value AS left_value
  FROM public.nan_identity_left AS left_side
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.nan_identity_right AS right_side
    WHERE right_side.value=left_side.value
      AND right_side.kind=1
  )" "
  SELECT left_value
  FROM shiba.nan_anti_result" \
  "canonical NaN anti-Join bootstrap"
wait_for_query "1|2|0" "
  SELECT count(*) || '|' || sum(multiplicity) || '|' ||
         count(*) FILTER (
           WHERE row_key IS DISTINCT FROM
                 pg_catalog.record_send(
                   ((row_value)::text)::${nan_inner_left_type}
                 )
         )
  FROM ${nan_inner_left_state}" \
  "the canonical inner-Join NaN identity"
wait_for_query "1|2|0" "
  SELECT count(*) || '|' || sum(multiplicity) || '|' ||
         count(*) FILTER (
           WHERE row_key IS DISTINCT FROM
                 pg_catalog.record_send(
                   ((row_value)::text)::${nan_anti_left_type}
                 )
         )
  FROM ${nan_anti_left_state}" \
  "the canonical anti-Join NaN identity"
psql_fanout -qc "
  DELETE FROM public.nan_identity_left
  WHERE ctid = (
    SELECT ctid
    FROM public.nan_identity_left
    ORDER BY pg_catalog.encode(pg_catalog.float8send(value),'hex')
    LIMIT 1
  )"
assert_bag_equal "
  SELECT left_side.value AS left_value,
         right_side.value AS right_value
  FROM public.nan_identity_left AS left_side
  JOIN public.nan_identity_right AS right_side
    ON left_side.value=right_side.value" "
  SELECT left_value,right_value
  FROM shiba.nan_inner_result" \
  "the canonical NaN inner-Join delete"
assert_bag_equal "
  SELECT left_side.value AS left_value
  FROM public.nan_identity_left AS left_side
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.nan_identity_right AS right_side
    WHERE right_side.value=left_side.value
      AND right_side.kind=1
  )" "
  SELECT left_value
  FROM shiba.nan_anti_result" \
  "the canonical NaN anti-Join delete"
wait_for_query "1|1" "
  SELECT count(*) || '|' || sum(multiplicity)
  FROM ${nan_inner_left_state}" \
  "the inner-Join NaN delete to decrement one bag entry"
wait_for_query "1|1" "
  SELECT count(*) || '|' || sum(multiplicity)
  FROM ${nan_anti_left_state}" \
  "the anti-Join NaN delete to decrement one bag entry"

# pgoutput array text includes explicit dimensions. Ingress must retain that
# raw per-column text so the live row and its delete address the same
# lower-bound-sensitive identity as the Scan bootstrap row.
psql_fanout -qc "
  CREATE TABLE public.array_identity_left (
    value integer[] NOT NULL
  );
  CREATE TABLE public.array_identity_right (
    value integer[] NOT NULL
  );
  INSERT INTO public.array_identity_left
  VALUES ('[0:1]={10,20}'::integer[]);
  INSERT INTO public.array_identity_right
  VALUES ('[0:1]={10,20}'::integer[]);
  CREATE TABLE shiba.array_identity_result AS
  SELECT left_side.value
  FROM public.array_identity_left AS left_side
  JOIN public.array_identity_right AS right_side
    ON left_side.value=right_side.value"
wait_for_query "1|0|1" "
  SELECT count(*) || '|' || min(array_lower(value,1)) || '|' ||
         max(array_upper(value,1))
  FROM shiba.array_identity_result" \
  "the non-1 array lower bound bootstrap"
psql_fanout -qc "
  INSERT INTO public.array_identity_left
  VALUES ('[0:1]={10,20}'::integer[])"
wait_for_query "2|0|1" "
  SELECT count(*) || '|' || min(array_lower(value,1)) || '|' ||
         max(array_upper(value,1))
  FROM shiba.array_identity_result" \
  "the non-1 array lower bound live insert"
psql_fanout -qc "
  DELETE FROM public.array_identity_left
  WHERE ctid=(
    SELECT ctid
    FROM public.array_identity_left
    ORDER BY ctid
    LIMIT 1
  )"
wait_for_query "1|0|1" "
  SELECT count(*) || '|' || min(array_lower(value,1)) || '|' ||
         max(array_upper(value,1))
  FROM shiba.array_identity_result" \
  "the non-1 array lower bound live delete"

# A shared source stream feeds two Scan consumers, then joins those branches.
# Payload is stored once; fanout is represented only by consumer cursors.
psql_fanout -qc "
  CREATE TABLE public.branch_source (
    id integer PRIMARY KEY,
    group_id integer NOT NULL
  );
  CREATE TABLE shiba.branch_fanin AS
  SELECT left_side.id AS left_id,right_side.id AS right_id
  FROM public.branch_source AS left_side
  JOIN public.branch_source AS right_side
    ON left_side.group_id=right_side.group_id
   AND left_side.id<right_side.id;
  INSERT INTO public.branch_source VALUES (1,7),(2,7),(3,8)"
assert_bag_equal \
  "SELECT left_side.id AS left_id,right_side.id AS right_id
   FROM public.branch_source AS left_side
   JOIN public.branch_source AS right_side
     ON left_side.group_id=right_side.group_id
    AND left_side.id<right_side.id" \
  "SELECT left_id,right_id FROM shiba.branch_fanin" \
  "the shared-source branch fan-in"
assert_query "1|2" "
  SELECT count(DISTINCT stream.stream_id) || '|' || count(consumer.*)
  FROM shiba_internal.effect_streams AS stream
  JOIN shiba_internal.effect_stream_consumers AS consumer
    ON consumer.stream_id=stream.stream_id
  WHERE stream.producer_kind='source'
    AND stream.source_oid='public.branch_source'::regclass
    AND consumer.result_oid='shiba.branch_fanin'::regclass"

# Outer joins, semi/anti joins, and NOT IN use one generic condition evaluator.
# NULL-aware anti tracks TRUE and UNKNOWN counts independently.
psql_fanout -qc "
  CREATE TABLE public.join_left (
    id integer PRIMARY KEY,
    key integer
  );
  CREATE TABLE public.join_right (
    id integer PRIMARY KEY,
    key integer
  );

  CREATE TABLE shiba.left_join_result AS
  SELECT left_side.id AS left_id,right_side.id AS right_id
  FROM public.join_left AS left_side
  LEFT JOIN public.join_right AS right_side
    ON left_side.key=right_side.key;
  CREATE TABLE shiba.right_join_result AS
  SELECT left_side.id AS left_id,right_side.id AS right_id
  FROM public.join_left AS left_side
  RIGHT JOIN public.join_right AS right_side
    ON left_side.key=right_side.key;
  CREATE TABLE shiba.full_join_result AS
  SELECT left_side.id AS left_id,right_side.id AS right_id
  FROM public.join_left AS left_side
  FULL JOIN public.join_right AS right_side
    ON left_side.key=right_side.key;
  CREATE TABLE shiba.semi_join_result AS
  SELECT left_side.id,left_side.key
  FROM public.join_left AS left_side
  WHERE EXISTS (
    SELECT 1
    FROM public.join_right AS right_side
    WHERE right_side.key=left_side.key
  );
  CREATE TABLE shiba.anti_join_result AS
  SELECT left_side.id,left_side.key
  FROM public.join_left AS left_side
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.join_right AS right_side
    WHERE right_side.key=left_side.key
  );
  CREATE TABLE shiba.in_join_result AS
  SELECT left_side.id,left_side.key
  FROM public.join_left AS left_side
  WHERE left_side.key IN (
    SELECT right_side.key FROM public.join_right AS right_side
  );
  CREATE TABLE shiba.not_in_join_result AS
  SELECT left_side.id,left_side.key
  FROM public.join_left AS left_side
  WHERE left_side.key NOT IN (
    SELECT right_side.key FROM public.join_right AS right_side
  );

  INSERT INTO public.join_left VALUES (1,1),(2,2),(3,NULL);
  INSERT INTO public.join_right VALUES (10,1),(11,1),(12,NULL)"

assert_join_semantics() {
  assert_bag_equal \
    "SELECT left_side.id AS left_id,right_side.id AS right_id
     FROM public.join_left AS left_side
     LEFT JOIN public.join_right AS right_side
       ON left_side.key=right_side.key" \
    "SELECT left_id,right_id FROM shiba.left_join_result" \
    "LEFT Join zero crossings"
  assert_bag_equal \
    "SELECT left_side.id AS left_id,right_side.id AS right_id
     FROM public.join_left AS left_side
     RIGHT JOIN public.join_right AS right_side
       ON left_side.key=right_side.key" \
    "SELECT left_id,right_id FROM shiba.right_join_result" \
    "RIGHT Join zero crossings"
  assert_bag_equal \
    "SELECT left_side.id AS left_id,right_side.id AS right_id
     FROM public.join_left AS left_side
     FULL JOIN public.join_right AS right_side
       ON left_side.key=right_side.key" \
    "SELECT left_id,right_id FROM shiba.full_join_result" \
    "FULL Join zero crossings"
  assert_bag_equal \
    "SELECT left_side.id,left_side.key
     FROM public.join_left AS left_side
     WHERE EXISTS (
       SELECT 1 FROM public.join_right AS right_side
       WHERE right_side.key=left_side.key
     )" \
    "SELECT id,key FROM shiba.semi_join_result" \
    "Semi Join zero crossings"
  assert_bag_equal \
    "SELECT left_side.id,left_side.key
     FROM public.join_left AS left_side
     WHERE NOT EXISTS (
       SELECT 1 FROM public.join_right AS right_side
       WHERE right_side.key=left_side.key
     )" \
    "SELECT id,key FROM shiba.anti_join_result" \
    "Anti Join zero crossings"
  assert_bag_equal \
    "SELECT left_side.id,left_side.key
     FROM public.join_left AS left_side
     WHERE left_side.key IN (
       SELECT right_side.key FROM public.join_right AS right_side
     )" \
    "SELECT id,key FROM shiba.in_join_result" \
    "IN Semi Join"
  assert_bag_equal \
    "SELECT left_side.id,left_side.key
     FROM public.join_left AS left_side
     WHERE left_side.key NOT IN (
       SELECT right_side.key FROM public.join_right AS right_side
     )" \
    "SELECT id,key FROM shiba.not_in_join_result" \
    "NULL-aware NOT IN"
}

assert_join_semantics

# Every ordinary equality Join above should use the keyed arrangement. Check
# the physical invariant, not only the final bag: key_0 must equal the
# source row's key (including NULL), the composite lookup index must exist,
# and UNKNOWN counts must include NULL comparisons that the equality index
# intentionally cannot match.
check_keyed_join_state() {
  local result_name="$1"
  local expected_null_unknown="$2"
  local result_oid join_stage left_state right_state
  local left_binding right_binding left_source_slot right_source_slot
  local left_field right_field
  result_oid="$(psql_fanout -Atqc "SELECT '${result_name}'::regclass::oid::integer")"
  join_stage="$(psql_fanout -Atqc "
    SELECT stage.ordinality-1
    FROM shiba_internal.dataflows AS dataflow
    CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
      WITH ORDINALITY AS stage(value,ordinality)
    WHERE dataflow.result_oid=${result_oid}::oid
      AND stage.value->'spec'->>'operator'='join'")"
  left_binding="$(psql_fanout -Atqc "
    SELECT stage.value->'spec'->'config'->'equi_keys'->0->>'left_binding'
    FROM shiba_internal.dataflows AS dataflow
    CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
      WITH ORDINALITY AS stage(value,ordinality)
    WHERE dataflow.result_oid=${result_oid}::oid
      AND stage.ordinality-1=${join_stage}")"
  right_binding="$(psql_fanout -Atqc "
    SELECT stage.value->'spec'->'config'->'equi_keys'->0->>'right_binding'
    FROM shiba_internal.dataflows AS dataflow
    CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
      WITH ORDINALITY AS stage(value,ordinality)
    WHERE dataflow.result_oid=${result_oid}::oid
      AND stage.ordinality-1=${join_stage}")"
  left_source_slot="$(psql_fanout -Atqc "
    SELECT mapping.value->>'source_slot'
    FROM shiba_internal.dataflows AS dataflow
    CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
      WITH ORDINALITY AS stage(value,ordinality)
    CROSS JOIN LATERAL jsonb_array_elements(stage.value->'inputs'->0->'bindings')
      AS mapping(value)
    WHERE dataflow.result_oid=${result_oid}::oid
      AND stage.ordinality-1=${join_stage}
      AND mapping.value->>'target_binding'='${left_binding}'")"
  right_source_slot="$(psql_fanout -Atqc "
    SELECT mapping.value->>'source_slot'
    FROM shiba_internal.dataflows AS dataflow
    CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
      WITH ORDINALITY AS stage(value,ordinality)
    CROSS JOIN LATERAL jsonb_array_elements(stage.value->'inputs'->1->'bindings')
      AS mapping(value)
    WHERE dataflow.result_oid=${result_oid}::oid
      AND stage.ordinality-1=${join_stage}
      AND mapping.value->>'target_binding'='${right_binding}'")"
  left_field="slot_${left_source_slot}"
  right_field="slot_${right_source_slot}"
  left_state="$(psql_fanout -Atqc "
    SELECT format('%I.%I',namespace.nspname,relation.relname)
    FROM shiba_internal.operator_state_relations AS catalog
    JOIN pg_catalog.pg_class AS relation ON relation.oid=catalog.relation_oid
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
    WHERE catalog.result_oid=${result_oid}::oid
      AND catalog.stage_id=${join_stage}
      AND catalog.state_slot=0")"
  right_state="$(psql_fanout -Atqc "
    SELECT format('%I.%I',namespace.nspname,relation.relname)
    FROM shiba_internal.operator_state_relations AS catalog
    JOIN pg_catalog.pg_class AS relation ON relation.oid=catalog.relation_oid
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
    WHERE catalog.result_oid=${result_oid}::oid
      AND catalog.stage_id=${join_stage}
      AND catalog.state_slot=1")"
  assert_query "1" "
    SELECT jsonb_array_length(plan_stage.value->'spec'->'config'->'equi_keys')
    FROM shiba_internal.dataflows AS dataflow
    CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
      WITH ORDINALITY AS plan_stage(value,ordinality)
    WHERE dataflow.result_oid=${result_oid}::oid
      AND plan_stage.ordinality-1=${join_stage}"
  assert_query "0" "
    SELECT count(*)
    FROM ${left_state}
    WHERE key_0 IS DISTINCT FROM ((row_value).${left_field})"
  assert_query "0" "
    SELECT count(*)
    FROM ${right_state}
    WHERE key_0 IS DISTINCT FROM ((row_value).${right_field})"
  assert_query "2" "
    SELECT count(*)
    FROM pg_catalog.pg_index AS lookup_index
    WHERE lookup_index.indrelid IN ('${left_state}'::regclass,'${right_state}'::regclass)
      AND lookup_index.indnkeyatts=2
      AND lookup_index.indnatts=2
      AND lookup_index.indkey[0]=4
      AND lookup_index.indkey[1]=1
      AND lookup_index.indisvalid
      AND lookup_index.indisready
      AND lookup_index.indislive
      AND lookup_index.indexprs IS NULL
      AND lookup_index.indpred IS NULL"
  assert_query "${expected_null_unknown}" "
    SELECT count(*) || '|' || coalesce(sum(unknown_count),0)
    FROM ${left_state}
    WHERE ((row_value).${left_field}) IS NULL"
}

check_keyed_join_state shiba.left_join_result "1|3"
check_keyed_join_state shiba.right_join_result "1|3"
check_keyed_join_state shiba.full_join_result "1|3"
check_keyed_join_state shiba.semi_join_result "1|3"
check_keyed_join_state shiba.anti_join_result "1|3"
check_keyed_join_state shiba.in_join_result "1|3"

# Equality extraction must coexist with a residual predicate.  The keyed
# lookup may narrow candidates, but the residual remains the final authority
# for TRUE/FALSE/UNKNOWN output semantics.
psql_fanout -qc "
  CREATE TABLE public.residual_key_left (
    id integer PRIMARY KEY,
    key integer,
    value integer
  );
  CREATE TABLE public.residual_key_right (
    id integer PRIMARY KEY,
    key integer,
    value integer
  );
  CREATE TABLE shiba.residual_key_result AS
  SELECT left_side.id AS left_id,right_side.id AS right_id
  FROM public.residual_key_left AS left_side
  LEFT JOIN public.residual_key_right AS right_side
    ON left_side.key=right_side.key
   AND left_side.value>right_side.value;
  INSERT INTO public.residual_key_left VALUES
    (1,1,20),(2,1,5),(3,NULL,20);
  INSERT INTO public.residual_key_right VALUES
    (10,1,10),(11,1,30),(12,NULL,10)"
assert_bag_equal \
  "SELECT left_side.id AS left_id,right_side.id AS right_id
   FROM public.residual_key_left AS left_side
   LEFT JOIN public.residual_key_right AS right_side
     ON left_side.key=right_side.key
    AND left_side.value>right_side.value" \
  "SELECT left_id,right_id FROM shiba.residual_key_result" \
  "the keyed Join with a residual predicate"
check_keyed_join_state shiba.residual_key_result "1|2"

# A two-column equality key must be materialized and used as a conjunction;
# sharing the first key component must not produce a false match.
psql_fanout -qc "
  CREATE TABLE public.composite_key_left (
    id integer PRIMARY KEY,
    tenant integer NOT NULL,
    item integer NOT NULL,
    value integer NOT NULL
  );
  CREATE TABLE public.composite_key_right (
    id integer PRIMARY KEY,
    tenant integer NOT NULL,
    item integer NOT NULL,
    value integer NOT NULL
  );
  INSERT INTO public.composite_key_left VALUES
    (1,10,1,100),(2,10,2,200),(3,20,1,300);
  INSERT INTO public.composite_key_right VALUES
    (11,10,1,1000),(12,10,1,1001),(13,10,2,2000),(14,20,2,3000);
  CREATE TABLE shiba.composite_key_result AS
  SELECT left_side.id AS left_id,right_side.id AS right_id
  FROM public.composite_key_left AS left_side
  JOIN public.composite_key_right AS right_side
    ON left_side.tenant=right_side.tenant
   AND left_side.item=right_side.item"
assert_bag_equal \
  "SELECT left_side.id AS left_id,right_side.id AS right_id
   FROM public.composite_key_left AS left_side
   JOIN public.composite_key_right AS right_side
     ON left_side.tenant=right_side.tenant
    AND left_side.item=right_side.item" \
  "SELECT left_id,right_id FROM shiba.composite_key_result" \
  "the multi-column keyed Join"
composite_result_oid="$(psql_fanout -Atqc "SELECT 'shiba.composite_key_result'::regclass::oid::integer")"
assert_query "2" "
  SELECT jsonb_array_length(stage.value->'spec'->'config'->'equi_keys')
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${composite_result_oid}::oid
    AND stage.value->'spec'->>'operator'='join'"
composite_join_stage="$(psql_fanout -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${composite_result_oid}::oid
    AND stage.value->'spec'->>'operator'='join'")"
composite_left_key0_binding="$(psql_fanout -Atqc "
  SELECT stage.value->'spec'->'config'->'equi_keys'->0->>'left_binding'
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${composite_result_oid}::oid
    AND stage.ordinality-1=${composite_join_stage}")"
composite_left_key1_binding="$(psql_fanout -Atqc "
  SELECT stage.value->'spec'->'config'->'equi_keys'->1->>'left_binding'
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${composite_result_oid}::oid
    AND stage.ordinality-1=${composite_join_stage}")"
composite_left_key0_slot="$(psql_fanout -Atqc "
  SELECT mapping.value->>'source_slot'
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  CROSS JOIN LATERAL jsonb_array_elements(stage.value->'inputs'->0->'bindings')
    AS mapping(value)
  WHERE dataflow.result_oid=${composite_result_oid}::oid
    AND stage.ordinality-1=${composite_join_stage}
    AND mapping.value->>'target_binding'='${composite_left_key0_binding}'")"
composite_left_key1_slot="$(psql_fanout -Atqc "
  SELECT mapping.value->>'source_slot'
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  CROSS JOIN LATERAL jsonb_array_elements(stage.value->'inputs'->0->'bindings')
    AS mapping(value)
  WHERE dataflow.result_oid=${composite_result_oid}::oid
    AND stage.ordinality-1=${composite_join_stage}
    AND mapping.value->>'target_binding'='${composite_left_key1_binding}'")"
composite_left_key0_field="slot_${composite_left_key0_slot}"
composite_left_key1_field="slot_${composite_left_key1_slot}"
composite_left_state="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.operator_state_relations AS catalog
  JOIN pg_catalog.pg_class AS relation ON relation.oid=catalog.relation_oid
  JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE catalog.result_oid=${composite_result_oid}::oid
    AND catalog.stage_id=${composite_join_stage}
    AND catalog.state_slot=0")"
assert_query "0" "
  SELECT count(*)
  FROM ${composite_left_state}
  WHERE key_0 IS DISTINCT FROM ((row_value).${composite_left_key0_field})
     OR key_1 IS DISTINCT FROM ((row_value).${composite_left_key1_field})"
assert_query "1" "
  SELECT count(*)
  FROM pg_catalog.pg_index AS lookup_index
  WHERE lookup_index.indrelid='${composite_left_state}'::regclass
    AND lookup_index.indnkeyatts=3
    AND lookup_index.indnatts=3
    AND lookup_index.indkey[0]=4
    AND lookup_index.indkey[1]=5
    AND lookup_index.indkey[2]=1
    AND lookup_index.indisvalid
    AND lookup_index.indisready
    AND lookup_index.indislive"

# Force one LEFT Join candidate's ordered pair/zero-cross actions across two
# one-row chunks. The pair commits first; the typed continuation then owns the
# pending transition and may resume after a crash without replaying the pair.
left_result_oid="$(psql_fanout -Atqc "
  SELECT 'shiba.left_join_result'::regclass::oid::integer")"
left_join_stage="$(psql_fanout -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${left_result_oid}::oid
    AND stage.value->'spec'->>'operator'='join'")"
left_join_stream="$(psql_fanout -Atqc "
  SELECT stream_id
  FROM shiba_internal.effect_streams
  WHERE producer_kind='operator'
    AND producer_result_oid=${left_result_oid}::oid
    AND producer_stage_id=${left_join_stage}")"
left_join_continuation="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.operator_continuation_relations AS catalog
  JOIN pg_class AS relation ON relation.oid=catalog.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE catalog.result_oid=${left_result_oid}::oid
    AND catalog.stage_id=${left_join_stage}")"
left_join_payload="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.effect_streams AS stream
  JOIN pg_class AS relation ON relation.oid=stream.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE stream.stream_id=${left_join_stream}")"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.operator_checkpoints AS checkpoint
  WHERE checkpoint.result_oid=${left_result_oid}::oid
    AND (
      checkpoint.has_continuation
      OR EXISTS (
        SELECT 1
        FROM shiba_internal.effect_stream_consumers AS consumer
        JOIN shiba_internal.effect_streams AS input
          ON input.stream_id=consumer.stream_id
        WHERE consumer.result_oid=checkpoint.result_oid
          AND consumer.consumer_stage_id=checkpoint.stage_id
          AND (
            consumer.next_chunk_seq<input.next_chunk_seq
            OR (
              input.producer_kind='source'
              AND EXISTS (
                SELECT 1
                FROM shiba_internal.ingress_replay_state AS replay
                WHERE replay.slot_generation=input.slot_generation
                  AND replay.published_lsn IS NOT NULL
                  AND consumer.consumed_frontier_lsn<replay.published_lsn
              )
            )
          )
      )
    )" \
  "the LEFT Join DAG to become idle before action splitting"
psql_fanout -qc "
  UPDATE shiba_internal.effect_streams
  SET target_chunk_rows=1
  WHERE stream_id=${left_join_stream}"

split_owner_pid="$(runtime_pid)"
psql_fanout -qc "
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_after_commit',
    ${split_owner_pid},
    ${left_result_oid}::oid,
    ${left_join_stage},
    1200
  );
  INSERT INTO public.join_right VALUES (13,2)"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the split action input owner"
psql_fanout -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${left_result_oid}::oid"
wait_for_runtime_replacement "${split_owner_pid}"
split_phase="$(psql_fanout -Atqc "
  SELECT phase FROM ${left_join_continuation}")"
if test "${split_phase}" != "3"; then
  fail "expected the bounded quantum to stop at PendingTransition, got ${split_phase}"
fi
split_start_chunk="$(psql_fanout -Atqc "
  SELECT next_chunk_seq-1
  FROM shiba_internal.effect_streams
  WHERE stream_id=${left_join_stream}")"
assert_query "1" "
  SELECT row_count
  FROM shiba_internal.effect_stream_chunks
  WHERE stream_id=${left_join_stream}
    AND chunk_seq=${split_start_chunk}"
before_transition_state="$(psql_fanout -Atqc "
  SELECT stream.next_chunk_seq || '|' || checkpoint.revision || '|' ||
         continuation.phase || '|' ||
         coalesce(continuation.candidate_after::text,'') || '|' ||
         coalesce(continuation.pending_row_id::text,'')
  FROM shiba_internal.effect_streams AS stream
  CROSS JOIN shiba_internal.operator_checkpoints AS checkpoint
  CROSS JOIN ${left_join_continuation} AS continuation
  WHERE stream.stream_id=${left_join_stream}
    AND checkpoint.result_oid=${left_result_oid}::oid
    AND checkpoint.stage_id=${left_join_stage}")"

split_before_commit_pid="$(runtime_pid)"
psql_fanout -qc "
  UPDATE public.shiba_runtime_failpoints
  SET kind='operator_step_before_commit',
      runtime_pid=${split_before_commit_pid},
      fired=false
  WHERE kind='operator_step_after_commit';
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${left_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_log \
  "operator_step_before_commit result ${left_result_oid} stage ${left_join_stage}" \
  "the pending transition pre-commit crash"
psql_fanout -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${left_result_oid}::oid"
wait_for_runtime_replacement "${split_before_commit_pid}"
assert_query "${before_transition_state}" "
  SELECT stream.next_chunk_seq || '|' || checkpoint.revision || '|' ||
         continuation.phase || '|' ||
         coalesce(continuation.candidate_after::text,'') || '|' ||
         coalesce(continuation.pending_row_id::text,'')
  FROM shiba_internal.effect_streams AS stream
  CROSS JOIN shiba_internal.operator_checkpoints AS checkpoint
  CROSS JOIN ${left_join_continuation} AS continuation
  WHERE stream.stream_id=${left_join_stream}
    AND checkpoint.result_oid=${left_result_oid}::oid
    AND checkpoint.stage_id=${left_join_stage}"

split_after_commit_pid="$(runtime_pid)"
psql_fanout -qc "
  UPDATE public.shiba_runtime_failpoints
  SET kind='operator_step_after_commit',
      runtime_pid=${split_after_commit_pid},
      fired=false
  WHERE kind='operator_step_before_commit';
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${left_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_query "t" "
  SELECT fired AND runtime_pid=${split_after_commit_pid}
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the recovered pending transition"
psql_fanout -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${left_result_oid}::oid"
wait_for_runtime_replacement "${split_after_commit_pid}"
# The pending candidate was the complete probe page, so committing its second
# action advances directly to Finalize rather than scheduling an empty Probe.
assert_query "4" "SELECT phase FROM ${left_join_continuation}"
assert_query "1|1" "
  SELECT string_agg(row_count::text,'|' ORDER BY chunk_seq)
  FROM shiba_internal.effect_stream_chunks
  WHERE stream_id=${left_join_stream}
    AND chunk_seq IN (${split_start_chunk},${split_start_chunk}+1)"
assert_query "1|-1" "
  SELECT string_agg(weight::text,'|' ORDER BY chunk_seq,row_ordinal)
  FROM ${left_join_payload}
  WHERE stream_id=${left_join_stream}
    AND chunk_seq IN (${split_start_chunk},${split_start_chunk}+1)"
psql_fanout -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  UPDATE shiba_internal.effect_streams
  SET target_chunk_rows=4
  WHERE stream_id=${left_join_stream};
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${left_result_oid}::oid"
assert_join_semantics
check_keyed_join_state shiba.left_join_result "1|4"
psql_fanout -qc "DELETE FROM public.join_right WHERE id=13"
assert_join_semantics
check_keyed_join_state shiba.left_join_result "1|3"

psql_fanout -qc "
  DELETE FROM public.join_right WHERE id=10;
  DELETE FROM public.join_right WHERE id=11;
  DELETE FROM public.join_right WHERE id=12"
assert_join_semantics
check_keyed_join_state shiba.left_join_result "1|0"
psql_fanout -qc "
  INSERT INTO public.join_right VALUES (20,2),(21,NULL);
  DELETE FROM public.join_left WHERE id=2;
  INSERT INTO public.join_left VALUES (2,2)"
assert_join_semantics
check_keyed_join_state shiba.left_join_result "1|2"
psql_fanout -qc "
  UPDATE public.join_right SET key=3 WHERE id=21;
  INSERT INTO public.join_left VALUES (4,3)"
assert_join_semantics
check_keyed_join_state shiba.left_join_result "1|2"

# Sink pages a single large signed weight in the same transaction as its
# continuation, input cursor, checkpoint, and result DML. Duplicate rows on
# the right collapse to one Join arrangement entry with multiplicity 12, so
# inserting and deleting the left row exercise positive and negative
# remaining_weight without fabricating internal stream rows in the test.
psql_fanout -qc "
  CREATE TABLE public.sink_weight_left (
    id integer NOT NULL,
    key integer NOT NULL
  );
  ALTER TABLE public.sink_weight_left REPLICA IDENTITY FULL;
  CREATE TABLE public.sink_weight_right (
    key integer NOT NULL
  );
  ALTER TABLE public.sink_weight_right REPLICA IDENTITY FULL;
  INSERT INTO public.sink_weight_right
  SELECT 1 FROM generate_series(1,12);
  CREATE TABLE shiba.sink_weight_result AS
  SELECT left_side.id
  FROM public.sink_weight_left AS left_side
  JOIN public.sink_weight_right AS right_side
    ON right_side.key=left_side.key"
sink_weight_oid="$(psql_fanout -Atqc "
  SELECT 'shiba.sink_weight_result'::regclass::oid::integer")"
sink_weight_stage="$(psql_fanout -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${sink_weight_oid}::oid
    AND stage.value->'spec'->>'operator'='sink'")"
sink_weight_continuation="$(psql_fanout -Atqc "
  SELECT format('%I.%I',namespace.nspname,relation.relname)
  FROM shiba_internal.operator_continuation_relations AS catalog
  JOIN pg_class AS relation ON relation.oid=catalog.relation_oid
  JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE catalog.result_oid=${sink_weight_oid}::oid
    AND catalog.stage_id=${sink_weight_stage}")"

wait_for_sink_weight_idle() {
  local description="$1"
  wait_for_query "0" "
    SELECT count(*)
    FROM shiba_internal.operator_checkpoints AS checkpoint
    WHERE checkpoint.result_oid=${sink_weight_oid}::oid
      AND (
        checkpoint.has_continuation
        OR EXISTS (
          SELECT 1
          FROM shiba_internal.effect_stream_consumers AS consumer
          JOIN shiba_internal.effect_streams AS input
            ON input.stream_id=consumer.stream_id
          WHERE consumer.result_oid=checkpoint.result_oid
            AND consumer.consumer_stage_id=checkpoint.stage_id
            AND (
              consumer.next_chunk_seq<input.next_chunk_seq
              OR (
                input.producer_kind='source'
                AND EXISTS (
                  SELECT 1
                  FROM shiba_internal.ingress_replay_state AS replay
                  WHERE replay.slot_generation=input.slot_generation
                    AND replay.published_lsn IS NOT NULL
                    AND consumer.consumed_frontier_lsn<replay.published_lsn
                )
              )
            )
        )
      )" "${description}"
}

wait_for_sink_weight_idle "the large-weight Sink DAG bootstrap"

sink_weight_authority() {
  psql_fanout -Atqc "
    SELECT consumer.next_chunk_seq || '|' ||
           checkpoint.revision || '|' ||
           checkpoint.has_continuation || '|' ||
           (SELECT count(*) FROM ${sink_weight_continuation})
    FROM shiba_internal.operator_checkpoints AS checkpoint
    JOIN shiba_internal.effect_stream_consumers AS consumer
      ON consumer.result_oid=checkpoint.result_oid
     AND consumer.consumer_stage_id=checkpoint.stage_id
     AND consumer.input_port=0
    WHERE checkpoint.result_oid=${sink_weight_oid}::oid
      AND checkpoint.stage_id=${sink_weight_stage}"
}

sink_weight_cursor() {
  psql_fanout -Atqc "
    SELECT consumer.next_chunk_seq
    FROM shiba_internal.operator_checkpoints AS checkpoint
    JOIN shiba_internal.effect_stream_consumers AS consumer
      ON consumer.result_oid=checkpoint.result_oid
     AND consumer.consumer_stage_id=checkpoint.stage_id
     AND consumer.input_port=0
    WHERE checkpoint.result_oid=${sink_weight_oid}::oid
      AND checkpoint.stage_id=${sink_weight_stage}"
}

sink_weight_revision() {
  psql_fanout -Atqc "
    SELECT revision
    FROM shiba_internal.operator_checkpoints
    WHERE result_oid=${sink_weight_oid}::oid
      AND stage_id=${sink_weight_stage}"
}

# The first Sink page inserts four copies, then the before-commit failpoint
# aborts the whole PostgreSQL transaction. Stop this DAG while the old Runtime
# is still inside the failpoint so its replacement cannot retry before the
# rollback authority is inspected.
positive_sink_authority="$(sink_weight_authority)"
positive_sink_cursor="$(sink_weight_cursor)"
positive_sink_revision="$(sink_weight_revision)"
positive_sink_runtime="$(runtime_pid)"
positive_sink_pattern="operator_step_before_commit result ${sink_weight_oid} stage ${sink_weight_stage}"
positive_sink_logs="$(grep -Fc "${positive_sink_pattern}" "${pg_log_file}" || true)"
psql_fanout -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_before_commit',
    ${positive_sink_runtime},
    ${sink_weight_oid}::oid,
    ${sink_weight_stage},
    1200
  );
  INSERT INTO public.sink_weight_left VALUES (7,1)"
for ((attempt = 1; attempt <= wait_attempts; attempt++)); do
  current_logs="$(grep -Fc "${positive_sink_pattern}" "${pg_log_file}" || true)"
  if test "${current_logs}" -gt "${positive_sink_logs}"; then
    break
  fi
  sleep 0.05
done
if test "${current_logs:-0}" -le "${positive_sink_logs}"; then
  fail "timed out waiting for the positive Sink pre-commit crash"
fi
psql_fanout -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${sink_weight_oid}::oid"
wait_for_runtime_replacement "${positive_sink_runtime}"
assert_query "0" "SELECT count(*) FROM shiba.sink_weight_result"
if test "$(sink_weight_authority)" != "${positive_sink_authority}"; then
  fail "the positive Sink abort changed its cursor, checkpoint, or continuation"
fi

# Now crash after the first four inserts commit. The continuation trigger arms
# the failpoint only for the exact +8 suffix and freezes this DAG in the same
# transaction, leaving a stable committed state for inspection.
psql_fanout -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  CREATE FUNCTION public.arm_positive_sink_suffix_crash()
  RETURNS trigger
  LANGUAGE plpgsql
  AS \$arm\$
  BEGIN
    IF NEW.remaining_weight=8 THEN
      INSERT INTO public.shiba_runtime_failpoints(
        kind,runtime_pid,result_oid,stage_id,pause_ms
      )
      VALUES(
        'operator_step_after_commit',
        pg_backend_pid(),
        ${sink_weight_oid}::oid,
        ${sink_weight_stage},
        3000
      )
      ON CONFLICT(kind) DO NOTHING;
      UPDATE shiba_internal.dataflows
      SET active=false
      WHERE result_oid=${sink_weight_oid}::oid;
    END IF;
    RETURN NEW;
  END
  \$arm\$;
  CREATE TRIGGER arm_positive_sink_suffix_crash
  AFTER INSERT OR UPDATE ON ${sink_weight_continuation}
  FOR EACH ROW
  EXECUTE FUNCTION public.arm_positive_sink_suffix_crash();
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${sink_weight_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the first committed positive Sink page"
positive_suffix_runtime="$(psql_fanout -Atqc "
  SELECT runtime_pid
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'")"
assert_query "4|8|0|true|true|true" "
  SELECT (SELECT count(*) FROM shiba.sink_weight_result)
         || '|' || continuation.remaining_weight
         || '|' || continuation.row_ordinal
         || '|' || checkpoint.has_continuation
         || '|' || (
           consumer.next_chunk_seq=${positive_sink_cursor}
         )
         || '|' || (
           checkpoint.revision=${positive_sink_revision}+1
         )
  FROM ${sink_weight_continuation} AS continuation
  JOIN shiba_internal.operator_checkpoints AS checkpoint
    ON checkpoint.result_oid=${sink_weight_oid}::oid
   AND checkpoint.stage_id=${sink_weight_stage}
  JOIN shiba_internal.effect_stream_consumers AS consumer
    ON consumer.result_oid=checkpoint.result_oid
   AND consumer.consumer_stage_id=checkpoint.stage_id
   AND consumer.input_port=0"
wait_for_runtime_replacement "${positive_suffix_runtime}"
psql_fanout -qc "
  DROP TRIGGER arm_positive_sink_suffix_crash
  ON ${sink_weight_continuation};
  DROP FUNCTION public.arm_positive_sink_suffix_crash();
  DELETE FROM public.shiba_runtime_failpoints;
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${sink_weight_oid}::oid;
  SELECT shiba._ensure_runtime()"
assert_bag_equal \
  "SELECT 7::integer AS id FROM generate_series(1,12)" \
  "SELECT id FROM shiba.sink_weight_result" \
  "the positive remaining_weight Sink recovery"
wait_for_sink_weight_idle "the positive Sink recovery frontier"

# Deleting the left row produces one weight -12 effect. The same crash point
# must roll back the first four deletes and preserve the old cursor and
# continuation before retrying the negative suffix exactly once.
negative_sink_authority="$(sink_weight_authority)"
negative_sink_cursor="$(sink_weight_cursor)"
negative_sink_revision="$(sink_weight_revision)"
negative_sink_runtime="$(runtime_pid)"
negative_sink_logs="$(grep -Fc "${positive_sink_pattern}" "${pg_log_file}" || true)"
psql_fanout -qc "
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_before_commit',
    ${negative_sink_runtime},
    ${sink_weight_oid}::oid,
    ${sink_weight_stage},
    1200
  );
  DELETE FROM public.sink_weight_left WHERE id=7"
for ((attempt = 1; attempt <= wait_attempts; attempt++)); do
  current_logs="$(grep -Fc "${positive_sink_pattern}" "${pg_log_file}" || true)"
  if test "${current_logs}" -gt "${negative_sink_logs}"; then
    break
  fi
  sleep 0.05
done
if test "${current_logs:-0}" -le "${negative_sink_logs}"; then
  fail "timed out waiting for the negative Sink pre-commit crash"
fi
psql_fanout -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${sink_weight_oid}::oid"
wait_for_runtime_replacement "${negative_sink_runtime}"
assert_query "12" "SELECT count(*) FROM shiba.sink_weight_result"
if test "$(sink_weight_authority)" != "${negative_sink_authority}"; then
  fail "the negative Sink abort changed its cursor, checkpoint, or continuation"
fi

# Repeat the committed-prefix crash for the signed delete suffix. Eight result
# rows and -8 remaining_weight must survive together while the input cursor
# still points at the same effect row.
psql_fanout -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  CREATE FUNCTION public.arm_negative_sink_suffix_crash()
  RETURNS trigger
  LANGUAGE plpgsql
  AS \$arm\$
  BEGIN
    IF NEW.remaining_weight=-8 THEN
      INSERT INTO public.shiba_runtime_failpoints(
        kind,runtime_pid,result_oid,stage_id,pause_ms
      )
      VALUES(
        'operator_step_after_commit',
        pg_backend_pid(),
        ${sink_weight_oid}::oid,
        ${sink_weight_stage},
        3000
      )
      ON CONFLICT(kind) DO NOTHING;
      UPDATE shiba_internal.dataflows
      SET active=false
      WHERE result_oid=${sink_weight_oid}::oid;
    END IF;
    RETURN NEW;
  END
  \$arm\$;
  CREATE TRIGGER arm_negative_sink_suffix_crash
  AFTER INSERT OR UPDATE ON ${sink_weight_continuation}
  FOR EACH ROW
  EXECUTE FUNCTION public.arm_negative_sink_suffix_crash();
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${sink_weight_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the first committed negative Sink page"
negative_suffix_runtime="$(psql_fanout -Atqc "
  SELECT runtime_pid
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'")"
assert_query "8|-8|0|true|true|true" "
  SELECT (SELECT count(*) FROM shiba.sink_weight_result)
         || '|' || continuation.remaining_weight
         || '|' || continuation.row_ordinal
         || '|' || checkpoint.has_continuation
         || '|' || (
           consumer.next_chunk_seq=${negative_sink_cursor}
         )
         || '|' || (
           checkpoint.revision=${negative_sink_revision}+1
         )
  FROM ${sink_weight_continuation} AS continuation
  JOIN shiba_internal.operator_checkpoints AS checkpoint
    ON checkpoint.result_oid=${sink_weight_oid}::oid
   AND checkpoint.stage_id=${sink_weight_stage}
  JOIN shiba_internal.effect_stream_consumers AS consumer
    ON consumer.result_oid=checkpoint.result_oid
   AND consumer.consumer_stage_id=checkpoint.stage_id
   AND consumer.input_port=0"
wait_for_runtime_replacement "${negative_suffix_runtime}"
psql_fanout -qc "
  DROP TRIGGER arm_negative_sink_suffix_crash
  ON ${sink_weight_continuation};
  DROP FUNCTION public.arm_negative_sink_suffix_crash();
  DELETE FROM public.shiba_runtime_failpoints;
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${sink_weight_oid}::oid;
  SELECT shiba._ensure_runtime()"
assert_bag_equal \
  "SELECT NULL::integer AS id WHERE false" \
  "SELECT id FROM shiba.sink_weight_result" \
  "the negative remaining_weight Sink recovery"
wait_for_sink_weight_idle "the negative Sink recovery frontier"
wait_for_query "false|0|true" "
  SELECT checkpoint.has_continuation || '|' ||
         (SELECT count(*) FROM ${sink_weight_continuation}) || '|' ||
         (consumer.next_chunk_seq=input.next_chunk_seq)
  FROM shiba_internal.operator_checkpoints AS checkpoint
  JOIN shiba_internal.effect_stream_consumers AS consumer
    ON consumer.result_oid=checkpoint.result_oid
   AND consumer.consumer_stage_id=checkpoint.stage_id
   AND consumer.input_port=0
  JOIN shiba_internal.effect_streams AS input
    ON input.stream_id=consumer.stream_id
  WHERE checkpoint.result_oid=${sink_weight_oid}::oid
      AND checkpoint.stage_id=${sink_weight_stage}" \
  "the recovered Sink cursor and continuation authority"

# Exercise the complete generic composition shown in ARCHITECTURE.md. This is
# deliberately a small data set: the assertion is about one plan crossing
# every stateful kernel boundary, not about repeating the large fanout load
# that the earlier fixtures already cover.
psql_fanout -qc "
  CREATE TABLE public.complex_fact (
    fact_id integer PRIMARY KEY,
    first_key integer NOT NULL,
    payload integer NOT NULL
  );
  CREATE TABLE public.complex_first (
    first_id integer PRIMARY KEY,
    first_key integer NOT NULL,
    second_key integer NOT NULL
  );
  CREATE TABLE public.complex_second (
    second_id integer PRIMARY KEY,
    second_key integer NOT NULL,
    label integer NOT NULL
  );
  INSERT INTO public.complex_fact VALUES (1,1,10),(2,2,20);
  INSERT INTO public.complex_first VALUES
    (1,1,10),(2,1,10),(3,2,20);
  INSERT INTO public.complex_second VALUES
    (1,10,100),(2,10,200),(3,20,300);

  CREATE TABLE shiba.complex_ranked AS
  SELECT first_key,
         joined_rows,
         row_number() OVER (
           ORDER BY joined_rows DESC,first_key
         ) AS rank
  FROM (
    SELECT fact.first_key,
           count(*) AS joined_rows
    FROM public.complex_fact AS fact
    JOIN public.complex_first AS first_side
      ON first_side.first_key=fact.first_key
    JOIN public.complex_second AS second_side
      ON second_side.second_key=first_side.second_key
    GROUP BY fact.first_key
  ) AS grouped
  ORDER BY joined_rows DESC,first_key
  LIMIT 100"

complex_result_oid="$(psql_fanout -Atqc "
  SELECT 'shiba.complex_ranked'::regclass::oid::integer")"
assert_query "2|1|1|1|1" "
  SELECT count(*) FILTER (
           WHERE stage.value->'spec'->>'operator'='join'
         ) || '|' ||
         count(*) FILTER (
           WHERE stage.value->'spec'->>'operator'='aggregate'
         ) || '|' ||
         count(*) FILTER (
           WHERE stage.value->'spec'->>'operator'='window'
         ) || '|' ||
         count(*) FILTER (
           WHERE stage.value->'spec'->>'operator'='topn'
         ) || '|' ||
         count(*) FILTER (
           WHERE stage.value->'spec'->>'operator'='sink'
         )
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    AS stage(value)
  WHERE dataflow.result_oid=${complex_result_oid}::oid"

complex_expected="
  SELECT first_key,
         joined_rows,
         row_number() OVER (
           ORDER BY joined_rows DESC,first_key
         ) AS rank
  FROM (
    SELECT fact.first_key,
           count(*) AS joined_rows
    FROM public.complex_fact AS fact
    JOIN public.complex_first AS first_side
      ON first_side.first_key=fact.first_key
    JOIN public.complex_second AS second_side
      ON second_side.second_key=first_side.second_key
    GROUP BY fact.first_key
  ) AS grouped
  ORDER BY joined_rows DESC,first_key
  LIMIT 100"
assert_bag_equal "${complex_expected}" \
  "SELECT first_key,joined_rows,rank FROM shiba.complex_ranked" \
  "the complete Join -> Join -> Aggregate -> Window -> TopN -> Sink bootstrap"

psql_fanout -qc "
  INSERT INTO public.complex_fact VALUES (3,2,30);
  UPDATE public.complex_first SET second_key=10 WHERE first_id=3"
assert_bag_equal "${complex_expected}" \
  "SELECT first_key,joined_rows,rank FROM shiba.complex_ranked" \
  "the complete generic DAG live insert and update"

psql_fanout -qc "
  DELETE FROM public.complex_fact WHERE fact_id=1"
assert_bag_equal "${complex_expected}" \
  "SELECT first_key,joined_rows,rank FROM shiba.complex_ranked" \
  "the complete generic DAG live retraction"

# Crash the full Join -> Join -> Aggregate -> Window -> TopN chain before an
# Aggregate step commits. The source mutation is already durable; recovery
# must replay its pending effect rather than expose a half-committed operator
# state to the downstream Window, TopN, or Sink.
complex_aggregate_stage="$(psql_fanout -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${complex_result_oid}::oid
    AND stage.value->'spec'->>'operator'='aggregate'
")"
complex_sink_stage="$(psql_fanout -Atqc "
  SELECT stage.ordinality-1
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid=${complex_result_oid}::oid
    AND stage.value->'spec'->>'operator'='sink'
")"
complex_operator_runtime="$(runtime_pid)"
psql_fanout -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_before_commit',
    ${complex_operator_runtime},
    ${complex_result_oid}::oid,
    ${complex_aggregate_stage},
    1200
  );
  INSERT INTO public.complex_fact VALUES (4,2,40)
"
wait_for_log \
  "operator_step_before_commit result ${complex_result_oid} stage ${complex_aggregate_stage}" \
  "the complex Aggregate pre-commit crash"
wait_for_runtime_replacement "${complex_operator_runtime}"
psql_fanout -qc "
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_before_commit'
"
assert_bag_equal "${complex_expected}" \
  "SELECT first_key,joined_rows,rank FROM shiba.complex_ranked" \
  "the complex DAG after Aggregate pre-commit recovery"

# Now crash after the real Sink commits. This is the opposite authority
# boundary: the result write and Sink cursor are durable, while the Runtime
# dies before scheduling the next work item. A fresh SQL evaluation remains
# the oracle after restart.
complex_sink_runtime="$(runtime_pid)"
psql_fanout -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_after_commit',
    ${complex_sink_runtime},
    ${complex_result_oid}::oid,
    ${complex_sink_stage},
    1200
  );
  INSERT INTO public.complex_first VALUES (4,2,20)
"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'
" "the complex Sink post-commit crash"
wait_for_runtime_replacement "${complex_sink_runtime}"
psql_fanout -qc "
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'
"
assert_bag_equal "${complex_expected}" \
  "SELECT first_key,joined_rows,rank FROM shiba.complex_ranked" \
  "the complex DAG after Sink post-commit recovery"
wait_for_query "0|0" "
  SELECT
    (SELECT count(*)
     FROM shiba_internal.operator_checkpoints AS checkpoint
     WHERE checkpoint.result_oid=${complex_result_oid}::oid
       AND checkpoint.has_continuation)
    || '|' ||
    (SELECT count(*)
     FROM shiba_internal.effect_stream_consumers AS consumer
     JOIN shiba_internal.effect_streams AS input
       ON input.stream_id=consumer.stream_id
     WHERE consumer.result_oid=${complex_result_oid}::oid
       AND consumer.next_chunk_seq<input.next_chunk_seq)
" "the complex DAG continuation and pending-effect convergence"

# Every completed Join frontier is the minimum of its two durable input
# frontiers. A pending continuation may hold it back, never move it ahead.
assert_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_streams AS output
  WHERE output.producer_kind='operator'
    AND output.producer_stage_id IN (
      SELECT stage.ordinality-1
      FROM shiba_internal.dataflows AS dataflow
      CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
        WITH ORDINALITY AS stage(value,ordinality)
      WHERE dataflow.result_oid=output.producer_result_oid
        AND stage.value->'spec'->>'operator'='join'
    )
    AND output.published_frontier_lsn > (
      SELECT min(consumer.consumed_frontier_lsn)
      FROM shiba_internal.effect_stream_consumers AS consumer
      WHERE consumer.result_oid=output.producer_result_oid
        AND consumer.consumer_stage_id=output.producer_stage_id
    )"

wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.operator_checkpoints AS checkpoint
  WHERE checkpoint.has_continuation
     OR EXISTS (
       SELECT 1
       FROM shiba_internal.effect_stream_consumers AS consumer
       JOIN shiba_internal.effect_streams AS input
         ON input.stream_id=consumer.stream_id
       WHERE consumer.result_oid=checkpoint.result_oid
         AND consumer.consumer_stage_id=checkpoint.stage_id
         AND (
           consumer.next_chunk_seq<input.next_chunk_seq
           OR (
             input.producer_kind='source'
             AND EXISTS (
               SELECT 1
               FROM shiba_internal.ingress_replay_state AS replay
               WHERE replay.slot_generation=input.slot_generation
                 AND replay.published_lsn IS NOT NULL
                 AND consumer.consumed_frontier_lsn<replay.published_lsn
             )
           )
         )
     )" \
  "every Join DAG to reach a durable idle point"

printf '%s\n' \
  "generic bounded Join fanout/recovery/backpressure gate passed"
