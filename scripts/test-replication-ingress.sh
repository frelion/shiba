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
large_tx_rows="${SHIBA_INGRESS_LARGE_TX_ROWS:-1001}"

cleanup() {
  if test "${SHIBA_KEEP_TEST_CLUSTER:-0}" = "1"; then
    printf 'Retained test cluster: %s\n' "${pg_data_dir}" >&2
    printf 'Retained test socket: %s\n' "${pg_socket_dir}" >&2
    return
  fi
  "${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -m immediate stop \
    >/dev/null 2>&1 || true
  rm -rf "${pg_data_dir}" "${pg_socket_dir}"
}
trap cleanup EXIT

psql_ingress() {
  PGOPTIONS="-c statement_timeout=30000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

fail() {
  printf 'replication ingress test failed: %s\n' "$1" >&2
  tail -n 160 "${pg_log_file}" >&2 || true
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

wait_for_log() {
  local pattern="$1"
  local description="$2"
  local attempt
  for attempt in {1..600}; do
    if grep -Fq "${pattern}" "${pg_log_file}"; then
      return
    fi
    sleep 0.1
  done
  fail "timed out waiting for ${description}"
}

cd "${project_root}"
PG_CONFIG="${pg_config_path}" \
  cargo pgrx install --pg-config "${pg_config_path}" --features pg_test

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
  printf "shiba.ingress_batch_rows = 4\n"
  printf "shiba.ingress_batch_bytes = '32kB'\n"
  printf "shiba.ingress_retention = '10min'\n"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s connect_timeout=5'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "${database_user}"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -l "${pg_log_file}" \
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
" "the Runtime and its walsender"

active_generation="$(psql_ingress -Atqc "
  SELECT slot_generation
  FROM shiba_internal.ingress_replay_state
  WHERE state='active'
")"

# Real replication sources feed one shared stream each. Two consumers attach to
# source_a, proving fanout adds cursors rather than duplicate payload.
psql_ingress -qc "
  CREATE TABLE public.source_a (
    id bigint PRIMARY KEY,
    payload text NOT NULL,
    dimensions integer[]
  );
  CREATE TABLE public.source_b (
    id bigint PRIMARY KEY,
    payload text NOT NULL
  );
  CREATE TABLE public.consumer_a_result (marker bigint);
  CREATE TABLE public.consumer_b_result (marker bigint);
  CREATE TABLE public.consumer_c_result (marker bigint);

  SELECT shiba_internal.prepare_dataflow_source(
    'public.source_a'::regclass
  );
  SELECT shiba_internal.prepare_dataflow_source(
    'public.source_b'::regclass
  );

  INSERT INTO shiba_internal.dataflows(
    result_oid,plan,activation_lsn,active
  )
  VALUES
    (
      'public.consumer_a_result'::regclass,
      '{}',pg_current_wal_lsn(),false
    ),
    (
      'public.consumer_b_result'::regclass,
      '{}',pg_current_wal_lsn(),false
    ),
    (
      'public.consumer_c_result'::regclass,
      '{}',pg_current_wal_lsn(),false
    );
  INSERT INTO shiba_internal.operator_checkpoints(result_oid,stage_id)
  VALUES
    ('public.consumer_a_result'::regclass,0),
    ('public.consumer_b_result'::regclass,0),
    ('public.consumer_c_result'::regclass,0);
"

source_a_stream="$(psql_ingress -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES (
    'source',${active_generation},'public.source_a'::regclass,
    4,4096,
    1000000,10000000,1073741824,
    1,1,1
  )
  RETURNING stream_id
")"
source_b_stream="$(psql_ingress -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES (
    'source',${active_generation},'public.source_b'::regclass,
    4,4096,
    1000000,10000000,1073741824,
    1,1,1
  )
  RETURNING stream_id
")"

psql_ingress -qc "
  SELECT shiba_internal.create_effect_stream_payload(
    ${source_a_stream},
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'slot_id',NULL,
          'attnum',attribute.attnum,
          'name',attribute.attname,
          'type_oid',attribute.atttypid::bigint,
          'typmod',attribute.atttypmod,
          'collation_oid',attribute.attcollation::bigint,
          'nullable',NOT attribute.attnotnull
        )
        ORDER BY attribute.attnum
      )
      FROM pg_attribute AS attribute
      WHERE attribute.attrelid='public.source_a'::regclass
        AND attribute.attnum > 0
        AND NOT attribute.attisdropped
    )
  );
  SELECT shiba_internal.create_effect_stream_payload(
    ${source_b_stream},
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'slot_id',NULL,
          'attnum',attribute.attnum,
          'name',attribute.attname,
          'type_oid',attribute.atttypid::bigint,
          'typmod',attribute.atttypmod,
          'collation_oid',attribute.attcollation::bigint,
          'nullable',NOT attribute.attnotnull
        )
        ORDER BY attribute.attnum
      )
      FROM pg_attribute AS attribute
      WHERE attribute.attrelid='public.source_b'::regclass
        AND attribute.attnum > 0
        AND NOT attribute.attisdropped
    )
  );
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${source_a_stream},'public.consumer_a_result'::regclass,0,0,
    pg_current_wal_lsn()
  );
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${source_a_stream},'public.consumer_b_result'::regclass,0,0,
    pg_current_wal_lsn()
  );
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${source_b_stream},'public.consumer_c_result'::regclass,0,0,
    pg_current_wal_lsn()
  );

  CREATE TABLE public.shiba_runtime_failpoints (
    kind text PRIMARY KEY,
    runtime_pid integer,
    result_oid oid,
    stage_id integer,
    commit_lsn pg_lsn,
    pause_ms integer NOT NULL DEFAULT 0 CHECK (pause_ms >= 0),
    fired boolean NOT NULL DEFAULT false
  );
"

assert_query "1|2" "
  SELECT count(DISTINCT stream.stream_id),count(consumer.*)
  FROM shiba_internal.effect_streams AS stream
  JOIN shiba_internal.effect_stream_consumers AS consumer USING (stream_id)
  WHERE stream.slot_generation=${active_generation}
    AND stream.source_oid='public.source_a'::regclass
"

# The batch API is exactly-once.
# A separate generation keeps the deterministic replay checks away from the
# live logical slot.
psql_ingress -qc "
  INSERT INTO shiba_internal.ingress_replay_state(
    slot_generation,slot_name,database_oid,system_identifier,slot_baseline_lsn
  )
  VALUES (
    9001,'synthetic_ingress',(
      SELECT oid FROM pg_database WHERE datname=current_database()
    ),'synthetic','0/0'
  );
"
synthetic_txn="$(psql_ingress -Atqc "
  SELECT ingress_txn_id
  FROM shiba_internal.claim_ingress_transaction(9001,41,'0/100')
")"
assert_query "9|0|1|9" "
  SELECT inserted_count||'|'||replayed_count||'|'||
         first_input_seq||'|'||last_input_seq
  FROM shiba_internal.insert_ingress_events(
    ${synthetic_txn},
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'change_lsn','0/90',
          'change_ordinal',id,
          'image_ordinal',0,
          'source_oid','public.source_a'::regclass::oid::bigint,
          'weight',1,
          'payload',jsonb_build_object(
            'id',-id,'payload','synthetic'
          )
        )
        ORDER BY id
      )
      FROM generate_series(1,9) AS id
    )
  )
"
assert_query "0|9|1|9" "
  SELECT inserted_count||'|'||replayed_count||'|'||
         first_input_seq||'|'||last_input_seq
  FROM shiba_internal.insert_ingress_events(
    ${synthetic_txn},
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'change_lsn','0/90',
          'change_ordinal',id,
          'image_ordinal',0,
          'source_oid','public.source_a'::regclass::oid::bigint,
          'weight',1,
          'payload',jsonb_build_object(
            'id',-id,'payload','synthetic'
          )
        )
        ORDER BY id
      )
      FROM generate_series(1,9) AS id
    )
  )
"
assert_query "9|1|1|1|1" "
  SELECT txn.event_count||'|'||txn.pending_publications||'|'||
         txn.batch_count||'|'||
         count(DISTINCT batch.batch_ordinal)||'|'||
         count(DISTINCT publication.batch_ordinal)
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.ingress_apply_batches AS batch USING (ingress_txn_id)
  JOIN shiba_internal.source_publications AS publication
    USING (ingress_txn_id,batch_ordinal)
  WHERE txn.ingress_txn_id=${synthetic_txn}
  GROUP BY txn.event_count,txn.pending_publications,txn.batch_count
"

# Commit must lock only the replay/header authorities, regardless of how many
# admitted events and batches are behind the header.
assert_query "t" "
  SELECT pg_get_functiondef(
           'shiba_internal.commit_ingress_transaction(bigint,pg_lsn,pg_lsn)'
             ::regprocedure
         ) !~ 'change_log|ingress_apply_batches|source_publications'
"
assert_query "t" "
  SELECT pg_get_functiondef(
           'shiba_internal.publish_source_batch(bigint)'::regprocedure
         ) !~ 'advance_ingress_publication_frontier'
"
assert_query "3" "
  SELECT count(*)
  FROM pg_indexes
  WHERE schemaname='shiba_internal'
    AND indexname IN (
      'ingress_publication_order_idx',
      'ingress_pending_publication_idx',
      'source_publications_ready_idx'
    )
"
assert_query $'t\n0' "
  BEGIN;
  SELECT finalized
  FROM shiba_internal.commit_ingress_transaction(
    ${synthetic_txn},'0/100','0/110'
  );
  SELECT count(*)
  FROM pg_locks
  WHERE pid=pg_backend_pid()
    AND relation IN (
      'shiba_internal.change_log'::regclass,
      'shiba_internal.ingress_apply_batches'::regclass,
      'shiba_internal.source_publications'::regclass
    );
  ROLLBACK;
"
psql_ingress -qc "
  SELECT shiba_internal.commit_ingress_transaction(
    ${synthetic_txn},'0/100','0/110'
  )
"
assert_query "discarded|0|false" "
  SELECT outcome||'|'||coalesce(chunk_seq,0)||'|'||has_pending
  FROM shiba_internal.publish_source_batch(9001)
"
assert_query "0/100" "
  SELECT shiba_internal.advance_ingress_publication_frontier(9001)
"

# Add a stream only after the no-consumer transaction. A rolled-back publisher
# transaction must roll back chunk metadata, typed rows, cursor, and the header
# pending counter together.
psql_ingress -qc "
  CREATE TABLE public.synthetic_consumer_result (marker bigint);
  INSERT INTO shiba_internal.dataflows(
    result_oid,plan,activation_lsn,active
  )
  VALUES (
    'public.synthetic_consumer_result'::regclass,
    '{}','0/0',false
  );
  INSERT INTO shiba_internal.operator_checkpoints(result_oid,stage_id)
  VALUES ('public.synthetic_consumer_result'::regclass,0);
"
synthetic_stream="$(psql_ingress -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES (
    'source',9001,'public.source_a'::regclass,
    4,4096,
    100,1000,1048576,
    1,1,1
  )
  RETURNING stream_id
")"
psql_ingress -qc "
  SELECT shiba_internal.create_effect_stream_payload(
    ${synthetic_stream},
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'slot_id',NULL,
          'attnum',attribute.attnum,
          'name',attribute.attname,
          'type_oid',attribute.atttypid::bigint,
          'typmod',attribute.atttypmod,
          'collation_oid',attribute.attcollation::bigint,
          'nullable',NOT attribute.attnotnull
        )
        ORDER BY attribute.attnum
      )
      FROM pg_attribute AS attribute
      WHERE attribute.attrelid='public.source_a'::regclass
        AND attribute.attnum > 0
        AND NOT attribute.attisdropped
    )
  );
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${synthetic_stream},
    'public.synthetic_consumer_result'::regclass,0,0,'0/100'
  );
"
synthetic_payload_relation="$(psql_ingress -Atqc "
  SELECT relation_oid::regclass
  FROM shiba_internal.effect_stream_payloads
  WHERE stream_id=${synthetic_stream}
")"
cas_txn="$(psql_ingress -Atqc "
  SELECT ingress_txn_id
  FROM shiba_internal.claim_ingress_transaction(9001,42,'0/200')
")"
psql_ingress -qc "
  SELECT shiba_internal.insert_ingress_events(
    ${cas_txn},
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'change_lsn','0/190',
          'change_ordinal',id,
          'image_ordinal',0,
          'source_oid','public.source_a'::regclass::oid::bigint,
          'weight',1,
          'payload',jsonb_build_object(
            'id',-100-id,
            'payload','cursor-cas',
            'dimensions','[0:1]={10,20}'
          )
        )
        ORDER BY id
      )
      FROM generate_series(1,9) AS id
    )
  );
  SELECT shiba_internal.commit_ingress_transaction(
    ${cas_txn},'0/200','0/210'
  );
"
assert_query "9|[0:1]={10,20}" "
  SELECT count(*)||'|'||min(event.payload->>'dimensions')
  FROM shiba_internal.change_log AS event
  WHERE event.ingress_txn_id=${cas_txn}
"
psql_ingress -qc "
  BEGIN;
  SELECT shiba_internal.publish_source_batch(9001);
  ROLLBACK;
"
assert_query "1|1|1|0|0/100" "
  SELECT txn.pending_publications||'|'||
         publication.next_input_seq||'|'||
         stream.next_chunk_seq||'|'||
         count(chunk.*)||'|'||
         replay.published_lsn
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.source_publications AS publication
    USING (ingress_txn_id)
  JOIN shiba_internal.effect_streams AS stream
    ON stream.stream_id=${synthetic_stream}
  JOIN shiba_internal.ingress_replay_state AS replay
    ON replay.slot_generation=txn.slot_generation
  LEFT JOIN shiba_internal.effect_stream_chunks AS chunk
    ON chunk.stream_id=stream.stream_id
  WHERE txn.ingress_txn_id=${cas_txn}
  GROUP BY txn.pending_publications,publication.next_input_seq,
           stream.next_chunk_seq,replay.published_lsn
"
assert_query "appended|1|true" "
  SELECT outcome||'|'||chunk_seq||'|'||has_pending
  FROM shiba_internal.publish_source_batch(9001)
"
assert_query "appended|2|true" "
  SELECT outcome||'|'||chunk_seq||'|'||has_pending
  FROM shiba_internal.publish_source_batch(9001)
"
assert_query "completed|3|false" "
  SELECT outcome||'|'||chunk_seq||'|'||has_pending
  FROM shiba_internal.publish_source_batch(9001)
"
assert_query "0/200" "
  SELECT shiba_internal.advance_ingress_publication_frontier(9001)
"
assert_query "0|3|9" "
  SELECT txn.pending_publications||'|'||
         count(chunk.*)||'|'||sum(chunk.row_count)
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.effect_stream_chunks AS chunk
    ON chunk.stream_id=${synthetic_stream}
  WHERE txn.ingress_txn_id=${cas_txn}
  GROUP BY txn.pending_publications
"
assert_query "9|0|1" "
  SELECT count(*)||'|'||
         min(array_lower((payload.row_value).dimensions,1))||'|'||
         max(array_upper((payload.row_value).dimensions,1))
  FROM ${synthetic_payload_relation} AS payload
  WHERE payload.stream_id=${synthetic_stream}
"

# Byte bounding uses the same typed logical-size formula everywhere. A row
# wider than target_chunk_bytes is admitted alone; the following narrow row
# remains a continuation instead of making the step unbounded.
wide_txn="$(psql_ingress -Atqc "
  SELECT ingress_txn_id
  FROM shiba_internal.claim_ingress_transaction(9001,43,'0/300')
")"
psql_ingress -qc "
  SELECT shiba_internal.insert_ingress_events(
    ${wide_txn},
    jsonb_build_array(
      jsonb_build_object(
        'change_lsn','0/290',
        'change_ordinal',1,
        'image_ordinal',0,
        'source_oid','public.source_a'::regclass::oid::bigint,
        'weight',1,
        'payload',jsonb_build_object(
          'id',-1000,'payload',repeat('w',5000)
        )
      ),
      jsonb_build_object(
        'change_lsn','0/290',
        'change_ordinal',2,
        'image_ordinal',0,
        'source_oid','public.source_a'::regclass::oid::bigint,
        'weight',1,
        'payload',jsonb_build_object(
          'id',-1001,'payload','narrow'
        )
      )
    )
  );
  SELECT shiba_internal.commit_ingress_transaction(
    ${wide_txn},'0/300','0/310'
  );
"
assert_query "appended|4" "
  SELECT outcome||'|'||chunk_seq
  FROM shiba_internal.publish_source_batch(9001)
"
assert_query "1|true" "
  SELECT chunk.row_count||'|'||(chunk.payload_bytes > 4096)
  FROM shiba_internal.effect_stream_chunks AS chunk
  WHERE chunk.stream_id=${synthetic_stream}
    AND chunk.chunk_seq=4
"
assert_query "completed|5" "
  SELECT outcome||'|'||chunk_seq
  FROM shiba_internal.publish_source_batch(9001)
"
assert_query "0/300" "
  SELECT shiba_internal.advance_ingress_publication_frontier(9001)
"

# The causal-completion watermark follows a contiguous header prefix; it does
# not delay data chunks. A later sealed transaction cannot pass an earlier
# open header even after that header's source work reaches terminal state.
# Each call advances at most one header.
psql_ingress -qc "
  INSERT INTO shiba_internal.ingress_replay_state(
    slot_generation,slot_name,database_oid,system_identifier,slot_baseline_lsn
  )
  VALUES (
    9100,'frontier_order',(
      SELECT oid FROM pg_database WHERE datname=current_database()
    ),'synthetic','0/0'
  );
"
frontier_head_txn="$(psql_ingress -Atqc "
  SELECT ingress_txn_id
  FROM shiba_internal.claim_ingress_transaction(9100,1,'0/400')
")"
frontier_later_txn="$(psql_ingress -Atqc "
  SELECT ingress_txn_id
  FROM shiba_internal.claim_ingress_transaction(9100,2,'0/500')
")"
psql_ingress -qc "
  SELECT shiba_internal.insert_ingress_events(
    ${frontier_head_txn},
    jsonb_build_array(
      jsonb_build_object(
        'change_lsn','0/390',
        'change_ordinal',1,
        'image_ordinal',0,
        'source_oid','public.source_a'::regclass::oid::bigint,
        'weight',1,
        'payload',jsonb_build_object(
          'id',-2000,'payload','frontier-head'
        )
      )
    )
  );
  SELECT shiba_internal.commit_ingress_transaction(
    ${frontier_later_txn},'0/500','0/510'
  );
"
assert_query "null" "
  SELECT coalesce(
    shiba_internal.advance_ingress_publication_frontier(9100)::text,
    'null'
  )
"
assert_query "discarded|false" "
  SELECT outcome||'|'||has_pending
  FROM shiba_internal.publish_source_batch(9100)
"
assert_query "open|0|null" "
  SELECT txn.status||'|'||txn.pending_publications||'|'||
         coalesce(replay.published_lsn::text,'null')
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.ingress_replay_state AS replay USING (slot_generation)
  WHERE txn.ingress_txn_id=${frontier_head_txn}
"
assert_query "null" "
  SELECT coalesce(
    shiba_internal.advance_ingress_publication_frontier(9100)::text,
    'null'
  )
"
psql_ingress -qc "
  SELECT shiba_internal.commit_ingress_transaction(
    ${frontier_head_txn},'0/400','0/410'
  )
"
assert_query "0/400" "
  SELECT shiba_internal.advance_ingress_publication_frontier(9100)
"
assert_query "0/500" "
  SELECT shiba_internal.advance_ingress_publication_frontier(9100)
"

# A source chunk is usable before pgoutput Commit. Consumer progress and chunk
# GC are independent of the transaction header; replay metadata remains until
# Commit, publication frontier, slot confirmation, and retention all permit
# the ingress transaction to be removed.
psql_ingress -qc "
  INSERT INTO shiba_internal.ingress_replay_state(
    slot_generation,slot_name,database_oid,system_identifier,slot_baseline_lsn
  )
  VALUES (
    9200,'precommit_gc',(
      SELECT oid FROM pg_database WHERE datname=current_database()
    ),'synthetic','0/0'
  );
  CREATE TABLE public.precommit_consumer_result (marker bigint);
  INSERT INTO shiba_internal.dataflows(
    result_oid,plan,activation_lsn,active
  )
  VALUES (
    'public.precommit_consumer_result'::regclass,
    '{}','0/0',false
  );
  INSERT INTO shiba_internal.operator_checkpoints(result_oid,stage_id)
  VALUES ('public.precommit_consumer_result'::regclass,0);
"
precommit_stream="$(psql_ingress -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES (
    'source',9200,'public.source_a'::regclass,
    4,4096,
    100,1000,1048576,
    1,1,1
  )
  RETURNING stream_id
")"
psql_ingress -qc "
  SELECT shiba_internal.create_effect_stream_payload(
    ${precommit_stream},
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'slot_id',NULL,
          'attnum',attribute.attnum,
          'name',attribute.attname,
          'type_oid',attribute.atttypid::bigint,
          'typmod',attribute.atttypmod,
          'collation_oid',attribute.attcollation::bigint,
          'nullable',NOT attribute.attnotnull
        )
        ORDER BY attribute.attnum
      )
      FROM pg_attribute AS attribute
      WHERE attribute.attrelid='public.source_a'::regclass
        AND attribute.attnum > 0
        AND NOT attribute.attisdropped
    )
  );
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${precommit_stream},
    'public.precommit_consumer_result'::regclass,0,0,'0/0'
  );
"
precommit_payload_relation="$(psql_ingress -Atqc "
  SELECT relation_oid::regclass
  FROM shiba_internal.effect_stream_payloads
  WHERE stream_id=${precommit_stream}
")"
precommit_txn="$(psql_ingress -Atqc "
  SELECT ingress_txn_id
  FROM shiba_internal.claim_ingress_transaction(9200,1,'0/600')
")"
psql_ingress -qc "
  SELECT shiba_internal.insert_ingress_events(
    ${precommit_txn},
    jsonb_build_array(
      jsonb_build_object(
        'change_lsn','0/590',
        'change_ordinal',1,
        'image_ordinal',0,
        'source_oid','public.source_a'::regclass::oid::bigint,
        'weight',1,
        'payload',jsonb_build_object(
          'id',-3000,'payload','precommit'
        )
      )
    )
  )
"
assert_query "completed|1|false" "
  SELECT outcome||'|'||chunk_seq||'|'||has_pending
  FROM shiba_internal.publish_source_batch(9200)
"
assert_query "open|0|1|null" "
  SELECT txn.status||'|'||txn.pending_publications||'|'||
         count(chunk.*)||'|'||
         coalesce(replay.published_lsn::text,'null')
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.ingress_replay_state AS replay USING (slot_generation)
  LEFT JOIN shiba_internal.effect_stream_chunks AS chunk
    ON chunk.stream_id=${precommit_stream}
  WHERE txn.ingress_txn_id=${precommit_txn}
  GROUP BY txn.status,txn.pending_publications,replay.published_lsn
"
assert_query "2|0/0" "
  SELECT next_chunk_seq||'|'||consumed_frontier_lsn
  FROM shiba_internal.advance_effect_stream_consumer(
    ${precommit_stream},
    'public.precommit_consumer_result'::regclass,0,0,
    1,2,'0/0','0/0',
    1,100,100000
  )
"
assert_query "1|1" "
  SELECT deleted_chunks||'|'||deleted_rows
  FROM shiba_internal.gc_effect_stream(
    ${precommit_stream},1,100,100000
  )
"
assert_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks
  WHERE stream_id=${precommit_stream}
"
assert_query "0" "
  SELECT count(*) FROM ${precommit_payload_relation}
"
assert_query "open|1|1|1" "
  SELECT txn.status||'|'||
         count(DISTINCT event.input_seq)||'|'||
         count(DISTINCT batch.batch_ordinal)||'|'||
         count(DISTINCT publication.source_oid)
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.change_log AS event USING (ingress_txn_id)
  JOIN shiba_internal.ingress_apply_batches AS batch USING (ingress_txn_id)
  JOIN shiba_internal.source_publications AS publication
    USING (ingress_txn_id,batch_ordinal)
  WHERE txn.ingress_txn_id=${precommit_txn}
  GROUP BY txn.status
"
psql_ingress -qc "
  SELECT shiba_internal.commit_ingress_transaction(
    ${precommit_txn},'0/600','0/610'
  );
  SELECT shiba_internal.advance_ingress_publication_frontier(9200);
  UPDATE shiba_internal.ingress_transactions
  SET finalized_at=clock_timestamp()-interval '11 minutes'
  WHERE ingress_txn_id=${precommit_txn};
  UPDATE shiba_internal.ingress_replay_state
  SET confirmed_lsn='0/610',
      replay_safe_lsn='0/610'
  WHERE slot_generation=9200;
"
assert_query "1" "
  SELECT shiba._gc_change_log(1)
"
assert_query "0|0|0|0" "
  SELECT
    (SELECT count(*) FROM shiba_internal.ingress_transactions
     WHERE ingress_txn_id=${precommit_txn})||'|'||
    (SELECT count(*) FROM shiba_internal.change_log
     WHERE ingress_txn_id=${precommit_txn})||'|'||
    (SELECT count(*) FROM shiba_internal.ingress_apply_batches
     WHERE ingress_txn_id=${precommit_txn})||'|'||
    (SELECT count(*) FROM shiba_internal.source_publications
     WHERE ingress_txn_id=${precommit_txn})
"

# A real large pgoutput transaction is admitted and published in bounded
# prefixes before its Commit record is read.
psql_ingress -qc "
  INSERT INTO public.shiba_runtime_failpoints(kind,pause_ms)
  VALUES ('source_publication_after_commit',3000)
"
psql_ingress -qc "
  INSERT INTO public.source_a
  SELECT id,'large-'||id
  FROM generate_series(1,${large_tx_rows}) AS id
"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='source_publication_after_commit'
" "the post-publication crash point"
assert_query "open|true|true" "
  SELECT txn.status||'|'||
         (txn.event_count < ${large_tx_rows})||'|'||
         EXISTS (
           SELECT 1
           FROM shiba_internal.effect_stream_chunks AS chunk
           WHERE chunk.stream_id=${source_a_stream}
             AND chunk.chunk_lsn=txn.final_lsn
         )
  FROM shiba_internal.ingress_transactions AS txn
  JOIN public.shiba_runtime_failpoints AS failpoint
    ON failpoint.commit_lsn=txn.final_lsn
  WHERE failpoint.kind='source_publication_after_commit'
"
wait_for_query "1" "
  SELECT count(*)
  FROM shiba_internal.ingress_transactions AS txn
  WHERE txn.status='committed'
    AND txn.event_count=${large_tx_rows}
    AND txn.pending_publications=0
    AND EXISTS (
      SELECT 1
      FROM shiba_internal.change_log AS event
      WHERE event.ingress_txn_id=txn.ingress_txn_id
        AND event.source_oid='public.source_a'::regclass
    )
" "the complete large transaction"
large_txn="$(psql_ingress -Atqc "
  SELECT txn.ingress_txn_id
  FROM shiba_internal.ingress_transactions AS txn
  WHERE txn.status='committed'
    AND txn.event_count=${large_tx_rows}
  ORDER BY txn.ingress_txn_id DESC
  LIMIT 1
")"
assert_query "${large_tx_rows}|${large_tx_rows}" "
  SELECT sum(chunk.row_count)||'|'||
         (
           SELECT count(*)
           FROM shiba_internal.change_log
           WHERE ingress_txn_id=${large_txn}
         )
  FROM shiba_internal.effect_stream_chunks AS chunk
  WHERE chunk.stream_id=${source_a_stream}
    AND chunk.chunk_lsn=(
      SELECT final_lsn
      FROM shiba_internal.ingress_transactions
      WHERE ingress_txn_id=${large_txn}
    )
"

# PostgreSQL may encode an unchanged out-of-line value as the pgoutput `u`
# marker. REPLICA IDENTITY FULL supplies the old row; ingress must reconstruct
# the complete new row instead of rejecting a normal wide-column UPDATE.
psql_ingress -qc "
  UPDATE public.source_a
  SET payload=repeat('t',5000)
  WHERE id=1
"
wait_for_query "1" "
  SELECT count(*)
  FROM shiba_internal.ingress_transactions AS txn
  WHERE txn.status='committed'
    AND txn.pending_publications=0
    AND (
      SELECT count(*)
      FROM shiba_internal.change_log AS event
      WHERE event.ingress_txn_id=txn.ingress_txn_id
        AND event.source_oid='public.source_a'::regclass
        AND event.weight=1
        AND event.payload->>'id'='1'
        AND length(event.payload->>'payload')=5000
    )=1
" "the initial wide source update"
psql_ingress -qc "
  UPDATE public.source_a
  SET id=-1
  WHERE id=1
"
wait_for_query "1" "
  SELECT count(*)
  FROM shiba_internal.ingress_transactions AS txn
  WHERE txn.status='committed'
    AND txn.pending_publications=0
    AND (
      SELECT count(*)
      FROM shiba_internal.change_log AS event
      WHERE event.ingress_txn_id=txn.ingress_txn_id
        AND event.source_oid='public.source_a'::regclass
        AND length(event.payload->>'payload')=5000
        AND (
          (event.weight=-1 AND event.payload->>'id'='1')
          OR
          (event.weight=1 AND event.payload->>'id'='-1')
        )
    )=2
" "the unchanged-TOAST source update"

# Rows and pgoutput Commit in the same ingress batch seal the header while the
# source task is still pending. Crashing before publisher commit leaves no
# partial chunk and recovery appends it exactly once.
before_id="$((large_tx_rows + 1))"
psql_ingress -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  INSERT INTO public.shiba_runtime_failpoints(kind,pause_ms)
  VALUES ('source_publication_before_commit',3000);
  INSERT INTO public.source_a
  VALUES
    (${before_id},'before-a'),
    ($((before_id + 1)),'before-b'),
    ($((before_id + 2)),'before-c');
"
wait_for_log \
  "source_publication_before_commit at" \
  "the pre-publication crash point"
before_txn="$(psql_ingress -Atqc "
  SELECT txn.ingress_txn_id
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.change_log AS event USING (ingress_txn_id)
  WHERE event.source_oid='public.source_a'::regclass
    AND event.payload->>'id'='${before_id}'
")"
assert_query "committed|1|0|true" "
  SELECT txn.status||'|'||txn.pending_publications||'|'||
         count(chunk.*)||'|'||
         (replay.published_lsn < txn.final_lsn)
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.ingress_replay_state AS replay
    USING (slot_generation)
  LEFT JOIN shiba_internal.effect_stream_chunks AS chunk
    ON chunk.stream_id=${source_a_stream}
   AND chunk.chunk_lsn=txn.final_lsn
  WHERE txn.ingress_txn_id=${before_txn}
  GROUP BY txn.status,txn.pending_publications,
           replay.published_lsn,txn.final_lsn
"
psql_ingress -qc "
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='source_publication_before_commit'
"
wait_for_query "0|1|3" "
  SELECT txn.pending_publications||'|'||
         count(chunk.*)||'|'||sum(chunk.row_count)
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.effect_stream_chunks AS chunk
    ON chunk.stream_id=${source_a_stream}
   AND chunk.chunk_lsn=txn.final_lsn
  WHERE txn.ingress_txn_id=${before_txn}
  GROUP BY txn.pending_publications
" "pre-commit publication crash recovery"

# One source transaction touching two relations creates two source tasks and
# one chunk per source, then advances the one global contiguous frontier.
multi_a_id="$((before_id + 10))"
multi_b_id="$((before_id + 20))"
psql_ingress -qc "
  BEGIN;
  INSERT INTO public.source_a
  VALUES (${multi_a_id},'multi-a'),($((multi_a_id + 1)),'multi-a2');
  INSERT INTO public.source_b
  VALUES (${multi_b_id},'multi-b'),($((multi_b_id + 1)),'multi-b2');
  COMMIT;
"
wait_for_query "2|0" "
  SELECT count(publication.*)||'|'||txn.pending_publications
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.source_publications AS publication USING (ingress_txn_id)
  WHERE EXISTS (
    SELECT 1
    FROM shiba_internal.change_log AS event
    WHERE event.ingress_txn_id=txn.ingress_txn_id
      AND event.source_oid='public.source_b'::regclass
      AND event.payload->>'id'='${multi_b_id}'
  )
  GROUP BY txn.pending_publications
" "the two-source publication"
assert_query "2|2" "
  SELECT
    (
      SELECT sum(chunk.row_count)
      FROM shiba_internal.effect_stream_chunks AS chunk
      WHERE chunk.stream_id=${source_a_stream}
        AND chunk.chunk_lsn=txn.final_lsn
    )||'|'||
    (
      SELECT sum(chunk.row_count)
      FROM shiba_internal.effect_stream_chunks AS chunk
      WHERE chunk.stream_id=${source_b_stream}
        AND chunk.chunk_lsn=txn.final_lsn
    )
  FROM shiba_internal.ingress_transactions AS txn
  WHERE EXISTS (
    SELECT 1
    FROM shiba_internal.change_log AS event
    WHERE event.ingress_txn_id=txn.ingress_txn_id
      AND event.source_oid='public.source_b'::regclass
      AND event.payload->>'id'='${multi_b_id}'
  )
"

# Source publication deliberately stops reading more WAL while a shared source
# stream is backpressured. The Runtime must still send periodic standby status
# updates, otherwise a low wal_sender_timeout kills the walsender and restarts
# the whole Runtime while it is waiting for the consumer.
psql_ingress -qc "ALTER SYSTEM SET wal_sender_timeout='1s'"
psql_ingress -qc "SELECT pg_reload_conf()"
wait_for_query "1s" "
  SELECT current_setting('wal_sender_timeout')
" "the low walsender timeout"

psql_ingress -qc "
  CREATE TABLE public.heartbeat_source (
    id bigint PRIMARY KEY,
    payload text NOT NULL
  );
  CREATE TABLE public.heartbeat_consumer_result (marker bigint);
  SELECT shiba_internal.prepare_dataflow_source(
    'public.heartbeat_source'::regclass
  );
  INSERT INTO shiba_internal.dataflows(
    result_oid,plan,activation_lsn,active
  )
  VALUES (
    'public.heartbeat_consumer_result'::regclass,
    '{}',pg_current_wal_lsn(),false
  );
  INSERT INTO shiba_internal.operator_checkpoints(result_oid,stage_id)
  VALUES ('public.heartbeat_consumer_result'::regclass,0);
"
heartbeat_stream="$(psql_ingress -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES (
    'source',${active_generation},'public.heartbeat_source'::regclass,
    4,4096,
    1,4,4096,
    0,0,0
  )
  RETURNING stream_id
")"
psql_ingress -qc "
  SELECT shiba_internal.create_effect_stream_payload(
    ${heartbeat_stream},
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'slot_id',NULL,
          'attnum',attribute.attnum,
          'name',attribute.attname,
          'type_oid',attribute.atttypid::bigint,
          'typmod',attribute.atttypmod,
          'collation_oid',attribute.attcollation::bigint,
          'nullable',NOT attribute.attnotnull
        )
        ORDER BY attribute.attnum
      )
      FROM pg_attribute AS attribute
      WHERE attribute.attrelid='public.heartbeat_source'::regclass
        AND attribute.attnum > 0
        AND NOT attribute.attisdropped
    )
  );
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${heartbeat_stream},
    'public.heartbeat_consumer_result'::regclass,0,0,
    pg_current_wal_lsn()
  );
"
heartbeat_runtime_pid="$(psql_ingress -Atqc "
  SELECT pid
  FROM pg_stat_activity
  WHERE backend_type='shiba runtime'
")"
heartbeat_sender_pid="$(psql_ingress -Atqc "
  SELECT pid
  FROM pg_stat_activity
  WHERE backend_type='walsender'
    AND application_name='shiba'
")"

psql_ingress -qc "
  INSERT INTO public.heartbeat_source
  SELECT id,'heartbeat-'||id
  FROM generate_series(1,12) AS id
"
wait_for_query "open|1|true" "
  SELECT txn.status||'|'||txn.pending_publications||'|'||
         stream.backpressured
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.change_log AS event USING (ingress_txn_id)
  JOIN shiba_internal.effect_streams AS stream
    ON stream.stream_id=${heartbeat_stream}
  WHERE event.source_oid='public.heartbeat_source'::regclass
  GROUP BY txn.status,txn.pending_publications,stream.backpressured
" "the deliberately backpressured source publication"

wait_for_query "t" "
  SELECT replay.persisted_lsn=slot.confirmed_flush_lsn
     AND replay.confirmed_lsn=slot.confirmed_flush_lsn
     AND replay.replay_safe_lsn=slot.confirmed_flush_lsn
     AND NOT EXISTS (
       SELECT 1
       FROM shiba_internal.ingress_transactions AS earlier
       WHERE earlier.slot_generation=replay.slot_generation
         AND earlier.status='committed'
         AND (
           replay.published_lsn IS NULL
           OR earlier.final_lsn > replay.published_lsn
         )
     )
  FROM shiba_internal.ingress_replay_state AS replay
  JOIN pg_replication_slots AS slot
    ON slot.slot_name=replay.slot_name
  WHERE replay.slot_generation=${active_generation}
" "the stable ingress replay watermark"
heartbeat_replay_xmin="$(psql_ingress -Atqc "
  SELECT xmin::text
  FROM shiba_internal.ingress_replay_state
  WHERE slot_generation=${active_generation}
")"
psql_ingress -qc "SELECT pg_sleep(3)"
assert_query "${heartbeat_replay_xmin}" "
  SELECT xmin::text
  FROM shiba_internal.ingress_replay_state
  WHERE slot_generation=${active_generation}
"
assert_query "1|1|streaming" "
  SELECT
    count(*) FILTER (
      WHERE activity.pid=${heartbeat_runtime_pid}
        AND activity.backend_type='shiba runtime'
    )||'|'||
    count(*) FILTER (
      WHERE activity.pid=${heartbeat_sender_pid}
        AND activity.backend_type='walsender'
        AND activity.application_name='shiba'
    )||'|'||
    coalesce(
      (
        SELECT replication.state
        FROM pg_stat_replication AS replication
        WHERE replication.pid=${heartbeat_sender_pid}
      ),
      'missing'
    )
  FROM pg_stat_activity AS activity
"
assert_query "open|1|true" "
  SELECT txn.status||'|'||txn.pending_publications||'|'||
         stream.backpressured
  FROM shiba_internal.ingress_transactions AS txn
  JOIN shiba_internal.change_log AS event USING (ingress_txn_id)
  JOIN shiba_internal.effect_streams AS stream
    ON stream.stream_id=${heartbeat_stream}
  WHERE event.source_oid='public.heartbeat_source'::regclass
  GROUP BY txn.status,txn.pending_publications,stream.backpressured
"

psql_ingress -qc "
  DELETE FROM shiba_internal.effect_stream_consumers
  WHERE stream_id=${heartbeat_stream}
"
wait_for_query "1" "
  SELECT count(*)
  FROM shiba_internal.ingress_transactions AS txn
  WHERE txn.status='committed'
    AND txn.event_count=12
    AND txn.pending_publications=0
    AND EXISTS (
      SELECT 1
      FROM shiba_internal.change_log AS event
      WHERE event.ingress_txn_id=txn.ingress_txn_id
        AND event.source_oid='public.heartbeat_source'::regclass
    )
" "the source transaction after releasing backpressure"

printf 'replication ingress test passed\n'
