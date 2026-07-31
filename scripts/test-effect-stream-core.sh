#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-$("${project_root}/scripts/resolve-pg-config.sh")}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-effect-core-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-effect-core-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_EFFECT_CORE_TEST_PORT:-$((60000 + $$ % 3000))}"
database_name="shiba_effect_core"

psql_core() {
  PGOPTIONS="-c statement_timeout=10000 -c lock_timeout=5000" \
    "${pg_bin_dir}/psql" -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

test_name="effect stream core test"
test_psql_command=psql_core
test_log_lines=120
test_wait_attempts=100
test_wait_sleep=0.05
source "${project_root}/scripts/test-lib.sh"
trap cleanup EXIT

cd "${project_root}"
install_test_extension "${pg_config_path}"

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" \
  --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\n"
  printf "wal_level = logical\n"
  printf "max_worker_processes = 16\n"
  printf "listen_addresses = ''\n"
  printf "unix_socket_directories = '%s'\n" "${pg_socket_dir}"
  printf "port = %s\n" "${pg_port}"
} >>"${pg_data_dir}/postgresql.conf"
"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" \
  -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

# Exercise the generated extension SQL and Rust entry points together.
psql_core -qc "CREATE EXTENSION shiba"
psql_core -qc "
  CREATE TABLE public.source_events (id bigint PRIMARY KEY);
  CREATE TABLE public.unwatched_source (id bigint PRIMARY KEY);
  CREATE TABLE public.idle_source_a (id bigint PRIMARY KEY);
  CREATE TABLE public.idle_source_b (id bigint PRIMARY KEY);
  CREATE TABLE public.concurrent_source (id bigint PRIMARY KEY);
  CREATE TABLE public.result_a (id bigint);
  CREATE TABLE public.result_b (id bigint);

  INSERT INTO shiba_internal.dataflows(
    result_oid,plan,activation_lsn
  )
  VALUES
    ('public.result_a'::regclass,'{}','0/0'),
    ('public.result_b'::regclass,'{}','0/0');
  INSERT INTO shiba_internal.operator_checkpoints(result_oid,stage_id)
  VALUES
    ('public.result_a'::regclass,0),
    ('public.result_a'::regclass,1),
    ('public.result_a'::regclass,2),
    ('public.result_b'::regclass,0),
    ('public.result_b'::regclass,1),
    ('public.result_b'::regclass,2),
    ('public.result_b'::regclass,3);

  INSERT INTO shiba_internal.ingress_replay_state(
    slot_generation,slot_name,database_oid,system_identifier,
    slot_baseline_lsn,persisted_lsn,published_lsn
  )
  VALUES
    (1,'test_slot_1',1,'test','0/0','0/100',NULL),
    (2,'test_slot_2',1,'test','0/0','0/200','0/100'),
    (3,'test_slot_3',1,'test','0/0','0/300',NULL);

  CREATE TABLE shiba_internal.test_effect_payload (
    stream_id bigint NOT NULL,
    chunk_seq bigint NOT NULL,
    row_ordinal integer NOT NULL CHECK (row_ordinal > 0),
    payload text NOT NULL,
    PRIMARY KEY(stream_id,chunk_seq,row_ordinal),
    FOREIGN KEY(stream_id,chunk_seq)
      REFERENCES shiba_internal.effect_stream_chunks(stream_id,chunk_seq)
      ON DELETE CASCADE
  );
"

# Binary effect accounting must be independent of TOAST storage and must
# retain the actual typed record, including non-default array dimensions.
psql_core -qc "
  CREATE TABLE public.effect_size_probe (
    payload text NOT NULL,
    values integer[] NOT NULL
  );
  INSERT INTO public.effect_size_probe
  VALUES (
    repeat('abcdefghijklmnopqrstuvwxyz',20000),
    '[0:2]={10,20,30}'::integer[]
  )
"
assert_query "true|true|0|2" "
  WITH measured AS (
    SELECT effect_size_probe AS stored_row,
           ROW(
             repeat('abcdefghijklmnopqrstuvwxyz',20000),
             '[0:2]={10,20,30}'::integer[]
           )::public.effect_size_probe AS producer_row
    FROM public.effect_size_probe
  )
  SELECT
    (
      shiba_internal.effect_row_bytes(producer_row)
        = shiba_internal.effect_row_bytes(stored_row)
    )
    || '|' ||
    (
      shiba_internal.effect_row_bytes(stored_row)
        = pg_catalog.octet_length(pg_catalog.record_send(stored_row)) + 8
    )
    || '|' || array_lower((stored_row).values,1)
    || '|' || array_upper((stored_row).values,1)
  FROM measured
"

# The core exposes only the Stage-based API.  Old node-authority and global
# position columns must not survive the cutover.
assert_query "0" "
  SELECT count(*)
  FROM information_schema.columns
  WHERE table_schema='shiba_internal'
    AND table_name IN (
      'effect_streams','effect_stream_chunks',
      'effect_stream_consumers','operator_checkpoints'
    )
    AND column_name IN (
      'producer_node_id','consumer_node_id','node_id',
      'next_data_position','first_position','last_position','frontier_lsn'
    )
"
assert_query "0" "
  SELECT count(*)
  FROM pg_proc
  WHERE pronamespace='shiba_internal'::regnamespace
    AND proname IN (
      'attach_effect_stream_consumer',
      'append_effect_stream_chunk',
      'advance_effect_stream_consumer',
      'gc_effect_stream'
    )
    AND has_function_privilege('public',oid,'EXECUTE')
"

source_stream_id="$(psql_core -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES (
    'source',1,'public.source_events'::regclass,
    4,400,
    4,8,800,
    1,4,400
  )
  RETURNING stream_id
")"

# One globally shared source stream feeds two result graphs.
psql_core -qc "
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${source_stream_id},'public.result_a'::regclass,0,0,'0/0'
  );
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${source_stream_id},'public.result_b'::regclass,0,0,'0/0'
  );
"
assert_query "source|2|1|0/0" "
  SELECT stream.producer_kind || '|' || count(consumer.*) || '|' ||
         min(consumer.next_chunk_seq) || '|' ||
         min(consumer.consumed_frontier_lsn)
  FROM shiba_internal.effect_streams AS stream
  JOIN shiba_internal.effect_stream_consumers AS consumer USING (stream_id)
  WHERE stream.stream_id=${source_stream_id}
  GROUP BY stream.producer_kind
"

assert_query "appended|1" "
  SELECT outcome || '|' || appended_chunk_seq
  FROM shiba_internal.append_effect_stream_chunk(
    ${source_stream_id},1,'data',4,400,'0/10'
  )
"
assert_query "appended|2" "
  SELECT outcome || '|' || appended_chunk_seq
  FROM shiba_internal.append_effect_stream_chunk(
    ${source_stream_id},2,'data',4,400,'0/10'
  )
"
assert_query "2|8|800|true|0/10" "
  SELECT buffered_chunks || '|' || buffered_rows || '|' ||
         buffered_bytes || '|' || backpressured || '|' || latest_data_lsn
  FROM shiba_internal.effect_streams
  WHERE stream_id=${source_stream_id}
"
assert_query "blocked|" "
  SELECT outcome || '|' || coalesce(appended_chunk_seq::text,'')
  FROM shiba_internal.append_effect_stream_chunk(
    ${source_stream_id},3,'data',1,100,'0/20'
  )
"

# The generation frontier becomes visible only after every source chunk in its
# contiguous commit prefix is durable.
psql_core -qc "
  UPDATE shiba_internal.ingress_replay_state
  SET published_lsn='0/10'
  WHERE slot_generation=1
"
assert_query "3|0/10" "
  SELECT next_chunk_seq || '|' || consumed_frontier_lsn
  FROM shiba_internal.advance_effect_stream_consumer(
    ${source_stream_id},'public.result_a'::regclass,0,0,
    1,3,'0/0','0/10',2,8,800
  )
"
assert_query "0|1|true" "
  SELECT deleted_chunks || '|' || first_retained_chunk_seq || '|' ||
         backpressured
  FROM shiba_internal.gc_effect_stream(${source_stream_id},2,8,800)
"

# A frontier cannot jump over a published source chunk that this consumer has
# not consumed yet.
expect_failure \
  "frontier would skip published data" \
  "SELECT * FROM shiba_internal.advance_effect_stream_consumer(
     ${source_stream_id},'public.result_b'::regclass,0,0,
     1,2,'0/0','0/10',1,4,400
   )"
assert_query "3|0/10" "
  SELECT next_chunk_seq || '|' || consumed_frontier_lsn
  FROM shiba_internal.advance_effect_stream_consumer(
    ${source_stream_id},'public.result_b'::regclass,0,0,
    1,3,'0/0','0/10',2,8,800
  )
"
assert_query "2|8|800|3|false" "
  SELECT deleted_chunks || '|' || deleted_rows || '|' || deleted_bytes ||
         '|' || first_retained_chunk_seq || '|' || backpressured
  FROM shiba_internal.gc_effect_stream(${source_stream_id},2,8,800)
"

# A late Scan backfills through 0/30 and joins the current tail.  Replication
# is still published only through 0/10, so an old 0/30 source batch can arrive
# after attachment.  It advances the cursor but cannot be applied again.
assert_query "appended|3" "
  SELECT outcome || '|' || appended_chunk_seq
  FROM shiba_internal.append_effect_stream_chunk(
    ${source_stream_id},3,'data',4,400,'0/20'
  )
"
psql_core -qc "
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${source_stream_id},'public.result_a'::regclass,2,0,'0/30'
  )
"
assert_query "4|0/30" "
  SELECT next_chunk_seq || '|' || consumed_frontier_lsn
  FROM shiba_internal.effect_stream_consumers
  WHERE stream_id=${source_stream_id}
    AND result_oid='public.result_a'::regclass
    AND consumer_stage_id=2
"
assert_query "appended|4" "
  SELECT outcome || '|' || appended_chunk_seq
  FROM shiba_internal.append_effect_stream_chunk(
    ${source_stream_id},4,'data',1,100,'0/30'
  )
"
assert_query "5|0/30" "
  SELECT next_chunk_seq || '|' || consumed_frontier_lsn
  FROM shiba_internal.advance_effect_stream_consumer(
    ${source_stream_id},'public.result_a'::regclass,2,0,
    4,5,'0/30','0/30',1,1,100
  )
"
psql_core -qc "
  UPDATE shiba_internal.ingress_replay_state
  SET published_lsn='0/30'
  WHERE slot_generation=1
"
expect_failure \
  "must follow published ingress frontier" \
  "SELECT * FROM shiba_internal.append_effect_stream_chunk(
     ${source_stream_id},5,'data',1,100,'0/30'
   )"
assert_query "appended|5" "
  SELECT outcome || '|' || appended_chunk_seq
  FROM shiba_internal.append_effect_stream_chunk(
    ${source_stream_id},5,'data',1,100,'0/40'
  )
"
expect_failure \
  "not monotonic" \
  "SELECT * FROM shiba_internal.append_effect_stream_chunk(
     ${source_stream_id},6,'data',1,100,'0/35'
   )"
psql_core -qc "
  UPDATE shiba_internal.ingress_replay_state
  SET published_lsn='0/40'
  WHERE slot_generation=1
"
assert_query "6|0/40" "
  SELECT next_chunk_seq || '|' || consumed_frontier_lsn
  FROM shiba_internal.advance_effect_stream_consumer(
    ${source_stream_id},'public.result_a'::regclass,2,0,
    5,6,'0/30','0/40',1,1,100
  )
"
expect_failure \
  "source streams contain data chunks only" \
  "SELECT * FROM shiba_internal.append_effect_stream_chunk(
     ${source_stream_id},6,'frontier',0,0,'0/50'
   )"

# No consumer means no effect and therefore no payload validation.  Even an
# invalid, oversized input is discarded after the replay sequence CAS.
unwatched_stream_id="$(psql_core -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES (
    'source',1,'public.unwatched_source'::regclass,
    4,400,4,8,800,1,4,400
  )
  RETURNING stream_id
")"
assert_query "discarded|" "
  SELECT outcome || '|' || coalesce(appended_chunk_seq::text,'')
  FROM shiba_internal.append_effect_stream_chunk(
    ${unwatched_stream_id},1,'invalid',999,999999,NULL
  )
"
assert_query "1|0" "
  SELECT next_chunk_seq || '|' || buffered_chunks
  FROM shiba_internal.effect_streams
  WHERE stream_id=${unwatched_stream_id}
"
psql_core -qc "
  SELECT shiba_internal.create_effect_stream_payload(
    ${unwatched_stream_id},
    jsonb_build_array(jsonb_build_object(
      'slot_id',NULL,
      'attnum',1,
      'name','id',
      'type_oid','bigint'::regtype::oid::bigint,
      'typmod',-1,
      'collation_oid',0,
      'nullable',false
    ))
  );
  SELECT shiba_internal.validate_effect_stream_payload(
    ${unwatched_stream_id},
    jsonb_build_array(jsonb_build_object(
      'slot_id',NULL,
      'attnum',1,
      'name','id',
      'type_oid','bigint'::regtype::oid::bigint,
      'typmod',-1,
      'collation_oid',0,
      'nullable',false
    ))
  );
"
assert_query "stream_id,chunk_seq,row_ordinal,weight,row_value" "
  SELECT string_agg(attribute.attname,',' ORDER BY attribute.attnum)
  FROM shiba_internal.effect_streams AS stream
  JOIN pg_attribute AS attribute
    ON attribute.attrelid=stream.relation_oid
   AND attribute.attnum>0
   AND NOT attribute.attisdropped
  WHERE stream.stream_id=${unwatched_stream_id}
"

# Operator output, typed payload, continuation and checkpoint commit together.
operator_stream_id="$(psql_core -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,producer_result_oid,producer_stage_id,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES (
    'operator','public.result_a'::regclass,0,
    4,100,
    4,8,200,
    1,4,100
  )
  RETURNING stream_id
")"
psql_core -qc "
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${operator_stream_id},'public.result_a'::regclass,1,0,'0/40'
  )
"
# Operator edges start before their producer's activation SnapshotFrontier.
# The dataflow activation boundary applies only to source-stream consumers.
assert_query "1|0/0|0/0" "
  SELECT next_chunk_seq || '|' || activation_lsn || '|' ||
         consumed_frontier_lsn
  FROM shiba_internal.effect_stream_consumers
  WHERE stream_id=${operator_stream_id}
    AND result_oid='public.result_a'::regclass
    AND consumer_stage_id=1
    AND input_port=0
"
psql_core -qc "
  BEGIN;
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${operator_stream_id},1,'data',1,1000,'0/50'
  );
  INSERT INTO shiba_internal.test_effect_payload(
    stream_id,chunk_seq,row_ordinal,payload
  )
  VALUES(${operator_stream_id},1,1,repeat('x',1000));
  UPDATE shiba_internal.operator_checkpoints
  SET revision=revision+1,
      has_continuation=true,
      updated_at=clock_timestamp()
  WHERE result_oid='public.result_a'::regclass
    AND stage_id=0
    AND revision=0;
  COMMIT;
"
assert_query "1000|true|1|true|1" "
  SELECT stream.buffered_bytes || '|' || stream.backpressured || '|' ||
         checkpoint.revision || '|' || checkpoint.has_continuation || '|' ||
         (SELECT count(*) FROM shiba_internal.test_effect_payload
          WHERE stream_id=${operator_stream_id})
  FROM shiba_internal.effect_streams AS stream
  CROSS JOIN shiba_internal.operator_checkpoints AS checkpoint
  WHERE stream.stream_id=${operator_stream_id}
    AND checkpoint.result_oid='public.result_a'::regclass
    AND checkpoint.stage_id=0
"
assert_query "1:data:0/50" "
  SELECT string_agg(chunk_seq || ':' || chunk_kind || ':' || chunk_lsn, ',')
  FROM (
    SELECT chunk_seq,chunk_kind,chunk_lsn
    FROM shiba_internal.effect_stream_chunks
    WHERE stream_id=${operator_stream_id}
    ORDER BY chunk_seq
    LIMIT 1
  ) AS bounded
"
assert_query "2|0/0" "
  SELECT next_chunk_seq || '|' || consumed_frontier_lsn
  FROM shiba_internal.advance_effect_stream_consumer(
    ${operator_stream_id},'public.result_a'::regclass,1,0,
    1,2,'0/0','0/0',1,0,10
  )
"
assert_query "1|1|1000|2|false" "
  SELECT deleted_chunks || '|' || deleted_rows || '|' || deleted_bytes ||
         '|' || first_retained_chunk_seq || '|' || backpressured
  FROM shiba_internal.gc_effect_stream(${operator_stream_id},1,0,10)
"
assert_query "0" "
  SELECT count(*) FROM shiba_internal.test_effect_payload
  WHERE stream_id=${operator_stream_id}
"

# A failed local step cannot leave metadata, typed payload, or checkpoint
# authority behind.
psql_core -qc "
  BEGIN;
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${operator_stream_id},2,'data',1,50,'0/60'
  );
  INSERT INTO shiba_internal.test_effect_payload(
    stream_id,chunk_seq,row_ordinal,payload
  )
  VALUES(${operator_stream_id},2,1,'rolled back');
  UPDATE shiba_internal.operator_checkpoints
  SET revision=revision+1,
      has_continuation=false,
      updated_at=clock_timestamp()
  WHERE result_oid='public.result_a'::regclass
    AND stage_id=0
    AND revision=1;
  ROLLBACK;
"
assert_query "0|0|1" "
  SELECT
    (SELECT count(*) FROM shiba_internal.effect_stream_chunks
     WHERE stream_id=${operator_stream_id}),
    (SELECT count(*) FROM shiba_internal.test_effect_payload
     WHERE stream_id=${operator_stream_id}),
    (SELECT revision FROM shiba_internal.operator_checkpoints
     WHERE result_oid='public.result_a'::regclass AND stage_id=0)
"
psql_core -qc "
  BEGIN;
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${operator_stream_id},2,'data',1,50,'0/60'
  );
  INSERT INTO shiba_internal.test_effect_payload(
    stream_id,chunk_seq,row_ordinal,payload
  )
  VALUES(${operator_stream_id},2,1,'durable');
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${operator_stream_id},3,'frontier',0,0,'0/60'
  );
  UPDATE shiba_internal.operator_checkpoints
  SET revision=revision+1,
      has_continuation=false,
      updated_at=clock_timestamp()
  WHERE result_oid='public.result_a'::regclass
    AND stage_id=0
    AND revision=1;
  COMMIT;
"
expect_failure \
  "operator data cannot follow its causal frontier" \
  "SELECT * FROM shiba_internal.append_effect_stream_chunk(
     ${operator_stream_id},4,'data',1,50,'0/60'
   )"
expect_failure \
  "operator frontier is not advancing" \
  "SELECT * FROM shiba_internal.append_effect_stream_chunk(
     ${operator_stream_id},4,'frontier',0,0,'0/60'
   )"
assert_query "4|0/60" "
  SELECT next_chunk_seq || '|' || consumed_frontier_lsn
  FROM shiba_internal.advance_effect_stream_consumer(
    ${operator_stream_id},'public.result_a'::regclass,1,0,
    2,4,'0/0','0/60',2,1,100
  )
"
expect_failure \
  "must match consumed frontier chunks" \
  "SELECT * FROM shiba_internal.advance_effect_stream_consumer(
     ${operator_stream_id},'public.result_a'::regclass,1,0,
     4,4,'0/60','0/70',1,0,0
   )"
assert_query "0" "
  WITH changed AS (
    UPDATE shiba_internal.operator_checkpoints
    SET revision=revision+1
    WHERE result_oid='public.result_a'::regclass
      AND stage_id=0
      AND revision=1
    RETURNING 1
  )
  SELECT count(*) FROM changed
"

# A later data chunk remains valid after a published frontier when its causal
# LSN strictly advances that frontier. This is the cross-step rule mirrored by
# StepContext::record_output_append.
psql_core -qc "
  BEGIN;
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${operator_stream_id},4,'data',1,50,'0/70'
  );
  INSERT INTO shiba_internal.test_effect_payload(
    stream_id,chunk_seq,row_ordinal,payload
  )
  VALUES(${operator_stream_id},4,1,'after frontier');
  COMMIT;
"
assert_query "5|0/70" "
  SELECT next_chunk_seq || '|' || latest_data_lsn
  FROM shiba_internal.effect_streams
  WHERE stream_id=${operator_stream_id}
"

# A single database-level publication frontier advances both inputs of a
# multi-source fan-in.  The idle source needs no empty source chunk.
idle_stream_a="$(psql_core -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES(
    'source',2,'public.idle_source_a'::regclass,
    4,400,8,16,1600,2,8,800
  )
  RETURNING stream_id
")"
idle_stream_b="$(psql_core -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES(
    'source',2,'public.idle_source_b'::regclass,
    4,400,8,16,1600,2,8,800
  )
  RETURNING stream_id
")"
psql_core -qc "
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${idle_stream_a},'public.result_b'::regclass,2,0,'0/100'
  );
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${idle_stream_b},'public.result_b'::regclass,2,1,'0/100'
  );
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${idle_stream_a},1,'data',1,20,'0/110'
  );
  UPDATE shiba_internal.ingress_replay_state
  SET published_lsn='0/110'
  WHERE slot_generation=2;
"
assert_query "2|0/110" "
  SELECT next_chunk_seq || '|' || consumed_frontier_lsn
  FROM shiba_internal.advance_effect_stream_consumer(
    ${idle_stream_a},'public.result_b'::regclass,2,0,
    1,2,'0/100','0/110',1,1,20
  )
"
assert_query "1|0/110" "
  SELECT next_chunk_seq || '|' || consumed_frontier_lsn
  FROM shiba_internal.advance_effect_stream_consumer(
    ${idle_stream_b},'public.result_b'::regclass,2,1,
    1,1,'0/100','0/110',1,0,0
  )
"
assert_query "0/110" "
  SELECT min(consumed_frontier_lsn)
  FROM shiba_internal.effect_stream_consumers
  WHERE result_oid='public.result_b'::regclass
    AND consumer_stage_id=2
"

# Concurrent append and GC serialize on the stream row.  GC may reclaim the
# already-consumed prefix but cannot lose the concurrently appended tail.
concurrent_stream_id="$(psql_core -Atqc "
  INSERT INTO shiba_internal.effect_streams(
    producer_kind,slot_generation,source_oid,
    target_chunk_rows,target_chunk_bytes,
    high_chunks,high_rows,high_bytes,
    low_chunks,low_rows,low_bytes
  )
  VALUES(
    'source',3,'public.concurrent_source'::regclass,
    4,400,16,64,6400,4,16,1600
  )
  RETURNING stream_id
")"
psql_core -qc "
  SELECT shiba_internal.attach_effect_stream_consumer(
    ${concurrent_stream_id},'public.result_b'::regclass,3,0,'0/200'
  );
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${concurrent_stream_id},1,'data',1,10,'0/210'
  );
  SELECT * FROM shiba_internal.advance_effect_stream_consumer(
    ${concurrent_stream_id},'public.result_b'::regclass,3,0,
    1,2,'0/200','0/200',1,1,10
  );
"
append_gc_output="${pg_data_dir}/append-gc.out"
PGAPPNAME="shiba_effect_append_gc" psql_core -qc "
  BEGIN;
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${concurrent_stream_id},2,'data',1,10,'0/220'
  );
  SELECT pg_sleep(0.8);
  COMMIT;
" >"${append_gc_output}" 2>&1 &
append_gc_pid=$!
wait_for_query "1" "
  SELECT count(*)
  FROM pg_stat_activity
  WHERE application_name='shiba_effect_append_gc'
    AND wait_event='PgSleep'
" "append transaction to hold the stream lock"
psql_core -qc "
  SELECT * FROM shiba_internal.gc_effect_stream(
    ${concurrent_stream_id},4,4,400
  )
"
wait "${append_gc_pid}"
assert_query "3|2|1|1|10|2" "
  SELECT stream.next_chunk_seq || '|' ||
         stream.first_retained_chunk_seq || '|' ||
         stream.buffered_chunks || '|' || stream.buffered_rows || '|' ||
         stream.buffered_bytes || '|' || min(chunk.chunk_seq)
  FROM shiba_internal.effect_streams AS stream
  JOIN shiba_internal.effect_stream_chunks AS chunk USING(stream_id)
  WHERE stream.stream_id=${concurrent_stream_id}
  GROUP BY stream.stream_id
"

# Two publishers using the same expected sequence cannot both commit.
cas_winner_output="${pg_data_dir}/cas-winner.out"
cas_loser_output="${pg_data_dir}/cas-loser.out"
PGAPPNAME="shiba_effect_cas_winner" psql_core -qc "
  BEGIN;
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${concurrent_stream_id},3,'data',1,10,'0/230'
  );
  SELECT pg_sleep(0.8);
  COMMIT;
" >"${cas_winner_output}" 2>&1 &
cas_winner_pid=$!
wait_for_query "1" "
  SELECT count(*)
  FROM pg_stat_activity
  WHERE application_name='shiba_effect_cas_winner'
    AND wait_event='PgSleep'
" "CAS winner to hold the stream lock"
if psql_core -qc "
  SELECT * FROM shiba_internal.append_effect_stream_chunk(
    ${concurrent_stream_id},3,'data',1,10,'0/230'
  )
" >"${cas_loser_output}" 2>&1; then
  fail "concurrent append CAS loser unexpectedly committed"
fi
wait "${cas_winner_pid}"
if ! grep -Fq "effect stream expected chunk" "${cas_loser_output}"; then
  fail "concurrent append CAS loser returned the wrong error"
fi
assert_query "4|1" "
  SELECT stream.next_chunk_seq || '|' || count(chunk.*)
  FROM shiba_internal.effect_streams AS stream
  JOIN shiba_internal.effect_stream_chunks AS chunk USING(stream_id)
  WHERE stream.stream_id=${concurrent_stream_id}
    AND chunk.chunk_seq=3
  GROUP BY stream.next_chunk_seq
"

printf 'Effect stream core tests passed.\n'
