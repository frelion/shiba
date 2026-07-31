#!/usr/bin/env bash
set -euo pipefail

# Real PostgreSQL acceptance gate for the bounded Window and TopN kernels.
# Every maintained result is compared with a fresh PostgreSQL recomputation.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

static_require() {
  local file="$1"
  local pattern="$2"
  local description="$3"
  if ! grep -Eq "${pattern}" "${project_root}/${file}"; then
    printf 'Window/TopN static gate failed: %s\n' "${description}" >&2
    exit 1
  fi
}

static_require src/execution/window/output.rs \
  'bounded_prefix AS MATERIALIZED' \
  'Window does not page the primary comparison relation first'
static_require src/execution/window/output.rs \
  'input_rows: compared_rows' \
  'Window does not account compared rows as input work'
static_require src/execution/window/step.rs \
  'cursor_repeat' \
  'Window has no durable huge-multiplicity residual cursor'
for kernel in src/execution/topn/runtime.rs; do
  static_require "${kernel}" \
    'bounded_prefix AS MATERIALIZED' \
    "${kernel} does not page the primary comparison relation first"
  static_require "${kernel}" \
    'input_rows: compared_rows' \
    "${kernel} does not account compared rows as input work"
  static_require "${kernel}" \
    'cursor_repeat' \
    "${kernel} has no durable huge-multiplicity residual cursor"
done
static_require src/execution/window/provision.rs \
  'window_dirty_partitions_' \
  'Window has no partial dirty-partition index'
static_require src/execution/window/provision.rs \
  'window_candidate_page_' \
  'Window has no partition/candidate page index'
static_require src/execution/window/provision.rs \
  'window_visible_page_' \
  'Window has no partition/visible page index'
static_require src/execution/window/output.rs \
  'JOIN source AS source_row' \
  'Window output does not reuse the materialized source page'
if grep -Fq 'ON input_row.entry_id=updated.entry_id' \
  "${project_root}/src/execution/window/output.rs"; then
  printf '%s\n' \
    'Window/TopN static gate failed: output phase re-queries input for updated rows' >&2
  exit 1
fi
static_require src/execution/window/mod.rs \
  'WINDOW_FOLD_WORK_ITEM_CAP' \
  'Window aggregate Fold has no explicit empty-frame work-item cap'
static_require src/execution/window/provision.rs \
  'fold_ready' \
  'Window aggregate Fold has no durable ready-to-finalize state'
static_require src/execution/window/primitives.rs \
  'scalar_work_bytes_sql' \
  'Window aggregate Fold does not account materialized function bytes'
static_require src/execution/window/primitives.rs \
  'missing_frame' \
  'Window aggregate Fold treats a missing frame as an empty frame'
for interval in 1 2 3; do
  static_require src/execution/window/primitives.rs \
    "interval_${interval} AS MATERIALIZED" \
    "Window aggregate frame interval ${interval} is not independently bounded"
done
static_require src/execution/window/primitives.rs \
  'SELECT selected\.row_value AS row_value' \
  'Window aggregate Fold does not reuse the interval row payload'
if grep -Fq 'ON current_input.entry_id=selected.entry_id' \
  "${project_root}/src/execution/window/primitives.rs"; then
  printf '%s\n' \
    'Window/TopN static gate failed: aggregate Fold re-queries input for selected rows' >&2
  exit 1
fi
if grep -Eq 'pg_catalog\.record_send' \
  "${project_root}/src/execution/window/output.rs" \
  "${project_root}/src/execution/window/primitives.rs" \
  "${project_root}/src/execution/topn/runtime.rs"; then
  printf '%s\n' \
    'Window/TopN static gate failed: raw record_send identity remains' >&2
  exit 1
fi
if grep -Eq \
  'OR \(ordered\.ordinal BETWEEN frame\.start_2' \
  "${project_root}/src/execution/window/output.rs" \
  "${project_root}/src/execution/window/primitives.rs"; then
  printf '%s\n' \
    'Window/TopN static gate failed: aggregate frame still uses one OR range scan' >&2
  exit 1
fi

pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-window-topn-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-window-topn-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_WINDOW_TOPN_TEST_PORT:-$((62000 + $$ % 3000))}"
database_name="shiba_window_topn"
wait_attempts="${SHIBA_WINDOW_TOPN_WAIT_ATTEMPTS:-2400}"
progress_hard_seconds="${SHIBA_WINDOW_TOPN_PROGRESS_HARD_SECONDS:-600}"
progress_stall_seconds="${SHIBA_WINDOW_TOPN_PROGRESS_STALL_SECONDS:-120}"

psql_gate() {
  PGOPTIONS="-c statement_timeout=60000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

test_name="Window/TopN kernel gate"
test_psql_command=psql_gate
test_log_lines=240
test_wait_attempts="${wait_attempts}"
test_wait_sleep=0.05
test_retain_log=1
source "${project_root}/scripts/test-lib.sh"
trap cleanup EXIT

wait_for_target_progress_query() {
  local expected="$1"
  local query="$2"
  local result_oid="$3"
  local description="$4"
  local snapshot=""
  local previous_snapshot=""
  local actual=""
  local started_at="${SECONDS}"
  local last_progress_at="${SECONDS}"
  while test $((SECONDS - started_at)) -lt "${progress_hard_seconds}"; do
    if snapshot="$(psql_gate -Atqc "
      WITH observed AS (${query})
      SELECT observed.value,
             coalesce((
               SELECT string_agg(
                 checkpoint.stage_id || ':' ||
                 checkpoint.revision || ':' ||
                 checkpoint.has_continuation,
                 ',' ORDER BY checkpoint.stage_id
               )
               FROM shiba_internal.operator_checkpoints AS checkpoint
               WHERE checkpoint.result_oid=${result_oid}::oid
             ),''),
             coalesce((
               SELECT string_agg(
                 consumer.consumer_stage_id || ':' ||
                 consumer.input_port || ':' ||
                 consumer.next_chunk_seq || '/' ||
                 input.next_chunk_seq || ':' ||
                 consumer.consumed_frontier_lsn,
                 ',' ORDER BY
                   consumer.consumer_stage_id,consumer.input_port
               )
               FROM shiba_internal.effect_stream_consumers AS consumer
               JOIN shiba_internal.effect_streams AS input
                 ON input.stream_id=consumer.stream_id
               WHERE consumer.result_oid=${result_oid}::oid
             ),'')
      FROM observed" 2>/dev/null)"; then
      actual="${snapshot%%|*}"
      if test "${actual}" = "${expected}"; then
        return
      fi
      if test "${snapshot}" != "${previous_snapshot}"; then
        previous_snapshot="${snapshot}"
        last_progress_at="${SECONDS}"
      fi
    fi
    if test $((SECONDS - last_progress_at)) \
      -ge "${progress_stall_seconds}"; then
      fail "stalled waiting for ${description}; target snapshot was [${snapshot}]"
    fi
    sleep 1
  done
  fail "timed out waiting for ${description}; last value was [${actual}]"
}

assert_bag_equal_with_progress() {
  local expected_sql="$1"
  local actual_sql="$2"
  local result_oid="$3"
  local description="$4"
  wait_for_target_progress_query "0" "
    WITH expected AS (${expected_sql}),
    actual AS (${actual_sql}),
    difference AS (
      (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
      UNION ALL
      (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
    )
    SELECT count(*) AS value FROM difference" \
    "${result_oid}" "${description}"
}

stage_id() {
  local result_oid="$1"
  local operator="$2"
  local occurrence="${3:-1}"
  psql_gate -Atqc "
    SELECT stage_id
    FROM (
      SELECT stage.ordinality-1 AS stage_id,
             row_number() OVER (ORDER BY stage.ordinality) AS occurrence
      FROM shiba_internal.dataflows AS dataflow
      CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
        WITH ORDINALITY AS stage(value,ordinality)
      WHERE dataflow.result_oid=${result_oid}::oid
        AND stage.value->'spec'->>'operator'='${operator}'
    ) AS matching
    WHERE occurrence=${occurrence}"
}

window_expected="
  SELECT id,
         grp,
         score,
         payload,
         row_number() OVER rows_window AS row_number_value,
         rank() OVER peer_window AS rank_value,
         dense_rank() OVER peer_window AS dense_rank_value,
         ntile(tile_count) OVER rows_window AS tile_value,
         lag(payload,2,'missing') OVER rows_window AS lag_value,
         lead(score,1,-999) OVER rows_window AS lead_value,
         first_value(payload) OVER rows_window AS first_value_value,
         last_value(payload) OVER rows_window AS last_value_value,
         nth_value(payload,2) OVER rows_window AS nth_value_value,
         count(*) FILTER (WHERE payload IS NOT NULL)
           OVER rows_window AS frame_count,
         max(score) FILTER (WHERE payload IS NOT NULL)
           OVER rows_window AS frame_max
  FROM public.kernel_source
  WINDOW
    peer_window AS (
      PARTITION BY grp
      ORDER BY score DESC NULLS LAST
    ),
    rows_window AS (
      PARTITION BY grp
      ORDER BY score DESC NULLS LAST,id
      ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
    )"

topn_expected="
  SELECT id,grp,score,payload
  FROM public.kernel_source
  ORDER BY score DESC NULLS LAST,id
  OFFSET 3 ROWS FETCH FIRST 7 ROWS ONLY"

ties_expected="
  SELECT id,grp,score,payload
  FROM public.kernel_source
  ORDER BY score DESC NULLS LAST
  FETCH FIRST 4 ROWS WITH TIES"

window_topn_expected="
  SELECT id,
         grp,
         score,
         row_number() OVER (
           PARTITION BY grp
           ORDER BY score DESC NULLS LAST,id
         ) AS partition_row
  FROM public.kernel_source
  ORDER BY partition_row,id
  OFFSET 2 ROWS FETCH FIRST 11 ROWS ONLY"

window_bag_expected="
  SELECT grp_bucket,
         score_bucket,
         row_number() OVER (
           PARTITION BY grp_bucket
           ORDER BY score_bucket
         ) AS partition_row
  FROM (
    SELECT (id%2)::integer AS grp_bucket,
           (id%3)::integer AS score_bucket
    FROM public.kernel_source
  ) AS projected"

ordered_fold_expected="
  SELECT id,
         sum(value) OVER full_partition AS ordered_sum,
         ntile(bucket_count) OVER full_partition AS null_tiles
  FROM public.fold_source
  WINDOW full_partition AS (
    ORDER BY sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  )"

empty_fold_expected="
  SELECT id,payload,
         sum(value) OVER (
           ORDER BY sequence
           ROWS BETWEEN CURRENT ROW AND CURRENT ROW
           EXCLUDE CURRENT ROW
         ) AS empty_sum
  FROM public.fold_source"

ntile_suffix_expected="
  SELECT id,ntile(bucket_count) OVER (ORDER BY id) AS tile
  FROM public.ntile_source"

ntile_recovery_expected="
  SELECT id,ntile(bucket_count) OVER (ORDER BY id) AS tile
  FROM public.ntile_recovery_source"

assert_all_results() {
  local description="$1"
  assert_bag_equal_with_progress "${window_expected}" "
    SELECT id,grp,score,payload,row_number_value,rank_value,
           dense_rank_value,tile_value,lag_value,lead_value,
           first_value_value,last_value_value,nth_value_value,
           frame_count,frame_max
    FROM shiba.window_result" "${window_result_oid}" \
    "${description}: Window"
  assert_bag_equal "${topn_expected}" "
    SELECT id,grp,score,payload
    FROM shiba.topn_result" \
    "${description}: TopN OFFSET/LIMIT"
  assert_bag_equal "${ties_expected}" "
    SELECT id,grp,score,payload
    FROM shiba.topn_ties_result" \
    "${description}: TopN WITH TIES"
  assert_bag_equal "${window_topn_expected}" "
    SELECT id,grp,score,partition_row
    FROM shiba.window_topn_result" \
    "${description}: Window -> TopN"
  assert_bag_equal "${window_bag_expected}" "
    SELECT grp_bucket,score_bucket,partition_row
    FROM shiba.window_bag_result" \
    "${description}: Window multiplicity paging"
  assert_bag_equal "${ordered_fold_expected}" "
    SELECT id,ordered_sum,null_tiles
    FROM shiba.ordered_fold_result" \
    "${description}: strict ordered aggregate fold"
  assert_bag_equal "${empty_fold_expected}" "
    SELECT id,payload,empty_sum
    FROM shiba.empty_fold_result" \
    "${description}: batched empty aggregate frames"
  assert_bag_equal "${ntile_suffix_expected}" "
    SELECT id,tile
    FROM shiba.ntile_suffix_result" \
    "${description}: ntile leading NULL suffix"
  assert_bag_equal "${ntile_recovery_expected}" "
    SELECT id,tile
    FROM shiba.ntile_recovery_result" \
    "${description}: ntile durable recovery"
}

cd "${project_root}"
install_test_extension "${pg_config_path}"

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
  printf "shiba.batch_rows = 8\n"
  printf "shiba.batch_bytes = 4096\n"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "$(id -un)"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_gate -qc "CREATE EXTENSION shiba"
psql_gate -qc "SELECT shiba.activate()"
wait_for_query "1" \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'" \
  "the singleton Runtime"

psql_gate -qc "
  CREATE TABLE public.shiba_runtime_failpoints (
    kind text PRIMARY KEY,
    runtime_pid integer,
    result_oid oid,
    stage_id integer,
    commit_lsn pg_lsn,
    pause_ms integer NOT NULL DEFAULT 0 CHECK (pause_ms>=0),
    fired boolean NOT NULL DEFAULT false,
    cursor_before bigint
  );
  CREATE TABLE public.kernel_source (
    id bigint PRIMARY KEY,
    grp integer,
    score integer,
    payload text,
    tile_count integer NOT NULL CHECK(tile_count>0)
  );
  CREATE TABLE public.fold_source (
    id bigint PRIMARY KEY,
    sequence integer NOT NULL UNIQUE,
    value double precision NOT NULL,
    payload text NOT NULL,
    bucket_count integer
  );
  CREATE TABLE public.ntile_source (
    id integer PRIMARY KEY,
    bucket_count integer
  );
  CREATE TABLE public.ntile_recovery_source (
    id integer PRIMARY KEY,
    bucket_count integer
  );
  INSERT INTO public.kernel_source
  SELECT id,
         CASE WHEN id%17=0 THEN NULL ELSE (id%2)::integer END,
         CASE WHEN id%13=0 THEN NULL ELSE (id%5)::integer END,
         CASE WHEN id%11=0 THEN NULL ELSE 'payload-'||id END,
         (2+id%4)::integer
  FROM generate_series(1,60) AS id;
  INSERT INTO public.fold_source
  SELECT id,id::integer,
         CASE id
           WHEN 1 THEN '1e16'::double precision
           WHEN 2 THEN 1::double precision
           WHEN 3 THEN '-1e16'::double precision
           WHEN 4 THEN 1::double precision
           ELSE 0::double precision
         END,
         'fold-'||id,
         CASE WHEN id=1 THEN NULL ELSE 4 END
  FROM generate_series(1,64) AS id;
  INSERT INTO public.ntile_source
  SELECT id,CASE WHEN id<=2 THEN NULL ELSE 3 END
  FROM generate_series(1,10) AS id;
  INSERT INTO public.ntile_recovery_source
  SELECT id,CASE WHEN id<=2 THEN NULL ELSE 3 END
  FROM generate_series(1,257) AS id"

psql_gate -qc "
  CREATE TABLE shiba.window_result AS
  ${window_expected};

  CREATE TABLE shiba.topn_result AS
  ${topn_expected};

  CREATE TABLE shiba.topn_ties_result AS
  ${ties_expected};

  CREATE TABLE shiba.window_topn_result AS
  ${window_topn_expected};

  CREATE TABLE shiba.window_bag_result AS
  ${window_bag_expected};

  CREATE TABLE shiba.ordered_fold_result AS
  ${ordered_fold_expected};

  CREATE TABLE shiba.empty_fold_result AS
  ${empty_fold_expected};

  CREATE TABLE shiba.ntile_suffix_result AS
  ${ntile_suffix_expected};

  CREATE TABLE shiba.ntile_recovery_result AS
  ${ntile_recovery_expected}"

window_result_oid="$(psql_gate -Atqc "
  SELECT 'shiba.window_result'::regclass::oid::integer")"
topn_result_oid="$(psql_gate -Atqc "
  SELECT 'shiba.topn_result'::regclass::oid::integer")"
fold_result_oid="$(psql_gate -Atqc "
  SELECT 'shiba.ordered_fold_result'::regclass::oid::integer")"
empty_fold_result_oid="$(psql_gate -Atqc "
  SELECT 'shiba.empty_fold_result'::regclass::oid::integer")"
ntile_recovery_result_oid="$(psql_gate -Atqc "
  SELECT 'shiba.ntile_recovery_result'::regclass::oid::integer")"
window_stage="$(stage_id "${window_result_oid}" window 1)"
topn_stage="$(stage_id "${topn_result_oid}" topn 1)"
fold_stage="$(stage_id "${fold_result_oid}" window 1)"
empty_fold_stage="$(stage_id "${empty_fold_result_oid}" window 1)"
ntile_recovery_stage="$(
  stage_id "${ntile_recovery_result_oid}" window 1)"
fold_continuation="$(psql_gate -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_continuation_relations
  WHERE result_oid=${fold_result_oid}::oid
    AND stage_id=${fold_stage}")"
empty_fold_input="$(psql_gate -Atqc "
  SELECT stream_id
  FROM shiba_internal.effect_stream_consumers
  WHERE result_oid=${empty_fold_result_oid}::oid
    AND consumer_stage_id=${empty_fold_stage}
    AND input_port=0")"
empty_fold_continuation="$(psql_gate -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_continuation_relations
  WHERE result_oid=${empty_fold_result_oid}::oid
    AND stage_id=${empty_fold_stage}")"
empty_fold_candidate="$(psql_gate -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${empty_fold_result_oid}::oid
    AND stage_id=${empty_fold_stage}
    AND state_slot=5")"
empty_fold_accumulator="$(psql_gate -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${empty_fold_result_oid}::oid
    AND stage_id=${empty_fold_stage}
    AND state_slot=1001")"
ntile_recovery_continuation="$(psql_gate -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_continuation_relations
  WHERE result_oid=${ntile_recovery_result_oid}::oid
    AND stage_id=${ntile_recovery_stage}")"
ntile_recovery_state="$(psql_gate -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${ntile_recovery_result_oid}::oid
    AND stage_id=${ntile_recovery_stage}
    AND state_slot=2001")"

if test -z "${window_stage}" || test -z "${topn_stage}" ||
   test -z "${fold_stage}" || test -z "${fold_continuation}" ||
   test -z "${empty_fold_stage}" || test -z "${empty_fold_input}" ||
   test -z "${empty_fold_continuation}" ||
   test -z "${empty_fold_candidate}" ||
   test -z "${empty_fold_accumulator}" ||
   test -z "${ntile_recovery_stage}" ||
   test -z "${ntile_recovery_continuation}" ||
   test -z "${ntile_recovery_state}"; then
  fail "the lowered dataflows omitted Window or TopN"
fi

wait_for_empty_fold_idle() {
  local description="$1"
  wait_for_query "t" "
    SELECT NOT checkpoint.has_continuation
           AND consumer.next_chunk_seq=stream.next_chunk_seq
    FROM shiba_internal.operator_checkpoints AS checkpoint
    JOIN shiba_internal.effect_stream_consumers AS consumer
      ON consumer.result_oid=checkpoint.result_oid
     AND consumer.consumer_stage_id=checkpoint.stage_id
     AND consumer.input_port=0
    JOIN shiba_internal.effect_streams AS stream
      ON stream.stream_id=consumer.stream_id
    WHERE checkpoint.result_oid=${empty_fold_result_oid}::oid
      AND checkpoint.stage_id=${empty_fold_stage}" \
    "${description}"
}

assert_all_results "bootstrap snapshot"
assert_query "t" "
  SELECT count(*)>8
  FROM shiba.topn_ties_result"
assert_query "1|1|4" "
  SELECT count(DISTINCT ordered_sum),min(null_tiles),max(null_tiles)
  FROM shiba.ordered_fold_result"
assert_query "NULL|1|1|2|2|3|3|4|4" "
  SELECT string_agg(coalesce(null_tiles::text,'NULL'),'|' ORDER BY id)
  FROM shiba.ordered_fold_result
  WHERE id IN (1,2,17,18,33,34,49,50,64)"
assert_query "NULL|NULL|1|1|1|1|2|2|2|3" "
  SELECT string_agg(coalesce(tile::text,'NULL'),'|' ORDER BY id)
  FROM shiba.ntile_suffix_result"

# Every output has an empty aggregate frame, so Fold debits no frame-input rows
# or bytes. Finalizing each output still debits one row plus its exact
# function/candidate bytes; this fixture is primarily bounded by the 8-row
# stage budget. One update must rebuild all 64 outputs without reverting to one
# checkpoint revision per output ordinal. Unit coverage separately exercises
# the 64-item control-work cap.
wait_for_empty_fold_idle "the idle empty-frame Window"
empty_fold_revision="$(psql_gate -Atqc "
  SELECT revision
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${empty_fold_result_oid}::oid
    AND stage_id=${empty_fold_stage}")"
empty_fold_lsn="$(psql_gate -Atqc "
  UPDATE public.fold_source
  SET value=value+0.5::double precision
  WHERE id=63;
  SELECT pg_current_wal_lsn()")"
wait_for_query "t" "
  SELECT consumed_frontier_lsn>='${empty_fold_lsn}'::pg_lsn
  FROM shiba_internal.effect_stream_consumers
  WHERE stream_id=${empty_fold_input}
    AND result_oid=${empty_fold_result_oid}::oid
    AND consumer_stage_id=${empty_fold_stage}
    AND input_port=0" \
  "the batched empty-frame Window frontier"
assert_query "t" "
  SELECT revision-${empty_fold_revision}<=96
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${empty_fold_result_oid}::oid
    AND stage_id=${empty_fold_stage}"
assert_all_results "empty-frame Fold batching"
wait_for_empty_fold_idle "the completed empty-frame Fold batching"

# The first six small outputs in the final 8-row quantum leave too little byte
# budget for output 63. Fold must commit its complete accumulator as
# ready-to-finalize without writing that oversized candidate. The next step
# may materialize the oversized row only as its sole work item.
# Arm the crash from the continuation INSERT itself. This state-triggered
# failpoint cannot accidentally attach to an earlier Drain epoch or miss the
# short gap between two Runtime transactions.
psql_gate -qc "
  CREATE FUNCTION public.arm_empty_fold_ready_crash()
  RETURNS trigger
  LANGUAGE plpgsql
  AS \$arm\$
  BEGIN
    IF NEW.phase=5
       AND NEW.output_ordinal=63
       AND NEW.fold_ready THEN
      INSERT INTO public.shiba_runtime_failpoints(
        kind,runtime_pid,result_oid,stage_id,pause_ms,cursor_before
      )
      VALUES(
        'operator_step_after_commit',
        pg_backend_pid(),
        ${empty_fold_result_oid}::oid,
        ${empty_fold_stage},
        3000,
        57
      )
      ON CONFLICT(kind) DO NOTHING;
      UPDATE shiba_internal.dataflows
      SET active=false
      WHERE result_oid=${empty_fold_result_oid}::oid;
    END IF;
    RETURN NEW;
  END
  \$arm\$;
  CREATE TRIGGER arm_empty_fold_ready_crash
  AFTER INSERT ON ${empty_fold_continuation}
  FOR EACH ROW
  EXECUTE FUNCTION public.arm_empty_fold_ready_crash()"
large_empty_fold_lsn="$(psql_gate -Atqc "
  UPDATE public.fold_source
  SET payload=repeat('x',8192)
  WHERE id=63;
  SELECT pg_current_wal_lsn()")"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'
    AND result_oid=${empty_fold_result_oid}::oid
    AND stage_id=${empty_fold_stage}
    AND cursor_before=57" \
  "the committed byte-blocked empty-frame finalization"
large_empty_runtime_pid="$(psql_gate -Atqc "
  SELECT runtime_pid
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'")"
assert_query "5|63|true|62|1" "
  SELECT continuation.phase
         || '|' || continuation.output_ordinal
         || '|' || continuation.fold_ready
         || '|' || (SELECT count(*) FROM ${empty_fold_candidate})
         || '|' || (SELECT count(*) FROM ${empty_fold_accumulator})
  FROM ${empty_fold_continuation} AS continuation"
wait_for_runtime_replacement "${large_empty_runtime_pid}"
psql_gate -qc "
  DROP TRIGGER arm_empty_fold_ready_crash
  ON ${empty_fold_continuation};
  DROP FUNCTION public.arm_empty_fold_ready_crash();
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit';
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${empty_fold_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_query "t" "
  SELECT consumed_frontier_lsn>='${large_empty_fold_lsn}'::pg_lsn
  FROM shiba_internal.effect_stream_consumers
  WHERE stream_id=${empty_fold_input}
    AND result_oid=${empty_fold_result_oid}::oid
    AND consumer_stage_id=${empty_fold_stage}
    AND input_port=0" \
  "the oversized empty-frame Window frontier"
assert_all_results "oversized empty-frame finalization"
wait_for_empty_fold_idle "the completed oversized empty-frame finalization"

# A pre-commit crash now targets a Fold step whose clean cursor can cover
# several output ordinals. Candidate rows from the entire uncommitted batch
# and its advanced cursor must both roll back to the preceding commit.
rollback_runtime_pid="$(runtime_pid)"
psql_gate -qc "
  CREATE FUNCTION public.arm_empty_fold_rollback()
  RETURNS trigger
  LANGUAGE plpgsql
  AS \$arm\$
  BEGIN
    IF NEW.phase=5
       AND NEW.output_ordinal=9
       AND NOT NEW.fold_ready
       AND NEW.cursor_row_id IS NULL THEN
      INSERT INTO public.shiba_runtime_failpoints(
        kind,runtime_pid,result_oid,stage_id,pause_ms,cursor_before
      )
      VALUES(
        'operator_step_before_commit',
        pg_backend_pid(),
        ${empty_fold_result_oid}::oid,
        ${empty_fold_stage},
        3000,
        1
      )
      ON CONFLICT(kind) DO NOTHING;
    END IF;
    RETURN NEW;
  END
  \$arm\$;
  CREATE TRIGGER arm_empty_fold_rollback
  AFTER INSERT ON ${empty_fold_continuation}
  FOR EACH ROW
  EXECUTE FUNCTION public.arm_empty_fold_rollback()"
rollback_empty_fold_lsn="$(psql_gate -Atqc "
  UPDATE public.fold_source
  SET payload='rollback-'||id
  WHERE id=63;
  SELECT pg_current_wal_lsn()")"
wait_for_log \
  "operator_step_before_commit result ${empty_fold_result_oid} stage ${empty_fold_stage}" \
  "a paused pre-commit multi-ordinal Fold crash"
psql_gate -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${empty_fold_result_oid}::oid"
assert_query "5|1|false|0|0" "
  SELECT continuation.phase
         || '|' || continuation.output_ordinal
         || '|' || continuation.fold_ready
         || '|' || (SELECT count(*) FROM ${empty_fold_candidate})
         || '|' || (SELECT count(*) FROM ${empty_fold_accumulator})
  FROM ${empty_fold_continuation} AS continuation"
wait_for_runtime_replacement "${rollback_runtime_pid}"
psql_gate -qc "
  DROP TRIGGER arm_empty_fold_rollback
  ON ${empty_fold_continuation};
  DROP FUNCTION public.arm_empty_fold_rollback();
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_before_commit';
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${empty_fold_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_query "t" "
  SELECT consumed_frontier_lsn>='${rollback_empty_fold_lsn}'::pg_lsn
  FROM shiba_internal.effect_stream_consumers
  WHERE stream_id=${empty_fold_input}
    AND result_oid=${empty_fold_result_oid}::oid
    AND consumer_stage_id=${empty_fold_stage}
    AND input_port=0" \
  "the recovered multi-ordinal Fold frontier"
assert_all_results "pre-commit multi-ordinal Fold recovery"

# Ties, NULL ordering, group movement, updates, and deletes all rebuild from
# typed authoritative state and converge to a fresh PostgreSQL result.
psql_gate -qc "
  INSERT INTO public.kernel_source
  SELECT id,
         CASE WHEN id%19=0 THEN NULL ELSE (id%7)::integer END,
         CASE WHEN id%10=0 THEN NULL ELSE (id%5)::integer END,
         CASE WHEN id%8=0 THEN NULL ELSE 'stream-'||id END,
         (2+id%4)::integer
  FROM generate_series(61,120) AS id;
  DELETE FROM public.kernel_source
  WHERE id IN (2,7,13,29,61,88);
  UPDATE public.kernel_source
  SET grp=CASE WHEN id%2=0 THEN NULL ELSE (grp+2)%7 END,
      score=CASE WHEN id%3=0 THEN NULL ELSE 8-score END,
      payload=CASE WHEN id%5=0 THEN NULL ELSE payload||'-updated' END,
      tile_count=(2+id%3)::integer
  WHERE id%14=0 OR id IN (1,9,33,74)"
assert_all_results "streamed insert/update/delete"

# No effect stream may exceed either configured chunk target, except for one
# individually admitted oversized row.
assert_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks
  WHERE row_count>8
     OR (payload_bytes>4096 AND row_count<>1)"

# Force a multi-page full-frame fold, then crash while the persisted phase is
# still FoldAggregate. The paused pre-commit step must leave both accumulator
# state and its exact frame cursor at the prior commit.
psql_gate -qc "
  UPDATE public.fold_source SET value=2::double precision WHERE id=64"
wait_for_query "5" \
  "SELECT phase FROM ${fold_continuation}" \
  "the strict aggregate fold phase"
runtime_before="$(runtime_pid)"
psql_gate -qc "
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_before_commit',
    ${runtime_before},
    ${fold_result_oid}::oid,
    ${fold_stage},
    3000
  )"
wait_for_log \
  "operator_step_before_commit result ${fold_result_oid} stage ${fold_stage}" \
  "a paused pre-commit aggregate fold crash"
assert_query "5" "SELECT phase FROM ${fold_continuation}"
wait_for_runtime_replacement "${runtime_before}"
psql_gate -qc "
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_before_commit'"
assert_all_results "pre-commit ordered fold recovery"

# Make ntile span hundreds of Evaluate commits. Once its bucket and starting
# ordinal are durable, kill the following page before commit and prove that
# both state and cursor remain at the preceding boundary.
psql_gate -qc "ALTER SYSTEM SET shiba.batch_rows='1'"
psql_gate -qc "SELECT pg_reload_conf()"
wait_for_query "1" "
  SELECT setting::integer
  FROM pg_settings
  WHERE name='shiba.batch_rows'" \
  "the single-row ntile evaluation page"
psql_gate -qc "
  UPDATE public.ntile_recovery_source SET bucket_count=7 WHERE id=257"
wait_for_query "1" "
  SELECT count(*)
  FROM ${ntile_recovery_continuation} AS continuation
  CROSS JOIN ${ntile_recovery_state} AS state
  WHERE continuation.phase=6
    AND continuation.function_ordinal=1
    AND continuation.cursor_row_id IS NOT NULL
    AND continuation.cursor_row_id<128
    AND state.singleton
    AND state.bucket_count=3
    AND state.first_ordinal=3" \
  "initialized durable ntile state"
runtime_before="$(runtime_pid)"
psql_gate -qc "
  BEGIN;
  SELECT pg_advisory_xact_lock(
    shiba_internal.dataflow_lock_key(${ntile_recovery_result_oid}::oid)
  );
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms,cursor_before
  )
  SELECT
    'operator_step_before_commit',
    ${runtime_before},
    ${ntile_recovery_result_oid}::oid,
    ${ntile_recovery_stage},
    3000,
    continuation.cursor_row_id
  FROM ${ntile_recovery_continuation} AS continuation;
  COMMIT"
ntile_cursor_before="$(psql_gate -Atqc "
  SELECT cursor_before
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_before_commit'")"
wait_for_log \
  "operator_step_before_commit result ${ntile_recovery_result_oid} stage ${ntile_recovery_stage}" \
  "a paused pre-commit ntile evaluation crash"
assert_query "6|${ntile_cursor_before}|3|3" "
  SELECT continuation.phase
         || '|' || continuation.cursor_row_id
         || '|' || state.bucket_count
         || '|' || state.first_ordinal
  FROM ${ntile_recovery_continuation} AS continuation
  CROSS JOIN ${ntile_recovery_state} AS state
  WHERE state.singleton"
wait_for_runtime_replacement "${runtime_before}"
psql_gate -qc "ALTER SYSTEM SET shiba.batch_rows='8'"
psql_gate -qc "SELECT pg_reload_conf()"
psql_gate -qc "
  DELETE FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_before_commit'"
assert_bag_equal "${ntile_recovery_expected}" "
  SELECT id,tile
  FROM shiba.ntile_recovery_result" \
  "pre-commit ntile state recovery"
wait_for_query "0|0" "
  SELECT (SELECT count(*) FROM ${ntile_recovery_continuation})
         || '|' || (SELECT count(*) FROM ${ntile_recovery_state})" \
  "released ntile recovery state"

# Crash after a committed Window step. Its state, continuation, output chunk,
# and checkpoint have already committed together; the replacement Runtime
# resumes at the following bounded step.
runtime_before="$(runtime_pid)"
psql_gate -qc "
  INSERT INTO public.shiba_runtime_failpoints(
    kind,runtime_pid,result_oid,stage_id,pause_ms
  )
  VALUES(
    'operator_step_after_commit',
    ${runtime_before},
    ${window_result_oid}::oid,
    ${window_stage},
    0
  );
  INSERT INTO public.kernel_source
  SELECT id,(id%5)::integer,(id%5)::integer,'recovery-'||id,
         (2+id%4)::integer
  FROM generate_series(1001,1040) AS id"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "a committed Window step crash"
wait_for_runtime_replacement "${runtime_before}"
assert_all_results "post-commit Window recovery"

# Crash before a TopN step commits. PostgreSQL rolls its candidate state,
# continuation, output and checkpoint back as one transaction; retry must not
# duplicate either side of the old-to-new result delta.
runtime_before="$(runtime_pid)"
psql_gate -qc "
  UPDATE public.shiba_runtime_failpoints
  SET kind='operator_step_before_commit',
      runtime_pid=${runtime_before},
      result_oid=${topn_result_oid}::oid,
      stage_id=${topn_stage},
      fired=false
  WHERE kind='operator_step_after_commit';
  UPDATE public.kernel_source
  SET score=100,payload='topn-recovery'
  WHERE id IN (1001,1002,1003)"
wait_for_log \
  "operator_step_before_commit result ${topn_result_oid} stage ${topn_stage}" \
  "a pre-commit TopN step crash"
wait_for_runtime_replacement "${runtime_before}"
assert_all_results "pre-commit TopN recovery"

# A fresh TopN sees 8-row upstream chunks but has a 64-row admission quantum.
# Stop after its first committed Apply: dirty authoritative state is durable,
# while candidate selection and visible-output reconciliation have not started.
psql_gate -qc "ALTER SYSTEM SET shiba.batch_rows='64'"
psql_gate -qc "SELECT pg_reload_conf()"
wait_for_query "64" "
  SELECT setting::integer
  FROM pg_settings
  WHERE name='shiba.batch_rows'" \
  "the TopN Apply admission budget"
psql_gate -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  BEGIN;
  CREATE TABLE shiba.topn_batch_result AS
  SELECT id,grp,score,payload
  FROM public.kernel_source
  ORDER BY score DESC NULLS LAST
  FETCH FIRST 4 ROWS WITH TIES;
  UPDATE shiba_internal.effect_streams
  SET target_chunk_rows=8
  WHERE stream_id IN (
    SELECT stream_id
    FROM shiba_internal.effect_stream_consumers
    WHERE result_oid='shiba.topn_batch_result'::regclass
  );
  INSERT INTO public.shiba_runtime_failpoints(
    kind,result_oid,stage_id,pause_ms
  )
  SELECT 'operator_step_after_commit',
         'shiba.topn_batch_result'::regclass,
         stage.ordinality-1,
         3000
  FROM shiba_internal.dataflows AS dataflow
  CROSS JOIN LATERAL jsonb_array_elements(dataflow.plan->'stages')
    WITH ORDINALITY AS stage(value,ordinality)
  WHERE dataflow.result_oid='shiba.topn_batch_result'::regclass
    AND stage.value->'spec'->>'operator'='topn';
  COMMIT"
wait_for_query "t" "
  SELECT fired
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'" \
  "the first committed TopN Apply"
batch_runtime_pid="$(psql_gate -Atqc "
  SELECT runtime_pid
  FROM public.shiba_runtime_failpoints
  WHERE kind='operator_step_after_commit'")"
batch_result_oid="$(psql_gate -Atqc "
  SELECT 'shiba.topn_batch_result'::regclass::oid::integer")"
batch_topn_stage="$(stage_id "${batch_result_oid}" topn 1)"
psql_gate -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${batch_result_oid}::oid"
wait_for_runtime_replacement "${batch_runtime_pid}"

batch_control="$(psql_gate -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${batch_result_oid}::oid
    AND stage_id=${batch_topn_stage}
    AND state_slot=3")"
batch_candidate="$(psql_gate -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_state_relations
  WHERE result_oid=${batch_result_oid}::oid
    AND stage_id=${batch_topn_stage}
    AND state_slot=1")"
batch_continuation="$(psql_gate -Atqc "
  SELECT relation_oid::regclass::text
  FROM shiba_internal.operator_continuation_relations
  WHERE result_oid=${batch_result_oid}::oid
    AND stage_id=${batch_topn_stage}")"
batch_input_stream="$(psql_gate -Atqc "
  SELECT stream_id
  FROM shiba_internal.effect_stream_consumers
  WHERE result_oid=${batch_result_oid}::oid
    AND consumer_stage_id=${batch_topn_stage}
    AND input_port=0")"
assert_query "8|true|true|false|0|0" "
  SELECT checkpoint.admitted_rows
         || '|' || control.dirty
         || '|' || (control.causal_lsn IS NOT NULL)
         || '|' || checkpoint.has_continuation
         || '|' || (SELECT count(*) FROM ${batch_continuation})
         || '|' || (SELECT count(*) FROM ${batch_candidate})
  FROM shiba_internal.operator_checkpoints AS checkpoint
  CROSS JOIN ${batch_control} AS control
  WHERE checkpoint.result_oid=${batch_result_oid}::oid
    AND checkpoint.stage_id=${batch_topn_stage}
    AND control.singleton"
assert_query "2" "
  SELECT next_chunk_seq
  FROM shiba_internal.effect_stream_consumers
  WHERE stream_id=${batch_input_stream}
    AND result_oid=${batch_result_oid}::oid
    AND consumer_stage_id=${batch_topn_stage}
    AND input_port=0"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks
  WHERE stream_id=${batch_input_stream}
    AND chunk_seq=1" \
  "GC of a TopN input chunk after Apply"

# Lower the shared batch budget just above the already admitted prefix. The
# next Apply consumes another full chunk, advances the consumer, and enters
# pure Drain.
psql_gate -qc "ALTER SYSTEM SET shiba.batch_rows='9'"
psql_gate -qc "SELECT pg_reload_conf()"
wait_for_query "9" "
  SELECT setting::integer
  FROM pg_settings
  WHERE name='shiba.batch_rows'" \
  "the temporary TopN batch budget"
drain_runtime_pid="$(runtime_pid)"
psql_gate -qc "
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
  "the TopN Apply-to-Drain cutover"
psql_gate -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${batch_result_oid}::oid"
wait_for_runtime_replacement "${drain_runtime_pid}"
assert_query "2|true|true|true|16" "
  SELECT continuation.phase
         || '|' || (continuation.input_chunk_seq IS NULL)
         || '|' || (continuation.input_row_ordinal IS NULL)
         || '|' || checkpoint.has_continuation
         || '|' || checkpoint.admitted_rows
  FROM ${batch_continuation} AS continuation
  JOIN shiba_internal.operator_checkpoints AS checkpoint
    ON checkpoint.result_oid=${batch_result_oid}::oid
   AND checkpoint.stage_id=${batch_topn_stage}"

# Kill the first Select page before commit. Candidate rows and the exact
# phase-2 continuation must remain at the preceding committed state.
select_runtime_pid="$(runtime_pid)"
psql_gate -qc "
  UPDATE public.shiba_runtime_failpoints
  SET kind='operator_step_before_commit',
      runtime_pid=${select_runtime_pid},
      fired=false,
      pause_ms=3000
  WHERE kind='operator_step_after_commit';
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${batch_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
wait_for_log \
  "operator_step_before_commit result ${batch_result_oid} stage ${batch_topn_stage}" \
  "the pre-commit TopN Select crash"
psql_gate -qc "
  UPDATE shiba_internal.dataflows
  SET active=false
  WHERE result_oid=${batch_result_oid}::oid"
wait_for_runtime_replacement "${select_runtime_pid}"
assert_query "2|0|16" "
  SELECT continuation.phase
         || '|' || (SELECT count(*) FROM ${batch_candidate})
         || '|' || checkpoint.admitted_rows
  FROM ${batch_continuation} AS continuation
  JOIN shiba_internal.operator_checkpoints AS checkpoint
    ON checkpoint.result_oid=${batch_result_oid}::oid
   AND checkpoint.stage_id=${batch_topn_stage}"

psql_gate -qc "ALTER SYSTEM SET shiba.batch_rows='8'"
psql_gate -qc "SELECT pg_reload_conf()"
psql_gate -qc "
  DELETE FROM public.shiba_runtime_failpoints;
  UPDATE shiba_internal.dataflows
  SET active=true
  WHERE result_oid=${batch_result_oid}::oid;
  SELECT shiba._ensure_runtime()"
assert_bag_equal "
  SELECT id,grp,score,payload
  FROM public.kernel_source
  ORDER BY score DESC NULLS LAST
  FETCH FIRST 4 ROWS WITH TIES" "
  SELECT id,grp,score,payload
  FROM shiba.topn_batch_result" \
  "TopN Apply/Drain recovery"

# A large peer group forces many bounded Diff pages. A second TopN receives a
# duplicate bag, proving that selection, ties, and output weights preserve
# multiplicity rather than silently deduplicating equal rows.
psql_gate -qc "
  CREATE TABLE shiba.topn_bag_result AS
  SELECT grp_bucket,score_bucket
  FROM (
    SELECT (id%2)::integer AS grp_bucket,
           (score%3)::integer AS score_bucket
    FROM public.kernel_source
  ) AS projected
  ORDER BY score_bucket DESC NULLS LAST,grp_bucket
  FETCH FIRST 5 ROWS WITH TIES"
topn_bag_expected="
  SELECT grp_bucket,score_bucket
  FROM (
    SELECT (id%2)::integer AS grp_bucket,
           (score%3)::integer AS score_bucket
    FROM public.kernel_source
  ) AS projected
  ORDER BY score_bucket DESC NULLS LAST,grp_bucket
  FETCH FIRST 5 ROWS WITH TIES"
assert_bag_equal "${topn_bag_expected}" "
  SELECT grp_bucket,score_bucket
  FROM shiba.topn_bag_result" \
  "TopN duplicate-bag bootstrap"

# Restore the larger shared batch budget before the independent high-fanout
# stress case so repeated intermediate rebuilds do not dominate this gate.
psql_gate -qc "ALTER SYSTEM SET shiba.batch_rows='16384'"
psql_gate -qc "ALTER SYSTEM SET shiba.batch_bytes='16777216'"
psql_gate -qc "SELECT pg_reload_conf()"
wait_for_query "16384|16777216" "
  SELECT (SELECT setting FROM pg_settings
          WHERE name='shiba.batch_rows')
         || '|' ||
         (SELECT setting FROM pg_settings
          WHERE name='shiba.batch_bytes')" \
  "the high-fanout batch budget"

psql_gate -qc "
  INSERT INTO public.kernel_source
  SELECT id,(id%11)::integer,10000,'fanout-'||id,
         (2+id%4)::integer
  FROM generate_series(2001,2257) AS id"
assert_bag_equal_with_progress "${window_expected}" "
  SELECT id,grp,score,payload,row_number_value,rank_value,
         dense_rank_value,tile_value,lag_value,lead_value,
         first_value_value,last_value_value,nth_value_value,
         frame_count,frame_max
  FROM shiba.window_result" \
  "${window_result_oid}" "large TopN fanout: Window"
assert_all_results "large TopN fanout"
assert_bag_equal "
  SELECT id,grp,score,payload
  FROM public.kernel_source
  ORDER BY score DESC NULLS LAST
  FETCH FIRST 4 ROWS WITH TIES" "
  SELECT id,grp,score,payload
  FROM shiba.topn_batch_result" \
  "large TopN peer group"
assert_bag_equal "${topn_bag_expected}" "
  SELECT grp_bucket,score_bucket
  FROM shiba.topn_bag_result" \
  "TopN duplicate-bag fanout"
assert_query "257" "
  SELECT count(*)
  FROM shiba.topn_batch_result
  WHERE score=10000"
assert_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks
  WHERE row_count>8
     OR (payload_bytes>4096 AND row_count<>1)"
wait_for_query "0|0|false|false|true|0" "
  SELECT checkpoint.admitted_rows
         || '|' || checkpoint.admitted_bytes
         || '|' || checkpoint.has_continuation
         || '|' || control.dirty
         || '|' || (control.causal_lsn IS NULL)
         || '|' || (SELECT count(*) FROM ${batch_candidate})
  FROM shiba_internal.operator_checkpoints AS checkpoint
  CROSS JOIN ${batch_control} AS control
  WHERE checkpoint.result_oid=${batch_result_oid}::oid
    AND checkpoint.stage_id=${batch_topn_stage}
    AND control.singleton" \
  "clean TopN control and admission state"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.effect_stream_chunks AS chunk
  JOIN shiba_internal.effect_stream_consumers AS consumer
    ON consumer.stream_id=chunk.stream_id
  WHERE consumer.result_oid=${batch_result_oid}::oid
    AND consumer.consumer_stage_id=${batch_topn_stage}
    AND consumer.input_port=0
    AND chunk.chunk_seq<consumer.next_chunk_seq" \
  "GC of every consumed TopN input chunk"

# Reconcile a result that is already identical to its rebuilt candidate.
# Both Diff legs must still advance through durable compared-row cursors in
# bounded pages; a zero-difference suffix may not collapse into one full scan.
psql_gate -qc "ALTER SYSTEM SET shiba.batch_rows='8'"
psql_gate -qc "ALTER SYSTEM SET shiba.batch_bytes='4096'"
psql_gate -qc "SELECT pg_reload_conf()"
wait_for_query "8|4096" "
  SELECT (SELECT setting FROM pg_settings
          WHERE name='shiba.batch_rows')
         || '|' ||
         (SELECT setting FROM pg_settings
          WHERE name='shiba.batch_bytes')" \
  "the zero-difference paging budget"
psql_gate -qc "
  CREATE TABLE public.zero_diff_source(
    id integer PRIMARY KEY,
    payload text NOT NULL,
    noise integer NOT NULL
  );
  INSERT INTO public.zero_diff_source
  SELECT id,'stable-'||id,0
  FROM generate_series(1,513) AS id;

  CREATE TABLE shiba.window_zero_diff_result AS
  SELECT id,payload,
         row_number() OVER (ORDER BY id) AS window_ordinal
  FROM public.zero_diff_source;

  CREATE TABLE shiba.topn_zero_diff_result AS
  SELECT id,payload
  FROM public.zero_diff_source
  ORDER BY id
  FETCH FIRST 513 ROWS ONLY"
assert_bag_equal "
  SELECT id,payload,row_number() OVER (ORDER BY id)
  FROM public.zero_diff_source" "
  SELECT id,payload,window_ordinal
  FROM shiba.window_zero_diff_result" \
  "large zero-difference Window bootstrap"
assert_bag_equal "
  SELECT id,payload
  FROM public.zero_diff_source
  ORDER BY id
  FETCH FIRST 513 ROWS ONLY" "
  SELECT id,payload
  FROM shiba.topn_zero_diff_result" \
  "large zero-difference TopN bootstrap"

zero_window_oid="$(psql_gate -Atqc "
  SELECT 'shiba.window_zero_diff_result'::regclass::oid::integer")"
zero_topn_oid="$(psql_gate -Atqc "
  SELECT 'shiba.topn_zero_diff_result'::regclass::oid::integer")"
zero_window_stage="$(stage_id "${zero_window_oid}" window 1)"
zero_topn_stage="$(stage_id "${zero_topn_oid}" topn 1)"
zero_window_input="$(psql_gate -Atqc "
  SELECT stream_id
  FROM shiba_internal.effect_stream_consumers
  WHERE result_oid=${zero_window_oid}::oid
    AND consumer_stage_id=${zero_window_stage}
    AND input_port=0")"
zero_topn_input="$(psql_gate -Atqc "
  SELECT stream_id
  FROM shiba_internal.effect_stream_consumers
  WHERE result_oid=${zero_topn_oid}::oid
    AND consumer_stage_id=${zero_topn_stage}
    AND input_port=0")"
zero_window_revision="$(psql_gate -Atqc "
  SELECT revision
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${zero_window_oid}::oid
    AND stage_id=${zero_window_stage}")"
zero_topn_revision="$(psql_gate -Atqc "
  SELECT revision
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${zero_topn_oid}::oid
    AND stage_id=${zero_topn_stage}")"

zero_diff_lsn="$(psql_gate -Atqc "
  UPDATE public.zero_diff_source
  SET payload=payload,noise=noise+1
  WHERE id=257;
  SELECT pg_current_wal_lsn()")"
wait_for_query "t" "
  SELECT consumed_frontier_lsn>='${zero_diff_lsn}'::pg_lsn
  FROM shiba_internal.effect_stream_consumers
  WHERE stream_id=${zero_window_input}
    AND result_oid=${zero_window_oid}::oid
    AND consumer_stage_id=${zero_window_stage}
    AND input_port=0" \
  "the large zero-difference Window frontier"
wait_for_query "t" "
  SELECT consumed_frontier_lsn>='${zero_diff_lsn}'::pg_lsn
  FROM shiba_internal.effect_stream_consumers
  WHERE stream_id=${zero_topn_input}
    AND result_oid=${zero_topn_oid}::oid
    AND consumer_stage_id=${zero_topn_stage}
    AND input_port=0" \
  "the large zero-difference TopN frontier"
assert_query "t" "
  SELECT revision-${zero_window_revision}>=620
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${zero_window_oid}::oid
    AND stage_id=${zero_window_stage}"
assert_query "t" "
  SELECT revision-${zero_topn_revision}>=250
  FROM shiba_internal.operator_checkpoints
  WHERE result_oid=${zero_topn_oid}::oid
    AND stage_id=${zero_topn_stage}"
assert_bag_equal "
  SELECT id,payload,row_number() OVER (ORDER BY id)
  FROM public.zero_diff_source" "
  SELECT id,payload,window_ordinal
  FROM shiba.window_zero_diff_result" \
  "large zero-difference Window reconcile"
assert_bag_equal "
  SELECT id,payload
  FROM public.zero_diff_source
  ORDER BY id
  FETCH FIRST 513 ROWS ONLY" "
  SELECT id,payload
  FROM shiba.topn_zero_diff_result" \
  "large zero-difference TopN reconcile"

assert_query "1" "
  SELECT count(*)
  FROM pg_stat_activity
  WHERE backend_type='shiba runtime'"

printf '%s\n' \
  "bounded Window/TopN differential and recovery gate passed"
