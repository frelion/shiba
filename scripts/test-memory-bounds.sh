#!/usr/bin/env bash
set -euo pipefail

# Resource-bound acceptance gate for the database-scoped Runtime.
#
# The default sizes are suitable for the daily correctness suite. Increase
# them to exercise larger commits and hotter multiplicities:
#
#   SHIBA_MEMORY_AGG_ROWS             rows in one high-cardinality commit
#                                     (default 6000)
#   SHIBA_MEMORY_HOT_ROWS             duplicate rows on each side of the hot
#                                     Join key (default 80, fanout is N*N)
#   SHIBA_MEMORY_WORK_MEM             Runtime PostgreSQL work_mem (default 64kB)
#   SHIBA_MEMORY_TEMP_FILE_LIMIT       Runtime temp_file_limit (default 64MB)
#   SHIBA_MEMORY_STAGE_CHUNK_ROWS      target rows per Stage chunk (default 257)
#   SHIBA_MEMORY_MAX_STAGE_ROWS        normal Stage row quota (default 100000)
#   SHIBA_MEMORY_RESOURCE_SECONDS     stable-PID observation after a configured
#                                     resource failure (default 3)
#   SHIBA_MEMORY_WAIT_ATTEMPTS        100ms asynchronous wait attempts
#                                     (default 1800)
#   SHIBA_MEMORY_MAX_RUNTIME_RSS_KB   optional hard Runtime RSS ceiling
#   SHIBA_MEMORY_TEST_PORT            isolated PostgreSQL port
#   SHIBA_KEEP_TEST_CLUSTER=1         retain the cluster and log

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-memory-bounds-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-memory-bounds-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_MEMORY_TEST_PORT:-$((60000 + $$ % 4000))}"
database_name="shiba_memory_bounds"

aggregate_rows="${SHIBA_MEMORY_AGG_ROWS:-6000}"
hot_rows="${SHIBA_MEMORY_HOT_ROWS:-80}"
runtime_work_mem="${SHIBA_MEMORY_WORK_MEM:-64kB}"
runtime_temp_file_limit="${SHIBA_MEMORY_TEMP_FILE_LIMIT:-64MB}"
stage_chunk_rows="${SHIBA_MEMORY_STAGE_CHUNK_ROWS:-257}"
max_stage_rows="${SHIBA_MEMORY_MAX_STAGE_ROWS:-100000}"
resource_observe_seconds="${SHIBA_MEMORY_RESOURCE_SECONDS:-3}"
wait_attempts="${SHIBA_MEMORY_WAIT_ATTEMPTS:-1800}"
max_runtime_rss_kb="${SHIBA_MEMORY_MAX_RUNTIME_RSS_KB:-}"
peak_runtime_rss_kb=0

cleanup() {
  if test "${SHIBA_KEEP_TEST_CLUSTER:-0}" = "1"; then
    printf 'Retained test cluster: %s\n' "${pg_data_dir}" >&2
    printf 'Retained test socket: %s\n' "${pg_socket_dir}" >&2
    printf 'PostgreSQL log: %s\n' "${pg_log_file}" >&2
    return
  fi
  "${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -m immediate stop \
    >/dev/null 2>&1 || true
  rm -rf "${pg_data_dir}" "${pg_socket_dir}"
}
trap cleanup EXIT

psql_memory() {
  PGOPTIONS="-c statement_timeout=120000 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" \
      -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

fail() {
  printf 'Runtime resource-bound gate failed: %s\n' "$1" >&2
  printf 'PostgreSQL log: %s\n' "${pg_log_file}" >&2
  exit 1
}

assert_query() {
  local expected="$1"
  local query="$2"
  local actual
  actual="$(psql_memory -Atqc "${query}")"
  if test "${actual}" != "${expected}"; then
    fail "expected [${expected}], got [${actual}] for: ${query}"
  fi
}

runtime_pid() {
  psql_memory -Atqc "
    SELECT pid
    FROM pg_stat_activity
    WHERE backend_type='shiba runtime'"
}

sample_runtime_rss() {
  local pid="${1:-}"
  local rss_kb
  if test -z "${pid}"; then
    pid="$(runtime_pid 2>/dev/null || true)"
  fi
  if ! [[ "${pid}" =~ ^[0-9]+$ ]]; then
    return
  fi
  rss_kb="$(ps -o rss= -p "${pid}" 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ "${rss_kb}" =~ ^[0-9]+$ ]] &&
     test "${rss_kb}" -gt "${peak_runtime_rss_kb}"; then
    peak_runtime_rss_kb="${rss_kb}"
  fi
}

wait_for_query() {
  local expected="$1"
  local query="$2"
  local description="$3"
  local actual=""
  local attempt
  for ((attempt = 1; attempt <= wait_attempts; attempt++)); do
    if actual="$(psql_memory -Atqc "${query}" 2>/dev/null)" &&
       test "${actual}" = "${expected}"; then
      sample_runtime_rss
      return 0
    fi
    sample_runtime_rss
    sleep 0.1
  done
  fail "timed out waiting for ${description}; last value was [${actual}]"
}

wait_for_resource_pause() {
  local result_relation="$1"
  local expected_runtime_pid="$2"
  local state=""
  local current_runtime_pid=""
  local attempt
  for attempt in {1..300}; do
    current_runtime_pid="$(runtime_pid 2>/dev/null || true)"
    if test "${current_runtime_pid}" != "${expected_runtime_pid}"; then
      fail "configured resource ceiling restarted the singleton Runtime: ${expected_runtime_pid} -> ${current_runtime_pid:-no Runtime}"
    fi
    state="$(psql_memory -Atqc "
      SELECT runtime.active || '|' ||
             (runtime.last_error LIKE '[53400] %') || '|' ||
             (SELECT count(*)
              FROM shiba_internal.dag_inbox inbox
              WHERE inbox.result_oid=runtime.result_oid)
      FROM shiba_internal.dag_runtime_state runtime
      WHERE runtime.result_oid='${result_relation}'::regclass
    " 2>/dev/null || true)"
    if test "${state}" = "false|true|1"; then
      return 0
    fi
    sample_runtime_rss "${current_runtime_pid}"
    sleep 0.1
  done
  fail "configured resource ceiling did not atomically pause ${result_relation}; last DAG state was [${state}]"
}

assert_one_runtime() {
  assert_query "1|0|0|1" "
    SELECT
      count(*) FILTER (WHERE backend_type='shiba runtime')
      || '|' ||
      count(*) FILTER (WHERE backend_type='shiba router')
      || '|' ||
      count(*) FILTER (WHERE backend_type='shiba executor')
      || '|' ||
      count(DISTINCT state.owner_pid)
    FROM pg_stat_activity activity
    CROSS JOIN shiba_internal.runtime_state state
    WHERE state.singleton
      AND state.active"
}

wait_for_native_match() {
  local expected_query="$1"
  local result_relation="$2"
  local result_projection="$3"
  local description="$4"
  wait_for_query "0" "
    WITH expected AS MATERIALIZED (
      ${expected_query}
    ),
    actual AS MATERIALIZED (
      SELECT ${result_projection} FROM ${result_relation}
    ),
    difference AS (
      (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
      UNION ALL
      (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
    )
    SELECT count(*) FROM difference" \
    "${description} to equal native PostgreSQL recomputation"
}

assert_stages_empty() {
  local result_relation="$1"
  psql_memory -q <<SQL
DO \$body\$
DECLARE
  stage record;
  shared_stage_name text;
  shared_stage_oid regclass;
  has_rows boolean;
BEGIN
  FOR stage IN
    SELECT relation_oid,stage_name
    FROM shiba_internal.physical_stages
    WHERE result_oid='${result_relation}'::regclass
  LOOP
    EXECUTE format(
      'SELECT EXISTS (SELECT 1 FROM %s LIMIT 1)',
      stage.relation_oid::regclass
    ) INTO has_rows;
    IF has_rows THEN
      RAISE EXCEPTION
        'physical Stage % for % retained live rows after commit',
        stage.stage_name,'${result_relation}';
    END IF;
  END LOOP;

  -- Some operators use shared, result-keyed fold Stages rather than a
  -- per-plan physical_stages row. Keep this lookup dynamic so the test gives a
  -- useful failure both before and after those operator Stages are introduced.
  FOREACH shared_stage_name IN ARRAY ARRAY[
    'shiba_internal.aggregate_group_fold_stage',
    'shiba_internal.aggregate_distinct_fold_stage',
    'shiba_internal.distinct_fold_stage'
  ]
  LOOP
    shared_stage_oid := to_regclass(shared_stage_name);
    CONTINUE WHEN shared_stage_oid IS NULL;
    EXECUTE format(
      'SELECT EXISTS (SELECT 1 FROM %s WHERE result_oid=\$1 LIMIT 1)',
      shared_stage_oid
    ) INTO has_rows USING '${result_relation}'::regclass::oid;
    IF has_rows THEN
      RAISE EXCEPTION
        'shared physical Stage % for % retained live rows after commit',
        shared_stage_name,'${result_relation}';
    END IF;
  END LOOP;
END
\$body\$;
SQL
}

validate_positive_integer() {
  local name="$1"
  local value="$2"
  if ! [[ "${value}" =~ ^[1-9][0-9]*$ ]]; then
    fail "${name} must be a positive integer, got [${value}]"
  fi
}

validate_positive_integer "SHIBA_MEMORY_AGG_ROWS" "${aggregate_rows}"
validate_positive_integer "SHIBA_MEMORY_HOT_ROWS" "${hot_rows}"
if test "${hot_rows}" -lt 2; then
  fail "SHIBA_MEMORY_HOT_ROWS must be at least 2"
fi
validate_positive_integer \
  "SHIBA_MEMORY_STAGE_CHUNK_ROWS" "${stage_chunk_rows}"
validate_positive_integer \
  "SHIBA_MEMORY_MAX_STAGE_ROWS" "${max_stage_rows}"
validate_positive_integer \
  "SHIBA_MEMORY_RESOURCE_SECONDS" "${resource_observe_seconds}"
validate_positive_integer "SHIBA_MEMORY_WAIT_ATTEMPTS" "${wait_attempts}"
if ! [[ "${runtime_work_mem}" =~ ^[1-9][0-9]*(kB|MB|GB)$ ]]; then
  fail "SHIBA_MEMORY_WORK_MEM must look like 64kB, 16MB, or 1GB"
fi
if ! [[ "${runtime_temp_file_limit}" =~ ^[1-9][0-9]*(kB|MB|GB)$ ]]; then
  fail "SHIBA_MEMORY_TEMP_FILE_LIMIT must look like 64kB, 16MB, or 1GB"
fi
if test -n "${max_runtime_rss_kb}"; then
  validate_positive_integer \
    "SHIBA_MEMORY_MAX_RUNTIME_RSS_KB" "${max_runtime_rss_kb}"
fi

cd "${project_root}"
cargo pgrx install --pg-config "${pg_config_path}"

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
  printf "work_mem = '%s'\n" "${runtime_work_mem}"
  printf "hash_mem_multiplier = 1\n"
  printf "temp_file_limit = '%s'\n" "${runtime_temp_file_limit}"
  printf "log_temp_files = 0\n"
  printf "shiba.runtime_work_mem = '%s'\n" "${runtime_work_mem}"
  printf "shiba.runtime_temp_file_limit = '%s'\n" \
    "${runtime_temp_file_limit}"
  printf "shiba.stage_chunk_rows = %s\n" "${stage_chunk_rows}"
  printf "shiba.max_stage_rows = %s\n" "${max_stage_rows}"
} >>"${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" \
  -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" \
  -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"

psql_memory -qc "CREATE EXTENSION shiba"
psql_memory -Atqc "SELECT shiba.activate()" >/dev/null
wait_for_query "1" "
  SELECT count(*) FROM pg_stat_activity
  WHERE backend_type='shiba runtime'" \
  "the singleton Runtime to start"
assert_one_runtime
initial_runtime_pid="$(runtime_pid)"
configured_work_mem="$(psql_memory -Atqc "SHOW work_mem")"

# Statement triggers record the configuration seen inside the Runtime backend,
# rather than assuming that a client SHOW describes the background worker.
psql_memory -qc "
  CREATE TABLE public.runtime_setting_observations (
    observed_for text NOT NULL,
    runtime_pid integer NOT NULL,
    work_mem text NOT NULL
  );
  CREATE FUNCTION public.observe_runtime_settings()
  RETURNS trigger
  LANGUAGE plpgsql
  AS \$trigger\$
  BEGIN
    INSERT INTO public.runtime_setting_observations
      (observed_for,runtime_pid,work_mem)
    VALUES (TG_TABLE_NAME,pg_backend_pid(),current_setting('work_mem'));
    RETURN NULL;
  END
  \$trigger\$"

printf '\n==> One Runtime applies one large, high-cardinality source commit\n'
psql_memory -qc "
  CREATE TABLE public.memory_aggregate_source (
    event_id integer PRIMARY KEY,
    group_id integer NOT NULL,
    amount integer NOT NULL
  );
  CREATE TABLE shiba.memory_aggregate_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.memory_aggregate_source
  GROUP BY group_id;
  CREATE TRIGGER observe_memory_aggregate_runtime
  AFTER INSERT OR UPDATE OR DELETE ON shiba.memory_aggregate_result
  FOR EACH STATEMENT EXECUTE FUNCTION public.observe_runtime_settings();
  UPDATE shiba_internal.dag_runtime_state
  SET active=false
  WHERE result_oid='shiba.memory_aggregate_result'::regclass"

# This INSERT is deliberately one source transaction. Pausing the DAG lets the
# test prove that routing created one inbox item containing every row delta.
psql_memory -qc "
  INSERT INTO public.memory_aggregate_source
  SELECT value,value,(value % 97)-48
  FROM generate_series(1,${aggregate_rows}) AS value"
wait_for_query "1|${aggregate_rows}|1" "
  SELECT count(DISTINCT inbox.commit_lsn)
         || '|' || count(change.sequence)
         || '|' || count(DISTINCT change.commit_lsn)
  FROM shiba_internal.dag_inbox inbox
  JOIN shiba_internal.change_log change USING(commit_lsn)
  WHERE inbox.result_oid='shiba.memory_aggregate_result'::regclass" \
  "one routed source commit with every high-cardinality delta"
psql_memory -qc "
  UPDATE shiba_internal.dag_runtime_state
  SET active=true
  WHERE result_oid='shiba.memory_aggregate_result'::regclass"
wait_for_native_match "
  SELECT group_id,count(*)::bigint,sum(amount)::bigint
  FROM public.memory_aggregate_source
  GROUP BY group_id
" "shiba.memory_aggregate_result" \
  "group_id,row_count::bigint,total_amount::bigint" \
  "the high-cardinality aggregate"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.memory_aggregate_result'::regclass" \
  "the large aggregate commit acknowledgement"
assert_stages_empty "shiba.memory_aggregate_result"
assert_one_runtime
assert_query "${initial_runtime_pid}" "SELECT pid FROM pg_stat_activity
  WHERE backend_type='shiba runtime'"

printf '\n==> Hot duplicate multiplicity stays correct at low work_mem\n'
psql_memory -qc "
  CREATE TABLE public.memory_hot_facts (
    fact_id integer PRIMARY KEY,
    join_key integer NOT NULL,
    bucket integer NOT NULL,
    amount integer NOT NULL
  );
  CREATE TABLE public.memory_hot_dimensions (
    dimension_id integer PRIMARY KEY,
    join_key integer NOT NULL,
    marker integer NOT NULL
  );
  CREATE TABLE shiba.memory_hot_result AS
  SELECT f.bucket,count(*) AS row_count,sum(f.amount) AS total_amount
  FROM public.memory_hot_facts f
  JOIN public.memory_hot_dimensions d ON d.join_key=f.join_key
  GROUP BY f.bucket;
  CREATE TRIGGER observe_memory_hot_runtime
  AFTER INSERT OR UPDATE OR DELETE ON shiba.memory_hot_result
  FOR EACH STATEMENT EXECUTE FUNCTION public.observe_runtime_settings()"
assert_query "1" "
  SELECT count(*)
  FROM shiba_internal.physical_stages
  WHERE result_oid='shiba.memory_hot_result'::regclass
    AND storage='unlogged'"

psql_memory -qc "
  INSERT INTO public.memory_hot_dimensions
  SELECT value,1,value
  FROM generate_series(1,${hot_rows}) value"
wait_for_query "${hot_rows}" "
  SELECT coalesce(sum(multiplicity),0)
  FROM shiba_internal.join_arrangements
  WHERE result_oid='shiba.memory_hot_result'::regclass
    AND input_side='right'" \
  "the hot Join right arrangement"
hot_candidate_limit="$((hot_rows * hot_rows - 1))"
psql_memory -qc \
  "ALTER SYSTEM SET shiba.max_stage_rows = '${hot_candidate_limit}'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
psql_memory -qc "
  INSERT INTO public.memory_hot_facts
  SELECT value,1,3,11
  FROM generate_series(1,${hot_rows}) value"
wait_for_resource_pause \
  "shiba.memory_hot_result" "${initial_runtime_pid}"
assert_query "t" "
  SELECT last_error LIKE '% may generate % candidates, limit %'
  FROM shiba_internal.dag_runtime_state
  WHERE result_oid='shiba.memory_hot_result'::regclass"
assert_query "1" "
  SELECT count(*)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.memory_hot_result'::regclass"
psql_memory -qc "ALTER SYSTEM SET shiba.max_stage_rows = '1000000'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
assert_query "t" "SELECT shiba.resume('shiba.memory_hot_result'::regclass)"
wait_for_native_match "
  SELECT f.bucket,count(*)::bigint,sum(f.amount)::bigint
  FROM public.memory_hot_facts f
  JOIN public.memory_hot_dimensions d ON d.join_key=f.join_key
  GROUP BY f.bucket
" "shiba.memory_hot_result" \
  "bucket,row_count::bigint,total_amount::bigint" \
  "the hot-multiplicity Join"
assert_query "${hot_rows}|${hot_rows}|${hot_rows}|${hot_rows}" "
  SELECT
    (SELECT count(*)
     FROM shiba_internal.join_arrangements
     WHERE result_oid='shiba.memory_hot_result'::regclass
       AND input_side='left')
    || '|' ||
    (SELECT sum(multiplicity)
     FROM shiba_internal.join_arrangements
     WHERE result_oid='shiba.memory_hot_result'::regclass
       AND input_side='left')
    || '|' ||
    (SELECT count(*)
     FROM shiba_internal.join_arrangements
     WHERE result_oid='shiba.memory_hot_result'::regclass
       AND input_side='right')
    || '|' ||
    (SELECT sum(multiplicity)
     FROM shiba_internal.join_arrangements
     WHERE result_oid='shiba.memory_hot_result'::regclass
       AND input_side='right')"
assert_stages_empty "shiba.memory_hot_result"
assert_one_runtime
assert_query "${initial_runtime_pid}" "SELECT pid FROM pg_stat_activity
  WHERE backend_type='shiba runtime'"

printf '\n==> SEMI Join presence and net-zero commits avoid false quota pauses\n'
psql_memory -qc "
  CREATE TABLE public.memory_semi_left (
    left_id integer PRIMARY KEY,
    join_key integer NOT NULL,
    amount integer NOT NULL
  );
  CREATE TABLE public.memory_semi_right (
    right_id integer PRIMARY KEY,
    join_key integer NOT NULL
  );
  CREATE TABLE shiba.memory_semi_result AS
  SELECT left_row.join_key AS group_key,
         count(*) AS row_count,
         sum(left_row.amount) AS total_amount
  FROM public.memory_semi_left left_row
  WHERE EXISTS (
    SELECT 1
    FROM public.memory_semi_right right_row
    WHERE right_row.join_key=left_row.join_key
  )
  GROUP BY left_row.join_key;
  INSERT INTO public.memory_semi_left VALUES (1,7,11);
  INSERT INTO public.memory_semi_right
  SELECT value,7 FROM generate_series(1,100) value"
wait_for_query "1" "SELECT count(*) FROM shiba.memory_semi_result" \
  "the initial SEMI presence transition"
psql_memory -qc "ALTER SYSTEM SET shiba.max_stage_rows = '1'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
psql_memory -qc "INSERT INTO public.memory_semi_right VALUES (101,7)"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.memory_semi_result'::regclass" \
  "an already-visible SEMI key commit"
assert_query "true|1" "
  SELECT runtime.active || '|' || count(result.*)
  FROM shiba_internal.dag_runtime_state runtime
  CROSS JOIN shiba.memory_semi_result result
  WHERE runtime.result_oid='shiba.memory_semi_result'::regclass
  GROUP BY runtime.active"
psql_memory -qc "
  BEGIN;
  DELETE FROM public.memory_semi_right WHERE right_id=101;
  INSERT INTO public.memory_semi_right VALUES (101,7);
  COMMIT"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.memory_semi_result'::regclass" \
  "a net-zero SEMI source commit"
assert_query "true|1" "
  SELECT runtime.active || '|' || count(result.*)
  FROM shiba_internal.dag_runtime_state runtime
  CROSS JOIN shiba.memory_semi_result result
  WHERE runtime.result_oid='shiba.memory_semi_result'::regclass
  GROUP BY runtime.active"
psql_memory -qc "ALTER SYSTEM SET shiba.max_stage_rows = '1000000'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
assert_one_runtime
assert_query "${initial_runtime_pid}" "SELECT pid FROM pg_stat_activity
  WHERE backend_type='shiba runtime'"

assert_query "t" "
  SELECT count(*)>0
         AND bool_and(runtime_pid=${initial_runtime_pid})
         AND bool_and(work_mem='${configured_work_mem}')
  FROM public.runtime_setting_observations
  WHERE observed_for IN (
    'memory_aggregate_result',
    'memory_hot_result'
  )"

printf '\n==> A configured resource ceiling pauses one DAG without restart loops\n'
psql_memory -qc "
  CREATE TABLE public.memory_limited_source (
    event_id integer PRIMARY KEY,
    group_id integer NOT NULL,
    amount integer NOT NULL
  );
  CREATE TABLE shiba.memory_limited_result AS
  SELECT group_id,count(*) AS row_count,sum(amount) AS total_amount
  FROM public.memory_limited_source
  GROUP BY group_id"

# Force the real aggregate Stage quota below the two distinct folded groups.
# shiba.max_stage_rows is SIGHUP-scoped; pg_reload_conf signals the Runtime,
# which reloads before its next apply attempt.
psql_memory -qc "ALTER SYSTEM SET shiba.max_stage_rows = '1'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
psql_memory -qc "
  INSERT INTO public.memory_limited_source
  VALUES (1,9,90),(2,10,100)"

wait_for_resource_pause \
  "shiba.memory_limited_result" "${initial_runtime_pid}"
assert_stages_empty "shiba.memory_limited_result"

resource_runtime_pid="$(runtime_pid)"
if test "${resource_runtime_pid}" != "${initial_runtime_pid}"; then
  fail "Runtime restarted while classifying a configured resource ceiling: ${initial_runtime_pid} -> ${resource_runtime_pid}"
fi
resource_observe_ticks=$((resource_observe_seconds * 10))
for ((tick = 1; tick <= resource_observe_ticks; tick++)); do
  assert_one_runtime
  assert_query "${resource_runtime_pid}" "
    SELECT pid FROM pg_stat_activity
    WHERE backend_type='shiba runtime'"
  sample_runtime_rss "${resource_runtime_pid}"
  sleep 0.1
done
assert_query "1" "
  SELECT count(*)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.memory_limited_result'::regclass"

# Raising the configured quota is intentionally separate from resume: a
# resource-blocked DAG must remain paused until an administrator opts in.
psql_memory -qc "
  ALTER SYSTEM SET shiba.max_stage_rows = '${max_stage_rows}'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
assert_query "false|true|1" "
  SELECT runtime.active || '|' ||
         (runtime.last_error LIKE '[53400] %') || '|' ||
         (SELECT count(*)
          FROM shiba_internal.dag_inbox inbox
          WHERE inbox.result_oid=runtime.result_oid)
  FROM shiba_internal.dag_runtime_state runtime
  WHERE runtime.result_oid='shiba.memory_limited_result'::regclass"
assert_query "t" "SELECT shiba.resume('shiba.memory_limited_result')"
wait_for_native_match "
  SELECT group_id,count(*)::bigint,sum(amount)::bigint
  FROM public.memory_limited_source
  GROUP BY group_id
" "shiba.memory_limited_result" \
  "group_id,row_count::bigint,total_amount::bigint" \
  "the resumed resource-limited aggregate"
wait_for_query "0" "
  SELECT count(*)
  FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.memory_limited_result'::regclass" \
  "the resumed resource-limited commit acknowledgement"
assert_stages_empty "shiba.memory_limited_result"
assert_one_runtime
assert_query "${initial_runtime_pid}" "SELECT pid FROM pg_stat_activity
  WHERE backend_type='shiba runtime'"

printf '\n==> Commit row and byte admission pause before operator execution\n'
psql_memory -qc "ALTER SYSTEM SET shiba.max_commit_rows = '1'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
psql_memory -qc "
  INSERT INTO public.memory_limited_source
  VALUES (3,11,110),(4,12,120)"
wait_for_resource_pause \
  "shiba.memory_limited_result" "${initial_runtime_pid}"
assert_query "2|1" "
  SELECT routed.event_count || '|' || count(inbox.*)
  FROM shiba_internal.dag_inbox inbox
  JOIN shiba_internal.routed_transactions routed USING(commit_lsn)
  WHERE inbox.result_oid='shiba.memory_limited_result'::regclass
  GROUP BY routed.event_count"
psql_memory -qc "ALTER SYSTEM SET shiba.max_commit_rows = '1000000'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
assert_query "t" "SELECT shiba.resume('shiba.memory_limited_result')"
wait_for_query "0" "
  SELECT count(*) FROM shiba_internal.dag_inbox
  WHERE result_oid='shiba.memory_limited_result'::regclass" \
  "the row-admitted commit replay"

psql_memory -qc "ALTER SYSTEM SET shiba.max_commit_bytes = '1'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
psql_memory -qc "
  INSERT INTO public.memory_limited_source VALUES (5,13,130)"
wait_for_resource_pause \
  "shiba.memory_limited_result" "${initial_runtime_pid}"
assert_query "t" "
  SELECT routed.payload_bytes>1
  FROM shiba_internal.dag_inbox inbox
  JOIN shiba_internal.routed_transactions routed USING(commit_lsn)
  WHERE inbox.result_oid='shiba.memory_limited_result'::regclass"
psql_memory -qc \
  "ALTER SYSTEM SET shiba.max_commit_bytes = '1073741824'"
psql_memory -Atqc "SELECT pg_reload_conf()" >/dev/null
assert_query "t" "SELECT shiba.resume('shiba.memory_limited_result')"
wait_for_native_match "
  SELECT group_id,count(*)::bigint,sum(amount)::bigint
  FROM public.memory_limited_source
  GROUP BY group_id
" "shiba.memory_limited_result" \
  "group_id,row_count::bigint,total_amount::bigint" \
  "the byte-admitted commit replay"
assert_one_runtime
assert_query "${initial_runtime_pid}" "SELECT pid FROM pg_stat_activity
  WHERE backend_type='shiba runtime'"

sample_runtime_rss "${initial_runtime_pid}"
if test -n "${max_runtime_rss_kb}" &&
   test "${peak_runtime_rss_kb}" -gt "${max_runtime_rss_kb}"; then
  fail "Runtime peak RSS ${peak_runtime_rss_kb} KiB exceeded configured ceiling ${max_runtime_rss_kb} KiB"
fi

printf '\nRuntime resource-bound gate passed.\n'
printf '  aggregate commit rows: %s\n' "${aggregate_rows}"
printf '  hot Join multiplicity: %s x %s\n' "${hot_rows}" "${hot_rows}"
printf '  Runtime work_mem: %s\n' "${configured_work_mem}"
printf '  observed peak Runtime RSS: %s KiB\n' "${peak_runtime_rss_kb}"
