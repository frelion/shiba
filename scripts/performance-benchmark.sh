#!/usr/bin/env bash
set -euo pipefail

# Repeatable, isolated PostgreSQL 17 performance benchmark for Shiba's one
# execution architecture.  This is deliberately not a correctness gate: its
# results are machine-readable measurements which a caller can compare to a
# checked-in or CI baseline.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
profile="smoke"
json_out=""
csv_out=""

usage() {
  cat <<'USAGE'
Usage: scripts/performance-benchmark.sh [--profile smoke|full] [--json-out FILE] [--csv-out FILE]

Profiles are intentionally fixed so results are comparable. smoke is suitable
for a CI smoke job; full is for a dedicated, quiet runner.
USAGE
}

while test "$#" -gt 0; do
  case "$1" in
    --profile) profile="${2:?missing profile}"; shift 2 ;;
    --json-out) json_out="${2:?missing JSON path}"; shift 2 ;;
    --csv-out) csv_out="${2:?missing CSV path}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

case "${profile}" in
  smoke)
    # The matrix executes a smoke workload four times (one warm-up plus
    # three samples). Keep this deliberately small: it checks the complete
    # measurement path without turning an artifact job into a long benchmark.
    ingress_rows=500
    fanout_width=64
    complex_fact_rows=200
    complex_keys=16
    stage_chunk_rows=64
    ingress_batch_rows=64
    ;;
  full)
    ingress_rows=1000000
    fanout_width=20000
    complex_fact_rows=500000
    complex_keys=256
    stage_chunk_rows=1024
    ingress_batch_rows=1024
    ;;
  *) printf 'profile must be smoke or full, got: %s\n' "${profile}" >&2; exit 2 ;;
esac

pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-benchmark-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-benchmark-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_BENCHMARK_PORT:-$((64000 + $$ % 1000))}"
database_name="shiba_benchmark"
database_user="$(id -un)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
commit="$(git -C "${project_root}" rev-parse --verify HEAD)"
result_dir="${project_root}/benchmarks/results"
monitor_file="${pg_data_dir}/resource-samples.csv"
metrics_file="${pg_data_dir}/metrics.csv"
monitor_pid=""

cleanup() {
  if test -n "${monitor_pid}"; then
    kill "${monitor_pid}" >/dev/null 2>&1 || true
    wait "${monitor_pid}" >/dev/null 2>&1 || true
  fi
  if test "${SHIBA_KEEP_BENCHMARK_CLUSTER:-0}" = "1"; then
    printf 'Retained benchmark cluster: %s\n' "${pg_data_dir}" >&2
    printf 'Retained benchmark socket: %s\n' "${pg_socket_dir}" >&2
    return
  fi
  "${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -m immediate stop \
    >/dev/null 2>&1 || true
  rm -rf "${pg_data_dir}" "${pg_socket_dir}"
}
trap cleanup EXIT

psql_bench() {
  PGOPTIONS="-c statement_timeout=0 -c lock_timeout=10000" \
    "${pg_bin_dir}/psql" -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

fail() {
  printf 'benchmark failed: %s\n' "$1" >&2
  tail -n 160 "${pg_log_file}" >&2 || true
  exit 1
}

wait_for_query() {
  local expected="$1" query="$2" description="$3" actual=""
  local attempt
  for ((attempt = 1; attempt <= 3600; attempt++)); do
    if actual="$(psql_bench -Atqc "${query}" 2>/dev/null)" &&
       test "${actual}" = "${expected}"; then
      return
    fi
    sleep 0.05
  done
  fail "timed out waiting for ${description}; last value was [${actual}]"
}

assert_query() {
  local expected="$1" query="$2" actual
  actual="$(psql_bench -Atqc "${query}")"
  test "${actual}" = "${expected}" || fail "expected [${expected}], got [${actual}] for: ${query}"
}

now_seconds() {
  perl -MTime::HiRes=time -e 'printf "%.6f", time'
}

number_subtract() {
  awk -v end="$1" -v start="$2" 'BEGIN { printf "%.6f", end - start }'
}

rate() {
  awk -v rows="$1" -v seconds="$2" 'BEGIN { if (seconds <= 0) print 0; else printf "%.3f", rows / seconds }'
}

resource_sample() {
  psql_bench -Atqc "
    SELECT coalesce(pg_database_size(current_database()), 0),
           coalesce(sum(buffered_bytes), 0),
           coalesce(sum(buffered_rows), 0)
    FROM shiba_internal.effect_streams" 2>/dev/null || true
}

monitor_resources() {
  : >"${monitor_file}"
  while :; do
    resource_sample >>"${monitor_file}"
    sleep 0.1
  done
}

result_stats() {
  local result_oid="$1"
  psql_bench -Atqc "
    SELECT
      (SELECT coalesce(sum(next_chunk_seq - 1), 0)
       FROM shiba_internal.effect_streams
       WHERE producer_result_oid=${result_oid}::oid),
      (SELECT coalesce(sum(buffered_bytes), 0)
       FROM shiba_internal.effect_streams
       WHERE producer_result_oid=${result_oid}::oid),
      (SELECT coalesce(sum(pg_total_relation_size(relation_oid)), 0)
       FROM shiba_internal.operator_state_relations
       WHERE result_oid=${result_oid}::oid),
      (SELECT coalesce(sum(revision), 0)
       FROM shiba_internal.operator_checkpoints
       WHERE result_oid=${result_oid}::oid)"
}

record_metric() {
  local scenario="$1" input_rows="$2" expected_rows="$3" started="$4" result_name="$5" sample_start="$6" source_name="$7"
  local ended elapsed actual_rows result_oid stats source_chunks chunks buffered_bytes state_bytes checkpoint_advances db_bytes peak_bytes peak_buffered peak_rows
  ended="$(now_seconds)"
  elapsed="$(number_subtract "${ended}" "${started}")"
  actual_rows="$(psql_bench -Atqc "SELECT count(*) FROM ${result_name}")"
  result_oid="$(psql_bench -Atqc "SELECT '${result_name}'::regclass::oid::integer")"
  stats="$(result_stats "${result_oid}")"
  IFS='|' read -r chunks buffered_bytes state_bytes checkpoint_advances <<<"${stats}"
  source_chunks="$(psql_bench -Atqc "
    SELECT coalesce(sum(next_chunk_seq - 1), 0)
    FROM shiba_internal.effect_streams
    WHERE source_oid='${source_name}'::regclass")"
  db_bytes="$(psql_bench -Atqc 'SELECT pg_database_size(current_database())')"
  peak_bytes="$(awk -F'|' -v start="${sample_start}" 'NR > start && NF == 3 && $1 > max { max = $1 } END { print max + 0 }' "${monitor_file}")"
  peak_buffered="$(awk -F'|' -v start="${sample_start}" 'NR > start && NF == 3 && $2 > max { max = $2 } END { print max + 0 }' "${monitor_file}")"
  peak_rows="$(awk -F'|' -v start="${sample_start}" 'NR > start && NF == 3 && $3 > max { max = $3 } END { print max + 0 }' "${monitor_file}")"
  test "${actual_rows}" = "${expected_rows}" || fail "${scenario}: expected ${expected_rows} result rows, got ${actual_rows}"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${scenario}" "${input_rows}" "${actual_rows}" "${elapsed}" \
    "$(rate "${actual_rows}" "${elapsed}")" "${elapsed}" "${chunks}" "${buffered_bytes}" \
    "${state_bytes}" "${db_bytes}" "${peak_bytes}" "${peak_buffered}" "${peak_rows}" \
    "${source_chunks}" "${checkpoint_advances}" \
    >>"${metrics_file}"
}

cd "${project_root}"
# A benchmark must load the optimized extension.  `pg_test` selects the
# development/test build used by correctness scripts and would make every
# result a measurement of debug assertions instead of Runtime throughput.
cargo pgrx install --release --pg-config "${pg_config_path}"

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\n"
  printf "wal_level = logical\n"
  printf "max_replication_slots = 8\n"
  printf "max_wal_senders = 8\n"
  printf "max_worker_processes = 16\n"
  printf "listen_addresses = ''\n"
  printf "unix_socket_directories = '%s'\n" "${pg_socket_dir}"
  printf "port = %s\n" "${pg_port}"
  printf "shiba.ingress_batch_rows = %s\n" "${ingress_batch_rows}"
  printf "shiba.ingress_batch_bytes = '1MB'\n"
  printf "shiba.stage_chunk_rows = %s\n" "${stage_chunk_rows}"
  printf "shiba.stage_chunk_bytes = '1MB'\n"
  printf "shiba.replication_conninfo = 'host=%s port=%s dbname=%s user=%s connect_timeout=5'\n" \
    "${pg_socket_dir}" "${pg_port}" "${database_name}" "${database_user}"
} >>"${pg_data_dir}/postgresql.conf"
"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" -h "${pg_socket_dir}" -p "${pg_port}" "${database_name}"
psql_bench -qc 'CREATE EXTENSION shiba'
psql_bench -qc 'SELECT shiba.activate()'
wait_for_query 1 "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'" 'Runtime'

printf 'scenario,input_rows,result_rows,elapsed_seconds,throughput_rows_per_second,post_commit_convergence_seconds,stream_chunks,buffered_bytes_at_end,state_bytes,database_bytes_at_end,peak_database_bytes,peak_buffered_bytes,peak_buffered_rows,source_stream_chunks,checkpoint_advances\n' >"${metrics_file}"
monitor_resources &
monitor_pid=$!

# 1. One large source transaction: measure committed source DML through Sink.
psql_bench -qc "
  CREATE TABLE public.bench_ingress (id bigint PRIMARY KEY, payload text NOT NULL);
  CREATE TABLE shiba.bench_ingress_result AS
  SELECT id, payload FROM public.bench_ingress;"
ingress_sample_start="$(wc -l <"${monitor_file}")"
psql_bench -qc "INSERT INTO public.bench_ingress
  SELECT id, repeat('x', 64) FROM generate_series(1, ${ingress_rows}) AS id"
# The client command above has committed. This is deliberately post-commit
# convergence, not client-side INSERT time; the batch/publication metrics
# below expose how the source transaction was split after it reached ingress.
ingress_started="$(now_seconds)"
wait_for_query "${ingress_rows}" 'SELECT count(*) FROM shiba.bench_ingress_result' 'large transaction Sink visibility'
assert_query 0 "WITH d AS ((SELECT id,payload FROM public.bench_ingress EXCEPT ALL SELECT id,payload FROM shiba.bench_ingress_result) UNION ALL (SELECT id,payload FROM shiba.bench_ingress_result EXCEPT ALL SELECT id,payload FROM public.bench_ingress)) SELECT count(*) FROM d"
record_metric ingress_large_transaction "${ingress_rows}" "${ingress_rows}" "${ingress_started}" shiba.bench_ingress_result "${ingress_sample_start}" public.bench_ingress

# 2. One left row fans out to a large persisted right arrangement.
psql_bench -qc "
  CREATE TABLE public.bench_fanout_left (id bigint PRIMARY KEY, key integer NOT NULL);
  CREATE TABLE public.bench_fanout_right (id bigint PRIMARY KEY, key integer NOT NULL);
  CREATE TABLE shiba.bench_fanout_result AS
  SELECT left_side.id AS left_id, right_side.id AS right_id
  FROM public.bench_fanout_left AS left_side
  JOIN public.bench_fanout_right AS right_side ON right_side.key=left_side.key;
  INSERT INTO public.bench_fanout_right
  SELECT id, 1 FROM generate_series(1, ${fanout_width}) AS id;"
wait_for_query 0 'SELECT count(*) FROM shiba.bench_fanout_result' 'empty fanout before left input'
fanout_sample_start="$(wc -l <"${monitor_file}")"
psql_bench -qc 'INSERT INTO public.bench_fanout_left VALUES (1, 1)'
fanout_started="$(now_seconds)"
wait_for_query "${fanout_width}" 'SELECT count(*) FROM shiba.bench_fanout_result' 'fanout Sink visibility'
assert_query 0 "WITH d AS ((SELECT left_side.id,right_side.id FROM public.bench_fanout_left AS left_side JOIN public.bench_fanout_right AS right_side ON right_side.key=left_side.key EXCEPT ALL SELECT left_id,right_id FROM shiba.bench_fanout_result) UNION ALL (SELECT left_id,right_id FROM shiba.bench_fanout_result EXCEPT ALL SELECT left_side.id,right_side.id FROM public.bench_fanout_left AS left_side JOIN public.bench_fanout_right AS right_side ON right_side.key=left_side.key)) SELECT count(*) FROM d"
record_metric join_high_fanout 1 "${fanout_width}" "${fanout_started}" shiba.bench_fanout_result "${fanout_sample_start}" public.bench_fanout_left

# 3. Generic composition, not a fixed query family: two Joins -> Aggregate
# -> Window -> TopN -> Sink. Every source is empty at registration, then one
# committed transaction supplies all live effects.
psql_bench -qc "
  CREATE TABLE public.bench_fact (id bigint PRIMARY KEY, first_key integer NOT NULL);
  CREATE TABLE public.bench_first (first_key integer PRIMARY KEY, second_key integer NOT NULL);
  CREATE TABLE public.bench_second (second_key integer PRIMARY KEY);
  CREATE TABLE shiba.bench_complex_result AS
  SELECT first_key,
         joined_rows,
         row_number() OVER (ORDER BY joined_rows DESC, first_key) AS rank
  FROM (
    SELECT fact.first_key, count(*) AS joined_rows
    FROM public.bench_fact AS fact
    JOIN public.bench_first AS first_side ON first_side.first_key=fact.first_key
    JOIN public.bench_second AS second_side ON second_side.second_key=first_side.second_key
    GROUP BY fact.first_key
  ) AS grouped
  ORDER BY joined_rows DESC, first_key
  LIMIT 20;"
complex_sample_start="$(wc -l <"${monitor_file}")"
psql_bench -qc "BEGIN;
  INSERT INTO public.bench_first SELECT id, id FROM generate_series(1, ${complex_keys}) AS id;
  INSERT INTO public.bench_second SELECT id FROM generate_series(1, ${complex_keys}) AS id;
  INSERT INTO public.bench_fact SELECT id, ((id - 1) % ${complex_keys}) + 1 FROM generate_series(1, ${complex_fact_rows}) AS id;
  COMMIT;"
complex_started="$(now_seconds)"
complex_expected_rows=$((complex_keys < 20 ? complex_keys : 20))
wait_for_query "${complex_expected_rows}" 'SELECT count(*) FROM shiba.bench_complex_result' 'complex DAG Sink visibility'
assert_query 0 "WITH expected AS (
  SELECT first_key,joined_rows,row_number() OVER (ORDER BY joined_rows DESC,first_key) AS rank
  FROM (SELECT fact.first_key,count(*) AS joined_rows FROM public.bench_fact AS fact JOIN public.bench_first AS first_side ON first_side.first_key=fact.first_key JOIN public.bench_second AS second_side ON second_side.second_key=first_side.second_key GROUP BY fact.first_key) AS grouped
  ORDER BY joined_rows DESC,first_key LIMIT 20
), d AS ((SELECT * FROM expected EXCEPT ALL SELECT * FROM shiba.bench_complex_result) UNION ALL (SELECT * FROM shiba.bench_complex_result EXCEPT ALL SELECT * FROM expected)) SELECT count(*) FROM d"
record_metric complex_dag "${complex_fact_rows}" "${complex_expected_rows}" "${complex_started}" shiba.bench_complex_result "${complex_sample_start}" public.bench_fact

kill "${monitor_pid}" >/dev/null 2>&1 || true
wait "${monitor_pid}" >/dev/null 2>&1 || true
monitor_pid=""

mkdir -p "${result_dir}"
if test -z "${json_out}"; then
  json_out="${result_dir}/${run_id}-${profile}.json"
fi
if test -z "${csv_out}"; then
  csv_out="${json_out%.json}.csv"
fi
mkdir -p "$(dirname "${json_out}")" "$(dirname "${csv_out}")"
cp "${metrics_file}" "${csv_out}"
postgresql_version_num="$(psql_bench -Atqc 'SHOW server_version_num')"
extension_version="$(psql_bench -Atqc "SELECT extversion FROM pg_extension WHERE extname='shiba'")"
{
  printf '{"run_id":"%s","commit":"%s","profile":"%s","correctness":true,"environment_fingerprint":{"postgresql_version_num":%s,"extension_version":"%s","ingress_batch_rows":%s,"stage_chunk_rows":%s},"scenarios":[' "${run_id}" "${commit}" "${profile}" "${postgresql_version_num}" "${extension_version}" "${ingress_batch_rows}" "${stage_chunk_rows}"
  awk -F, 'NR > 1 { if (count++) printf ","; printf "{\"scenario\":\"%s\",\"correctness\":true,\"metrics\":{\"input_rows\":%s,\"result_rows\":%s,\"elapsed_seconds\":%s,\"throughput_rows_per_second\":%s,\"post_commit_convergence_seconds\":%s,\"stream_chunks\":%s,\"buffered_bytes_at_end\":%s,\"state_bytes\":%s,\"database_bytes_at_end\":%s,\"peak_database_bytes\":%s,\"peak_buffered_bytes\":%s,\"peak_buffered_rows\":%s,\"source_stream_chunks\":%s,\"checkpoint_advances\":%s}}", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15 }' "${metrics_file}"
  printf ']}\n'
} >"${json_out}"

printf 'Shiba benchmark complete\nJSON: %s\nCSV: %s\n' "${json_out}" "${csv_out}"
cat "${json_out}"
