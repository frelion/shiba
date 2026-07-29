#!/usr/bin/env bash
set -euo pipefail

# Reproducible performance baseline for Shiba. The benchmark uses an isolated
# PostgreSQL cluster and never connects to an existing developer database.

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-perf-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-perf-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_PERF_PORT:-55435}"
initial_rows="${SHIBA_PERF_INITIAL_ROWS:-300000}"
group_count="${SHIBA_PERF_GROUPS:-10000}"
write_clients="${SHIBA_PERF_WRITE_CLIENTS:-4}"
write_tx_per_client="${SHIBA_PERF_WRITE_TX_PER_CLIENT:-100}"
write_batch_size="${SHIBA_PERF_WRITE_BATCH_SIZE:-10}"
query_clients="${SHIBA_PERF_QUERY_CLIENTS:-8}"
query_seconds="${SHIBA_PERF_QUERY_SECONDS:-10}"
latency_samples="${SHIBA_PERF_LATENCY_SAMPLES:-40}"
large_tx_rows="${SHIBA_PERF_LARGE_TX_ROWS:-20000}"
run_id="${SHIBA_PERF_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
output_dir="${SHIBA_PERF_OUTPUT_DIR:-${project_root}/performance/results/${run_id}}"
metrics_file="${output_dir}/metrics.csv"
cluster_retained=0

mkdir -p "${output_dir}"
mkdir -p "${output_dir}/workload"
cp "${project_root}/scripts/performance-benchmark.sh" \
  "${output_dir}/workload/performance-benchmark.sh"
cp "${project_root}/benchmarks/insert-batch.sql" \
  "${project_root}/benchmarks/query-source.sql" \
  "${project_root}/benchmarks/query-shiba.sql" \
  "${output_dir}/workload/"
shasum -a 256 "${output_dir}/workload/"* "${project_root}/Cargo.lock" \
  > "${output_dir}/checksums.sha256"

cleanup() {
  if test "${SHIBA_KEEP_PERF_CLUSTER:-0}" = "1"; then
    cluster_retained=1
    printf 'Retained benchmark cluster: %s\n' "${pg_data_dir}" >&2
    printf 'Retained benchmark socket: %s\n' "${pg_socket_dir}" >&2
    return
  fi
  "${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "${pg_data_dir}" "${pg_socket_dir}"
}
trap cleanup EXIT

psql_db() {
  local database_name="$1"
  shift
  PGOPTIONS="-c statement_timeout=300000 -c lock_timeout=30000" \
    "${pg_bin_dir}/psql" -X -v ON_ERROR_STOP=1 \
      -h "${pg_socket_dir}" -p "${pg_port}" -d "${database_name}" "$@"
}

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%.3f\n", time() * 1000'
}

metric() {
  printf '%s,%s,%s,%s,%s\n' "$1" "$2" "$3" "$4" "$5" >> "${metrics_file}"
}

wait_for_sql() {
  local database_name="$1"
  local expected="$2"
  local query="$3"
  local description="$4"
  local actual=""
  local attempt
  for attempt in {1..3000}; do
    if actual="$(psql_db "${database_name}" -Atqc "${query}" 2>/dev/null)" &&
       test "${actual}" = "${expected}"; then
      return 0
    fi
    sleep 0.02
  done
  printf 'Timed out waiting for %s; last value was [%s]\n' "${description}" "${actual}" >&2
  return 1
}

wait_for_result_count() {
  local expected_count="$1"
  wait_for_sql shiba_perf "${expected_count}" \
    "SELECT coalesce(sum(row_count),0) FROM shiba.bench_stats" \
    "Shiba result to contain ${expected_count} source rows"
}

start_sampler() {
  local scenario="$1"
  local destination="${output_dir}/resources-${scenario}.csv"
  local postmaster_pid
  postmaster_pid="$(sed -n '1p' "${pg_data_dir}/postmaster.pid")"
  printf 'epoch_ms,cpu_percent,rss_kb\n' > "${destination}"
  (
    while kill -0 "${postmaster_pid}" 2>/dev/null; do
      local_pids="$(
        {
          printf '%s\n' "${postmaster_pid}"
          pgrep -P "${postmaster_pid}" 2>/dev/null || true
        } | paste -sd, -
      )"
      if test -n "${local_pids}"; then
        resource_values="$(
          ps -o %cpu=,rss= -p "${local_pids}" 2>/dev/null |
            awk '{cpu += $1; rss += $2} END {printf "%.2f,%d", cpu, rss}'
        )"
        printf '%s,%s\n' "$(now_ms)" "${resource_values}" >> "${destination}"
      fi
      sleep 0.2
    done
  ) &
  sampler_pid="$!"
}

stop_sampler() {
  if test -n "${sampler_pid:-}"; then
    kill "${sampler_pid}" 2>/dev/null || true
    wait "${sampler_pid}" 2>/dev/null || true
    unset sampler_pid
  fi
}

summarize_resources() {
  local scenario="$1"
  local source_file="${output_dir}/resources-${scenario}.csv"
  awk -F, -v scenario="${scenario}" '
    NR > 1 {
      cpu_sum += $2; samples += 1
      if ($2 > cpu_peak) cpu_peak = $2
      if ($3 > rss_peak) rss_peak = $3
    }
    END {
      if (samples > 0) {
        printf "%s,postgres_cpu_mean,%.3f,percent,200ms process samples\n", scenario, cpu_sum / samples
        printf "%s,postgres_cpu_peak,%.3f,percent,200ms process samples\n", scenario, cpu_peak
        printf "%s,postgres_rss_peak,%d,KiB,sum of postmaster and direct children\n", scenario, rss_peak
      }
    }
  ' "${source_file}" >> "${metrics_file}"
}

run_pgbench() {
  local scenario="$1"
  local database_name="$2"
  local script_file="$3"
  shift 3
  local raw_file="${output_dir}/pgbench-${scenario}.txt"
  start_sampler "${scenario}"
  "${pg_bin_dir}/pgbench" -h "${pg_socket_dir}" -p "${pg_port}" \
    -n -r -P 1 -f "${script_file}" "$@" "${database_name}" \
    > "${raw_file}" 2>&1
  stop_sampler
  summarize_resources "${scenario}"
  awk -F= -v scenario="${scenario}" '
    /^tps =/ && $0 !~ /excluding/ {
      value=$2; sub(/^[ \t]+/, "", value); sub(/[ \t].*$/, "", value)
      printf "%s,tps,%s,transactions_per_second,pgbench including connection setup\n", scenario, value
    }
    /^latency average =/ {
      value=$2; sub(/^[ \t]+/, "", value); sub(/[ \t].*$/, "", value)
      printf "%s,latency_average,%s,ms,pgbench client latency\n", scenario, value
    }
  ' "${raw_file}" >> "${metrics_file}"
}

printf 'scenario,metric,value,unit,notes\n' > "${metrics_file}"

cd "${project_root}"

{
  printf 'run_id=%s\n' "${run_id}"
  printf 'git_commit=%s\n' "$(git rev-parse HEAD)"
  printf 'git_dirty_files=%s\n' "$(git status --porcelain | wc -l | tr -d ' ')"
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'pg_config=%s\n' "${pg_config_path}"
  printf 'initial_rows=%s\n' "${initial_rows}"
  printf 'group_count=%s\n' "${group_count}"
  printf 'write_clients=%s\n' "${write_clients}"
  printf 'write_tx_per_client=%s\n' "${write_tx_per_client}"
  printf 'write_batch_size=%s\n' "${write_batch_size}"
  printf 'query_clients=%s\n' "${query_clients}"
  printf 'query_seconds=%s\n' "${query_seconds}"
  printf 'latency_samples=%s\n' "${latency_samples}"
  printf 'large_tx_rows=%s\n' "${large_tx_rows}"
  uname -a
  sw_vers
  sysctl -n hw.model hw.ncpu hw.memsize | paste -sd, -
  rustc --version
  cargo --version
  "${pg_bin_dir}/postgres" --version
  "${pg_bin_dir}/pgbench" --version
} > "${output_dir}/environment.txt"

printf 'Building and installing release extension...\n'
cargo pgrx install --release --pg-config "${pg_config_path}" \
  > "${output_dir}/build.txt" 2>&1

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" --no-locale --encoding=UTF8 \
  > "${output_dir}/initdb.txt"
{
  printf "session_preload_libraries = 'shiba'\n"
  printf "wal_level = logical\n"
  printf "max_replication_slots = 4\n"
  printf "max_worker_processes = 16\n"
  printf "listen_addresses = ''\n"
  printf "unix_socket_directories = '%s'\n" "${pg_socket_dir}"
  printf "port = %s\n" "${pg_port}"
  printf "shared_buffers = '1GB'\n"
  printf "work_mem = '64MB'\n"
  printf "maintenance_work_mem = '256MB'\n"
  printf "shiba.runtime_work_mem = '64MB'\n"
  printf "shiba.runtime_temp_file_limit = '1GB'\n"
  printf "max_wal_size = '4GB'\n"
  printf "checkpoint_timeout = '30min'\n"
  printf "synchronous_commit = on\n"
  printf "fsync = on\n"
  printf "full_page_writes = on\n"
  printf "jit = off\n"
  printf "track_io_timing = on\n"
  printf "log_min_messages = warning\n"
} >> "${pg_data_dir}/postgresql.conf"
cp "${pg_data_dir}/postgresql.conf" "${output_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -t 30 -w start >/dev/null
"${pg_bin_dir}/createdb" -h "${pg_socket_dir}" -p "${pg_port}" baseline_perf
"${pg_bin_dir}/createdb" -h "${pg_socket_dir}" -p "${pg_port}" shiba_perf

for database_name in baseline_perf shiba_perf; do
  psql_db "${database_name}" -qc "
      CREATE SEQUENCE bench_event_id_seq START WITH $((initial_rows + 1));
      CREATE TABLE bench_events (
        event_id bigint NOT NULL,
        group_id integer NOT NULL,
        amount integer NOT NULL
      );
      INSERT INTO bench_events
      SELECT value, value % ${group_count}, 1 + ((value * 31) % 1000)
      FROM generate_series(1, ${initial_rows}) AS value;
      ANALYZE bench_events;"
done

psql_db shiba_perf -qc "CREATE EXTENSION shiba"
psql_db shiba_perf -qc "SELECT shiba.activate()"
wait_for_sql shiba_perf 1 \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'" \
  "the Shiba Runtime"

backfill_start_ms="$(now_ms)"
psql_db shiba_perf -qc "
  CREATE TABLE shiba.bench_stats AS
  SELECT group_id, count(*) AS row_count, sum(amount) AS total_amount
  FROM bench_events
  GROUP BY group_id"
backfill_end_ms="$(now_ms)"
backfill_ms="$(awk -v start="${backfill_start_ms}" -v finish="${backfill_end_ms}" 'BEGIN {printf "%.3f", finish-start}')"
metric backfill wall_time "${backfill_ms}" ms "CTAS registration and initial state build"
metric backfill rows_per_second \
  "$(awk -v rows="${initial_rows}" -v ms="${backfill_ms}" 'BEGIN {printf "%.3f", rows*1000/ms}')" \
  rows_per_second "initial source rows divided by wall time"
wait_for_sql shiba_perf 1 \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'" \
  "one Runtime after registering the benchmark DAG"
runtime_pid="$(psql_db shiba_perf -Atqc "
  SELECT pid FROM pg_stat_activity WHERE backend_type='shiba runtime'")"

psql_db shiba_perf -Atqc "
  EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
  SELECT group_id, count(*) AS row_count, sum(amount) AS total_amount
  FROM bench_events GROUP BY group_id" \
  > "${output_dir}/explain-source-full-aggregate.json"
psql_db shiba_perf -Atqc "
  EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
  SELECT group_id, row_count, total_amount FROM shiba.bench_stats" \
  > "${output_dir}/explain-shiba-full-result.json"

run_pgbench query_source shiba_perf "${project_root}/benchmarks/query-source.sql" \
  -c "${query_clients}" -j "${query_clients}" -T "${query_seconds}"
run_pgbench query_shiba shiba_perf "${project_root}/benchmarks/query-shiba.sql" \
  -c "${query_clients}" -j "${query_clients}" -T "${query_seconds}"

run_pgbench write_baseline baseline_perf "${project_root}/benchmarks/insert-batch.sql" \
  -c "${write_clients}" -j "${write_clients}" -t "${write_tx_per_client}" \
  -D "batch_size=${write_batch_size}"

shiba_write_start_ms="$(now_ms)"
run_pgbench write_shiba shiba_perf "${project_root}/benchmarks/insert-batch.sql" \
  -c "${write_clients}" -j "${write_clients}" -t "${write_tx_per_client}" \
  -D "batch_size=${write_batch_size}"
shiba_write_ingress_end_ms="$(now_ms)"
write_expected_rows="$(psql_db shiba_perf -Atqc "SELECT count(*) FROM bench_events")"
wait_for_result_count "${write_expected_rows}"
shiba_write_drained_ms="$(now_ms)"
metric write_shiba ingress_wall_time \
  "$(awk -v start="${shiba_write_start_ms}" -v finish="${shiba_write_ingress_end_ms}" 'BEGIN {printf "%.3f", finish-start}')" \
  ms "pgbench source-write phase"
metric write_shiba end_to_end_wall_time \
  "$(awk -v start="${shiba_write_start_ms}" -v finish="${shiba_write_drained_ms}" 'BEGIN {printf "%.3f", finish-start}')" \
  ms "source writes plus asynchronous drain"
metric write_shiba end_to_end_rows_per_second \
  "$(awk -v rows="$((write_clients * write_tx_per_client * write_batch_size))" \
    -v start="${shiba_write_start_ms}" -v finish="${shiba_write_drained_ms}" \
    'BEGIN {printf "%.3f", rows*1000/(finish-start)}')" \
  rows_per_second "committed rows divided by ingress plus drain time"

printf 'sample,commit_to_apply_ms\n' > "${output_dir}/visibility-latency.csv"
for sample in $(seq 1 "${latency_samples}"); do
  unique_group=$((1000000 + sample))
  start_epoch_ms="$(
    psql_db shiba_perf -Atqc "
      INSERT INTO bench_events(event_id,group_id,amount)
      VALUES(nextval('bench_event_id_seq'),${unique_group},${sample});
      SELECT extract(epoch FROM clock_timestamp())*1000"
  )"
  wait_for_sql shiba_perf 1 \
    "SELECT count(*) FROM shiba.bench_stats WHERE group_id=${unique_group}" \
    "visibility latency sample ${sample}"
  apply_epoch_ms="$(
    psql_db shiba_perf -Atqc "
      SELECT extract(epoch FROM updated_at)*1000
      FROM shiba.progress('shiba.bench_stats')"
  )"
  awk -v sample="${sample}" -v start="${start_epoch_ms}" -v finish="${apply_epoch_ms}" \
    'BEGIN {printf "%d,%.3f\n", sample, finish-start}' \
    >> "${output_dir}/visibility-latency.csv"
done
awk -F, '
  NR > 1 {values[++n]=$2; sum += $2}
  END {
    for (i=1; i<=n; i++) {
      for (j=i+1; j<=n; j++) {
        if (values[j] < values[i]) {
          tmp=values[i]; values[i]=values[j]; values[j]=tmp
        }
      }
    }
    p50=values[int((n-1)*0.50)+1]
    p95=values[int((n-1)*0.95)+1]
    p99=values[int((n-1)*0.99)+1]
    printf "visibility_latency,mean,%.3f,ms,sequential commit-to-Runtime-apply timestamp\n", sum/n
    printf "visibility_latency,p50,%.3f,ms,nearest-rank over raw samples\n", p50
    printf "visibility_latency,p95,%.3f,ms,nearest-rank over raw samples\n", p95
    printf "visibility_latency,p99,%.3f,ms,nearest-rank over raw samples\n", p99
    printf "visibility_latency,min,%.3f,ms,raw sample minimum\n", values[1]
    printf "visibility_latency,max,%.3f,ms,raw sample maximum\n", values[n]
  }
' "${output_dir}/visibility-latency.csv" >> "${metrics_file}"

psql_db shiba_perf -qc "
  UPDATE shiba_internal.dag_runtime_state
  SET active=false
  WHERE result_oid='shiba.bench_stats'::regclass"
wait_for_sql shiba_perf 1 \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='shiba runtime'" \
  "the Runtime to remain alive while the benchmark DAG is inactive"

large_tx_start_ms="$(now_ms)"
psql_db shiba_perf -qc "
  INSERT INTO bench_events(event_id,group_id,amount)
  SELECT nextval('bench_event_id_seq'), 2000000 + (value % 100), 1 + (value % 1000)
  FROM generate_series(1,${large_tx_rows}) AS value"
large_tx_commit_ms="$(now_ms)"
large_tx_expected_rows="$(psql_db shiba_perf -Atqc "SELECT count(*) FROM bench_events")"
wait_for_sql shiba_perf t \
  "SELECT EXISTS (
     SELECT 1 FROM shiba_internal.dag_inbox
     WHERE result_oid='shiba.bench_stats'::regclass
   )" \
  "the Runtime to durably enqueue the large transaction"
large_tx_apply_start_ms="$(now_ms)"
psql_db shiba_perf -qc "
  UPDATE shiba_internal.dag_runtime_state
  SET active=true
  WHERE result_oid='shiba.bench_stats'::regclass;
  SELECT shiba.activate()"
wait_for_sql shiba_perf "${runtime_pid}" \
  "SELECT pid FROM pg_stat_activity WHERE backend_type='shiba runtime'" \
  "the same Runtime to drain the large transaction"
wait_for_result_count "${large_tx_expected_rows}"
large_tx_apply_end_ms="$(now_ms)"
metric large_transaction source_commit_wall_time \
  "$(awk -v start="${large_tx_start_ms}" -v finish="${large_tx_commit_ms}" 'BEGIN {printf "%.3f", finish-start}')" \
  ms "single transaction source insert"
metric large_transaction apply_wall_time \
  "$(awk -v start="${large_tx_apply_start_ms}" -v finish="${large_tx_apply_end_ms}" 'BEGIN {printf "%.3f", finish-start}')" \
  ms "DAG reactivation through applied LSN"
metric large_transaction apply_rows_per_second \
  "$(awk -v rows="${large_tx_rows}" -v start="${large_tx_apply_start_ms}" -v finish="${large_tx_apply_end_ms}" \
    'BEGIN {printf "%.3f", rows*1000/(finish-start)}')" \
  rows_per_second "large transaction rows divided by apply time"

correctness_difference_count="$(
  psql_db shiba_perf -Atqc "
  WITH expected AS (
    SELECT group_id,count(*)::bigint AS row_count,sum(amount)::bigint AS total_amount
    FROM bench_events GROUP BY group_id
  ),
  actual AS (
    SELECT group_id,row_count::bigint,total_amount::bigint FROM shiba.bench_stats
  ),
  differences AS (
    (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
  )
  SELECT count(*) FROM differences"
)"
printf '%s\n' "${correctness_difference_count}" \
  > "${output_dir}/correctness-difference-count.txt"
if test "${correctness_difference_count}" != "0"; then
  printf 'Benchmark correctness check failed with %s differences\n' \
    "${correctness_difference_count}" >&2
  exit 1
fi

psql_db shiba_perf -Atqc "
  SELECT jsonb_pretty(jsonb_build_object(
    'database_size_bytes',pg_database_size(current_database()),
    'source_total_bytes',pg_total_relation_size('bench_events'),
    'result_total_bytes',pg_total_relation_size('shiba.bench_stats'),
    'extension_state_bytes',(
      SELECT sum(pg_total_relation_size(c.oid))
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='shiba_internal' AND c.relkind IN ('r','m')
    ),
    'source_rows',(SELECT count(*) FROM bench_events),
    'result_rows',(SELECT count(*) FROM shiba.bench_stats),
    'pending_inbox_rows',(
      SELECT count(*) FROM shiba_internal.dag_inbox
      WHERE result_oid='shiba.bench_stats'::regclass
    ),
    'pending_change_log_rows',(
      SELECT count(*) FROM shiba_internal.change_log
    ),
    'pending_routed_transactions',(
      SELECT count(*) FROM shiba_internal.routed_transactions
    ),
    'progress',(SELECT to_jsonb(progress) FROM shiba.progress('shiba.bench_stats') progress)
  ))" > "${output_dir}/final-state.json"

cp "${pg_log_file}" "${output_dir}/postgresql.log"
{
  printf 'finished_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'cluster_retained=%s\n' "${cluster_retained}"
} >> "${output_dir}/environment.txt"

printf 'Performance benchmark completed: %s\n' "${output_dir}"
