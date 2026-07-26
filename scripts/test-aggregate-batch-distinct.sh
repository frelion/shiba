#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_config_path="${PG_CONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
pg_bin_dir="$("${pg_config_path}" --bindir)"
pg_data_dir="$(mktemp -d /tmp/shiba-aggregate-batch-data.XXXXXX)"
pg_socket_dir="$(mktemp -d /tmp/shiba-aggregate-batch-socket.XXXXXX)"
pg_log_file="${pg_data_dir}/postgresql.log"
pg_port="${SHIBA_AGGREGATE_BATCH_PORT:-$((57000 + $$ % 5000))}"

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

cd "${project_root}"
cargo pgrx install --pg-config "${pg_config_path}"

"${pg_bin_dir}/initdb" -D "${pg_data_dir}" \
  --no-locale --encoding=UTF8 >/dev/null
{
  printf "session_preload_libraries = 'shiba'\\n"
  printf "wal_level = logical\\n"
  printf "max_replication_slots = 4\\n"
  printf "unix_socket_directories = '%s'\\n" "${pg_socket_dir}"
  printf "port = %s\\n" "${pg_port}"
} >> "${pg_data_dir}/postgresql.conf"

"${pg_bin_dir}/pg_ctl" -D "${pg_data_dir}" -l "${pg_log_file}" \
  -o "-k ${pg_socket_dir} -p ${pg_port}" -w start >/dev/null
"${pg_bin_dir}/createdb" -h "${pg_socket_dir}" -p "${pg_port}" \
  shiba_aggregate_batch
"${pg_bin_dir}/psql" -X -v ON_ERROR_STOP=1 \
  -h "${pg_socket_dir}" -p "${pg_port}" -d shiba_aggregate_batch \
  -c "CREATE EXTENSION shiba" >/dev/null
"${pg_bin_dir}/psql" -X -v ON_ERROR_STOP=1 \
  -h "${pg_socket_dir}" -p "${pg_port}" -d shiba_aggregate_batch \
  -f tests/aggregate_batch_distinct.sql

printf 'aggregate COUNT(DISTINCT) batch test passed\n'
