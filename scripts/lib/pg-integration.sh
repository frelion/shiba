#!/usr/bin/env bash
# Shared mechanics for real-pgoutput milestone gates. Callers own test intent.

shiba_pg_integration_setup() {
  local script_name="$1"
  local temp_prefix="$2"
  shift 2

  if [[ $# -ne 1 || "$1" != /* || ! -x "$1" ]]; then
    echo "usage: $script_name <absolute-pg_config-for-17-or-18>" >&2
    exit 64
  fi

  SHIBA_TEST_PG_CONFIG="$1"
  SHIBA_TEST_PG_MAJOR="$("$SHIBA_TEST_PG_CONFIG" --version | sed -E 's/.*PostgreSQL ([0-9]+)\..*/\1/')"
  if [[ "$SHIBA_TEST_PG_MAJOR" != "17" && "$SHIBA_TEST_PG_MAJOR" != "18" ]]; then
    echo "only PostgreSQL 17 or 18 is supported (got: $SHIBA_TEST_PG_MAJOR)" >&2
    exit 64
  fi
  if [[ -n "${SHIBA_TEST_LOGICAL_DECODING_WORK_MEM:-}" \
    && "$SHIBA_TEST_LOGICAL_DECODING_WORK_MEM" != "64kB" ]]; then
    echo "SHIBA_TEST_LOGICAL_DECODING_WORK_MEM only supports 64kB" >&2
    exit 64
  fi
  SHIBA_TEST_WAL_SENDER_TIMEOUT="${SHIBA_TEST_WAL_SENDER_TIMEOUT:-0}"
  if [[ "$SHIBA_TEST_WAL_SENDER_TIMEOUT" != "0" \
    && "$SHIBA_TEST_WAL_SENDER_TIMEOUT" != "2s" ]]; then
    echo "SHIBA_TEST_WAL_SENDER_TIMEOUT only supports 0 or 2s" >&2
    exit 64
  fi

  SHIBA_TEST_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  SHIBA_TEST_PG_BINDIR="$("$SHIBA_TEST_PG_CONFIG" --bindir)"
  SHIBA_TEST_PG_SHAREDIR="$("$SHIBA_TEST_PG_CONFIG" --sharedir)/extension"
  SHIBA_TEST_PG_PKGLIBDIR="$("$SHIBA_TEST_PG_CONFIG" --pkglibdir)"
  SHIBA_TEST_TMP="$(mktemp -d "/tmp/${temp_prefix}-pg${SHIBA_TEST_PG_MAJOR}.XXXXXX")"
  SHIBA_TEST_DATA="$SHIBA_TEST_TMP/data"
  SHIBA_TEST_SOCKET="$SHIBA_TEST_TMP/socket"
  SHIBA_TEST_PACKAGE="$SHIBA_TEST_TMP/package"
  SHIBA_TEST_CAPTURE="$SHIBA_TEST_TMP/capture"
  SHIBA_TEST_STARTED=0
  SHIBA_TEST_INSTALLED_CONTROL=""
  SHIBA_TEST_INSTALLED_SQL=""
  SHIBA_TEST_INSTALLED_LIBRARY=""

  trap shiba_pg_integration_cleanup EXIT
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 143' TERM

  SHIBA_TEST_PORT="$(python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
  SHIBA_TEST_USER="$(id -un)"

  cd "$SHIBA_TEST_ROOT"
  local feature="pg$SHIBA_TEST_PG_MAJOR"
  cargo pgrx package --manifest-path crates/shiba-catalog/Cargo.toml \
    --pg-config "$SHIBA_TEST_PG_CONFIG" --no-default-features --features "$feature" \
    --out-dir "$SHIBA_TEST_PACKAGE"

  local control_source sql_source library_source
  control_source="$(find "$SHIBA_TEST_PACKAGE" -type f -name 'shiba_catalog.control' -print -quit)"
  sql_source="$(find "$SHIBA_TEST_PACKAGE" -type f -name 'shiba_catalog--*.sql' -print -quit)"
  library_source="$(find "$SHIBA_TEST_PACKAGE" -type f \( -name 'shiba_catalog*.dylib' -o -name 'shiba_catalog*.so' \) -print -quit)"
  if [[ -z "$control_source" || -z "$sql_source" || -z "$library_source" ]]; then
    echo "pgrx package did not produce a complete extension package" >&2
    exit 1
  fi

  local control_target sql_target library_target
  control_target="$SHIBA_TEST_PG_SHAREDIR/$(basename "$control_source")"
  sql_target="$SHIBA_TEST_PG_SHAREDIR/$(basename "$sql_source")"
  library_target="$SHIBA_TEST_PG_PKGLIBDIR/$(basename "$library_source")"
  if [[ -e "$control_target" || -e "$sql_target" || -e "$library_target" ]]; then
    echo "refusing to overwrite an existing shiba_catalog installation" >&2
    exit 1
  fi
  SHIBA_TEST_INSTALLED_CONTROL="$control_target"
  install -m 0644 "$control_source" "$control_target"
  SHIBA_TEST_INSTALLED_SQL="$sql_target"
  install -m 0644 "$sql_source" "$sql_target"
  SHIBA_TEST_INSTALLED_LIBRARY="$library_target"
  install -m 0755 "$library_source" "$library_target"

  mkdir -p "$SHIBA_TEST_SOCKET" "$SHIBA_TEST_CAPTURE"
  "$SHIBA_TEST_PG_BINDIR/initdb" -D "$SHIBA_TEST_DATA" \
    --no-locale --encoding=UTF8 --auth=trust >/dev/null
  printf '%s\n' 'wal_level=logical' 'max_replication_slots=4' 'max_wal_senders=4' \
    "wal_sender_timeout=$SHIBA_TEST_WAL_SENDER_TIMEOUT" >> "$SHIBA_TEST_DATA/postgresql.conf"
  if [[ -n "${SHIBA_TEST_LOGICAL_DECODING_WORK_MEM:-}" ]]; then
    printf 'logical_decoding_work_mem=%s\n' "$SHIBA_TEST_LOGICAL_DECODING_WORK_MEM" \
      >> "$SHIBA_TEST_DATA/postgresql.conf"
  fi
  "$SHIBA_TEST_PG_BINDIR/pg_ctl" -D "$SHIBA_TEST_DATA" \
    -o "-k '$SHIBA_TEST_SOCKET' -p $SHIBA_TEST_PORT" -w start >/dev/null
  SHIBA_TEST_STARTED=1
}

shiba_pg_integration_cleanup() {
  local status=$?
  trap - EXIT HUP INT TERM
  if [[ "${SHIBA_TEST_STARTED:-0}" == 1 ]]; then
    "$SHIBA_TEST_PG_BINDIR/pg_ctl" -D "$SHIBA_TEST_DATA" -m immediate stop >/dev/null 2>&1 || true
  fi
  [[ -z "${SHIBA_TEST_INSTALLED_CONTROL:-}" ]] || rm -f -- "$SHIBA_TEST_INSTALLED_CONTROL"
  [[ -z "${SHIBA_TEST_INSTALLED_SQL:-}" ]] || rm -f -- "$SHIBA_TEST_INSTALLED_SQL"
  [[ -z "${SHIBA_TEST_INSTALLED_LIBRARY:-}" ]] || rm -f -- "$SHIBA_TEST_INSTALLED_LIBRARY"
  [[ -z "${SHIBA_TEST_TMP:-}" ]] || rm -rf -- "$SHIBA_TEST_TMP"
  exit "$status"
}
