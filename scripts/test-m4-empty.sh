#!/usr/bin/env bash
# M4.2 integration gate: empty tuples from a real pgoutput stream.
set -euo pipefail

if [[ $# -ne 1 || "$1" != /* || ! -x "$1" ]]; then
  echo "usage: $0 <absolute-pg_config-for-17-or-18>" >&2
  exit 64
fi
pg_config="$1"
pg_major="$($pg_config --version | sed -E 's/.*PostgreSQL ([0-9]+)\..*/\1/')"
if [[ "$pg_major" != "17" && "$pg_major" != "18" ]]; then
  echo "only PostgreSQL 17 or 18 is supported (got: $pg_major)" >&2
  exit 64
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bindir="$($pg_config --bindir)"
sharedir="$($pg_config --sharedir)/extension"
pkglibdir="$($pg_config --pkglibdir)"
tmp="$(mktemp -d "/tmp/shiba-m4-empty-pg${pg_major}.XXXXXX")"
data="$tmp/data"
socket="$tmp/socket"
package="$tmp/package"
capture="$tmp/capture"
port="$(python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
started=0
installed_control=""
installed_sql=""
installed_library=""

cleanup() {
  status=$?
  if [[ "$started" == 1 ]]; then
    "$bindir/pg_ctl" -D "$data" -m immediate stop >/dev/null 2>&1 || true
  fi
  [[ -z "$installed_control" ]] || rm -f -- "$installed_control"
  [[ -z "$installed_sql" ]] || rm -f -- "$installed_sql"
  [[ -z "$installed_library" ]] || rm -f -- "$installed_library"
  rm -rf -- "$tmp"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

cd "$root"
feature="pg$pg_major"
cargo pgrx package --manifest-path crates/shiba-catalog/Cargo.toml \
  --pg-config "$pg_config" --no-default-features --features "$feature" --out-dir "$package"

control_source="$(find "$package" -type f -name 'shiba_catalog.control' -print -quit)"
sql_source="$(find "$package" -type f -name 'shiba_catalog--*.sql' -print -quit)"
library_source="$(find "$package" -type f \( -name 'shiba_catalog*.dylib' -o -name 'shiba_catalog*.so' \) -print -quit)"
if [[ -z "$control_source" || -z "$sql_source" || -z "$library_source" ]]; then
  echo "pgrx package did not produce a complete extension package" >&2
  exit 1
fi
installed_control="$sharedir/$(basename "$control_source")"
installed_sql="$sharedir/$(basename "$sql_source")"
installed_library="$pkglibdir/$(basename "$library_source")"
if [[ -e "$installed_control" || -e "$installed_sql" || -e "$installed_library" ]]; then
  echo "refusing to overwrite an existing shiba_catalog installation" >&2
  exit 1
fi
install -m 0644 "$control_source" "$installed_control"
install -m 0644 "$sql_source" "$installed_sql"
install -m 0755 "$library_source" "$installed_library"

mkdir -p "$socket" "$capture"
"$bindir/initdb" -D "$data" --no-locale --encoding=UTF8 --auth=trust >/dev/null
printf '%s\n' 'wal_level=logical' 'max_replication_slots=4' 'max_wal_senders=4' \
  >> "$data/postgresql.conf"
"$bindir/pg_ctl" -D "$data" -o "-k '$socket' -p $port" -w start >/dev/null
started=1

SHIBA_M4_EMPTY_DATABASE_URL="host=$socket port=$port dbname=postgres user=$(id -un)" \
SHIBA_M4_EMPTY_PG_BINDIR="$bindir" \
SHIBA_M4_EMPTY_HOST="$socket" \
SHIBA_M4_EMPTY_PORT="$port" \
SHIBA_M4_EMPTY_USER="$(id -un)" \
SHIBA_M4_EMPTY_CAPTURE_DIR="$capture" \
PG_CONFIG="$pg_config" \
  cargo test -p shiba-runtime --test m4_empty -- --ignored --test-threads=1

echo "M4 empty integration passed for PostgreSQL $pg_major ($pg_config)"
