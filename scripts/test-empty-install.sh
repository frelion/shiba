#!/usr/bin/env bash
# Fresh-cluster Phase-1 installation probe. It owns exactly one temporary
# cluster and copies only package files it created; all cleanup is explicit.
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
tmp="$(mktemp -d "/tmp/shiba-v2-cleanroom-pg${pg_major}.XXXXXX")"
data="$tmp/data"
socket="$tmp/socket"
package="$tmp/package"
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

mkdir -p "$socket"
"$bindir/initdb" -D "$data" --no-locale --encoding=UTF8 --auth=trust >/dev/null
"$bindir/pg_ctl" -D "$data" -o "-k '$socket' -p $port" -w start >/dev/null
started=1
psql=("$bindir/psql" -X -v ON_ERROR_STOP=1 -h "$socket" -p "$port" -d postgres)

# The forced error proves CREATE EXTENSION remains fully transactional.
if "${psql[@]}" -c 'BEGIN; CREATE EXTENSION shiba_catalog; SELECT 1/0; COMMIT;' >/dev/null 2>&1; then
  echo "forced rollback probe unexpectedly succeeded" >&2
  exit 1
fi
rollback_state="$("${psql[@]}" -Atqc "SELECT coalesce(to_regnamespace('shiba')::text, 'absent') || '|' || coalesce(to_regnamespace('shiba_internal')::text, 'absent') || '|' || coalesce((SELECT extname FROM pg_extension WHERE extname = 'shiba_catalog'), 'absent')")"
[[ "$rollback_state" == "absent|absent|absent" ]] || {
  echo "CREATE EXTENSION rollback left state: $rollback_state" >&2
  exit 1
}

"${psql[@]}" -c 'CREATE EXTENSION shiba_catalog;' >/dev/null
versions="$("${psql[@]}" -Atqc "SELECT catalog_version || '|' || protocol_version FROM shiba.versions()")"
[[ "$versions" == "1|1" ]] || { echo "unexpected versions: $versions" >&2; exit 1; }
"${psql[@]}" -c 'CREATE ROLE shiba_cleanroom_probe LOGIN;' >/dev/null
probe_versions="$("${psql[@]}" -Atqc "SET ROLE shiba_cleanroom_probe; SELECT catalog_version || '|' || protocol_version FROM shiba.versions()")"
[[ "$probe_versions" == "1|1" ]] || { echo "PUBLIC cannot execute shiba.versions(): $probe_versions" >&2; exit 1; }
if "${psql[@]}" -c 'SET ROLE shiba_cleanroom_probe; SELECT * FROM shiba_internal.catalog_identity;' >/dev/null 2>&1; then
  echo "ordinary role can read private catalog state" >&2
  exit 1
fi

"${psql[@]}" -c 'DROP EXTENSION shiba_catalog;' >/dev/null
drop_state="$("${psql[@]}" -Atqc "SELECT coalesce(to_regnamespace('shiba')::text, 'absent') || '|' || coalesce(to_regnamespace('shiba_internal')::text, 'absent') || '|' || coalesce((SELECT extname FROM pg_extension WHERE extname = 'shiba_catalog'), 'absent')")"
[[ "$drop_state" == "absent|absent|absent" ]] || {
  echo "DROP EXTENSION left state: $drop_state" >&2
  exit 1
}

echo "empty install passed for PostgreSQL $pg_major ($pg_config)"
