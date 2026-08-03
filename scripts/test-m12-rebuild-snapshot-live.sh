#!/usr/bin/env bash
# M12.3 gate: active generation rebuild through exported snapshot and M10 live handoff.
set -euo pipefail

export SHIBA_TEST_WAL_SENDER_TIMEOUT=2s
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m12-rebuild-snapshot-live "$@"

target_key="$(rustc -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')"
database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER connect_timeout=5"

SHIBA_M12_REBUILD_DATABASE_URL="$database_url" \
SHIBA_M12_REBUILD_REPLICATION_URL="$database_url replication=database application_name=shiba_m12_rebuild_snapshot_live" \
PQ_LIB_DIR="$($SHIBA_TEST_PG_CONFIG --libdir)" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
env "PG_CONFIG_${target_key}=$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-ingress --test m12_rebuild_snapshot_live -- --ignored --test-threads=1

echo "M12 rebuild snapshot-to-live passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
