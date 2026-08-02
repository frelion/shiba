#!/usr/bin/env bash
# M11.2 gate: exported snapshot, concurrent WAL catch-up, and M10 live handoff.
set -euo pipefail

export SHIBA_TEST_WAL_SENDER_TIMEOUT=2s
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m11-bootstrap "$@"

target_key="$(rustc -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')"
database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER connect_timeout=5"

SHIBA_M11_BOOTSTRAP_DATABASE_URL="$database_url" \
SHIBA_M11_BOOTSTRAP_REPLICATION_URL="$database_url replication=database application_name=shiba_m11_bootstrap_receiver" \
PQ_LIB_DIR="$($SHIBA_TEST_PG_CONFIG --libdir)" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
env "PG_CONFIG_${target_key}=$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-ingress --test m11_bootstrap -- --ignored --test-threads=1

echo "M11 snapshot-to-live bootstrap passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
