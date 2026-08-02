#!/usr/bin/env bash
# M10.4 catalog gate: database-local ingress configuration and lifecycle.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m10-catalog "$@"

target_key="$(rustc -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')"
database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER"

SHIBA_M10_CATALOG_DATABASE_URL="$database_url" \
SHIBA_M10_CATALOG_REPLICATION_URL="$database_url replication=database application_name=shiba_m10_catalog_active" \
PQ_LIB_DIR="$($SHIBA_TEST_PG_CONFIG --libdir)" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
env "PG_CONFIG_${target_key}=$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-ingress --test m10_catalog -- --ignored --test-threads=1

echo "M10 catalog ingress passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
