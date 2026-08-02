#!/usr/bin/env bash
# M7.2 integration gate: source invalidation after object DROP.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m7-drop-invalidation "$@"

SHIBA_M7_DROP_INVALIDATION_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M7_DROP_INVALIDATION_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M7_DROP_INVALIDATION_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M7_DROP_INVALIDATION_PORT="$SHIBA_TEST_PORT" \
SHIBA_M7_DROP_INVALIDATION_USER="$SHIBA_TEST_USER" \
SHIBA_M7_DROP_INVALIDATION_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m7_drop_invalidation -- --ignored --test-threads=1

echo "M7 DROP-invalidation integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
