#!/usr/bin/env bash
# M5.3 integration gate: composite-key DELETE from a real pgoutput stream.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m5-composite-delete "$@"

SHIBA_M5_COMPOSITE_DELETE_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M5_COMPOSITE_DELETE_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M5_COMPOSITE_DELETE_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M5_COMPOSITE_DELETE_PORT="$SHIBA_TEST_PORT" \
SHIBA_M5_COMPOSITE_DELETE_USER="$SHIBA_TEST_USER" \
SHIBA_M5_COMPOSITE_DELETE_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m5_composite_delete -- --ignored --test-threads=1

echo "M5 composite DELETE integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
