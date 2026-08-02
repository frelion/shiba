#!/usr/bin/env bash
# M4.2 integration gate: empty tuples from a real pgoutput stream.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m4-empty "$@"

SHIBA_M4_EMPTY_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M4_EMPTY_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M4_EMPTY_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M4_EMPTY_PORT="$SHIBA_TEST_PORT" \
SHIBA_M4_EMPTY_USER="$SHIBA_TEST_USER" \
SHIBA_M4_EMPTY_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m4_empty -- --ignored --test-threads=1

echo "M4 empty integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
