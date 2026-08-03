#!/usr/bin/env bash
# M14.3 integration gate: nullable grouped keys and keyed aggregate state.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m14-grouped "$@"

SHIBA_M14_GROUPED_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M14_GROUPED_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M14_GROUPED_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M14_GROUPED_PORT="$SHIBA_TEST_PORT" \
SHIBA_M14_GROUPED_USER="$SHIBA_TEST_USER" \
SHIBA_M14_GROUPED_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m14_grouped -- --ignored --test-threads=1

echo "M14 grouped integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
