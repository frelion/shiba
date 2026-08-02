#!/usr/bin/env bash
# M3 integration gate: disposable logical-decoding cluster plus real pgoutput.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m3 "$@"

SHIBA_M3_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M3_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M3_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M3_PORT="$SHIBA_TEST_PORT" \
SHIBA_M3_USER="$SHIBA_TEST_USER" \
SHIBA_M3_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m3_pgoutput -- --ignored --test-threads=1

echo "M3 integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
