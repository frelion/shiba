#!/usr/bin/env bash
# M8.3 integration gate: bounded decode under a real pgoutput workload.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m8-bounded-decode "$@"

SHIBA_M8_BOUNDED_DECODE_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M8_BOUNDED_DECODE_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M8_BOUNDED_DECODE_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M8_BOUNDED_DECODE_PORT="$SHIBA_TEST_PORT" \
SHIBA_M8_BOUNDED_DECODE_USER="$SHIBA_TEST_USER" \
SHIBA_M8_BOUNDED_DECODE_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m8_bounded_decode -- --ignored --test-threads=1

echo "M8 bounded-decode integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
