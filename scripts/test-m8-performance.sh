#!/usr/bin/env bash
# M8.4 integration gate: bounded performance under a real pgoutput workload.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m8-performance "$@"

SHIBA_M8_PERFORMANCE_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M8_PERFORMANCE_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M8_PERFORMANCE_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M8_PERFORMANCE_PORT="$SHIBA_TEST_PORT" \
SHIBA_M8_PERFORMANCE_USER="$SHIBA_TEST_USER" \
SHIBA_M8_PERFORMANCE_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m8_performance -- --ignored --test-threads=1 --nocapture

echo "M8 performance integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
