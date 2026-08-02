#!/usr/bin/env bash
# M8.2 integration gate: concurrent progress across independent sources.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m8-concurrent-sources "$@"

SHIBA_M8_CONCURRENT_SOURCES_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M8_CONCURRENT_SOURCES_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M8_CONCURRENT_SOURCES_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M8_CONCURRENT_SOURCES_PORT="$SHIBA_TEST_PORT" \
SHIBA_M8_CONCURRENT_SOURCES_USER="$SHIBA_TEST_USER" \
SHIBA_M8_CONCURRENT_SOURCES_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m8_concurrent_sources -- --ignored --test-threads=1

echo "M8 concurrent-sources integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
