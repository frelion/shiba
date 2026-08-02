#!/usr/bin/env bash
# M7.5 integration gate: concurrent DDL invalidation ownership.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m7-concurrent-ddl "$@"

SHIBA_M7_CONCURRENT_DDL_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M7_CONCURRENT_DDL_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M7_CONCURRENT_DDL_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M7_CONCURRENT_DDL_PORT="$SHIBA_TEST_PORT" \
SHIBA_M7_CONCURRENT_DDL_USER="$SHIBA_TEST_USER" \
SHIBA_M7_CONCURRENT_DDL_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m7_concurrent_ddl -- --ignored --test-threads=1

echo "M7 concurrent-DDL integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
