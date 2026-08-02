#!/usr/bin/env bash
# M9.2 integration gate: compiled CountRows and SumInt8 share one atomic EffectBatch.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m9-count-sum "$@"

SHIBA_M9_COUNT_SUM_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M9_COUNT_SUM_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M9_COUNT_SUM_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M9_COUNT_SUM_PORT="$SHIBA_TEST_PORT" \
SHIBA_M9_COUNT_SUM_USER="$SHIBA_TEST_USER" \
SHIBA_M9_COUNT_SUM_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m9_count_sum -- --ignored --test-threads=1

echo "M9 count+sum integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
