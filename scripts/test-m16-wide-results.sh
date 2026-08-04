#!/usr/bin/env bash
# M16.2 integration gate: canonical wide scalar/keyed results and atomic sink failure.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m16-wide-results "$@"

SHIBA_M16_WIDE_RESULTS_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M16_WIDE_RESULTS_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M16_WIDE_RESULTS_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M16_WIDE_RESULTS_PORT="$SHIBA_TEST_PORT" \
SHIBA_M16_WIDE_RESULTS_USER="$SHIBA_TEST_USER" \
SHIBA_M16_WIDE_RESULTS_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m16_wide_results -- --ignored --test-threads=1

echo "M16 canonical wide results passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
