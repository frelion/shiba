#!/usr/bin/env bash
# M9.2 integration gate: two-operator lock order and independent-source progress.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m9-operator-concurrency "$@"

SHIBA_M9_OPERATOR_CONCURRENCY_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M9_OPERATOR_CONCURRENCY_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M9_OPERATOR_CONCURRENCY_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M9_OPERATOR_CONCURRENCY_PORT="$SHIBA_TEST_PORT" \
SHIBA_M9_OPERATOR_CONCURRENCY_USER="$SHIBA_TEST_USER" \
SHIBA_M9_OPERATOR_CONCURRENCY_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m9_operator_concurrency -- --ignored --test-threads=1

echo "M9 operator-concurrency integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
