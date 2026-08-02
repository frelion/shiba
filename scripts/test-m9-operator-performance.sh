#!/usr/bin/env bash
# M9.2 integration gate: CountRows + SumInt8 atomicity and regression budget.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m9-operator-performance "$@"

SHIBA_M9_OPERATOR_PERFORMANCE_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M9_OPERATOR_PERFORMANCE_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M9_OPERATOR_PERFORMANCE_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M9_OPERATOR_PERFORMANCE_PORT="$SHIBA_TEST_PORT" \
SHIBA_M9_OPERATOR_PERFORMANCE_USER="$SHIBA_TEST_USER" \
SHIBA_M9_OPERATOR_PERFORMANCE_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m9_operator_performance -- --ignored --test-threads=1

echo "M9 operator-performance integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
