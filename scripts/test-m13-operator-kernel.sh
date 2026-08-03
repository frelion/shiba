#!/usr/bin/env bash
# M13.3 integration gate: generic scalar and keyed operator sinks share one transaction.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m13-operator-kernel "$@"

SHIBA_M13_OPERATOR_KERNEL_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M13_OPERATOR_KERNEL_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M13_OPERATOR_KERNEL_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M13_OPERATOR_KERNEL_PORT="$SHIBA_TEST_PORT" \
SHIBA_M13_OPERATOR_KERNEL_USER="$SHIBA_TEST_USER" \
SHIBA_M13_OPERATOR_KERNEL_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m13_operator_kernel -- --ignored --test-threads=1

echo "M13 generic operator kernel passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
