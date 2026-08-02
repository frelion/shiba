#!/usr/bin/env bash
# M5.2 integration gate: replacement incompressible TOAST from real pgoutput.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m5-incompressible-toast "$@"

SHIBA_M5_INCOMPRESSIBLE_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M5_INCOMPRESSIBLE_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M5_INCOMPRESSIBLE_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M5_INCOMPRESSIBLE_PORT="$SHIBA_TEST_PORT" \
SHIBA_M5_INCOMPRESSIBLE_USER="$SHIBA_TEST_USER" \
SHIBA_M5_INCOMPRESSIBLE_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m5_incompressible_toast -- --ignored --test-threads=1

echo "M5 incompressible TOAST integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
