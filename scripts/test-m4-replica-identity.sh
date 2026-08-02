#!/usr/bin/env bash
# M4.6 integration gate: replica-identity admission from a real pgoutput stream.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m4-replica-identity "$@"

SHIBA_M4_REPLICA_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M4_REPLICA_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M4_REPLICA_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M4_REPLICA_PORT="$SHIBA_TEST_PORT" \
SHIBA_M4_REPLICA_USER="$SHIBA_TEST_USER" \
SHIBA_M4_REPLICA_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m4_replica_identity -- --ignored --test-threads=1

echo "M4 replica identity integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
