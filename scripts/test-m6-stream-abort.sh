#!/usr/bin/env bash
# M6.2 integration gate: streamed transactions aborted from real pgoutput.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
SHIBA_TEST_LOGICAL_DECODING_WORK_MEM=64kB
shiba_pg_integration_setup "$0" shiba-m6-stream-abort "$@"

SHIBA_M6_STREAM_ABORT_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M6_STREAM_ABORT_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M6_STREAM_ABORT_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M6_STREAM_ABORT_PORT="$SHIBA_TEST_PORT" \
SHIBA_M6_STREAM_ABORT_USER="$SHIBA_TEST_USER" \
SHIBA_M6_STREAM_ABORT_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m6_stream_abort -- --ignored --test-threads=1

echo "M6 stream-abort integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
