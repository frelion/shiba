#!/usr/bin/env bash
# M6.1 integration gate: streamed transactions committed from real pgoutput.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
SHIBA_TEST_LOGICAL_DECODING_WORK_MEM=64kB
shiba_pg_integration_setup "$0" shiba-m6-stream-commit "$@"

SHIBA_M6_STREAM_COMMIT_DATABASE_URL="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER" \
SHIBA_M6_STREAM_COMMIT_PG_BINDIR="$SHIBA_TEST_PG_BINDIR" \
SHIBA_M6_STREAM_COMMIT_HOST="$SHIBA_TEST_SOCKET" \
SHIBA_M6_STREAM_COMMIT_PORT="$SHIBA_TEST_PORT" \
SHIBA_M6_STREAM_COMMIT_USER="$SHIBA_TEST_USER" \
SHIBA_M6_STREAM_COMMIT_CAPTURE_DIR="$SHIBA_TEST_CAPTURE" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-runtime --test m6_stream_commit -- --ignored --test-threads=1

echo "M6 stream-commit integration passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
