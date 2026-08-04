#!/usr/bin/env bash
# M16.8 integration gate: bounded indexed MIN/MAX state reads.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m16-indexed-state "$@"

database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER connect_timeout=5"

SHIBA_M16_INDEXED_STATE_DATABASE_URL="$database_url" \
SHIBA_M16_INDEXED_STATE_REPLICATION_URL="$database_url replication=database application_name=shiba_m16_indexed_state" \
  cargo test -p shiba-ingress --test m16_indexed_state -- --ignored --nocapture --test-threads=1

echo "M16.8 indexed-state MIN/MAX passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
