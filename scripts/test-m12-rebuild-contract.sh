#!/usr/bin/env bash
# M12.1 failure-first gate: legacy pristine transitions cannot mutate an active source.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m12-rebuild-contract "$@"

target_key="$(rustc -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')"
database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER connect_timeout=5"

SHIBA_M12_REBUILD_DATABASE_URL="$database_url" \
SHIBA_M12_REBUILD_REPLICATION_URL="$database_url replication=database application_name=shiba_m12_contract_receiver" \
PQ_LIB_DIR="$($SHIBA_TEST_PG_CONFIG --libdir)" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
env "PG_CONFIG_${target_key}=$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-ingress --test m12_rebuild_contract -- --ignored --test-threads=1

echo "M12.1 rebuild failure-first contract passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
