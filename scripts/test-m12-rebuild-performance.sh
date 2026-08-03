#!/usr/bin/env bash
# M12.6: million-row active-source rebuild, boundedness, and SQL differential.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m12-rebuild-performance "$@"

target_key="$(rustc -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')"
database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER connect_timeout=10"

SHIBA_M12_PERFORMANCE_DATABASE_URL="$database_url" \
SHIBA_M12_PERFORMANCE_REPLICATION_URL="$database_url replication=database application_name=shiba_m12_performance_receiver" \
PQ_LIB_DIR="$("$SHIBA_TEST_PG_CONFIG" --libdir)" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
env "PG_CONFIG_${target_key}=$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-ingress --test m12_rebuild_performance -- \
    --ignored --test-threads=1 --nocapture

echo "M12.6 bounded rebuild performance passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
