#!/usr/bin/env bash
# M12 identity gate: exact durable replica-index authority and resume rejection.
set -euo pipefail

export SHIBA_TEST_WAL_SENDER_TIMEOUT=2s
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m12-rebuild-identity-authority "$@"

target_key="$(rustc -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')"
database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER connect_timeout=5"

SHIBA_M12_IDENTITY_DATABASE_URL="$database_url" \
SHIBA_M12_IDENTITY_REPLICATION_URL="$database_url replication=database application_name=shiba_m12_identity_authority" \
PQ_LIB_DIR="$($SHIBA_TEST_PG_CONFIG --libdir)" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
env "PG_CONFIG_${target_key}=$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-ingress --test m12_rebuild_identity_authority -- --ignored --test-threads=1

echo "M12 rebuild identity authority passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
