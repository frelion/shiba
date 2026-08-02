#!/usr/bin/env bash
# M11.3 gate: batch rollback/replay, ownership, restart, feedback, and cutover recovery.
set -euo pipefail

export SHIBA_TEST_WAL_SENDER_TIMEOUT=2s
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m11-recovery "$@"

target_key="$(rustc -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')"
database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER connect_timeout=5"

SHIBA_M11_RECOVERY_DATABASE_URL="$database_url" \
SHIBA_M11_RECOVERY_REPLICATION_URL="$database_url replication=database application_name=shiba_m11_recovery_receiver" \
SHIBA_TEST_PG_CTL="$SHIBA_TEST_PG_BINDIR/pg_ctl" \
SHIBA_TEST_PG_DATA="$SHIBA_TEST_DATA" \
SHIBA_TEST_PG_SOCKET="$SHIBA_TEST_SOCKET" \
SHIBA_TEST_PG_PORT="$SHIBA_TEST_PORT" \
PQ_LIB_DIR="$($SHIBA_TEST_PG_CONFIG --libdir)" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
env "PG_CONFIG_${target_key}=$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-ingress --test m11_bootstrap_recovery -- --ignored --test-threads=1

echo "M11 bootstrap recovery passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
