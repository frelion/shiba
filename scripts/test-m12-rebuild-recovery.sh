#!/usr/bin/env bash
# M12.4 deterministic rebuild crash/restart matrix.
set -euo pipefail

export SHIBA_TEST_WAL_SENDER_TIMEOUT=2s
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m12-rebuild-recovery "$@"

target_key="$(rustc -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')"
database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER connect_timeout=5"
control_role=shiba_m12_recovery_replication
"$SHIBA_TEST_PG_BINDIR/psql" -h "$SHIBA_TEST_SOCKET" -p "$SHIBA_TEST_PORT" -d postgres \
  -U "$SHIBA_TEST_USER" -v ON_ERROR_STOP=1 \
  -c "CREATE ROLE $control_role LOGIN REPLICATION"

SHIBA_M12_RECOVERY_DATABASE_URL="$database_url" \
SHIBA_M12_RECOVERY_REPLICATION_URL="$database_url replication=database application_name=shiba_m12_recovery_seed" \
SHIBA_M12_RECOVERY_CONTROL_REPLICATION_URL="$database_url user=$control_role replication=database application_name=shiba_m12_recovery_receiver" \
SHIBA_TEST_PG_CTL="$(dirname "$SHIBA_TEST_PG_CONFIG")/pg_ctl" \
SHIBA_TEST_PG_DATA="$SHIBA_TEST_DATA" \
SHIBA_TEST_PG_SOCKET="$SHIBA_TEST_SOCKET" \
SHIBA_TEST_PG_PORT="$SHIBA_TEST_PORT" \
PQ_LIB_DIR="$($SHIBA_TEST_PG_CONFIG --libdir)" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
env "PG_CONFIG_${target_key}=$SHIBA_TEST_PG_CONFIG" \
  cargo test -p shiba-ingress --test m12_rebuild_recovery -- --ignored --test-threads=1

echo "M12 rebuild recovery passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
