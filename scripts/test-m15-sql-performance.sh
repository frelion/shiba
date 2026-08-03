#!/usr/bin/env bash
# M15 frozen release-mode SQL frontend and registration performance budgets.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/pg-integration.sh"
shiba_pg_integration_setup "$0" shiba-m15-sql-performance "$@"

if rg -n 'parse_sql|compile_sql_and_register|shiba_sql_frontend' \
  crates/shiba-ingress/src crates/shiba-runtime/src; then
  echo "live Runtime/Ingress production code must not parse or register SQL" >&2
  exit 1
fi
if rg -n 'sqlparser' crates/shiba-ingress/Cargo.toml crates/shiba-runtime/Cargo.toml; then
  echo "Runtime/Ingress must not depend on the SQL parser" >&2
  exit 1
fi

cargo test --release -p shiba-sql-frontend --test performance -- \
  --ignored --test-threads=1 --nocapture

target_key="$(rustc -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')"
database_url="host=$SHIBA_TEST_SOCKET port=$SHIBA_TEST_PORT dbname=postgres user=$SHIBA_TEST_USER connect_timeout=5"

SHIBA_M15_SQL_PERFORMANCE_DATABASE_URL="$database_url" \
PQ_LIB_DIR="$($SHIBA_TEST_PG_CONFIG --libdir)" \
PG_CONFIG="$SHIBA_TEST_PG_CONFIG" \
env "PG_CONFIG_${target_key}=$SHIBA_TEST_PG_CONFIG" \
  cargo test --release -p shiba-ingress --test m15_sql_performance -- \
    --ignored --test-threads=1 --nocapture

echo "M15 SQL performance passed for PostgreSQL $SHIBA_TEST_PG_MAJOR ($SHIBA_TEST_PG_CONFIG)"
