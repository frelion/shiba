#!/usr/bin/env bash

set -euo pipefail

if test -n "${PG_CONFIG:-}"; then
  printf '%s\n' "${PG_CONFIG}"
  exit 0
fi

if command -v pg_config >/dev/null 2>&1; then
  command -v pg_config
  exit 0
fi

for candidate in \
  /opt/homebrew/opt/postgresql@18/bin/pg_config \
  /opt/homebrew/opt/postgresql@17/bin/pg_config \
  /usr/lib/postgresql/18/bin/pg_config \
  /usr/lib/postgresql/17/bin/pg_config; do
  if test -x "${candidate}"; then
    printf '%s\n' "${candidate}"
    exit 0
  fi
done

printf '%s\n' 'PostgreSQL 17 or 18 pg_config was not found; set PG_CONFIG' >&2
exit 1
