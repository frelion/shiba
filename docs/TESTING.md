# Testing strategy

## Phase-1 gates

Run from `/Users/zzhang/Documents/Shiba-v2-cleanroom`:

```bash
PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config ./scripts/test-l0.sh
PG_CONFIG=/opt/homebrew/opt/postgresql@18/bin/pg_config ./scripts/test-l0.sh
./scripts/test-empty-install.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-empty-install.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
```

`test-l0.sh` selects the matching `pg17` or `pg18` feature, then runs formatting,
Protocol/Catalog checks and tests, clippy with warnings denied, `git diff
--check`, forbidden-surface scans, the canonical fixture byte/digest check, and
the deferred-evidence manifest completeness check.

Each `test-empty-install.sh` invocation creates an isolated empty cluster. It
proves complete rollback after a forced failure in the `CREATE EXTENSION`
transaction; successful install and the single `1|1` version result; public API
access and private table denial for an ordinary role; and clean extension drop.
This re-proves only the Phase-1 installation rollback boundary, not later
component recovery.

## Evidence handling

Fixtures in `tests/fixtures` must be data, not copied executable implementation.
The canonical Protocol vector has the adjacent
`tests/fixtures/protocol/canonical-v1.provenance.md`, including source, legacy
commit, old command, and clean-room command. It has been re-proved. The
`tests/fixtures/pg/deferred-evidence.json` file is only an A-class evidence
index: rollback is `partially reproved` for extension installation and every
other PG scenario remains `deferred`. Differential tests use the legacy
repository solely as an oracle and must never link it, load its SQL, or share a
catalog authority.
