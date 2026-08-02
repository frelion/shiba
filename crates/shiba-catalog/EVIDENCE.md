# Catalog Phase-1 evidence boundary

This crate is a clean-room implementation. It does not copy or adapt prior SQL
or Rust. The legacy repository is used only as an external contract and test
oracle.

## Adopted A-class semantics

- Source: `/Users/zzhang/Documents/Shiba/docs/TESTING.md`, section
  `V2 live catalog bindings`; old gate:
  `scripts/test-catalog-bindings-v2.sh`.
  Adopted semantic: catalog facts are bound to live PostgreSQL catalog identity
  and must fail closed when their exact shape drifts. Phase 1 applies only the
  narrower consequence that its installation identity has one constrained,
  database-local authority row. No physical binding inspector is implemented.
- Source: `/Users/zzhang/Documents/Shiba/docs/v2/source-identity-registry.md`;
  old gates: `scripts/test-source-identity-pgoutput-v2.sh` and
  `scripts/test-source-identity-registry-index-v2.sh`.
  Adopted semantic: durable identity is database-local, exact, and independent
  of names, payload hashes, physical tuple locations, or scan row numbers.
  Phase 1 represents only installation identity with a fixed singleton key and
  positive frozen versions. It does not create a Source or registry authority.

The recorded legacy commands were:

```bash
PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config \
  ./scripts/test-catalog-bindings-v2.sh
PG_CONFIG=/opt/homebrew/opt/postgresql@18/bin/pg_config \
  ./scripts/test-catalog-bindings-v2.sh

./scripts/test-source-identity-pgoutput-v2.sh \
  /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-source-identity-pgoutput-v2.sh \
  /opt/homebrew/opt/postgresql@18/bin/pg_config

./scripts/test-source-identity-registry-index-v2.sh \
  /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-source-identity-registry-index-v2.sh \
  /opt/homebrew/opt/postgresql@18/bin/pg_config
```

## Rejected evidence and implementation

- `sql/00_catalog.sql` and every legacy catalog authority are C-class for this
  clean-room line: they were not consulted as implementation and are not
  copied, rewritten, aliased, or made authoritative.
- Legacy Rust catalog inspectors and binding structs are not migrated. Their
  live-binding gate is evidence of behavior, not a source template.
- Random install UUIDs, timestamps, database OIDs, and duplicate version
  mirrors are rejected because they add no Phase-1 authority semantics.

## Explicitly deferred or unproved

- Exact relation/index/column/constraint binding and drift detection.
- Source and replica-identity admission, registry lifecycle, and row identity.
- Compiler, Source Ingress/Apply, EffectStream, Runtime, operators, Result Sink,
  registration, recovery execution, and any compatibility behavior.
- This crate's isolated PostgreSQL 17/18 checks and empty-database installation
  gates prove only Phase-1 compilation and transactional installation. They do
  not elevate any deferred runtime claim.

## M2 clean-room evidence

M2 adds four independently written tables and no legacy SQL or Rust. The
PG17/18 `scripts/test-m2.sh` gate proves their single-transaction INSERT/count
path, replay, rollback, backend-termination recovery, and ordinary-role
permissions. This does not reclassify legacy runtime or SQL workflows: they
remain prohibited C-class material.
