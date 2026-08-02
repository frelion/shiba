# Shiba V2 clean-room

This worktree starts a new V2 on branch `codex/v2-cleanroom`. It is not a refactor
of the previous V1 or V2 code. Phase 1 established Protocol and Catalog; M2 adds
one transactional INSERT/count vertical path; M3.1 replaces its synthetic input
with a strict decoder fed by live PostgreSQL `pgoutput` version 1. M3.2 proves
safe slot replay across the post-result/pre-ack crash window. M4.1 adds a fixed
nullable `int8` payload while retaining the stable non-null `int8` row key.
M4.2 admits zero-column INSERT tuples without inventing a row identity.
M4.3 adds a fixed two-`int8` composite row identity. M4.4 admits a
single-key UPDATE that changes only the nullable payload.

## Scope decision

**Facts.** The legacy repository at `/Users/zzhang/Documents/Shiba` remains an
oracle and evidence archive. PostgreSQL 17 and 18 are supported test targets.

**Decisions.** Phase 1 establishes a single database-local catalog authority,
protocol value contracts, static L0 gates, and empty-cluster installation
checks. `shiba_internal` owns private state; `shiba` exposes only a read-only
SQL surface. A database may contain many schemas; the catalog
authority is per installed database, not per application schema or client.

M2 accepts one test-driven, ingress-independent committed `SourceTransaction`.
One runtime-owned PostgreSQL transaction atomically applies INSERT facts,
advances a deterministic count, publishes `shiba.count_result`, and records its
continuation last. Exact replay is a no-op.

M3.1 decodes a complete live `BEGIN → RELATION → INSERT+ → COMMIT` transaction
for one admitted `int8` relation before invoking the unchanged M2 processor.
Decode failures therefore cannot expose Apply, result, or continuation state.

M4.4 treats the existing Apply row as current source state. An unchanged-key
UPDATE mutates its payload in the same processor transaction, leaves count at
the number of inserted rows, and advances continuation only with that mutation.

**Not proved.** M4.4 has no production replication transport/slot lifecycle,
DELETE, key-changing UPDATE, replica-identity changes, TOAST, streaming
transaction, DDL invalidation, concurrent source, external effect,
compatibility path, alias, fallback, or dual write.

Read [architecture](ARCHITECTURE.md), [protocol contract](PROTOCOL_CONTRACT.md),
[catalog contract](CATALOG_CONTRACT.md), and the [reuse manifest](contracts/REUSE_MANIFEST.md)
before extending the workspace. Ingress work must also follow the
[pgoutput contract](PGOUTPUT_CONTRACT.md).
Nullable tuple work is bounded by the [tuple contract](TUPLE_CONTRACT.md).
