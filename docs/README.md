# Shiba V2 clean-room

This worktree starts a new V2 on branch `codex/v2-cleanroom`. It is not a refactor
of the previous V1 or V2 code. Phase 1 established Protocol and Catalog; M2 adds
one transactional INSERT/count vertical path.

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

**Not proved.** M2 has no Compiler, pgoutput Source Ingress, EffectStream,
Registration, worker, UPDATE/DELETE, streaming transaction, DDL invalidation,
concurrent source, external effect, publication, slot, compatibility path,
alias, fallback, or dual write.

Read [architecture](ARCHITECTURE.md), [protocol contract](PROTOCOL_CONTRACT.md),
[catalog contract](CATALOG_CONTRACT.md), and the [reuse manifest](contracts/REUSE_MANIFEST.md)
before extending the workspace.
