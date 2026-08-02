# Shiba V2 clean-room, Phase 1

This worktree starts a new V2 on branch `codex/v2-cleanroom`. It is not a refactor
of the previous V1 or V2 code. The only implementation in scope is a minimal
Cargo workspace, the Protocol crate, and the Catalog extension schema.

## Scope decision

**Facts.** The legacy repository at `/Users/zzhang/Documents/Shiba` remains an
oracle and evidence archive. PostgreSQL 17 and 18 are supported test targets.

**Decisions.** Phase 1 establishes a single database-local catalog authority,
protocol value contracts, static L0 gates, and empty-cluster installation
checks. `shiba_internal` owns private catalog state; `shiba` exposes only a
read-only metadata surface. A database may contain many schemas; the catalog
authority is per installed database, not per application schema or client.

**Not proved.** Phase 1 does not implement or claim Compiler, Source Ingress,
Source Apply, EffectStream, Runtime, Registration, an Operator, or Result Sink.
It does not create publications, replication slots, change logs, compatibility
paths, aliases, fallbacks, or dual writes.

Read [architecture](ARCHITECTURE.md), [protocol contract](PROTOCOL_CONTRACT.md),
[catalog contract](CATALOG_CONTRACT.md), and the [reuse manifest](contracts/REUSE_MANIFEST.md)
before extending the workspace.
