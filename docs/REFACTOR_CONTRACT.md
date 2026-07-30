# Refactor contract

This document is the invariant list for structural cleanup. A refactor may
change module names, file placement, and internal implementation details, but
it must preserve these externally observable contracts.

## Extension surface

- The extension remains version `0.1.0`, non-relocatable, and installed in the
  `shiba` schema.
- The seven extension SQL files remain registered in this order: catalog,
  runtime, ingress, effect stream, introspection, registration, and lifecycle.
- `shiba.version()`, `shiba.activate()`, and `shiba.deactivate()` remain the
  single Rust-owned public entry points for version and lifecycle control.
- Activation still requires the configured replication connection and the
  extension-owned publication, and deactivation still retires the logical slot
  only after generated result relations have been removed.

## Durable state

- Catalog relation names, column meanings, and the one-row continuation
  authority are persistence contracts. Renaming or changing them requires an
  explicit migration, not a source-only refactor.
- Persisted `DataflowPlan` JSON is strict: unknown fields are rejected at every
  plan boundary, and the planner validates stage topology before registration.
- Every resumable operator uses a typed continuation ABI and compare-and-set
  replacement. A stale worker must not delete or overwrite a newer committed
  continuation.
- Phase codes are positive, explicit, and operator-owned. Zero means no
  continuation; unknown or legacy codes are rejected.

## Replication and recovery

- Source changes become visible to downstream dataflows only after the source
  transaction commits. Abort and rollback paths publish no partial effects.
- Streamed PostgreSQL transactions may be interleaved. Each open transaction
  keeps independent state, while the Runtime still forwards one bounded batch
  at a time in deterministic order.
- Row and logical-byte limits are enforced at the ingress boundary. One
  indivisible oversized row may pass; two rows may not exceed the configured
  batch budget.
- Duplicate, disconnect, postmaster-restart, and reconnect recovery must be
  idempotent and must not skip or duplicate committed source changes.

## Refactor boundaries

- `planner` owns query lowering, the typed persisted plan, validation, scalar
  SQL compilation, and loaded plan state.
- `execution` owns the common step protocol and operator state machines.
  `execution::core` is the shared boundary for Runner, continuation, storage,
  and stream primitives.
- `ingress` and `pgoutput` own WAL decoding and bounded source admission;
  `worker` owns the single database-scoped event loop.
- Old `kernel`, `logical`, `query_lowering`, and `scalar_sql` module paths are
  not retained as compatibility aliases. Internal callers use the owning
  layer directly so the directory tree reflects the dependency graph.

## Required verification

The minimum handoff gate is:

```text
./scripts/test-all.sh
```

That command includes the static contract and clean-cut guards, formatting,
Clippy, all Rust/pgrx tests, logical-replication ingress, stateless kernels,
fanout recovery, aggregate/distinct recovery, and window/TopN recovery.
