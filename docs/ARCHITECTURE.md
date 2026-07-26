# Architecture

This document is a map of Shiba's execution path and its invariants. It is
intended to be useful even if you are new to Rust, PostgreSQL extensions, or
incremental view maintenance.

## One declaration, two phases

A streaming table starts as normal SQL:

```sql
CREATE TABLE shiba.sales_by_product AS
SELECT product_id, count(*) AS rows, sum(amount) AS total
FROM sales
GROUP BY product_id;
```

Shiba handles it in two phases:

1. **Declaration and backfill.** The DDL hook validates the query, PostgreSQL
   creates and fills the result table, and registration persists the operator
   graph and initial state.
2. **Incremental maintenance.** The WAL Router copies committed source changes
   into durable per-result inboxes. A DAG executor applies one source commit
   atomically and then acknowledges it.

The result is asynchronous: a source commit may become visible before its
Shiba result catches up. `view_progress.applied_lsn` records the durable
watermark.

## Declaration path

```text
PostgreSQL analyzed Query
  -> query_tree (unsafe PostgreSQL pointer adapter)
  -> QueryAnalysis (owned, stable wire model)
  -> ValidatedQuery (closed, operator-specific legal states)
  -> SQL registration and initial state
  -> LogicalPlan
  -> validated ExecutionDescriptor
```

The important boundary is between `query_tree` and `query_analysis`:

- `src/query_tree.rs` is the PostgreSQL adapter. Raw pointers and PostgreSQL
  node walking stay here.
- `src/query_analysis.rs` contains ordinary safe Rust data. Validation turns
  an open analysis record into one of the supported query families:
  Aggregate, Join, decorrelated subquery, Window, Distinct, or TopN.
- `src/ddl.rs` requires validation before PostgreSQL executes a Shiba CTAS.
  The validated source OIDs are also used for the pre-backfill locks.

Registration lives in `sql/30_registration.sql`. Operator-specific metadata is
prepared there, while common lifecycle work is centralized in:

- `_prepare_stream_registration`
- `_finalize_stream_registration`

These helpers keep source preparation, activation LSN capture, publications,
triggers, ownership, permissions, and worker activation in one order.

## Plans and execution authority

The Rust logical layer is split by responsibility:

- `src/logical/model.rs` — stable plan and delta wire types.
- `src/logical/compile.rs` — plan builder and compiler.
- `src/logical/validate.rs` — closed plan grammar, typed operator configs, and
  the execution descriptor.
- `src/logical/runtime.rs` — the PostgreSQL/SPI bridge used by a DAG worker.

The persisted `LogicalPlan` is the execution authority. A worker loads it once,
validates its exact topology and config shapes, and derives an
`ExecutionDescriptor` containing the physical pipeline, source ports, and Join
subtype. SQL catalog rows provide physical state and configuration; they are
checked against the descriptor and do not select a different route.

## Incremental path

```text
logical replication slot
  -> WAL Router
  -> routed_transactions checkpoint
  -> dag_inbox rows
  -> per-result DAG executor
  -> operator state and result sink
  -> view_progress and inbox acknowledgement
```

`src/worker.rs` contains the two worker loops:

- The **WAL Router** owns the logical slot. Routing a transaction and recording
  its checkpoint are one database transaction. Slot advancement is a separate
  transaction, so a crash between them causes a harmless replay.
- A **DAG executor** owns no slot. It locks the oldest inbox commit, calls
  `DagRuntime::apply_batch`, and deletes the inbox rows in the same database
  transaction.

The SQL execution layer is split into `sql/20_operator_filters.sql` through
`sql/25_operator_compat.sql`: common filters, Aggregate, unary batch kernels,
Join batch context, the dispatcher, and ordered compatibility kernels. A
source commit crosses Rust/SPI once and advances progress once.

Current batch strategies are:

- Aggregate: combine contributions per group; large batches also combine
  `COUNT(DISTINCT)` transitions per `(group, value)`.
- Distinct: combine projected-key multiplicities and update the sink only at a
  zero/nonzero boundary.
- TopN: combine row multiplicities and rebuild the bounded sink once per
  commit.
- Window: combine row changes and rebuild each affected partition once.
- Join: preserve ordered first/last-match transitions for outer, semi, anti,
  and null-aware anti semantics, while deduplicating sink synchronization to
  once per affected aggregate group.

Ordered-prefix checks matter even for a net-zero batch: an absent-row
retraction followed by an insertion is corruption, not a valid no-op.

## Transaction and recovery invariants

These rules are more important than any individual optimization:

1. A source transaction is never partially visible in a Shiba result.
2. Operator state, result rows, `view_progress`, and inbox acknowledgement
   commit or roll back together.
3. A routed WAL transaction is idempotent by commit LSN.
4. The replication slot advances only after durable routing.
5. A replay must either find the original inbox rows or reproduce exactly the
   same inbox rows.
6. Multiplicity, aggregate counts, and ordered prefixes must never become
   negative.
7. Runtime routing comes from the validated persisted plan.

The `pg_test`-only recovery failpoints exercise the two critical crash windows:
executor apply-before-ack and router route-before-slot-advance. They are
compiled out of production builds.

## Adding an operator

Keep changes moving through the same boundaries:

1. Add the analyzed shape and a closed validated spec in
   `query_analysis.rs`.
2. Add the logical `OperatorKind`, typed config, compiler shape, and exact
   validation grammar.
3. Add registration metadata and initial-state construction.
4. Add a batch kernel with explicit state invariants. Preserve source-commit
   order wherever visibility depends on first/last transitions.
5. Route it only through the `ExecutionDescriptor`; catalog strings must not
   become a second dispatcher.
6. Add plan-shape tests, batch-vs-reference tests, E2E SQL, transaction and
   restart recovery coverage, and performance-matrix operator coverage.

Run the complete correctness gates with:

```bash
./scripts/test-all.sh
```

Performance acceptance uses the unfiltered formal matrix in
`scripts/performance-matrix.py`; a filtered run is useful for diagnosis but is
not final evidence.
