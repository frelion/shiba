# Test strategy

Shiba is tested against a real PostgreSQL 17 server, not a mock database.

## Layers

1. **Rust unit tests** exhaustively exercise the filter grammar, logical-plan
   serialization, tuple mapping, LSN formatting, and every supported or
   malformed `pgoutput` message shape. Protocol tests truncate valid messages
   at every byte boundary and verify that malformed lengths cannot panic.
2. **pgrx integration tests** run against a real temporary PostgreSQL cluster.
   They cover catalog constraints, routing idempotency, DDL-hook resolution,
   source validation, registration, and lifecycle cleanup.
3. **Installation smoke test** packages and installs the built extension into PostgreSQL 17's real `pkglibdir` and `sharedir`, creates an isolated cluster, configures `session_preload_libraries`, and executes SQL through `psql`.
4. **Asynchronous acceptance test** activates Shiba, declares multiple result
   DAGs, waits for logical WAL application, and asserts exact deltas, rollback
   atomicity, metadata, progress, publication membership, worker lifecycle, and
   protected result tables.
5. **Deterministic differential tests** apply reproducible DML sequences and,
   after every commit, compare each Shiba result with PostgreSQL's fresh
   execution of the defining query using `EXCEPT ALL` in both directions. This
   preserves duplicate-row semantics and covers aggregate, HAVING,
   `COUNT(DISTINCT)`, top-level DISTINCT, TopN/OFFSET, windows, all supported
   joins, cross-input predicates, and semi/anti/null-aware anti joins.
6. **Concurrency and recovery tests** exercise concurrent mixed DML, large
   commit and rollback batches, durable inbox replay, persistent-slot replay
   across an immediate PostgreSQL restart, and result DROP racing with writers.
   Every blocking operation and poll has a hard timeout.

## Required scenarios

- `CREATE EXTENSION shiba` succeeds on PostgreSQL 17.
- one Router and one executor per active result DAG start; the harness asserts
  them through `pg_stat_activity`;
- normal PostgreSQL materialized views remain unchanged and do not refresh automatically;
- a Shiba declaration backfills existing source rows into a Shiba result table;
- inserts, updates, and deletes are decoded from a real `pgoutput` logical slot, then asynchronously apply correct `COUNT` and `SUM` deltas;
- rolled-back source writes leave the Shiba result unchanged;
- inner, left, right, and full joins preserve bag multiplicity and correctly
  compensate NULL-extended rows;
- cross-input predicates and `COUNT(DISTINCT)` update on insert, delete, and
  threshold-crossing update;
- `EXISTS`, `NOT EXISTS`, `IN`, and null-aware `NOT IN` follow PostgreSQL NULL
  semantics;
- window ranks and aggregate frames update only affected partitions;
- default, `ROWS`, `RANGE`, and `GROUPS` window frames, both sort directions,
  and peer groups remain equal to PostgreSQL recomputation after random DML;
- top-level DISTINCT and ordered LIMIT/OFFSET update from durable operator
  state;
- fixed-width type encoding survives initial backfill and WAL application,
  including boolean predicates, bigint boundary values, and a group SUM wider
  than `i64`;
- HAVING keeps hidden aggregate state and exposes/retracts groups at its
  threshold;
- no row-data capture trigger exists; exactly one wakeup trigger, one metadata row, publication membership, and an advanced commit-LSN watermark are registered;
- the legacy synchronous delta function is absent.
- after a PostgreSQL restart, the first source write starts replacement workers
  and drains WAL retained by the persistent logical slot;
- dropping a result while source DML is active completes without source/result
  lock inversion;
- initial-state and pgoutput encodings agree for boolean-bearing rows;
- source DROP and active-slot extension DROP are rejected, while qualified and
  search-path-resolved result DROP both quiesce their DAG;
- accepted SQL cannot silently lose aggregate/window FILTER, inner-subquery
  predicates, self-join input identity, mismatched HAVING inputs, or
  `FETCH ... WITH TIES`; each unsupported shape is rejected at registration.

## Running locally

Run the complete correctness gate before and after execution-engine or state
layout changes:

```bash
./scripts/test-all.sh
```

For a faster edit/test cycle, individual layers remain directly runnable:

```bash
cargo test --lib
./scripts/test-e2e.sh
./scripts/test-differential-single.sh
./scripts/test-join-differential.sh
./scripts/test-concurrency-recovery.sh
```

The differential single-source test defaults to seed `20260725` and 120
committed/rolled-back rounds. Failures print the seed, round, operation, source
rows, result progress, PostgreSQL log tail, and a replay SQL log. Reproduce or
expand it with:

```bash
SHIBA_DIFF_SEED=20260725 SHIBA_DIFF_ROUNDS=500 \
  ./scripts/test-differential-single.sh
```

Every server-level script uses a freshly created temporary data directory and
Unix socket, so it neither uses nor alters a developer's normal PostgreSQL
database cluster. The scripts only install Shiba's extension artifacts into
the selected PostgreSQL 17 installation.

On Apple Silicon macOS, `.cargo/config.toml` enables the standard dynamic-symbol lookup mode used by PostgreSQL loadable modules. This lets the module resolve PostgreSQL server symbols when the backend loads it.
