# Test strategy

Shiba is tested against a real PostgreSQL 17 server, not a mock database.

## Layers

1. **pgrx integration tests** run through `cargo pgrx test pg17` against a real temporary PostgreSQL cluster.
2. **Installation smoke test** packages and installs the built extension into PostgreSQL 17's real `pkglibdir` and `sharedir`, creates an isolated cluster, configures `session_preload_libraries`, and executes SQL through `psql`.
3. **Asynchronous acceptance test** activates Shiba, declares multiple result
   DAGs, waits for logical WAL application, and asserts exact deltas, rollback
   atomicity, metadata, progress, publication membership, worker lifecycle, and
   protected result tables.

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
- top-level DISTINCT and ordered LIMIT/OFFSET update from durable operator
  state;
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

```bash
cargo pgrx test pg17
./scripts/test-e2e.sh
```

The end-to-end script uses a freshly created temporary data directory and port, so it neither uses nor alters a developer's normal PostgreSQL database cluster. It only installs Shiba's extension artifacts into the selected PostgreSQL 17 installation.

On Apple Silicon macOS, `.cargo/config.toml` enables the standard dynamic-symbol lookup mode used by PostgreSQL loadable modules. This lets the module resolve PostgreSQL server symbols when the backend loads it.
