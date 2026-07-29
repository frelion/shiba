# Test strategy

Shiba is tested against a real PostgreSQL 17 server, not a mock database.

## Layers

1. **Rust unit tests** exercise query-shape validation, logical-plan
   serialization, deterministic `PhysicalDagPlan` compilation and round trips,
   Stage fusion/fanout/materialization decisions, tuple mapping, LSN
   formatting, and every supported or malformed `pgoutput` message shape.
   Protocol tests truncate valid messages at every byte boundary and verify
   that malformed lengths cannot panic.
2. **pgrx integration tests** run against a real temporary PostgreSQL cluster.
   They cover catalog constraints, routing idempotency, DDL-hook resolution,
   source validation, registration, and lifecycle cleanup.
3. **Installation smoke test** packages and installs the built extension into PostgreSQL 17's real `pkglibdir` and `sharedir`, creates an isolated cluster, configures `session_preload_libraries`, and executes SQL through `psql`.
4. **Asynchronous acceptance test** activates Shiba, declares multiple result
   DAGs, waits for logical WAL application, and asserts exact deltas, rollback
   atomicity, metadata, progress, publication membership, single-Runtime
   process lifecycle, shared payload fanout, persisted physical plans, typed
   UNLOGGED Stage creation/cleanup/DROP, `shiba.explain_physical`, and protected
   result tables.
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
- exactly one `shiba runtime` starts for an active database, independent of
  result-DAG count; no `shiba router`, `shiba executor`, or per-DAG worker is
  present in `pg_stat_activity`;
- the one Runtime schedules multiple `DagRuntime` instances round-robin at
  source-commit boundaries: after the current non-preemptible commit finishes,
  a continuously backlogged DAG cannot monopolize every subsequent commit
  slot; the test does not claim time slicing within one large commit;
- one source transaction stores each decoded delta exactly once in
  `change_log`, while `dag_inbox` stores exactly one row per affected DAG and
  commit, with no payload column;
- registration persists exactly one valid `PhysicalDagPlan` generation per
  DAG; Runtime execution loads that generation instead of compiling per
  commit;
- Join plans mark their input delta `statement_materialized` and create exactly
  one typed UNLOGGED `join_delta` relation; the catalog OID resolves to an
  UNLOGGED table in `shiba_internal`;
- `shiba.explain_physical(regclass)` reports the physical version, `plan_id`,
  Stage graph, storage choices, and typed schema/index metadata;
- normal commit execution contains no Stage DDL and creates no temporary
  table; typed UNLOGGED Stage DDL exists only in registration/finalization;
- after successful and deliberately rolled-back Join apply and Runtime reload,
  `join_delta` is empty and the durable result still matches PostgreSQL
  recomputation; the generic failpoint suite separately verifies that logged
  inbox/change-log work survives Runtime and server crashes;
- DROP serializes with apply through the DAG advisory lock, drops Stage
  relations in `stage_id` order, and removes physical-plan metadata;
- acknowledging the last DAG reference permits bounded GC to remove the shared
  payload, while a paused or quarantined DAG reference retains it;
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
- supported built-in type encoding survives initial backfill and WAL
  application while locale-sensitive/custom identity types are rejected,
  including boolean predicates, bigint boundary values, and a group SUM wider
  than `i64`;
- HAVING keeps hidden aggregate state and exposes/retracts groups at its
  threshold;
- no row-data capture trigger exists; exactly one wakeup trigger, one metadata row, publication membership, and an advanced commit-LSN watermark are registered;
- the legacy synchronous delta function is absent.
- after a PostgreSQL restart, no dynamic Runtime is assumed to survive:
  the next registered-source statement trigger or an explicit
  `SELECT shiba.activate()` restores exactly one Runtime and drains
  WAL/change-log/inbox state retained durably;
- Runtime crashes at apply-before-ack and route-before-slot-advance are
  recovered without duplicate process ownership, duplicate payload, lost
  references, or double application;
- a DAG load/apply error rolls back the current commit, retains its inbox,
  shared payload, and progress position, marks only that DAG failed, and does
  not stop healthy DAGs; `activate()` must not clear the quarantine, and an
  explicit repair/clear/retry path must resume it;
- while a deliberately long apply is running, the same Runtime PID remains the
  sole owner and newly committed source work is not routed until that apply
  completes; this records the single-process head-of-line behavior;
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

Single-Runtime architecture acceptance must additionally prove the
process/runtime boundary: result-DAG count does not increase PostgreSQL
background-worker count; Router and Executor processes do not exist; payload
fanout is shared; round-robin preserves per-DAG commit order; poison input
retains replay data; UNLOGGED Stage loss is rebuilt from logged
inbox/change-log input; and DROP allows unreferenced payload GC. Static
architecture checks additionally reject apply-time `CREATE UNLOGGED`, TEMP
tables, and per-data-row loops. The shell gate cannot directly prove the
absence of a Rust transaction-sized `Vec`; that requires a Rust-level
bounded-memory test or instrumentation hook.

Normal correctness and performance runs fail on every unexpected PostgreSQL
`WARNING`, `ERROR`, `FATAL`, or `PANIC`. Deterministic failpoint runs are the
only exception: they allow only the exact expected crash record for the armed
failpoint and still fail on every other warning-or-higher log entry.

## Performance verification

Run the complete, unfiltered matrix for performance acceptance:

```bash
SHIBA_MATRIX_RUN_ID=<candidate-id> ./scripts/performance-matrix.py
```

The complete matrix currently contains 27 operator scenarios. Its formal
defaults are 20,000 initial rows, 100 groups, 20 mutations, 40 visibility
probes per scenario, five-second query phases with four clients, a 5,000-row
large transaction, and three randomized repetitions. The output under
`performance/matrix-results/<candidate-id>/` includes the exact environment
and workload copies, operator coverage, raw and summarized metrics, latency
samples, plans, PostgreSQL log, Runtime/process resource samples, and
working-tree snapshot.

For a valid regression decision, compare against a retained run made on the
same machine and power mode with the same PostgreSQL/Rust/pgrx versions,
database configuration, workload checksums, scenario set, sizes, seed, and
repetition count. Compare per-scenario medians and dispersion for source-write
and apply/drain throughput, visibility p50/p95/p99, result-query throughput,
PostgreSQL and Runtime CPU/RSS, WAL/I/O, and large-transaction peak RSS. Also
require exact correctness, a drained inbox, one Runtime PID, no legacy workers,
and no unexpected warning-or-higher log entry.

Reduced or `SHIBA_MATRIX_SCENARIOS`-filtered runs are useful for diagnosis, but
they are not formal regression evidence. Historical per-DAG-worker and
Router/Executor results are topology history, not a matched Single Runtime
baseline. See [the benchmark protocol](../performance/README.md) for artifact
details.

On Apple Silicon macOS, `.cargo/config.toml` enables the standard dynamic-symbol lookup mode used by PostgreSQL loadable modules. This lets the module resolve PostgreSQL server symbols when the backend loads it.
