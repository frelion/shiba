# Architecture

> The current ingress and process contract is
> [REPLICATION-INGRESS-DESIGN.md](REPLICATION-INGRESS-DESIGN.md). Historical
> sections below that describe `routed_transactions` or slot peeking are
> superseded; the implementation has one durable ingress model and no legacy
> fallback.

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
   graph, versioned physical plan, pre-created Stage relations, and initial
   state.
2. **Incremental maintenance.** One database-scoped Runtime routes committed
   source changes into a shared durable change log, creates lightweight DAG
   work references, and schedules logical `DagRuntime` instances. One source
   commit is applied atomically before its DAG reference is acknowledged.

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
  -> versioned PhysicalDagPlan and Stage catalog
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
triggers, ownership, permissions, and background-process activation in one
order.

## Plans and execution authority

The Rust logical layer is split by responsibility:

- `src/logical/model.rs` — stable plan and delta wire types.
- `src/logical/compile.rs` — plan builder and compiler.
- `src/logical/validate.rs` — closed plan grammar, typed operator configs, and
  the execution descriptor.
- `src/logical/physical.rs` — deterministic Stage fusion, consumer analysis,
  and storage decisions.
- `src/logical/persist.rs` — registration-time typed Stage schema lowering and
  physical-plan persistence.
- `src/logical/runtime.rs` — the PostgreSQL/SPI bridge used by a logical
  `DagRuntime`.

The persisted `LogicalPlan` is the semantic source used at registration.
Registration validates it, derives an `ExecutionDescriptor`, deterministically
compiles a versioned `PhysicalDagPlan`, and stores that plan in
`shiba_internal.physical_plans`. The Runtime loads the persisted physical plan
by `plan_id`, validates its result identity, topology, Stage storage decisions,
source ports, and Join subtype, and then uses its encoded descriptor for
dispatch. Runtime execution does not recompile the logical graph per source
commit. SQL catalog rows provide physical state and configuration; they are
checked against the physical plan and do not select a different route.
V1 is deliberately a closed execution contract: the descriptor selects one
validated kernel program, and the Runtime verifies that program's complete
Stage IDs, relation shape, and storage. It is not yet a generic interpreter
that walks arbitrary future `PhysicalDagPlan` kernels.

The physical plan divides the graph into Stages. A Stage is an execution and
reuse boundary, not a PostgreSQL worker:

- `inline` fuses a relation into its consumer;
- `statement_materialized` uses a PostgreSQL `MATERIALIZED` CTE when the
  relation is reused inside one statement;
- `unlogged` names a typed relation that crosses SQL-statement boundaries.

Only UNLOGGED Stage relations have rows in
`shiba_internal.physical_stages`. They are created during DAG registration,
identified by catalog OID, and never created by the apply path.

## Process resources and logical runtimes

Shiba deliberately separates PostgreSQL process resources from DAG execution
state:

```text
PostgreSQL postmaster
  -> shiba runtime (one real background worker per active database)
       -> bounded Router phase
       -> round-robin Scheduler
            -> DagRuntime(result A)
            -> DagRuntime(result B)
            -> DagRuntime(result C)
       -> bounded GC phase
```

Adding a result DAG adds catalog rows, an inbox reference, operator state, and
cached plan metadata. It does not allocate a PostgreSQL process, OS thread,
connection, or dedicated CPU. The former `shiba.executor_count` setting and
Executor pool are not part of this topology.

The Runtime is a single PostgreSQL backend, so all SPI execution is serial.
Runnable DAGs are selected round-robin at source-commit boundaries. Each
selected source commit is one atomic, non-preemptible PostgreSQL transaction.
Every source statement schedules a transaction callback that sets the
Runtime's PostgreSQL latch only after commit, without copying row payload. The
25 ms idle poll remains a lost-wakeup and recovery fallback rather than the
normal visibility path.
The scheduler yields between commits but cannot time-slice a large commit after
it starts. A long apply therefore delays WAL routing, other DAGs, and GC. The
Runtime records route lag, inbox lag, and apply duration separately so this
head-of-line blocking remains observable.

## Incremental path

```text
logical replication slot
  -> PostgreSQL walsender / pgoutput protocol v2
  -> bounded Runtime ingress transactions
  -> ingress_transactions + shared change_log rows
  -> bounded routing_tasks fan-out
  -> dag_inbox transaction references
  -> round-robin scheduler
  -> per-result DagRuntime
  -> persisted PhysicalDagPlan
  -> inline / statement-materialized / UNLOGGED Stages
  -> operator state and result sink
  -> view_progress and inbox acknowledgement
  -> confirmed-LSN-fenced ingress GC
```

`src/worker.rs` contains one cooperative process loop with bounded phases:

1. persist a bounded replication batch;
2. route a bounded subscriber page;
3. apply one source transaction for one ready DAG;
4. rotate the round-robin cursor;
5. garbage-collect a bounded number of safe transactions;
6. service signals and wait on the latch when idle.

The Runtime owns a replication client; PostgreSQL's walsender owns the logical
decoding context. Replication socket I/O never occurs inside an SPI
transaction. Stable event identities make a crash after ingress commit but
before feedback a harmless replay. Transaction streaming is disabled:
PostgreSQL spills decoding state when needed and emits only committed,
savepoint-filtered changes, while Shiba consumes that output in bounded
CopyData batches.

The apply phase chooses a ready DAG, locks and revalidates its oldest inbox
reference, and invokes the logical runtime with
`(result_oid, ingress_txn_id, commit_lsn)`. Operator SQL reads ordered rows
from `effective_change_log`; Rust must not
collect the transaction payload into a `Vec`, construct a JSON array, or copy
it back through SPI. State, result, progress, and deletion of the one inbox
reference commit together.

The current Join program is two set-oriented statements. The first reads the
ordered commit from `change_log`, materializes its input-delta relation in the
statement, validates multiplicity prefixes, directly derives exact versioned
pair and match-presence differences, writes only net rows to the typed
UNLOGGED `join_delta` Stage, and updates the durable arrangements. The second consumes
`join_delta` to update downstream distinct/aggregate state and the sink. The
Stage is necessary because the exact Join delta has multiple consumers across
that statement boundary; it does not change the transaction boundary.

GC may delete an ingress transaction only after routing is complete, no inbox
references it, retention elapsed, and `replay_safe_lsn` reached its end LSN.
`replay_safe_lsn` comes from the slot's actual confirmed flush position, not
from feedback intent.

The SQL execution layer is split into `sql/20_operator_filters.sql` through
`sql/26_physical_stages.sql`: common filters, Aggregate, unary batch kernels,
Join batch context, the dispatcher, ordered compatibility kernels, and Stage
resolution/cleanup/observation. A source commit crosses Rust/SPI once and
advances progress once.

Current batch strategies are:

- Aggregate: combine contributions per group; large batches also combine
  `COUNT(DISTINCT)` transitions per `(group, value)`.
- Distinct: combine projected-key multiplicities and update the sink only at a
  zero/nonzero boundary.
- TopN: combine row multiplicities and rebuild the bounded sink once per
  commit. V1 still scans and sorts the DAG's complete retained multiset; its
  `NOT MATERIALIZED` pipeline removes an extra tuplestore but is not an
  incremental indexed TopN algorithm.
- Window: combine row changes and rebuild each affected partition once.
- Join: compare transaction-entry and final arrangements over affected keys,
  materialize exact `new output - old output` once, and consume that delta for
  outer, semi, anti, null-aware anti, distinct, aggregate, and sink changes.

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
8. Exactly one `shiba runtime` PID owns routing and application for an active
   database; no Router, Executor, or per-DAG worker process exists.
9. One decoded delta has one `change_log` payload row regardless of DAG fanout;
   inbox fanout runs once per complete source commit, not once per payload row,
   and one DAG has at most one inbox row per source transaction.
10. Round-robin scheduling preserves per-DAG commit order. DAGs do not execute
    concurrently, and a long apply may block every Runtime phase.
11. `change_log` data is collected only after its final inbox reference is
    acknowledged or removed by DAG DROP.
12. Persisted physical Stage contents never become recovery authority.
    UNLOGGED Stage loss must be recoverable from logged `dag_inbox` and
    `change_log` data.

## Stage storage, lifecycle, and locks

All durable authority remains in ordinary logged PostgreSQL relations:
registration metadata and physical plans, `ingress_replay_state`,
`ingress_transactions`, `change_log`, `routing_tasks`, `dag_inbox`, operator
arrangements and state, result rows, and `view_progress`. The UNLOGGED
`join_delta` relation is only a typed,
commit-scoped cache of derived rows. It may avoid WAL for an intermediate that
can be recomputed, but it is not a substitute for durable state.

The Stage lifecycle is explicit:

1. registration persists one physical plan and creates its typed UNLOGGED
   relations and declared indexes in stable `stage_id` order;
2. Runtime plan load clears those relations before accepting work;
3. apply fills the empty Join Stage, then transactionally consumes and removes
   its rows before progress and inbox acknowledgement;
4. an empty Stage that grows past 64 MiB is occasionally truncated under the
   DAG lock, bounding dead-tuple storage without per-commit TRUNCATE;
5. result DROP takes the DAG lock, drops Stage relations in `stage_id` order,
   and then deletes physical-plan metadata.

PostgreSQL truncates UNLOGGED relations after crash recovery. The next Runtime
load also clears them defensively. If apply did not commit, its logged state,
result, progress, and inbox acknowledgement roll back together; the retained
inbox reference causes the source commit to be replayed from `change_log`, so
the Stage is rebuilt. There is no per-commit `CREATE`, `DROP`, or `ALTER`, and
normal execution creates no `pg_temp` table.

The canonical apply lock order is:

```text
DAG advisory transaction lock
-> dag_runtime_state row
-> earliest dag_inbox row
-> physical Stage relations in stage_id order
-> durable operator-state rows
-> result rows
```

Runtime-load cleanup and lifecycle DROP take the same DAG advisory lock before
touching Stage relations, and always visit those relations in `stage_id`
order. This serializes apply with lifecycle operations and keeps multi-Stage
locking deterministic. Runtime-load `TRUNCATE` takes PostgreSQL's
`ACCESS EXCLUSIVE` lock on each private Stage relation; ordinary apply uses
row DML and does not truncate on the hot path.

The Runtime session prepares the two fixed Join statements once per
`(result_oid, plan_id)` and reuses PostgreSQL's prepared plans for later source
commits. The dedicated Runtime session forces generic prepared plans because
the physical SQL shape is fixed and only OID/LSN/value parameters vary. A
plan-generation change gets distinct prepared-statement identities; cache
eviction and generation replacement explicitly deallocate the obsolete pair.
Shared-state relations are analyzed only when no statistics exist. A Join
Stage containing at least 1,024 rows is analyzed before consume so the generic
consume plan is rebuilt from the real Stage cardinality, then analyzed empty
after consume to reset the estimate for a following small commit.

The supported observation surface is:

```sql
SELECT shiba.explain_physical('shiba.sales_by_product');
```

Its `plan` field contains the full Stage graph and storage decisions; its
top-level `stages` array contains relation identity plus schema/index metadata
only for UNLOGGED Stages. Stage contents are internal scratch data and are not
a public consistency surface.

## Process recovery and DAG failure isolation

The Runtime is a dynamic PostgreSQL background worker. PostgreSQL restarts it
after an abnormal exit while the postmaster remains up. The persistent
launch-generation/XID handshake and singleton ownership row prevent concurrent
or uncommitted `activate()` calls from creating duplicate owners. A process
that loses lifecycle ownership exits.

Dynamic background-worker registrations do not survive a postmaster restart.
The durable catalog, logical slot, inbox, and progress state do survive. After
restart, the next statement on a registered source table runs the statement
trigger that restores the Runtime; alternatively an operator can run
`SELECT shiba.activate()` explicitly. Until either activation path occurs,
asynchronous maintenance remains paused.

A DAG execution failure is isolated inside the Runtime loop. A plan load or
apply failure rolls back the current source-commit transaction, leaves its
inbox reference and shared payload durable, records the error, and marks only
that DAG failed
(quarantined). Other runnable DAGs continue to be scheduled. `shiba.activate()`
does not clear a failed DAG or retry poison input automatically: an operator
must repair the cause and explicitly clear/retry that DAG. This preserves
evidence and prevents one bad DAG from repeatedly terminating or monopolizing
the Runtime.

The `pg_test`-only recovery failpoints exercise the two critical crash windows:
Runtime apply-before-ack and Runtime route-before-slot-advance. They are
compiled out of production builds.

## Adding an operator

Keep changes moving through the same boundaries:

1. Add the analyzed shape and a closed validated spec in
   `query_analysis.rs`.
2. Add the logical `OperatorKind`, typed config, compiler shape, and exact
   validation grammar.
3. Add deterministic physical Stage lowering, including a typed schema only
   if output must cross SQL statements.
4. Add registration metadata, Stage lifecycle, and initial-state construction.
5. Add a batch kernel with explicit state invariants. Preserve source-commit
   order wherever visibility depends on first/last transitions.
6. Route it only through the persisted physical plan; catalog strings must not
   become a second dispatcher.
7. Add plan-shape tests, batch-vs-reference tests, E2E SQL, transaction and
   restart recovery coverage, and performance-matrix operator coverage.

Run the complete correctness gates with:

```bash
./scripts/test-all.sh
```

Performance acceptance uses the unfiltered formal matrix in
`scripts/performance-matrix.py`; a filtered run is useful for diagnosis but is
not final evidence. A refactor is accepted only after the full matrix is run
with its formal parameters and compared with the retained baseline under a
matched environment; merely recording that the command completed is not a
performance result.
