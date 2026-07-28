# Commit-Scoped Relational DAG Execution

Status: implemented execution contract

## Decision

Shiba uses one real PostgreSQL background worker per active database. The
worker owns routing, scheduling, apply, and garbage collection. A DAG runtime
is cached plan metadata and in-process scheduling state; it is not a process,
thread, connection, or CPU allocation.

The only apply unit is:

```text
one DAG x one source commit x one PostgreSQL transaction
```

Apply never combines source commits in one transaction. Throughput batching is
limited to all row changes inside one source commit and to busy-draining
multiple independent apply transactions without sleeping between them.

## Relational delta contract

Every logical edge carries a bag delta relation. A physical edge has the
logical row columns plus:

```text
__weight   bigint  -- signed multiplicity
__sequence bigint  -- source order, retained for prefix validation
```

Source UPDATE remains `-old` followed by `+new`. Before equal rows are
coalesced, stateful kernels validate that no ordered prefix would make an
existing multiplicity negative:

```text
old_multiplicity + minimum_prefix >= 0
```

After validation, kernels may group equal rows, sum `__weight`, and remove
zero-weight rows. Source sequence is correctness metadata, not the main
execution loop.

Rust must not materialize a source transaction payload in a transaction-sized
collection. The durable payload remains in `change_log`; routing normalizes
pgoutput text tuples into typed JSONB once per source relation and commit so
every consuming DAG reuses the same typed payload. Intermediate deltas and
operator state remain PostgreSQL relations.

## Logical and physical plans

`LogicalPlan` remains the persisted semantic source. During registration:

1. validates the complete graph and typed operator configuration;
2. topologically orders nodes and input ports;
3. compiles a deterministic, versioned `PhysicalDagPlan`;
4. fuses adjacent stateless Scan, Filter, Project, and Having nodes where the
   fusion does not cross fan-out or a state boundary;
5. analyzes physical consumers and selects `inline`,
   `statement_materialized`, or `unlogged` storage for every Stage;
6. persists the physical plan and pre-creates every typed UNLOGGED Stage
   relation.

The Runtime loads `PhysicalDagPlan` from
`shiba_internal.physical_plans`, validates it against the result OID, and
caches it by `plan_id`. It does not recompile the logical graph per commit. The
encoded execution descriptor remains a lowering detail inside the physical
plan, not an independent catalog-selected route.

The current V1 Runtime validates a closed descriptor-plus-Stage contract and
dispatches one fixed kernel program. It does not claim to be a generic
topological interpreter for arbitrary future physical kernels. Join's two SQL
statements are prepared once per Runtime session and `plan_id`.

The physical operator interface is:

```text
apply_batch(context, input relations, durable state)
    -> output delta relation
```

An implementation may use a fused SQL statement for several stateless nodes.
State boundaries split a DAG into set-oriented SQL Stages; they do not imply a
process, thread, connection, or CPU allocation. A physical edge is a query
expression or PostgreSQL executor tuple stream by default. The executor must
not use a per-event callback through the complete DAG.

## Operator semantics

- Filter and Project transform a complete input delta relation.
- Aggregate combines contributions per group and emits the difference between
  old and new aggregate rows.
- Distinct compares old and new key multiplicity and emits only zero-boundary
  transitions.
- Join fixes transaction-entry old arrangements, derives final new
  arrangements for affected keys, and emits exact `new output - old output`.
- Outer, semi, anti, and null-aware anti joins derive visibility from old and
  new match cardinality rather than procedural first/last-event callbacks.
- TopN updates its multiset and derives one final bounded result for the
  commit. V1 still reads and sorts the full retained multiset; the streaming
  CTE shape avoids an additional materialization but does not change that
  complexity.
- Window updates its multiset and rebuilds all affected partitions in one
  set-oriented operation.
- Sink applies one final delta relation to the protected result.

## Apply transaction and lock order

The only normal lock order is:

```text
DAG advisory transaction lock
-> dag_runtime_state row
-> earliest dag_inbox row
-> physical Stage relations in stage_id order
-> operator state rows
-> result rows
```

The transaction then:

1. reads the source commit from `change_log`;
2. validates and normalizes source deltas;
3. executes the persisted, validated V1 kernel and Stage program;
4. updates durable operator state and the result;
5. advances `view_progress`;
6. deletes exactly one matching inbox row;
7. commits all effects atomically.

Transient concurrency failures retry only from a new complete transaction.
Deterministic plan or data errors roll back all apply effects, quarantine the
one DAG, and retain its inbox reference. Resource, system, cancellation, and
unknown internal errors are not converted into DAG quarantine.

## Storage

Logged PostgreSQL relations remain the authority for registration metadata,
the physical plan, routing deduplication, source payload, DAG inbox, operator
arrangements and state, result rows, and progress. UNLOGGED storage is never
used for authoritative operator state.

Intermediate delta relations are derived data. The physical compiler chooses:

- `inline` for a fused expression with no reuse requirement;
- `statement_materialized` for a PostgreSQL `MATERIALIZED` CTE reused inside
  one statement;
- `unlogged` only when a typed relation must cross SQL-statement boundaries
  and can be rebuilt from logged input.

The current Join input delta is statement-materialized. The exact
`join_delta` is a pre-created typed UNLOGGED Stage containing commit LSN,
sequence, signed weight, and nullable left/right source-composite rows. The
first Join statement computes exact versioned multiplicity differences
directly and writes only the net delta while updating logged arrangements; the
second uses `DELETE ... RETURNING` to consume and clear that Stage into logged
downstream state and the result. Both statements execute inside the same
DAG/source-commit transaction.

UNLOGGED Stage relations are catalog objects created only during DAG
registration. Apply populates and transactionally consumes them, but performs
no DDL and creates no `pg_temp` table. Runtime load defensively truncates Stage
relations; successful Join apply leaves them empty by consuming their rows;
lifecycle DROP removes them in `stage_id` order before physical-plan metadata.
All Stage access is serialized by the DAG advisory lock once the DAG is
registered and runnable.

`DELETE ... RETURNING` can leave dead heap tuples. After consumption, Shiba
checks relation size and truncates only an empty Stage above 64 MiB while
holding the DAG lock. Ordinary commits therefore remain row-DML-only while
long-lived Stage storage stays bounded.

After a backend or postmaster crash, Stage contents are disposable.
PostgreSQL's UNLOGGED crash handling and Runtime-load cleanup leave them empty.
If the apply transaction did not commit, arrangements, state, result,
progress, and acknowledgement rolled back, while the logged `dag_inbox`
reference and `change_log` payload remain. Replaying that complete transaction
reconstructs the Stage. A Stage is therefore a cache, never a checkpoint.

## Scheduling and performance

The Runtime maintains a round-robin ready-DAG hint queue. Durable inbox rows
remain authoritative and rebuild the queue after restart.

Committed source transactions wake the Runtime through a post-commit latch
callback. No payload travels through the signal; the logical slot and logged
change log remain the only data path. Idle polling is retained for recovery
and missed-wakeup safety.

When work is available, the Runtime busy-drains bounded apply work:

```text
select next DAG
-> run one independent apply transaction
-> rotate DAG
-> repeat until the transaction/time budget expires
```

Router and GC checks are independently bounded and are not repeated after
every commit when their state cannot have changed. A large source commit
remains atomic and non-preemptible, so fairness is guaranteed only at source
commit boundaries.

Performance work must preserve the relational contract. Primary techniques
are:

- eliminate duplicate catalog lookups, locks, and SPI crossings;
- cache validated physical and prepared PostgreSQL plans by DAG generation;
- append source rows once, normalize them set-wise by source relation, and
  fan out inbox references once when routing the complete commit;
- coalesce deltas immediately after prefix validation;
- fuse stateless nodes and split SQL stages only at durable state boundaries;
- reuse derived relations inside a statement with `MATERIALIZED` CTEs;
- materialize across statements only when physical consumer analysis and
  measured plans justify a typed, pre-created UNLOGGED Stage;
- use affected-key, affected-group, and affected-partition relations;
- index durable state for the measured kernel access paths;
- let PostgreSQL spill set operations instead of moving payloads into Rust.

No performance result may be obtained by combining apply commits, weakening
prefix validation, adding a worker pool, moving authoritative state to
UNLOGGED storage, or changing durability settings without a separate explicit
semantic decision.

## Acceptance

Implementation is accepted only when:

- all operator differential, transaction, concurrency, crash-window, restart,
  Stage lifecycle, and architecture tests pass;
- batch output matches the ordered reference implementation for every
  supported operator and join type;
- the formal performance matrix has matched environment and repetition count;
- single-client and four-client end-to-end throughput do not regress from the
  retained baseline;
- large-transaction latency, PostgreSQL RSS, temporary storage, WAL, and
  result-query throughput remain within declared gates.

Inspect a registered result's version, Stage graph, storage choices, and typed
UNLOGGED relation metadata with:

```sql
SELECT shiba.explain_physical('shiba.result_name');
```
