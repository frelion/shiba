# Single Runtime design

> Current implementation (v1). The proposed bounded, resumable v2 execution
> contract is defined in
> [DAG-EXECUTION-SPEC.md](DAG-EXECUTION-SPEC.md). Until v2 is implemented,
> the commit-scoped limitations in this document remain real.

Status: implemented contract. Durable metadata, input, operator state, result,
and progress remain logged. Typed UNLOGGED storage is permitted only for
rebuildable physical Stage intermediates.

## Process topology

Each activated database owns exactly one dynamic PostgreSQL background worker:
`shiba runtime`. Router, scheduler, DAG execution, and garbage collection are
phases of that one SPI-connected backend. A `DagRuntime` is cached plan metadata,
not a process, thread, connection, or dedicated CPU allocation.

The cooperative loop uses bounded work:

1. route a bounded burst of complete source transactions;
2. apply one source transaction for one ready DAG;
3. rotate the DAG cursor;
4. perform bounded garbage collection;
5. service SIGHUP, graceful lifecycle SIGINT, and PostgreSQL shutdown SIGTERM,
   then wait on the latch when idle.

Statement-level source triggers register a PostgreSQL transaction callback
that sets the Runtime latch only after commit. Normal routing therefore wakes
immediately without carrying tuples or lowering the fallback idle-poll
interval. The backend-local callback state is deduplicated per top-level
transaction and cleared on abort or `PREPARE TRANSACTION`; a later
`COMMIT PREPARED` relies on the bounded fallback poll.

Routing and applying never share a PostgreSQL transaction. A long DAG apply can
delay routing, so every observable metric must distinguish route lag, inbox lag,
and apply duration.

## Durable event model

`shiba_internal.change_log` stores each decoded row delta once:

```text
(commit_lsn, sequence, source_oid, delta, row_data)
```

Its primary key is `(commit_lsn, sequence)`. `sequence` preserves source
transaction order. `row_data` remains JSONB initially; column pruning is a later
optimization.

`shiba_internal.dag_inbox` stores transaction-level DAG work:

```text
(result_oid, commit_lsn)
```

There is at most one inbox row for a DAG and source transaction, regardless of
the number of changed rows. A source transaction payload is therefore not copied
for every subscribing DAG.

The Router transaction atomically:

1. claims `commit_lsn` in `routed_transactions`;
2. inserts each decoded delta once into `change_log`;
3. inserts one `dag_inbox` row for every DAG affected by at least one delta.

The logical slot advances only after that routing transaction commits.

`routed_transactions`, `change_log`, and `dag_inbox` are logged recovery
authority. `dag_inbox` has no payload column: it points to the one shared
transaction payload in `change_log`.

## Persisted physical plan and Stages

Registration validates the persisted logical graph and deterministically
compiles one versioned `PhysicalDagPlan`. The plan records fused nodes, physical
kernel inputs and consumers, materialization reasons, and one of three storage
choices:

- `inline`;
- `statement_materialized`;
- `unlogged`.

The physical plan is persisted in `shiba_internal.physical_plans`; its
`plan_id` is the `DagRuntime` cache generation. The Runtime loads and validates
this plan rather than creating a new plan for every commit. Rows in
`shiba_internal.physical_stages` describe only the plan's pre-created typed
UNLOGGED relations.

V1 is a closed physical contract: Runtime dispatch uses the validated
descriptor and rejects any mismatch in the expected Stage program and relation
shape. It is not a generic interpreter for arbitrary future kernels. The two
Join statements are prepared once per `(result_oid, plan_id)` in the Runtime
session and reused across commits; that dedicated session forces generic plans
instead of paying PostgreSQL's first-five custom-plan cycle. Removing a DAG
from the Runtime cache or replacing its generation deallocates both statements,
so a long-lived Runtime does not retain obsolete physical programs.

A Stage is an in-process relational execution abstraction. It is unrelated to
the number of PostgreSQL background workers. The current Join plan uses a
statement-materialized input delta and a typed UNLOGGED `join_delta` Stage
because the exact Join delta crosses from the arrangement-update statement to
the downstream-state statement.

Runtime load initializes statistics only for shared state relations that have
never been analyzed; it does not repeat whole-table `ANALYZE` for every DAG.
When a Join commit produces at least 1,024 Stage rows, the Runtime analyzes
that private Stage before consume so PostgreSQL replans from the actual batch
cardinality, then analyzes the emptied Stage after consume so the next small
commit does not inherit a large-batch estimate.

## Apply contract

The scheduler chooses ready work in `(result_oid, commit_lsn)` order with a
round-robin cursor across result OIDs. Applying one inbox row uses one database
transaction:

1. lock and revalidate the DAG and inbox row;
2. read ordered events directly from `change_log`;
3. execute the persisted physical Stage program;
4. update operator state and the materialized result;
5. clear disposable Stage rows;
6. advance `view_progress`;
7. delete the `(result_oid, commit_lsn)` inbox row;
8. commit all effects atomically.

Rust must not collect all event payloads into a `Vec`, construct a JSON array, or
copy the source transaction back through SPI. Operator SQL consumes the
`change_log` relation by `commit_lsn`, filters to sources in the validated DAG,
and preserves `sequence`.

Registration also records the session settings that affect typed text/JSON
representation (`TimeZone`, `DateStyle`, `IntervalStyle`,
`extra_float_digits`, and `bytea_output`). Apply restores them transaction
locally before reconstructing row identities, so the dedicated Runtime cannot
reinterpret a source value differently from the registration session.

Transient PostgreSQL errors roll back the complete apply transaction and leave
the inbox row eligible for retry. Deterministic plan/operator errors quarantine
only that DAG and retain its inbox row for explicit repair.

For Join, the first set-oriented statement constructs a
`MATERIALIZED` input-delta CTE from `change_log`, validates ordered
multiplicity prefixes, directly computes versioned pair and match-presence
differences, writes the exact net signed bag difference to `join_delta`, and updates the logged
arrangements. A second statement reads `join_delta` to update logged
distinct/aggregate state and the result. These statements do not create two
transactions: they are one commit program inside the one apply transaction.

## Stage lifecycle and crash recovery

Typed UNLOGGED Stage relations are created during registration, not apply.
There is no per-commit DDL and no `pg_temp` scratch table. Runtime plan load
clears every UNLOGGED Stage relation for that DAG. Join writes `join_delta`,
then its consumer uses `DELETE ... RETURNING` so successful apply leaves the
Stage empty before progress and acknowledgement. Result DROP drops Stage
relations in ascending `stage_id` order before deleting their physical-plan
metadata.

An empty Stage above 64 MiB is truncated under the DAG lock. This bounds
dead-tuple storage while keeping TRUNCATE out of ordinary commit execution.

The canonical apply lock order is:

```text
DAG advisory transaction lock
-> dag_runtime_state row
-> earliest dag_inbox row
-> physical Stage relations in stage_id order
-> logged operator-state rows
-> result rows
```

Runtime-load cleanup and lifecycle DROP acquire the same DAG advisory lock
before Stage relations. This prevents apply/lifecycle races and gives every
multi-Stage path the same relation order.

An UNLOGGED Stage is not durable authority. If PostgreSQL crashes, its contents
may be truncated. If an apply did not commit, PostgreSQL also rolls back the
logged arrangement, state, result, progress, and acknowledgement changes. The
logged inbox reference and shared change-log payload remain, so the replacement
Runtime reloads the physical plan and replays the complete commit to rebuild
the Stage. No correctness decision depends on recovering Stage rows.

Inspect the plan and Stage metadata through the supported observation API:

```sql
SELECT shiba.explain_physical('shiba.result_name');
```

## Garbage collection

A `change_log` transaction may be deleted only when no `dag_inbox` row references
its `commit_lsn` and its one-second observability/replay grace period has
elapsed. GC is bounded and runs outside apply transactions. Dropping a DAG
cascades its inbox references, allowing otherwise-unreferenced payloads to be
collected.

A quarantined DAG deliberately retains its references. Retention limits and
operator-state rebuild are separate future policies; GC must never silently
discard data required by a quarantined DAG.

## Lifecycle

`shiba.activate()` idempotently starts one `shiba runtime` identity per database.
The persistent launch-generation/XID handshake prevents duplicate dynamic worker
registration across concurrent or uncommitted activation calls. The runtime
heartbeat and PID live in one singleton catalog row.

Changing or removing the former `shiba.executor_count` setting has no effect on
correctness because no Executor pool exists. Tests and documentation must stop
presenting per-DAG workers or multiple Executors as supported topology.

## Acceptance invariants

- exactly one live Shiba runtime PID per activated database;
- no Router or Executor worker processes;
- one `change_log` payload row per decoded delta, independent of DAG fanout;
- one set-oriented inbox fanout per complete commit, independent of its row count;
- at most one `dag_inbox` row per DAG/source transaction;
- one persisted, validated `PhysicalDagPlan` per DAG generation;
- Stage materialization never changes PostgreSQL worker count;
- Join input delta is statement-materialized and typed `join_delta` is
  UNLOGGED, pre-created, and commit-scoped;
- all recovery authority and operator state remain logged;
- crash/replay can rebuild every UNLOGGED Stage from retained inbox/change-log
  input;
- no apply-time DDL or temporary table;
- no Rust event-batch materialization proportional to source transaction size;
- source transaction ordering and DAG round-robin fairness;
- state, result, progress, and inbox acknowledgement are atomic;
- crash/retry and poison-DAG isolation retain required durable input;
- full correctness gates pass without warning-or-higher log surprises;
- performance is remeasured against a matching baseline for the new topology.
