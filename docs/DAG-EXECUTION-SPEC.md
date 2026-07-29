# Shiba DAG execution specification

Status: proposed normative contract for execution architecture v2

Target: PostgreSQL 17

Last updated: 2026-07-29

This document defines the execution semantics that the next Shiba runtime must
implement. It is intentionally stricter than an architecture sketch: `MUST`,
`MUST NOT`, `SHOULD`, and `MAY` are normative.

The current implementation is still commit-scoped. Its behavior is documented
in [SINGLE-RUNTIME-DESIGN.md](SINGLE-RUNTIME-DESIGN.md),
[COMMIT-SCOPED-DAG-EXECUTION.md](COMMIT-SCOPED-DAG-EXECUTION.md), and
[MEMORY-BOUND-RUNTIME.md](MEMORY-BOUND-RUNTIME.md). This specification
supersedes those documents only as the target contract; it does not claim that
v2 is already implemented.

## 1. Product definition

Shiba is a PostgreSQL-internal asynchronous incremental dataflow runtime.

- One real PostgreSQL background worker owns ingestion, scheduling, execution,
  and garbage collection for one active database.
- A DAG is a persisted plan plus in-process scheduling metadata. It is not a
  PostgreSQL worker, process, thread, connection, or CPU reservation.
- Source commit LSNs define input order and completion watermarks.
- Operator work is executed as bounded, atomic, recoverable work quanta.
- A result may be partially visible while its current source commit is being
  processed.
- When a DAG catches up, its result MUST equal a native recomputation of the
  declared query over the corresponding committed source state.

The fundamental choice is:

```text
arbitrarily large finite input or fan-out
+ bounded active work and short transactions
= no source-commit-atomic result visibility
```

Shiba chooses bounded work. It does not claim source-commit-atomic visibility.

## 2. Goals and explicit non-goals

### 2.1 Required goals

The runtime MUST provide:

1. transparent eventual processing of a finite source commit without a
   configured total-commit row or byte ceiling;
2. transparent eventual processing of a finite operator fan-out, including one
   Join input matching millions of rows;
3. memory use that does not grow with total source-commit size or total
   fan-out;
4. bounded transactions and cooperative fairness between DAGs;
5. durable exactly-once effects under backend restart and PostgreSQL crash;
6. set-oriented PostgreSQL execution inside each work quantum;
7. durable reuse of compiler-selected intermediate results;
8. deterministic progress, failure, repair, and rebuild behavior;
9. observable route, execution, completion, backlog, and resource state.

“Arbitrarily large” means finite, representable by PostgreSQL and Shiba data
types, and processable with sufficient durable storage. It does not mean
infinite disk, an unbounded individual tuple, or immunity to replication-slot
loss.

### 2.2 Non-goals

The v2 contract does not provide:

- source-row-atomic or source-commit-atomic result visibility;
- cross-DAG atomicity;
- rollback of already committed work quanta after a later quantum fails;
- a worker pool or concurrent DAG execution;
- an exact hard upper bound on PostgreSQL backend RSS;
- automatic recovery after loss or invalidation of required logical WAL;
- an as-of-LSN query interface over historical result versions;
- support for an operator that lacks a bounded, resumable physical
  implementation.

## 3. Vocabulary

| Term | Definition |
| --- | --- |
| Runtime | The one real `shiba runtime` PostgreSQL background worker for a database. |
| DAG instance | A registered result, its logical and physical plan, durable state, and scheduling state. |
| Source epoch | One committed source transaction, identified and ordered by commit LSN. |
| Work quantum | One bounded unit of DAG work committed in one PostgreSQL transaction. |
| Task | Durable operator work plus its continuation for one source epoch. |
| Continuation | The monotonic cursor and phase required to resume a task. |
| Stage | A compiler-selected relational boundary used for fusion, scratch, durability, or reuse. |
| Barrier | Control information stating that all data for a source epoch has entered an edge or Stage. |
| `completed_lsn` | The greatest source commit LSN whose barrier has reached and completed the result sink. |
| Durable edge | A LOGGED Stage whose rows or consumer cursors survive work-quantum commits. |
| Scratch Stage | A pre-created UNLOGGED relation whose contents never survive a successful work-quantum boundary. |

The term “DAG worker” MUST NOT be used for a DAG instance or task because it
implies a PostgreSQL or operating-system execution resource that does not
exist.

## 4. Process topology

```text
PostgreSQL postmaster
  -> logical walsender (one PostgreSQL-owned backend per active database)
       -> pgoutput protocol v2 over a replication connection
  -> shiba runtime (one Shiba BGW per active database)
       -> bounded ingress step
       -> DAG scheduler
            -> DAG A / runnable task
            -> DAG B / runnable task
            -> DAG C / runnable task
       -> bounded Stage and input GC
       -> latch wait
```

The Runtime:

- MUST use one SPI-connected backend;
- MUST own one replication-protocol client connection whose server side is a
  PostgreSQL walsender;
- MUST execute only one Shiba work quantum at a time;
- MUST set `max_parallel_workers_per_gather = 0` for v2 so a Shiba SQL program
  cannot silently create PostgreSQL parallel-query workers;
- MUST NOT create a Router worker, Executor worker, per-DAG worker, thread
  pool, connection pool, or worker pool;
- MAY cache validated plans and prepared SQL programs in backend memory, under
  a configured bounded LRU;
- MUST treat every backend-local cache as disposable after restart.

This topology bounds Shiba-owned execution concurrency to one. The walsender
is PostgreSQL-owned transport and decoding infrastructure; it never executes a
Shiba DAG or writes Shiba tables. Neither backend reserves a CPU core: the
operating system schedules them like other PostgreSQL backends.

The Runtime is dynamically registered. A backend crash while the postmaster
stays up uses the configured BGW restart path. Dynamic registration itself does
not survive a postmaster restart, so deployment MUST idempotently call
`shiba.activate()` for every active database during database startup; a source
trigger MAY remain a fallback activation signal. Absence of the Runtime MUST be
observable and never causes durable backlog to be discarded. An always-running
cross-database launcher would be an additional process and is not part of this
one-BGW contract.

## 5. User-visible consistency contract

### 5.1 Ordering

For each DAG independently:

1. committed source epochs MUST be admitted in commit-LSN order;
2. epoch `L2` MUST NOT begin changing operator state or the result until epoch
   `L1 < L2` has completed;
3. multiple DAGs MAY have different completion watermarks;
4. no ordering or atomicity is promised across DAGs.

Only the oldest incomplete source epoch for a DAG is runnable. This deliberately
trades same-DAG concurrency for deterministic state and bounded recovery.

### 5.2 Atomicity

Each work quantum is one PostgreSQL transaction. Within that quantum, all of
the following MUST commit or roll back together:

- operator-state mutations made by the quantum;
- durable Stage rows emitted by the quantum;
- result-table mutations made by the quantum;
- task or consumer continuation advancement;
- input acknowledgement made by the quantum;
- producer/consumer barrier metadata and any `completed_lsn` advancement made
  by the quantum;
- resource and progress counters for the quantum.

A source epoch may require any finite number of these transactions.

### 5.3 Partial visibility

Normal `SELECT` statements on a Shiba result MAY observe a partially applied
source epoch. Such a result:

- includes every completed source epoch;
- includes a deterministic physical execution prefix of at most one newer
  epoch;
- may not correspond to any atomic source-table snapshot;
- may temporarily contain outer-join boundary rows, rankings, or duplicate
  counts that later quanta of the same epoch retract or replace.

This is not treated as corruption. It is the declared result-visibility model.

### 5.4 Completion invariant

Let `Q` be the registered query and `S(L)` the conceptual committed source
state after source epoch `L`.

The transaction that advances a DAG to `completed_lsn = L` MUST leave:

```text
result(DAG) = Q(S(L))
```

and MUST leave no unfinished task, unconsumed durable Stage row, or unforwarded
barrier for that DAG at or below `L`.

Later work on `L2 > L` may immediately make the normally queried result a
partial prefix again. Therefore `completed_lsn = L` means “all effects through
L are included”, not “the table remains an immutable as-of-L snapshot”.

### 5.5 Waiting and observation

The supported status surface MUST expose at least:

- `completed_lsn`;
- `processing_lsn`, or NULL;
- current operator, Stage, task, and continuation phase;
- input, candidate, and output rows completed for the active epoch;
- durable ingress and Stage backlog rows and bytes;
- `caught_up`;
- active, paused, rebuild-required, and failed state;
- the last error and its SQLSTATE.

`caught_up` is an observation, not a lock against a concurrent source commit.
An exact historical read or a read that freezes DAG execution is outside v2.

## 6. Relational delta model

Every dataflow edge carries a weighted bag relation. Its logical columns are:

```text
payload columns...
__weight        bigint       -- signed multiplicity
__source_lsn    pg_lsn
__input_seq     bigint       -- stable order inside the source epoch
__output_seq    bigint       -- stable order emitted for one input/task
__record_id     durable identity
```

Rules:

- zero-weight rows MUST be removed before emission;
- multiplication and addition of weights MUST detect `bigint` overflow;
- a negative resulting stored multiplicity MUST fail the quantum;
- equal rows SHOULD be coalesced set-wise within a bounded page;
- multiplicity MUST remain compressed as a weight through intermediate
  operators;
- physical expansion into duplicate rows MUST be deferred to a bag result
  sink and paged there.

Every physical edge MUST define the stable order
`(source_lsn, input_seq, output_seq, record_id)`. Consumers process that order
unless the compiler proves that a bounded coalescing operation is commutative
and validates all multiplicity prefixes it removes. Generated retractions and
insertions, including Join boundary phases, MUST receive output sequence values
that preserve their declared transition order.

An UPDATE is represented as `-old` followed by `+new`. Ingress sequence is
retained so page-local coalescing can validate ordered prefixes before it
changes durable state. Full-epoch coalescing is an optimization, not a semantic
requirement.

## 7. Ingress and source-epoch creation

### 7.1 Required target

Ingress MUST persist a large source transaction in bounded, replayable
segments. It MUST NOT require a transaction-sized Rust collection, a
transaction-sized JSON value, or one Shiba database transaction containing the
complete source payload.

The target is one long-lived logical decoding context in a PostgreSQL
walsender, using `pgoutput`, protocol version 2, and `streaming = on`. The
Runtime BGW is the replication client. It receives complete wire messages,
persists them with ordinary short SPI transactions, and multiplexes ingestion
with DAG scheduling. Streamed transaction blocks are provisional until a
`Stream Commit` is received.

The replication connection and the Runtime's SPI connection are distinct
PostgreSQL backend sessions. The Runtime MUST NOT hold an SPI transaction open
while waiting for network or replication input. Connection setup and
authentication MUST be explicit deployment configuration; credentials MUST
use PostgreSQL-supported passfile, certificate, or peer mechanisms rather than
being persisted in Shiba catalog rows.

The production ingress MUST NOT repeatedly call the SQL
`pg_logical_slot_*_changes` SRFs:

- protocol version 1 checks `upto_nchanges` only after a complete transaction,
  so one transaction may exceed the requested count;
- the SQL SRF materializes output in a tuplestore;
- each call creates and destroys a decoding context;
- an open transaction pins `restart_lsn`, so repeatedly rebuilding a context
  can re-decode an ever-growing transaction prefix and approach quadratic
  work;
- slot confirmation and ordinary table commit do not form one atomic commit
  domain.

The SQL interface MAY be used by a feasibility test, but it is not the target
data path.

The walsender's logical-decoding output callbacks never write Shiba tables.
The Runtime copies only complete replication messages into a bounded ingress
batch. A batch ends at a configured byte/row budget or at `Stream Stop`,
normal Commit, Stream Commit, or Stream Abort. One individual tuple or
protocol message is the indivisible memory unit.

### 7.2 Durable ingress model

The logical schema MUST have the equivalent of:

```text
ingress_transactions(
  ingress_txn_id,
  slot_generation,
  xid,
  identity_lsn,
  status,                 -- open | committed | aborted
  commit_lsn,
  end_lsn,
  next_input_seq,
  event_count,
  payload_bytes
)

ingress_decode_batches(
  slot_generation,
  decode_end_lsn,
  message_digest,
  event_count,
  persisted_at
)

change_log(
  ingress_txn_id,
  change_lsn,
  change_ordinal,
  image_ordinal,
  input_seq,
  source_oid,
  weight,
  typed_payload,
  UNIQUE (
    ingress_txn_id,
    change_lsn,
    change_ordinal,
    image_ordinal
  )
)

ingress_sources(
  ingress_txn_id,
  source_oid
)

routing_tasks(
  ingress_txn_id,
  subscriber_cursor,
  status
)

ingress_replay_state(
  slot_generation,
  confirmed_lsn,
  replay_safe_lsn
)
```

The exact physical schema may differ, but it MUST preserve these invariants:

- payload is stored once, independent of DAG fan-out;
- open transactions are keyed independently of their not-yet-known commit LSN;
- event replay uses stable WAL change coordinates, not Stream block boundaries;
- a repeated event either deduplicates exactly or raises corruption;
- a decode-batch digest is a diagnostic and slot-confirmation checkpoint, not
  the sole exactly-once event identity;
- `input_seq`, event counts, and byte counts use `bigint`;
- tuple text is normalized to the registered PostgreSQL types while the
  decode batch is inserted;
- source OIDs and row/byte statistics are accumulated per batch;
- final commit does not rewrite every payload row;
- DAG fan-out reads `ingress_sources`, not the complete payload;
- subscriber fan-out is itself a keyset-paged routing task, so a commit
  touching many DAGs does not create one unbounded finalization transaction.

`confirmed_lsn` records the greatest table-persisted decode batch requested
for slot confirmation. `replay_safe_lsn` is more conservative: it advances
only after Shiba can prove that the slot position is durably saved against a
postmaster crash. It is the GC fence for ingress deduplication identities.

The identity of an open transaction MUST include slot generation, XID, and a
stable first-change coordinate so XID wrap or slot recreation cannot merge two
transactions. The stability of `(change_lsn, change_ordinal, image_ordinal)`
under replay, multi-insert records, subtransactions, and TOAST is a mandatory
feasibility test.

On replay, an existing event retains its original `input_seq`; deduplication
MUST NOT allocate another sequence or increment header statistics. A
conflicting payload for the same WAL event identity is corruption.

### 7.3 Decode-block transaction

For one complete replication batch the Runtime MUST:

1. read only complete replication messages and stop at a batch budget or a
   Stream Stop, Commit, Stream Commit, or Stream Abort boundary;
2. parse the messages incrementally;
3. lock or create the ingress transaction header;
4. insert the decode-batch descriptor and its payload idempotently;
5. advance `next_input_seq` and statistics;
6. commit the LOGGED payload;
7. only after that commit, send a replication Standby Status Update whose
   flush/apply LSN does not exceed `decode_end_lsn`;
8. yield to DAG scheduling and GC before decoding another block when their
   budgets require a turn.

A crash before step 6 leaves no durable batch and the slot replays it. A crash
between steps 6 and 7 replays events that deduplicate by WAL identity. A crash
after step 7 may still see slot-position rollback to a checkpoint and therefore
uses the same deduplication path.

Slot confirmation MUST NEVER exceed the greatest committed
`ingress_decode_batches.decode_end_lsn`.

### 7.4 Commit and abort

On normal Commit or `Stream Commit`, one short transaction MUST:

- mark the ingress transaction committed and record commit/end LSN;
- create one durable routing task;
- make the payload eligible for subscriber routing.

It MUST NOT scan or update every payload row, or fan out to every DAG in that
transaction.

The routing task keyset-pages the affected subscriptions. For each page it
creates at most one source epoch per DAG and exactly one durable barrier row
for every source input port of that DAG. Ports untouched by the transaction
receive an explicit `is_empty` barrier; absence of a row never means empty.
Activation LSN and plan generation are revalidated in the same transaction.
Routing tasks themselves are processed in commit-LSN order. A DAG scheduler
still selects only its oldest source epoch.

On `Stream Abort`, Shiba MUST mark the provisional transaction aborted.
Payload deletion MAY be bounded and asynchronous. No DAG may observe or apply
an open or aborted ingress transaction.

### 7.5 Mandatory PostgreSQL 17 feasibility gate

PostgreSQL's logical-decoding C API is internal rather than a stable extension
ABI. Before v2 ingress implementation is accepted, a focused spike MUST prove:

1. a same-backend persistent decoding context is either proven safe across
   ordinary SPI transactions or rejected before any unsafe XID assignment;
2. the replication-protocol fallback can maintain one walsender decoding
   context while the Runtime independently executes short SPI transactions;
3. protocol-v2 Stream blocks can be received, persisted, confirmed, and
   resumed without rebuilding the prior WAL prefix;
4. output callbacks can use a bounded buffer or disposable spill file without
   table writes or XID assignment;
5. persisted event identity is stable under crash replay, multi-insert records,
   multiple changes at one LSN, UPDATE old/new images, subtransactions,
   savepoints, and TOAST;
6. interleaved streamed XIDs, Stream Abort, ordinary Commit, and supported 2PC
   behavior are correct;
7. required PostgreSQL 17 symbols can be used through version-gated Rust
   bindings or a small C shim;
8. Shiba can force or observe durable slot-position persistence and advance
   `replay_safe_lsn` without mistaking an in-memory confirmation for a
   crash-safe horizon;
9. Runtime shutdown releases the decoding context and slot in a safe lock and
   ResourceOwner order.

`logical_decoding_work_mem` is a streaming/spill threshold, not an exact
decode-block or RSS ceiling. The resource contract therefore includes the
largest indivisible tuple/WAL record, and the spike MUST measure actual block
size.

The same-backend persistent-context spike failed on PostgreSQL 17:
`PROC_IN_LOGICAL_DECODING` excludes that backend's XID from ordinary snapshot
construction until the slot is released. Therefore the logical replication
protocol fallback is normative for v2. It preserves one Shiba Runtime BGW but
adds a PostgreSQL walsender/replication connection. If “one process in the
entire system” is absolute, PostgreSQL 17 offers no supported interface that
also provides bounded, recoverable, preemptible, linear-time decoding. The
implementation MUST surface that conflict rather than retaining protocol
version 1 and claiming bounded ingress.

Streaming and confirmation do not release WAL retained by an indefinitely open
source transaction: its `restart_lsn` may remain pinned until commit or abort.
The finite-input contract therefore also requires that a source transaction
eventually resolves and that sufficient `pg_wal` capacity exists.

Relevant PostgreSQL contracts:

- <https://www.postgresql.org/docs/17/functions-admin.html>
- <https://www.postgresql.org/docs/17/protocol-logical-replication.html>
- <https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html>
- <https://www.postgresql.org/docs/17/logicaldecoding-output-plugin.html>

## 8. Physical compilation and Stage selection

Registration MUST compile the validated logical DAG into a versioned physical
plan. The compiler MUST analyze:

- fan-out and number of consumers;
- repeated use of the same intermediate;
- state and blocking boundaries;
- whether an operator can yield;
- whether producer and all consumers can commit atomically in one bounded
  quantum;
- required keyset order and indexes;
- worst-case input-to-candidate and input-to-output cardinality;
- barrier behavior for every input port.

The compiler MUST reject a DAG if any operator or physical edge lacks a bounded
execution strategy.

### 8.1 Stage classes

The only Stage classes are:

| Class | PostgreSQL storage | Lifetime | Permitted use |
| --- | --- | --- | --- |
| Inline | query expression | one statement | fused, single-consumer bounded pipeline |
| Statement materialized | `MATERIALIZED` CTE/tuplestore | one statement | reuse inside one statement |
| Scratch | pre-created `UNLOGGED` relation | one work quantum | bounded derived data fully consumed before commit |
| Durable | pre-created typed `LOGGED` relation | multiple quanta | continuation, fan-out, reuse, backpressure, or recovery boundary |
| Operator state | typed `LOGGED` relation | DAG generation | arrangements, aggregates, multisets, indexes |
| Result sink | user result, `LOGGED` | DAG lifetime | user-visible materialization |

The following rule is absolute:

> If a transaction commits producer progress that cannot reproduce an
> intermediate from still-unacknowledged durable input, that intermediate
> MUST be LOGGED in the same transaction.

Consequences:

- UNLOGGED operator state is forbidden;
- a cross-transaction outbox or reused Stage is LOGGED;
- Scratch rows MUST be empty at successful quantum commit;
- postmaster crash may truncate every Scratch Stage without affecting
  correctness;
- there is no apply-time `CREATE`, `DROP`, `ALTER`, or `pg_temp` table;
- Stage relations and required indexes are created at registration.

### 8.2 Reuse

When one materialized output has multiple consumers, the payload SHOULD be
stored once in a Durable Stage. Each consumer has an independent durable
cursor. Rows may be garbage-collected only below the minimum completed
consumer cursor.

The producer persists the Stage barrier's complete terminal order key
`(source_lsn, input_seq, output_seq, record_id)`. Every consumer cursor uses
the same key domain. A bare maximum record ID is not sufficient unless the
physical plan proves that record ID is exactly this monotonic order.

This is the required meaning of “reuse”: consumers share a relational result,
not a Rust collection and not duplicated per-consumer payload.

### 8.3 Fusion

Adjacent Scan, Filter, Project, and Having operations SHOULD be fused when:

- there is one consumer;
- no state or barrier boundary is crossed;
- the fused statement can obey all quantum budgets;
- retry repeats the complete fused transaction safely.

Fusion MUST stop at a durable reuse boundary, an independently resumable
fan-out, or an operator whose output cannot be bounded in the same statement.

## 9. Durable execution metadata

The physical schema MUST represent the equivalent of:

```text
dag_epochs(
  result_oid,
  plan_id,
  source_lsn,
  ingress_txn_id,
  status,                 -- queued | running | complete | paused
  created_at,
  started_at,
  completed_at
)

source_port_barriers(
  result_oid,
  plan_id,
  source_lsn,
  input_port,
  is_empty,
  admitted
)

operator_tasks(
  result_oid,
  plan_id,
  source_lsn,
  operator_id,
  task_id,
  phase,
  continuation,
  input_cursor,
  candidate_cursor,
  output_cursor,
  status
)

stage_epochs(
  result_oid,
  plan_id,
  stage_id,
  source_lsn,
  producer_done,
  terminal_order_key
)

stage_consumers(
  result_oid,
  plan_id,
  stage_id,
  consumer_id,
  source_lsn,
  consumed_through_order_key,
  barrier_seen,
  status                  -- active | complete | cancelled | replaced
)
```

Hot continuations SHOULD use typed columns rather than repeatedly interpreted
JSONB. The representation is an implementation choice; the invariants are not.

Required database constraints include:

- one DAG epoch per `(result_oid, plan_id, source_lsn)`;
- one source barrier per
  `(result_oid, plan_id, source_lsn, input_port)`;
- one task identity per physical operator task;
- one Durable Stage record identity per producer output;
- one consumer cursor per Stage consumer and source epoch;
- every Stage terminal and consumer cursor uses the complete physical edge
  order key, not an unrelated record ID;
- nonnegative counts and monotonic cursor domains;
- plan-generation foreign keys on every task, Stage, and state relation;
- no `completed_lsn` beyond an incomplete epoch.

## 10. Bounded operator interface

Every physical kernel MUST implement the conceptual interface:

```text
step(task, budget) -> outcome

outcome =
    Yield(new_continuation, counters)
  | InputDone(counters)
  | BarrierDone(counters)
  | Blocked(reason)
  | Failed(sqlstate, reason)
```

A successful step MUST do one of:

- advance a monotonic input, candidate, or output cursor;
- complete a phase;
- consume or emit at least one durable record;
- forward a barrier.

It MUST NOT commit a zero-progress runnable task. Repeated zero-progress or
statement-timeout outcomes MUST transition to `Blocked`, rather than spin.

The continuation MUST contain all information needed to resume without
rescanning an unbounded prefix. `OFFSET` is forbidden for resumable scans.
Every paged scan MUST use a stable total key and a matching keyset predicate.

## 11. Work-quantum transaction

The canonical transaction is:

```text
BEGIN
  acquire DAG advisory xact lock
  lock and revalidate DAG generation/state
  lock oldest DAG epoch
  lock one runnable task or consumer cursor
  read a bounded page using a stable keyset cursor
  run one fixed-shape, set-oriented SQL program
  update operator state
  append Durable Stage output
  advance continuation / acknowledge consumed input
  record counters
COMMIT
```

Producer state, emitted Durable Stage rows, and producer continuation MUST be
one transaction. Consumer state or sink mutations and consumer
acknowledgement MUST be one transaction.

SQL programs:

- MUST have fixed shape per physical-plan generation;
- SHOULD be prepared and cached by `(result_oid, plan_id, kernel_id)`;
- MUST pass payload through relations, not interpolate a giant SQL string or
  transaction-sized parameter;
- MUST select candidate rows into a bounded CTE before a potentially
  multiplicative join;
- MUST set transaction-local `statement_timeout` and `lock_timeout`;
- MUST retry only by rolling back and replaying the complete quantum.

## 12. Scheduling and fairness

The single Runtime uses two-level cooperative scheduling:

1. choose a runnable DAG using round-robin or deficit round-robin;
2. choose one runnable task inside that DAG.

Each turn commits at most one work quantum before another DAG is eligible.
Within a priority class, task selection MUST use a stable order such as
`(source_lsn, operator_id, task_id)`.

Downstream priority MUST NOT violate edge order. A task may pass an earlier
task only when the physical plan marks them independent and proves that their
state keys and emitted prefixes cannot conflict.

Within a DAG, the scheduler SHOULD prioritize:

1. result-sink consumers;
2. consumers of a Durable Stage above its high-water backlog;
3. downstream stateful consumers;
4. upstream producers;
5. new source input.

This downstream-first policy is backpressure, not execution concurrency.

Fairness MUST be tested in committed quanta, not wall-clock aspirations. One
SQL statement may run until its statement timeout, so the configured timeout
is part of the fairness contract.

## 13. Barriers and completion watermarks

Every source epoch has one logical barrier. A producer may forward its barrier
only after:

- all input records before that barrier are consumed;
- every task created by those records is complete;
- all output records have been durably emitted.

A consumer may forward the barrier only after it consumes every Stage record
before that barrier.

A multi-input operator may forward epoch `L` only after the barrier for `L`
has arrived on every relevant input port and all pre-barrier tasks are
complete. The source-port barrier row is mandatory; an unchanged input
contributes an explicit empty barrier, not an absent row.

The producer transaction that emits its final output page MUST atomically
advance its continuation and persist `producer_done` plus the terminal Stage
order key. The consumer transaction that consumes the terminal order key MUST
atomically apply the final state/output page, advance its cursor, and persist
`barrier_seen`. No barrier flag may be committed ahead of the data or cursor it
certifies.

The sink advances `completed_lsn` only in a transaction that proves:

- its input barrier is ready;
- no sink-expansion task remains;
- every Stage consumer for the epoch is at its producer maximum;
- no runnable or blocked task remains at or below the epoch.

That same transaction persists the sink barrier, marks the DAG epoch complete,
and advances `completed_lsn`; these writes may not be split across
transactions.

The source epoch reference and source payload become GC-eligible only after all
subscribed DAGs have completed or been explicitly dropped/rebuilt.

## 14. Operator-specific bounded strategies

### 14.1 Stateless operators

Filter, Project, and non-stateful expressions MUST be fused into bounded input
pages where possible. They may change zero or one output row per weighted input
row and need only the upstream record cursor.

### 14.2 Aggregate and Distinct

Aggregate and Distinct MUST:

- read at most one bounded input page;
- validate page-local ordered multiplicity prefixes;
- group equal keys and combine weights set-wise;
- lock/update state keys in canonical key order;
- emit only old-to-new boundary or aggregate-row differences;
- atomically update state, output, and input cursor.

Their memory and statement work MUST be proportional to distinct keys in the
current page, not to total source-epoch cardinality.

An optional compiler-selected Durable fold Stage MAY combine a hot key across
pages. Because that fold survives a quantum, it MUST be LOGGED.

### 14.3 Join

Join is not allowed to use a total-fan-out preflight quota as its execution
mechanism. A finite fan-out MUST become a resumable task.

For an accepted input delta, the Join MUST atomically:

1. capture the input side, key, row identity, signed weight, and old/new
   multiplicity needed by the transition;
2. update or version the input-side arrangement;
3. create the durable Join task;
4. acknowledge that input record.

The Join MUST NOT accept a later input transition that can change the
arrangement observed by an unfinished Join task. V2 may conservatively allow
only one active Join transition per operator and source epoch.

A Join task has the conceptual continuation:

```text
source_lsn
input_seq
input_side
join_key
input_row
input_weight
old_self_multiplicity
new_self_multiplicity
old_input_key_total
new_input_key_total
opposite_key_total
old_null_visibility_state
new_null_visibility_state
phase
last_opposite_order_key
remaining_output_weight
plan_id
```

The physical plan MUST define a stable, unique opposite-row order and matching
index. Pagination uses:

```sql
WHERE join_key = $key
  AND opposite_order_key > $last_key
ORDER BY opposite_order_key
LIMIT $candidate_budget
```

It MUST NOT use `OFFSET`.

Join phases are subtype-specific but MUST be explicit. The common state
machine is:

```text
PREPARE
  -> OLD_BOUNDARY
  -> MATCH_PAGES
  -> NEW_BOUNDARY
  -> FINALIZE
```

- `OLD_BOUNDARY` retracts null-extended, semi, anti, or null-aware rows whose
  old visibility ends.
- `MATCH_PAGES` scans no more than the candidate budget and emits compressed
  weighted pair deltas.
- `NEW_BOUNDARY` inserts boundary rows whose new visibility begins.
- `FINALIZE` verifies cursor exhaustion and completes the task.

The exact order may differ when a subtype requires it, but the phase transition
and all emitted output MUST commit atomically.

Outer, semi, anti, full outer, and null-aware anti joins MUST treat a zero/nonzero
match-count transition as potentially unbounded fan-out. For example, inserting
one right-side row may retract millions of left null-extended rows. That work
uses the same keyset-paged task mechanism.

Null-aware anti join MUST also support a global task when the right-side NULL
count crosses zero. That task pages the complete affected left arrangement; it
must not collect the arrangement or reject it because of total size.

Weighted pair multiplicity remains compressed in Durable Stages. A pair with
weight ten million is one intermediate row, not ten million Rust values.

Example: one inserted right row matches ten million distinct left arrangement
rows and the candidate budget is 10,000. `PREPARE` durably updates the right
arrangement and creates one task. `MATCH_PAGES` then needs about 1,000
independent PostgreSQL transactions, each scanning at most 10,000 left keys,
emitting a LOGGED output page, and advancing the keyset cursor atomically.
Other DAGs receive scheduler turns between those transactions. The source
epoch barrier cannot reach the sink until all pages and any outer-boundary
phase complete.

### 14.4 TopN

TopN MUST retain an indexed multiset ordered by the declared sort key plus a
stable unique tie-breaker.

An input may move an unbounded number of retained rows when the declared limit
is itself large. TopN therefore MUST use a durable rebuild/diff task with:

- affected range or generation;
- old-result delete cursor;
- new-order keyset cursor;
- output position and remaining duplicate weight.

No quantum may sort or expand the entire retained multiset without a proven
configured bound.

An unfinished TopN rebuild/diff task blocks later TopN input transitions so
its ordered keyset remains stable.

### 14.5 Window

Window MUST retain an indexed per-partition multiset. A changed row creates or
coalesces a partition task.

A partition task MUST page:

- removal or versioning of the old partition result;
- ordered recomputation of the new partition;
- sink or downstream output.

The task continuation includes partition identity, phase, stable order key,
window frame state required by the supported function, and output cursor.

The compiler MUST reject a window frame/function whose state cannot be resumed
with bounded retained continuation or a disk-backed Durable Stage.

A later input that can change a partition is blocked until that partition's
task completes. Independent partitions MAY have separate durable tasks, but
the single Runtime still executes only one quantum at a time.

### 14.6 Result sink

A keyed result sink applies one bounded set of key transitions.

A bag sink receiving weight `w` MUST expand or retract at most the configured
sink-row budget per quantum:

- positive weight inserts a bounded `generate_series` page;
- negative weight deletes a bounded number of matching physical rows and
  verifies the affected count;
- the task stores the remaining signed weight;
- sink mutation and remaining-weight update commit together.

The sink MUST never expand an intermediate multiplicity into a
transaction-sized Rust collection or one unbounded SQL statement.

## 15. Locking and lifecycle

The canonical lock order is:

```text
DAG advisory transaction lock
-> dag_runtime_state row
-> oldest dag_epoch row
-> operator task / Stage consumer in (operator_id, task_id) order
-> Stage metadata
-> operator-state rows in canonical typed-key order
-> result rows
```

Even with one Runtime, lifecycle DDL, status APIs, rebuild, and user reads can
race with execution. Every code path that takes multiple Shiba locks MUST use
this order.

The internal schema, operator state, task relations, and result table MUST be
protected from unmediated user DML. Users may query the result, but only
Shiba's security-definer execution path may mutate it. Exactly-once guarantees
do not cover an administrator who deliberately bypasses these protections.

### 15.1 Plan generations

Every task, Stage row, cursor, state row, and prepared program is owned by one
`plan_id`.

- A Runtime MUST NOT execute a task with a different active plan generation.
- Prepared statements MUST be deallocated on generation replacement or cache
  eviction.
- A new plan may be activated only at a completed barrier, or by building a
  new generation and atomically replacing the result.
- In-flight v1 or old-generation continuations MUST NOT be interpreted by a
  new physical plan.

### 15.2 Source schema changes

The physical plan records source OIDs and a schema/type fingerprint.
Incompatible Relation messages, replica-identity changes, unsupported
TRUNCATE, or source DDL MUST stop affected DAGs before reinterpretation and
mark them rebuild-required when replay cannot preserve semantics.

### 15.3 Drop and rebuild

DROP or rebuild MUST first prevent new scheduling under the DAG lock.

Rebuild is a new generation:

1. capture a consistent source snapshot and activation LSN;
2. build new state and result in bounded restartable batches;
3. retain WAL from the activation point;
4. catch the new generation up;
5. atomically switch the active generation/result identity;
6. retire old tasks, Stages, state, and input references in bounded GC.

No migration may silently discard an incomplete epoch.

## 16. Failure and recovery

### 16.1 Crash recovery

After backend restart or PostgreSQL crash:

- LOGGED input, tasks, Durable Stages, state, sink, and continuations are
  authority;
- Scratch Stages are cleared before scheduling;
- an uncommitted quantum has no visible state, output, or cursor advancement;
- a committed quantum is resumed from its new continuation;
- runnable queues and plan caches are rebuilt from catalogs;
- completion is recomputed from durable barriers, never backend memory.

### 16.2 Exactly-once mechanism

Exactly-once is obtained by atomic state/outbox/checkpoint transactions, not by
assuming the scheduler runs once.

- producer output identity MUST have a unique database constraint;
- consumer state changes and cursor acknowledgement MUST share a transaction;
- repeated scheduling after rollback repeats the complete quantum;
- a duplicate durable output identity MUST deduplicate only when its content
  is identical; conflicting content is corruption;
- GC MUST NOT remove the final replay authority before every consumer and
  barrier is complete.

### 16.3 Error classes

| Class | Required behavior |
| --- | --- |
| Deadlock, serialization failure, lock timeout | Roll back complete quantum; bounded retry with backoff. |
| Statement timeout with progress impossible at current budget/index | Roll back; mark task resource-blocked after bounded retries. |
| Deterministic data, overflow, plan, or constraint error | Roll back failing quantum; pause only affected DAG; retain task/input. |
| Disk full, WAL/slot, catalog, or postmaster-level failure | Stop affected Runtime phase or Runtime; do not misclassify as one poison DAG. |
| Logical slot invalidation or missing required WAL | Mark affected DAGs rebuild-required; never advance watermarks. |

Because source-epoch atomic result visibility is not provided, a deterministic
error may leave earlier quanta of that epoch visible. Repair resumes from the
failing continuation; rebuild replaces the partial generation. Shiba MUST NOT
claim that it rolled the source epoch back.

## 17. Garbage collection and backpressure

GC MUST be bounded and restartable.

Durable Stage rows are eligible only when every consumer cursor has passed
them. A consumer explicitly dropped or replaced by a rebuild may enter a
durable `cancelled` or `replaced` terminal state instead of advancing its
cursor. That terminal transition MUST commit with the generation switch or
DROP decision and the release of its replay authority; merely inactive or
paused consumers remain live GC references.

The heavy source payload is eligible only when:

- the source transaction committed or aborted;
- its subscriber routing task completed or was cancelled by an explicit
  rebuild/drop decision;
- every subscribed DAG completed, dropped, or switched to a rebuild that no
  longer references it;
- no task, Stage row, or diagnostic retention policy references it.

Payload eligibility is not sufficient to delete ingress deduplication
authority. The ingress transaction tombstone and event/batch identities MUST
remain until `replay_safe_lsn` is strictly beyond the transaction's terminal
LSN. This handles a postmaster crash that rolls a recently confirmed slot back
and replays already applied WAL. If Shiba cannot prove a crash-safe replay
horizon, it retains the tombstones; it MUST NOT infer safety from the current
in-memory `confirmed_flush_lsn`.

High-water behavior:

- a large downstream backlog MUST make its consumers higher priority;
- a producer SHOULD stop producing into a Stage over its high-water byte or row
  limit while consumers are runnable;
- a paused consumer propagates backpressure to its producers;
- ingress and logical-slot lag remain observable even when apply is paused.

Logical decoding is asynchronous, so Shiba cannot reject a source commit that
already committed merely because internal disk is full. With insufficient
disk, the safe behavior is to stop acknowledgement and retain WAL/input until
an administrator frees capacity. If PostgreSQL invalidates the slot, rebuild
is required.

Autovacuum MUST remain enabled on hot input, task, Durable Stage, state, and
sink relations. Per-relation tuning must be based on measured dead tuples,
WAL, and table growth. Ordinary execution MUST NOT use per-quantum TRUNCATE or
DDL as a substitute for GC.

## 18. Resource contract

The Runtime MUST enforce independent per-quantum limits for:

- source input rows and bytes;
- candidate rows scanned;
- Durable Stage output rows and bytes;
- result rows inserted/deleted;
- state rows touched;
- statement duration;
- temporary-file bytes;
- cached DAGs and prepared programs.

The old `max_commit_rows`, `max_commit_bytes`, and total Join Stage quota MUST
not reject a finite epoch merely because of its total size. They are replaced
by quantum and backlog limits.

The Runtime session MUST set deliberately:

- `work_mem`;
- `hash_mem_multiplier`;
- `temp_file_limit`;
- `logical_decoding_work_mem`;
- `statement_timeout` per quantum;
- `lock_timeout` per quantum;
- `max_parallel_workers_per_gather = 0`;
- `synchronous_commit = on` for ingress authority, state, continuations, and
  slot-before-ack persistence;
- JIT policy, defaulting off until a measured kernel benefits.

`work_mem` is a budget per PostgreSQL executor node, not a backend RSS cap.
The contract is therefore:

> No Rust or SQL data structure may grow in proportion to total source-epoch
> size or total fan-out; active work is page-bounded and PostgreSQL may spill.

It is not:

> The Runtime process can never exceed exactly N bytes of RSS.

An exact process-memory ceiling requires an operating-system or container
limit in addition to this execution design.

A single tuple larger than the configured row-byte ceiling is indivisible. It
MUST produce an explicit resource-blocked error rather than bypassing the
limit.

## 19. Observability

`shiba.status(result)` or an equivalent supported API MUST expose:

```text
result_oid
plan_id
runtime_pid
state
completed_lsn
processing_lsn
operator_id
task_id
phase
input_cursor
candidate_cursor
output_cursor
epoch_input_rows / bytes
epoch_candidate_rows
epoch_output_rows / bytes
durable_stage_backlog_rows / bytes
ingress_lag_bytes
oldest_task_age
last_quantum_duration
last_error_sqlstate / message
```

`shiba.explain_physical(result)` MUST show:

- fused kernels;
- every Stage class;
- materialization/reuse reason;
- consumers;
- continuation schema;
- keyset order and indexes;
- per-kernel budgets;
- barrier input/output behavior;
- plan generation.

Metrics MUST distinguish ingress lag, DAG queue lag, active-epoch duration,
sink lag, GC lag, and source-to-completion latency.

## 20. Migration from the current implementation

The current and target units differ:

| Concern | Current v1 | Required v2 |
| --- | --- | --- |
| Apply transaction | one DAG × complete source commit | one DAG × one work quantum |
| Result visibility | source commit atomic | partial during active epoch |
| Total commit limit | hard row/byte rejection | no total limit; page limits |
| Join fan-out | preflight/quota, one transaction | persistent task and keyset pages |
| Cross-transaction intermediate | absent | LOGGED Durable Stage |
| UNLOGGED Stage | may cross SQL statements in one transaction | same-quantum Scratch only |
| Progress | `applied_lsn` after one transaction | `completed_lsn` after sink barrier |
| Ingress | protocol v1, complete-transaction SQL SRF | persistent protocol-v2 decoding context and LOGGED decode blocks, subject to feasibility gate |
| Scheduler yield | source-commit boundary | work-quantum boundary |

Migration MUST be expand-and-contract:

1. add v2 catalogs and physical plan version without changing v1 readers;
2. gate v2 registration/execution behind an explicit plan version;
3. keep existing DAGs on v1 until a completed barrier;
4. rebuild or explicitly convert each DAG into a fresh v2 generation;
5. never translate an in-flight v1 commit into a v2 continuation;
6. remove v1 catalogs and GUCs only after no v1 DAG remains.

## 21. Conformance tests

Correctness acceptance MUST include:

1. differential comparison with native SQL after every completed source epoch;
2. one source transaction larger than the old total-commit limits;
3. a one-row Join input with at least ten million logical matches;
4. outer, full, semi, anti, and null-aware anti zero-boundary fan-out;
5. both Join inputs changing in one source transaction;
6. a multiplicity too large for one sink quantum;
7. TopN and Window tasks spanning many quanta;
8. fan-out to multiple consumers with one stored Durable Stage payload;
9. two DAGs where one has huge work, proving the other receives turns;
10. crash injection before and after every state/output/continuation/barrier
    write boundary;
11. multi-input empty-port barriers and Stage terminal composite cursors;
12. postmaster crash proving Scratch loss is harmless;
13. slot confirmation rollback proving dedup tombstones survive until the
    replay-safe horizon;
14. consumer cancellation/replacement followed by bounded old-Stage GC;
15. deterministic error after earlier quanta, followed by resume and rebuild;
16. slot replay, streamed abort, slot invalidation, provisional ingress GC,
    and paged subscriber routing;
17. plan-generation replacement and source-schema mismatch;
18. disk/backpressure and autovacuum/bloat behavior.

Ingress tests MUST additionally cover source transactions of approximately
`1x`, `10x`, and `100x logical_decoding_work_mem`, two interleaved streamed
transactions that resolve in different order, multi-insert WAL, subtransaction
rollback, a large TOAST value, and crash points before spool commit, after
spool commit but before slot confirmation, and after confirmation. Decoded work
must scale approximately linearly with source WAL; repeated-prefix decoding is
a failure.

Tests SHOULD run with tiny budgets so ordinary fixtures force hundreds of
continuations. They MUST assert that no successful quantum exceeds its row or
byte counters and that no task can commit without monotonic progress.

## 22. Performance acceptance

Performance comparison requires the same PostgreSQL build, configuration,
dataset, client count, machine state, repetitions, and statistic. At minimum it
MUST report:

- source commit latency;
- small-commit end-to-end completion latency and throughput;
- large-commit time to first quantum and total completion;
- fairness delay imposed on another DAG;
- Runtime RSS over increasing total input/fan-out;
- temporary-file use;
- WAL generated by ingress, Durable Stages, state, and sink separately;
- WAL decoded per source WAL byte, detecting repeated-prefix or superlinear
  ingress work;
- durable backlog disk footprint and GC rate;
- result-query throughput during partial application and when caught up.

Correctness, crash recovery, and finite-work completion are hard gates.
Small-commit median throughput SHOULD remain within 5% of the matched v1
baseline and p95 completion latency SHOULD remain within 10%; exceeding either
requires optimization or an explicit accepted trade-off, not a filtered
benchmark.

The memory test MUST demonstrate a plateau or bounded band as total commit and
fan-out sizes increase. A single small-case RSS number is insufficient.

## 23. Definition of done

Execution architecture v2 is complete only when:

- the PostgreSQL 17 persistent-decoding-context feasibility gate passes or the
  process topology is explicitly revised;
- every accepted operator has a tested bounded continuation;
- no total source-commit or total fan-out quota is required for correctness;
- all cross-quantum recovery authority is LOGGED;
- all Scratch Stages are disposable at every committed boundary;
- barriers and `completed_lsn` satisfy the formal completion invariant;
- crash tests prove exactly-once state and sink effects;
- fairness, memory-scaling, disk, WAL, and performance gates pass;
- old v1 DAGs can be migrated or rebuilt without silent input loss;
- documentation and APIs state partial visibility plainly.
