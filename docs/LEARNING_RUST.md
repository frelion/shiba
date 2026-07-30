# Reading Shiba as a Rust project

This guide assumes you know structs, enums, `match`, `Option`, `Result`,
iterators, borrowing, and ownership. It does not assume PostgreSQL extension
experience.

Shiba has two main Rust paths:

```text
registration:
  analyzed PostgreSQL Query
    → DataflowPlan
    → typed streams/state/continuation relations

execution:
  pgoutput bytes
    → bounded ingress batch
    → source EffectStream
    → bounded Rust kernel steps
    → Sink
```

Rust owns protocol parsing, Query lowering, kernel phase/continuation state
machines, budgets, recovery decisions, and scheduling. PostgreSQL owns durable
typed relations, set operations, locks, and transaction commit.

Read [`ARCHITECTURE.md`](ARCHITECTURE.md) first if the execution path itself is
not yet clear.

For one live ingress batch, this is the function-level path to keep in view:

```text
shiba_runtime_main
├── ingest_and_publish_once
│   ├── publish_source_once
│   │     publishes work persisted by an earlier Runtime loop
│   ├── ReplicationIngress::poll_batch
│   └── persist_ingress_batch
│         makes the new publication work durable
└── step_ready_operators_bounded
    └── step_one_operator
        └── LoadedDataflow::step
            └── execute_operator_step
                └── crate::kernel::execute_step (dispatcher.rs)
                    └── linear/join/distinct/aggregate/window/topn/sink::execute
                        └── StepTxn::finish
```

`ingest_and_publish_once` tries pending publication before reading more WAL, so
a batch persisted near the bottom of one loop is normally published near the
top of the next. A committed output chunk then makes a downstream stage
runnable; the call tree is repeated once per bounded stage step until Sink
commits the corresponding result effects.

## 1. Small pure-Rust helpers

Start with [`src/postgres.rs`](../src/postgres.rs).

It parses and formats PostgreSQL LSNs and quotes identifiers. There are no
PostgreSQL pointers in this file. Look for:

- checked integer conversions;
- the difference between parsed values and display strings;
- specific error returns instead of fallback values;
- round-trip tests.

Run:

```bash
cargo test --lib postgres::tests
```

## 2. Parsing untrusted protocol bytes

Read [`src/pgoutput.rs`](../src/pgoutput.rs), then
[`src/replication.rs`](../src/replication.rs).

`pgoutput.rs` turns a borrowed byte slice into message enums.
`replication.rs` handles the libpq replication envelope and connection
lifetime.

Follow one INSERT from its tag byte to `Message::Insert`. Then follow an UPDATE
with `UnchangedToast`; ingress must receive a complete old and new row image.

Relevant Rust details:

- slice bounds are checked before indexing;
- protocol variants are enums, not string tags;
- the parser borrows bytes where ownership is unnecessary;
- `?` preserves the original error path;
- `Drop` releases libpq resources;
- `unsafe` is kept at the FFI boundary.

Run:

```bash
cargo test --lib pgoutput::tests
cargo test --lib replication::tests
```

The truncation tests cut valid messages at every byte offset. They verify that
short input returns an error rather than panicking.

## 3. The ingress state machine

Read [`src/ingress.rs`](../src/ingress.rs).

`ReplicationIngress::poll_batch` owns the in-memory state for the pgoutput
transaction currently being read. It returns one of:

- a bounded `IngressBatch`;
- `Pending`, when no complete work is available yet;
- `End`, when the replication connection ended.

Look at:

- `IngressBudget`;
- the row and wire-byte counters;
- `PendingBatch`;
- `IngressBoundary`;
- conversion of INSERT/DELETE/UPDATE into signed row effects.

Protocol-v2 transaction streaming is enabled. `StreamStart/StreamStop` batches
are durably staged under the first segment's WAL position but remain invisible
to the DAG. `IngressBoundary` records top-level Commit/Abort and subtransaction
Abort. Commit opens the publication gate and advances feedback with its
`end_lsn`. StreamAbort has no end LSN, so it is recorded durably but waits for a
later Commit to advance feedback; Abort leaves no effect for the DAG.

Run:

```bash
cargo test --lib ingress::tests
```

## 4. PostgreSQL Query to one owned plan

Read these files together:

1. [`src/ddl.rs`](../src/ddl.rs), starting at `inspect_ctas`
2. [`src/query_lowering.rs`](../src/query_lowering.rs)
3. [`src/logical/model.rs`](../src/logical/model.rs)
4. [`src/logical/validate.rs`](../src/logical/validate.rs)

`ddl.rs` receives PostgreSQL-owned `pg_sys::Query` pointers.
`query_lowering.rs` converts them immediately into owned Rust values:

- a stage ID is a `u32` equal to the stage's array position;
- `SlotId` identifies an output;
- `BindingId` identifies a local input;
- `SlotType` records type OID, typmod, collation, and nullability;
- `OperatorSpec` records one generic operator contract.

`DataflowPlan` is the only persisted plan. There is no second physical-plan
type and no runtime plan rewrite.

`validate.rs` checks the graph after lowering and after JSON reload. Serde
uses `deny_unknown_fields`, so removing a plan field is a clean cut: old JSON
does not silently acquire default behavior.

Useful Rust details:

- owned values leave the `unsafe` PostgreSQL pointer scope;
- `SlotId` and `BindingId` newtypes prevent mixing output and input identities;
- recursive enums represent scalar expressions;
- deterministic collections keep plan output stable;
- validation is separate from deserialization.

Run:

```bash
cargo test --lib query_lowering
cargo test --lib logical::
```

## 5. Compiling typed scalar SQL

Read [`src/scalar_sql.rs`](../src/scalar_sql.rs).

The persisted plan contains an AST and catalog OIDs, not user-provided SQL
fragments. The compiler:

1. resolves each OID again;
2. checks the trusted `pg_catalog` boundary and function properties;
3. quotes current identifiers;
4. renders typed constants and arguments.

`SqlBinding` maps a `BindingId` to a kernel-controlled table alias and column
name. This is why a kernel can generate dynamic typed SQL without concatenating
arbitrary query text.

The catalog is exposed through a small trait. Production uses SPI; unit tests
use a deterministic fake implementation.

## 6. Runnable state and the ready queue

Read:

1. [`src/logical/dataflow.rs`](../src/logical/dataflow.rs)
2. [`src/logical/runtime.rs`](../src/logical/runtime.rs)

The main types in `dataflow.rs` are:

- `OperatorId`;
- `InputFrontier`;
- `DurableOperatorState`;
- `WorkBudget`;
- `StepOutcome`;
- `ReadyQueue`.

The queue contains only operator IDs. PostgreSQL rows own input positions,
continuations, and output capacity. `ReadyQueue::rebuild` sorts runnable IDs,
so restart behavior does not depend on row-return order.

`runtime.rs` loads those durable facts with SPI and dispatches one stage. A
`LoadedDataflow` is only a validated plan cache plus ready queue; dropping it
does not lose progress.

Run:

```bash
cargo test --lib logical::dataflow::tests
```

## 7. The common step transaction

Read [`src/kernel/step.rs`](../src/kernel/step.rs).

`StepTxn` does not start a nested transaction. The background worker has
already opened one PostgreSQL transaction. `StepTxn::begin`:

1. applies the plan's execution settings;
2. locks input and output streams in stream-ID order;
3. locks the checkpoint and consumer cursors;
4. loads the checkpoint's shared admission row/byte counters;
5. checks output backpressure;
6. returns `Idle`, `Blocked`, or a ready `StepTxn`.

The kernel then uses `read`, `lock`, and `write` through this value.
`StepTxn::finish` updates the checkpoint only if its revision still equals the
value locked at step start. The kernel passes the continuation fact already
returned and validated by its bounded SQL primitive; the common path does not
issue a second `count(*)`.

Important invariant: after a kernel performs durable writes, it may finish
with `Progress`/`Yield` or return an error. It does not return `Idle`/`Blocked`
and commit partial work.

Read [`src/kernel/stream.rs`](../src/kernel/stream.rs) next. It contains the
shared chunk lookup, payload facts, output append, frontier append, and input
cursor advance operations.

## 8. Start with a stateless kernel

Read [`src/kernel/linear.rs`](../src/kernel/linear.rs).

The entry point handles Scan, Filter, and Project using the same control path.
Follow:

1. operator-spec validation;
2. current chunk and continuation loading;
3. input binding compilation;
4. one bounded set SQL statement;
5. output append;
6. input advance or continuation replacement;
7. `StepTxn::finish`.

This file shows the intended kernel boundary: Rust has the phase and validates
database facts; SQL performs typed set work over a bounded prefix.

Then read [`src/kernel/sink.rs`](../src/kernel/sink.rs). Sink has no output
stream. Its result-table DML and cursor/checkpoint changes still use the same
step transaction, which is the exactly-once boundary.

## 9. Read one high-fanout state machine

Read [`src/kernel/join.rs`](../src/kernel/join.rs).

Start with the Rust enums and structs, then inspect the generated SQL. The Rust
types represent:

- current input port;
- current input row;
- phase;
- opposite-side keyset cursor;
- match counters;
- continuation transition results.

Then follow `execute`. A high-fanout input row is not expanded into a Rust
`Vec` of all matches. Each step asks PostgreSQL for one bounded keyset prefix,
appends one output chunk, and persists the next cursor.

After Join, compare:

- [`distinct.rs`](../src/kernel/distinct.rs): exact SQL-key group state,
  a typed bag of physical representatives, then an immediate bounded Drain of
  the durable `-old,+new` effect queue;
- [`aggregate.rs`](../src/kernel/aggregate.rs): Apply into an input bag and
  dirty groups, then Drain rebuild and output replacement;
- [`window.rs`](../src/kernel/window.rs): Apply into partition state, then
  Drain through partition/frame/function phases;
- [`topn.rs`](../src/kernel/topn.rs): Apply into ordered state, then Drain its
  boundary and output diff.

For Aggregate, Window, and TopN, a completed input chunk may advance before
Drain starts. The durable Drain continuation therefore refers to typed state,
not to a consumed chunk. `StepTxn` keeps their common admitted row/byte count;
Aggregate keeps causal LSNs with dirty groups, Window with dirty partitions,
and TopN in its singleton control row. The counters are cumulative since the
last output frontier. Drain thresholds grow as `Q, 2Q, 4Q, ...` up to a fixed
interval cap; an ordinary Drain retains the counters, and forwarding the
frontier clears them in the same transaction.

Window aggregate Fold may visit several output ordinals in one step. It counts
frame-input rows/bytes, then charges one row plus the materialized
function/candidate bytes for each finalization. If an accumulator is complete
but finalization does not fit the remaining budget,
`WindowFoldCursor::ready_to_finalize` persists that exact state. A missing
frame relation row is a durable-state error, not an empty frame. Real empty
frames still consume work items and are capped at 64 ordinals per step.

Distinct is deliberately the exception: Apply pins its input chunk until its
per-page effect queue is empty. With a one-row output budget, one Drain step
publishes the `-old` retraction and a later step publishes the `+new` insertion;
neither admits later input or a frontier in between.

## 10. Registration and generated storage

Read [`src/kernel/register.rs`](../src/kernel/register.rs).

Registration walks the already validated `DataflowPlan` and creates:

- one typed payload relation per stream;
- one continuation relation per stage;
- typed state relations required by that operator;
- catalog rows that record relation and row-type OIDs.

The operator modules expose their own `provision` function, but common OID,
attribute, identifier, and payload checks are shared.

Then return to [`src/ddl.rs`](../src/ddl.rs). The CTAS hook:

1. lowers the query;
2. locks source tables in OID order;
3. asks PostgreSQL to create only the result schema;
4. registers the plan and generated storage;
5. copies the source snapshot into typed Scan bootstrap state.

This is the main `unsafe` area because PostgreSQL owns the utility-statement
pointers. The plan and registration contracts themselves are owned Rust data.

## 11. The one Runtime loop

Read [`src/worker.rs`](../src/worker.rs) by following calls from
`shiba_runtime_main`.

The loop performs:

```text
publish one pending source prefix
read one ingress batch when publication is drained
keep replication feedback alive while publication is backpressured
run a bounded round of ready operator steps
periodically GC durable staging
```

Every source publication and operator step is wrapped in
`BackgroundWorker::transaction`. A panic or error before the closure returns
aborts that transaction. A crash after return sees the committed cursor and
checkpoint on restart.

The outer loop has a time/step budget, but it cannot interrupt a PostgreSQL
statement. This is why each kernel's SQL primitive must enforce its own row
and byte bounds.

## 12. What remains in SQL files

Read the installation order in [`src/lib.rs`](../src/lib.rs):

| File | Responsibility |
| --- | --- |
| `sql/00_catalog.sql` | common durable catalog and constraints |
| `sql/10_runtime.sql` | activation and Runtime identity |
| `sql/11_ingress.sql` | ingress persistence and source publication |
| `sql/12_effect_stream.sql` | shared stream append/cursor/GC primitives |
| `sql/25_introspection.sql` | dataflow inspection |
| `sql/30_registration.sql` | SQL-visible registration boundary and grants |
| `sql/40_lifecycle.sql` | progress, index, drop, and deactivate APIs |

Operator phase machines do not live in PL/pgSQL. SQL files define catalog and
shared transactional primitives; Rust kernels generate the small typed set
operations needed for a particular step.

## Running checks

Use focused tests while reading:

```bash
cargo test --lib postgres::tests
cargo test --lib pgoutput::tests
cargo test --lib ingress::tests
cargo test --lib logical::
cargo test --lib kernel::
```

Run the complete gate before changing persistence, locking, continuation, or
recovery behavior:

```bash
./scripts/test-all.sh
```

Server-level gates and their invariants are listed in
[`TESTING.md`](TESTING.md).
