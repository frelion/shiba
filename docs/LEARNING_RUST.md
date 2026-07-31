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

For one Runtime loop, this is the function-level path to keep in view:

```text
shiba_runtime_main
├── ingest_and_publish_once
│   ├── publish_source_once
│   │     publishes committed work persisted by an earlier Runtime loop
│   ├── ReplicationIngress::poll_batch
│   └── persist_ingress_batch
│         makes the new publication work durable
└── step_ready_operators_bounded
    └── step_one_operator
        └── LoadedDataflow::step_quantum
            └── crate::execution::execute_step (dispatcher.rs)
                └── KernelRunner::run
                    ├── StepContext::begin
                    ├── linear/join/distinct/aggregate/window/topn/sink::step
                    │   └── StepReceipt
                    └── StepContext::commit
```

`ingest_and_publish_once` tries pending publication before reading more WAL.
An open transaction's persisted batch remains staged; after Commit, its
publication work is normally picked up near the top of the next loop. A
committed output chunk then makes a downstream stage runnable; the call tree is
repeated once per bounded stage step until Sink commits the corresponding
result effects.

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

Read [`src/replication/pgoutput.rs`](../src/replication/pgoutput.rs), then
[`src/replication/transport.rs`](../src/replication/transport.rs).

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
2. [`src/planner/lowering.rs`](../src/planner/lowering.rs)
3. [`src/planner/model.rs`](../src/planner/model.rs)
4. [`src/planner/validate.rs`](../src/planner/validate.rs)

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

Read [`src/planner/scalar_sql.rs`](../src/planner/scalar_sql.rs).

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

## 6. Durable runnable state

Read:

1. [`src/planner/dataflow.rs`](../src/planner/dataflow.rs)
2. [`src/planner/runtime.rs`](../src/planner/runtime.rs)

`dataflow.rs` contains the small bounded-work values shared by Runtime and
kernels:

- `WorkBudget`;
- `WorkUsage`;
- `WorkQuantum`;
- `StepOutcome`;

There is no in-memory ready queue. The one readiness predicate in
[`src/worker.rs`](../src/worker.rs) reads checkpoints, input cursors and output
capacity directly from PostgreSQL. It is used both to find a runnable result
and to choose that result's next stage.

`LoadedDataflow` contains only a validated plan and a stage cursor used for
fair rotation. Dropping either value does not lose work; the next query finds
the same durable continuation or unread stream chunk.

Run:

```bash
cargo test --lib logical::dataflow::tests
```

## 7. The common kernel protocol

Read [`src/execution/runner.rs`](../src/execution/runner.rs), then
[`src/execution/step.rs`](../src/execution/step.rs).

Every operator registers one `KernelContract` and one bounded `step` function.
`KernelRunner` is the only code allowed to open a `StepContext` or commit its
checkpoint. `StepContext` does not start a nested transaction; the background
worker has already opened one PostgreSQL transaction. Its setup:

1. applies the plan's execution settings;
2. locks input and output streams in stream-ID order;
3. locks the checkpoint and consumer cursors;
4. loads the checkpoint's shared admission row/byte counters;
5. checks output backpressure;
6. returns `Idle`, `Blocked`, or a ready context.

The kernel uses `read`, `lock`, and `write` through the context, then returns a
`StepReceipt`. It never returns `StepExecution` and cannot commit the
checkpoint. The Runner publishes pending output and updates the checkpoint only
if its revision still equals the value locked at step start.

The checkpoint revision is the step commit's CAS guard and sequence, while its
continuation flag is the authoritative presence bit. The typed continuation
row owns the phase and resume cursor. Loading it requires the bit and row
presence to agree; replacement compares the old typed fields and records the
new presence in the context. Transition creation and final commit both reject
a mismatch.

Important invariant: after a kernel performs durable writes, it may finish
with `Progress`/`Yield` or return an error. It does not return `Idle`/`Blocked`
and commit partial work.

Read [`src/execution/stream.rs`](../src/execution/stream.rs) next. It contains the
shared chunk lookup, payload facts, output append, frontier append, and input
cursor advance operations.

## 8. Start with a stateless kernel

Read [`src/execution/linear/mod.rs`](../src/execution/linear/mod.rs), then its
`machine.rs`, `runtime.rs`, and `storage.rs` siblings.

The entry point handles Scan, Filter, and Project using the same control path.
Follow:

1. operator-spec validation;
2. current chunk and continuation loading;
3. input binding compilation;
4. one bounded set SQL statement;
5. output append;
6. input advance or continuation replacement;
7. return one `StepReceipt`.

This file shows the intended kernel boundary: Rust has the phase and validates
database facts; SQL performs typed set work over a bounded prefix.

Then read [`src/execution/sink/mod.rs`](../src/execution/sink/mod.rs) and its
`machine.rs`/`runtime.rs` siblings. Sink has no output
stream. Its result-table DML and cursor/checkpoint changes still use the same
step transaction, which is the exactly-once boundary.

## 9. Read one high-fanout state machine

Read [`src/execution/join/mod.rs`](../src/execution/join/mod.rs), then compare
`planner.rs`, `runtime.rs`, and `provision.rs`.

Start with the Rust enums and structs, then inspect the generated SQL. The Rust
types represent:

- current input port;
- current input row;
- phase;
- opposite-side keyset cursor;
- match counters;
- continuation transition results.

Then follow `step`. A high-fanout input row is not expanded into a Rust
`Vec` of all matches. Each step asks PostgreSQL for one bounded keyset prefix,
appends one output chunk, and persists the next cursor.

After Join, compare:

- [`distinct/mod.rs`](../src/execution/distinct/mod.rs): exact SQL-key group state,
  a typed bag of physical representatives, then an immediate bounded Drain of
  the durable `-old,+new` effect queue;
- [`aggregate/mod.rs`](../src/execution/aggregate/mod.rs): Apply into an input bag and
  dirty groups, then Drain rebuild and output replacement;
- [`window/mod.rs`](../src/execution/window/mod.rs): Apply into partition state, then
  Drain through partition/frame/function phases;
- [`topn/mod.rs`](../src/execution/topn/mod.rs): Apply into ordered state, then Drain its
  boundary and output diff.

For Aggregate, Window, and TopN, a completed input chunk may advance before
Drain starts. The durable Drain continuation therefore refers to typed state,
not to a consumed chunk. `StepContext` keeps their common admitted row/byte count;
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

Read [`src/execution/register.rs`](../src/execution/register.rs).

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

The source transaction protocol is [`src/publication.rs`](../src/publication.rs);
its one dynamic typed CTE delegates only durable stream append to SQL.
Bounded WAL admission follows the same boundary in
[`src/admission.rs`](../src/admission.rs): Rust classifies replay and allocates
counters, then validates the facts returned by one atomic data-modifying CTE.

## 12. What remains in SQL files

Read the installation order in [`src/lib.rs`](../src/lib.rs):

| File | Responsibility |
| --- | --- |
| `sql/00_catalog.sql` | common durable catalog and constraints |
| `sql/10_runtime.sql` | Runtime locks, triggers, and source validation primitives |
| `sql/11_ingress.sql` | ingress header and finalization primitives |
| `sql/12_effect_stream.sql` | shared stream append/cursor/GC primitives |
| `sql/25_introspection.sql` | dataflow inspection |
| `sql/30_registration.sql` | SQL-visible registration boundary and grants |
| `sql/40_lifecycle.sql` | progress, index, and generated-storage drop primitives |

Operator phase machines do not live in PL/pgSQL. SQL files define catalog and
shared transactional primitives; Rust kernels generate the small typed set
operations needed for a particular step. Database activation, Runtime launch
deduplication, and deactivation are ordered Rust transitions in
`src/lifecycle.rs`.

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
