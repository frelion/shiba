# Testing

Shiba's execution tests use PostgreSQL 17. State layout, locking, WAL decoding,
crash behavior, and generated SQL are not tested against an in-memory database.

## Complete gate

```bash
./scripts/test-all.sh
```

The gate is intentionally explicit. Adding a new kernel requires adding its
real PostgreSQL test script to `test-all.sh`; a new test must not silently
replace an existing layer.

The current order is:

1. `scripts/test-clean-cut.sh`
2. `cargo fmt --all -- --check`
3. `cargo clippy --all-targets -- -D warnings`
4. `cargo test --lib`
5. `scripts/test-effect-stream-core.sh`
6. `scripts/test-replication-ingress.sh`
7. `scripts/test-stateless-kernels.sh`
8. `scripts/test-fanout-recovery.sh`
9. `scripts/test-aggregate-distinct-kernels.sh`
10. `scripts/test-window-topn-kernels.sh`

Each server-level script creates its own temporary data directory and Unix
socket. It never connects to the developer's normal database cluster. The
extension artifacts are installed into the selected PostgreSQL 17
installation, so do not run two `cargo pgrx install` jobs against the same
installation concurrently.

## What each layer proves

### Clean-cut guard

`test-clean-cut.sh` scans code, SQL, tests, and documentation for removed
types, functions, tables, GUCs, and filenames. This is a structural test: a
change cannot reintroduce an old decoder branch, catalog, alias, or execution
path just to make an old caller pass.

### Rust and pgrx tests

`cargo test --lib` covers:

- checked pgoutput and replication parsing, including every truncated byte
  boundary;
- unchanged-TOAST reconstruction for UPDATE;
- ingress state transitions and row/byte batching;
- `DataflowPlan` validation and exact JSON contracts;
- trusted scalar SQL generation from catalog OIDs;
- durable readiness selection and work-budget behavior;
- DDL hook, registration, catalog, and lifecycle integration through pgrx.

`cargo clippy --all-targets -- -D warnings` is required because test-only and
pgrx-only code must meet the same lint level as the extension library.

### EffectStream

`test-effect-stream-core.sh` exercises the stream protocol directly:

- typed payload and chunk metadata commit together;
- producer and consumer compare-and-swap positions;
- data and frontier ordering;
- fanout with one payload and multiple cursors;
- high/low row, byte, and chunk backpressure;
- bounded GC at the slowest consumer;
- binary row-byte accounting that is stable across TOAST storage and retains
  non-default array dimensions.

### Replication ingress

`test-replication-ingress.sh` starts a real logical slot and walsender. Required
cases include:

- INSERT, DELETE, and UPDATE as weighted row images;
- streamed top-level rollback remaining invisible and replaying after restart;
- a large source transaction admitted in many bounded batches;
- forced walsender termination while a streamed prefix is durable, followed by
  exact replay and publication after reconnect;
- immediate postmaster restart while a streamed prefix is durable, ensuring
  the crash-aborted source transaction releases its ingress admission budget;
- concurrent streamed writers sharing admission accounting, with exact-once
  publication and configured source-chunk bounds;
- streamed batches hidden from source streams until pgoutput `Commit`;
- top-level and subtransaction aborts producing no DAG effects;
- header-only Commit finalization;
- source publication crash before and after commit;
- open transactions not blocking known commits, while pending sealed
  transactions still block the global frontier;
- replay-safe feedback and bounded metadata/payload GC;
- shared source-stream fanout;
- unchanged TOAST values surviving an update that changes only another column;
- raw pgoutput per-column text surviving typed validation without a lossy
  typed-to-JSONB rewrite.

Set `SHIBA_INGRESS_LARGE_TX_ROWS` to increase the real large-transaction case:

```bash
SHIBA_INGRESS_LARGE_TX_ROWS=100000 \
  ./scripts/test-replication-ingress.sh
```

### Scan, Filter, Project, and Sink

`test-stateless-kernels.sh` verifies:

- CTAS uses `skipData` and returns with an empty result plus a durable typed
  Scan bootstrap;
- a large bootstrap drains over multiple committed steps;
- source locks prevent a snapshot/live-WAL gap;
- an empty source completes bootstrap correctly;
- Filter and Project evaluate typed PostgreSQL expressions;
- an all-filtered chunk still advances the causal frontier;
- wide TOAST output respects byte accounting;
- Sink crash before and after commit is exact-once;
- multiple active results with different generated composite types do not
  share a cached prepared parameter type;
- drop, source schema change, and new CTAS use only the new schema.

### Join fanout

`test-fanout-recovery.sh` must cover:

- inner, left, right, full, semi, anti, and null-aware anti joins;
- duplicates, NULLs, theta predicates, delete, and reinsert;
- one input row producing many output rows in bounded action chunks;
- checkpoint revision and transaction counts growing by chunk, not by output
  row;
- continuation resume before and after commit;
- output high/low backpressure;
- chained high-fanout Joins followed by fan-in and Sink;
- nonempty CTAS bootstrap followed by single-sided live updates;
- NaN payload canonicalization across bootstrap and pgoutput;
- non-1 array lower bounds across bootstrap, live insert, and live delete;
- output frontier safety across both input ports.

### Aggregate and Distinct

`test-aggregate-distinct-kernels.sh` must exercise catalog-driven aggregates,
not a list of hard-coded aggregate names:

- grouped and global aggregation;
- multiple aggregate expressions;
- FILTER, DISTINCT, and aggregate-local ORDER BY;
- top-level Distinct multiplicity boundaries;
- SQL-equal but physically distinct numeric representatives, including a
  same-page replacement recovered with its one-row `-old` and `+new` Drain
  legs still queued;
- delete-driven group rebuild;
- large groups and distinct sets resumed by typed keyset continuation;
- geometrically spaced Apply/Drain epochs for a hot group, followed by fixed
  bounded intervals at the cap;
- crash before/after commit and downstream backpressure;
- aggregate capability rejection for unsafe transition/final ABIs.

### Window and TopN

`test-window-topn-kernels.sh` must cover:

- multiple typed partition and order expressions;
- NULL ordering, peers, and both sort directions;
- supported PostgreSQL frame modes and multiple window functions;
- one update affecting a large partition over bounded committed steps;
- large LIMIT/OFFSET/WITH TIES output diff over bounded steps;
- Window and TopN chains;
- dirty input retained across idle gaps and Drain scheduling that survives a
  Runtime restart;
- crash before/after commit, delete/update, and backpressure;
- a check that step count grows by chunks rather than by emitted rows.

## Failure rules

Server-level gates fail on unexpected PostgreSQL `WARNING`, `ERROR`, `FATAL`,
or `PANIC`. A failpoint case may allow only the exact crash record it armed.
Every wait and blocking operation has a timeout.

A crash test must inspect durable state on both sides of the transaction
boundary:

```text
before commit: state + output + cursor + continuation all rolled back
after commit:  checkpoint and cursor identify the next exact prefix
```

Comparing only the final result is insufficient. Tests also inspect stream
cursors, checkpoint revisions, continuation rows, chunk counts, backpressure,
and generated state relation identities.

## Focused development loop

Run the narrowest relevant command while editing:

```bash
cargo test --lib pgoutput::tests
cargo test --lib ingress::tests
cargo test --lib logical::
./scripts/test-effect-stream-core.sh
./scripts/test-stateless-kernels.sh
```

Run `./scripts/test-all.sh` before handing off changes to planning, persistence,
operator state, recovery, or lifecycle code.
