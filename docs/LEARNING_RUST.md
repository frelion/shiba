# Learn Rust by reading Shiba

Shiba is a real PostgreSQL extension, but its Rust side has one linear story:

```text
bytes -> messages -> committed changes -> plans -> one Runtime loop -> SQL
```

You do not need to understand PostgreSQL internals before starting. Read the
files below in order and stop after any chapter; each one teaches a useful
piece of Rust on its own.

## 0. Get a green baseline

Shiba requires PostgreSQL 17, its development headers, Rust, and
`cargo-pgrx 0.19.1`. After the setup in the main README, run:

```bash
cargo test --lib
cargo clippy --all-targets -- -D warnings
```

The unit suite includes real pgrx integration tests. If both commands pass,
your environment matches CI.

## 1. Start with a round trip

Open [`src/postgres.rs`](../src/postgres.rs).

This is the smallest complete module in Shiba. It quotes PostgreSQL identifiers
and converts a WAL position between `u64` and PostgreSQL LSN text. It
demonstrates:

- small pure functions;
- `Result<T, E>` and the `?` operator;
- integer conversion without hidden allocation;
- table-driven tests;
- why an encoder and decoder should be tested together.

Run only this chapter's tests:

```bash
cargo test --lib postgres::tests
```

Try adding another malformed LSN case before changing the parser. The test
gives you a safe place to learn pattern matching and error propagation.

## 2. Read bytes without panicking

Next open [`src/pgoutput.rs`](../src/pgoutput.rs), then
[`src/replication.rs`](../src/replication.rs).

`pgoutput.rs` converts untrusted WAL bytes into typed messages.
`replication.rs` owns the smaller libpq transport envelope. Together they show:

- slices and checked offsets;
- big-endian integer decoding;
- lifetimes at a C FFI boundary;
- RAII cleanup through `Drop`;
- error enums and `std::error::Error`;
- why protocol parsers test every truncation point.

The important rule is simple: validate a length before slicing. Search for
`require_length` and follow one message from its tag byte to its enum variant.

```bash
cargo test --lib pgoutput::tests
cargo test --lib replication::tests
```

## 3. Follow a state machine

Open [`src/ingress.rs`](../src/ingress.rs).

Ingress accepts replication messages, remembers transaction state, and emits a
bounded batch only when it is safe. This chapter demonstrates:

- modeling states with enums instead of boolean combinations;
- keeping mutable state behind a narrow API;
- separating transport from protocol logic;
- applying row and byte budgets without splitting an indivisible message.

Draw the states on paper, then compare them with `IngressPoll` and
`IngressFinalization`. If a transition is hard to name, it probably needs a
type or test.

```bash
cargo test --lib ingress::tests
```

## 4. Turn PostgreSQL trees into Rust types

Read [`src/query_tree.rs`](../src/query_tree.rs) only at its public boundary,
then move to [`src/query_analysis.rs`](../src/query_analysis.rs).

The first file contains the unavoidable unsafe PostgreSQL pointer adapter. The
second contains owned, ordinary Rust and turns an open analysis record into a
closed `ValidatedQuery`. Together they show:

- keeping `unsafe` at one boundary;
- replacing related booleans with enums and structs;
- `TryFrom`-style validation;
- making unsupported states fail before execution.

## 5. Compile data into data

Read the `src/logical/` directory in this order:

1. [`model.rs`](../src/logical/model.rs) — the persisted JSON contract;
2. [`compile.rs`](../src/logical/compile.rs) — metadata becomes a logical DAG;
3. [`validate.rs`](../src/logical/validate.rs) — invalid graphs fail closed;
4. [`physical.rs`](../src/logical/physical.rs) — logical nodes become stages;
5. [`runtime.rs`](../src/logical/runtime.rs) — the thin PostgreSQL bridge.

This part shows a scalable Rust design: persisted data types stay small,
construction, validation, lowering, and execution live in separate modules,
and each boundary returns a typed result.

```bash
cargo test --lib logical::tests
cargo test --lib logical::physical::tests
```

## 6. See the whole system in one loop

Finally open [`src/worker.rs`](../src/worker.rs) and begin at
`shiba_runtime_main`.

Do not read it top to bottom. Follow the loop's four phases:

1. `route_ingress_once`;
2. `ready_dag_oids`;
3. `apply_ready_dags_bounded`;
4. `gc_change_log`.

Every phase is bounded, and only this one backend mutates its runtime cache.
That single-owner rule is why the cache needs no thread synchronization. The
hard database work stays in set-oriented SQL under `sql/`.

At this point, read [`src/lib.rs`](../src/lib.rs). The module declarations and
ordered `extension_sql_file!` calls should now look like a table of contents.

## Where Rust stops and SQL starts

Shiba does not force row-oriented work into Rust:

| Rust owns | SQL owns |
| --- | --- |
| hooks and process lifecycle | durable catalogs |
| WAL transport and decoding | set-oriented operator kernels |
| query-tree validation | result and operator state |
| logical and physical plans | transactional acknowledgement |
| bounded scheduling | garbage collection queries |

That boundary is deliberate. Rust is used where types, byte safety, and state
machines matter; PostgreSQL is used where transactions and relational
execution matter.

## Good first contributions

Keep early changes observable and reversible:

- improve a parser or protocol error message and add its failing test first;
- replace duplicated PostgreSQL text handling with a helper plus round-trip
  tests;
- document one enum's states or one Runtime invariant;
- add a malformed protocol case;
- simplify a function while keeping its test names and outcomes unchanged.

Before submitting a change, run the focused tests while editing and the full
gate described in [`docs/TESTING.md`](TESTING.md) when execution, persistence,
or recovery behavior changes.
