# Shiba

[![CI](https://github.com/frelion/shiba/actions/workflows/ci.yml/badge.svg)](https://github.com/frelion/shiba/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/frelion/shiba)](https://github.com/frelion/shiba/releases)
[![License: PostgreSQL](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](LICENSE)

Shiba is a small streaming database engine you can read end to end. It is
written in Rust, runs inside PostgreSQL, and keeps SQL result tables updated
from committed WAL changes—no refresh and no source-table rescan.

> Status: experimental v0.1. Shiba targets PostgreSQL 17 and intentionally
> supports a validated SQL subset. It is not yet a general-purpose streaming
> platform or a distributed execution engine.

## The whole idea

```text
SQL declaration -> logical plan -> committed WAL -> row deltas -> result table
```

There is one background Runtime per database. It reads one durable change log,
schedules every result in turn, and commits result changes with its progress
checkpoint. That is the architecture; the rest of the project makes those
boundaries safe.

New to Rust? Follow the [guided Rust code tour](docs/LEARNING_RUST.md). It starts
with small round-trip helpers and builds up to binary protocols, state
machines, PostgreSQL FFI, and the Runtime loop.

## Why Shiba?

Traditional materialized views require a refresh that repeatedly reads the
source data. Shiba turns a PostgreSQL result table into an incrementally
maintained projection:

- source writes remain fast because maintenance is asynchronous;
- only committed changes are consumed from PostgreSQL logical decoding;
- result state, output rows, progress, and inbox acknowledgement commit
  atomically per source transaction;
- crash recovery is based on durable commit-LSN checkpoints and idempotent
  replay;
- the implementation is intentionally one process and one visible data path,
  so its correctness story can be inspected rather than assumed.

## Quick start

### Requirements

- PostgreSQL 17 with development headers and `pg_config`;
- Rust toolchain;
- `cargo-pgrx 0.19.1`.

Install the extension from a checkout:

```bash
cargo install cargo-pgrx --version 0.19.1
cargo pgrx init --pg17 /path/to/pg_config
cargo pgrx install --pg-config /path/to/pg_config
```

Configure and restart the target PostgreSQL server:

```conf
session_preload_libraries = 'shiba'
wal_level = logical
max_replication_slots = 4
```

Activate Shiba once per database:

```sql
CREATE EXTENSION shiba;
SELECT shiba.activate();
```

Then declare a streaming table:

```sql
CREATE TABLE public.orders (
  product_id integer NOT NULL,
  amount integer NOT NULL
);

CREATE TABLE shiba.order_stats AS
SELECT product_id,
       count(*) AS order_count,
       sum(amount) AS total_amount
FROM public.orders
GROUP BY product_id;
```

`shiba` is reserved for Shiba-managed result tables. Native PostgreSQL
materialized views keep their normal `REFRESH MATERIALIZED VIEW` behavior.
The complete SQL subset, permissions, and managed-index contract live in
[docs/MVP.md](docs/MVP.md).

## Supported query families

The current MVP supports typed Filter/Project, equality inner and outer joins,
semi/anti joins from `EXISTS` and `IN`, grouped `COUNT`/`SUM`,
`COUNT(DISTINCT)`, `HAVING`, top-level `DISTINCT`, ordered `LIMIT/OFFSET`, and
partitioned windows. The exact contract and rejected SQL shapes are documented
in [docs/MVP.md](docs/MVP.md).

## Read the architecture

```text
Analyzed PostgreSQL Query
  -> validated logical plan
  -> CTAS backfill and registration
  -> persisted versioned PhysicalDagPlan
  -> pgoutput logical replication
  -> one database-scoped shiba runtime
       -> bounded WAL routing
       -> shared durable change_log
       -> lightweight per-DAG inbox references
       -> round-robin DagRuntime scheduling
       -> set-oriented physical Stages
       -> bounded change-log garbage collection
  -> operator state and result table
```

Three invariants organize the implementation:

1. Rust owns PostgreSQL hooks, WAL decoding, plan validation, and the one
   database-scoped Runtime; SQL owns catalogs and set-oriented operator kernels.
2. A row delta is stored once in `change_log`; each result receives only an
   inbox reference to the source transaction.
3. Result state, progress, and inbox acknowledgement commit atomically. Failed
   results keep their replay input and are quarantined independently.

Inspect the persisted physical plan with:

```sql
SELECT shiba.explain_physical('shiba.order_stats');
```

See the [guided code tour](docs/LEARNING_RUST.md) for the reading order and
[detailed architecture](docs/ARCHITECTURE.md) for execution and recovery
invariants.

## Development and testing

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
./scripts/test-all.sh
```

The full correctness gate covers unit tests, real PostgreSQL 17 end-to-end
tests, differential checks, join behavior, concurrency, transaction recovery,
failpoint recovery, physical-Stage lifecycle, and the single-Runtime
architecture. Performance acceptance uses the complete unfiltered,
three-repetition matrix and a matched retained baseline; smoke or filtered
runs are diagnostic only. See
[docs/TESTING.md](docs/TESTING.md).

## Releases

Pushing a tag such as `v0.1.0` runs the release workflow. It builds a
PostgreSQL 17 installation package, generates SHA-256 checksums, and publishes
the artifacts to a GitHub Release. The process is described in
[docs/RELEASING.md](docs/RELEASING.md).

## License

Shiba is released under the [PostgreSQL License](LICENSE).
