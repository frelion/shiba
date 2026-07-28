# Shiba

[![CI](https://github.com/frelion/shiba/actions/workflows/ci.yml/badge.svg)](https://github.com/frelion/shiba/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/frelion/shiba)](https://github.com/frelion/shiba/releases)
[![License: PostgreSQL](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](LICENSE)

Shiba is a Rust PostgreSQL extension for single-node, asynchronously maintained
streaming tables. Define a derived table with ordinary SQL; Shiba backfills it
once and then maintains it from committed WAL changes without refreshing or
rescanning the source table.

> Status: experimental v0.1. Shiba targets PostgreSQL 17 and intentionally
> supports a validated SQL subset. It is not yet a general-purpose streaming
> platform or a distributed execution engine.

## Why Shiba?

Traditional materialized views require a refresh that repeatedly reads the
source data. Shiba turns a PostgreSQL result table into an incrementally
maintained projection:

- source writes remain fast because maintenance is asynchronous;
- only committed changes are consumed from PostgreSQL logical decoding;
- result state, output rows, progress, and inbox acknowledgement commit
  atomically per source transaction;
- crash recovery is based on durable commit-LSN checkpoints and idempotent
  replay.

### Performance in user terms

The single-Runtime and shared-change-log design is being rebenchmarked. Older
per-DAG-worker and Router/Executor measurements do not describe this topology
and are not presented as a current SLA. Source commit latency, route lag, apply
lag, long-transaction blocking, and operator throughput are measured
separately because one large non-preemptible apply can delay all work in the
database-scoped Runtime.

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

## Supported query families

The current MVP supports typed Filter/Project, equality inner and outer joins,
semi/anti joins from `EXISTS` and `IN`, grouped `COUNT`/`SUM`,
`COUNT(DISTINCT)`, `HAVING`, top-level `DISTINCT`, ordered `LIMIT/OFFSET`, and
partitioned windows. The exact contract and rejected SQL shapes are documented
in [docs/MVP.md](docs/MVP.md).

## Architecture

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

The Rust layer owns PostgreSQL hooks, WAL decoding, plan validation, and exactly
one dynamic PostgreSQL background worker named `shiba runtime` per active
database. Routing, scheduling, DAG application, and garbage collection are
bounded phases in that one SPI-connected backend. A `DagRuntime` is cached plan
metadata, not a process or thread, so adding a result DAG does not allocate
another PostgreSQL worker or CPU resource. SQL functions own catalog state and
operator kernels. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the detailed execution and
recovery invariants.

Each decoded row delta is stored once in the shared `change_log`, even when
several DAGs consume it. `dag_inbox` stores only one lightweight reference per
DAG and source transaction. Scheduling is round-robin at atomic source-commit
boundaries; a large commit is not time-sliced and temporarily blocks routing
and other DAGs. A failed DAG is quarantined with its inbox reference retained
while healthy DAGs continue on later scheduling turns. Repair and retry are
explicit.

Registration compiles and persists a versioned `PhysicalDagPlan`. Its Stage
storage choices are part of the inspectable execution plan: fused expressions
stay inline, reusable relations inside one statement use `MATERIALIZED` CTEs,
and a relation that must cross statements may use a pre-created typed
`UNLOGGED` Stage. The current Join kernel uses statement-materialized input
deltas and one typed `join_delta` UNLOGGED Stage. Durable routing, inbox,
progress, result, arrangement, and operator-state tables remain logged and
authoritative. Stage contents are commit-scoped derived data; after a crash
they are rebuilt by replaying the retained `dag_inbox` reference and
`change_log` payload.

Normal apply performs no DDL and creates no temporary table. Inspect the
persisted plan and its materialized Stage relations with:

```sql
SELECT shiba.explain_physical('shiba.order_stats');
```

The Runtime is dynamically registered. After a postmaster restart, the next
registered-source statement or `SELECT shiba.activate()` restores it.

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
