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

In the current reproducible benchmark environment, a single 5,000-row commit
for a simple grouped `COUNT`/`SUM` result completed source commit in about
17 ms, and the result caught up in a median of about 196 ms. The batch apply
path processed about 25,500 changed rows/second, compared with about 5,850
rows/second for the previous executor path (4.36x faster for this scenario).

These are benchmark results, not a universal SLA: the fast path currently
applies to ordinary single-source aggregates. Join, TopN, Window, DISTINCT,
and high-cardinality workloads have different cost profiles.

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

Configure the target PostgreSQL server and reconnect clients:

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
  -> pgoutput logical replication
  -> WAL Router
  -> durable per-result inbox
  -> DAG executor
  -> operator state and result table
```

The Rust layer owns PostgreSQL hooks, WAL decoding, plan validation, and
background workers. SQL functions own catalog state and operator kernels. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the detailed execution and
recovery invariants.

## Development and testing

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
./scripts/test-all.sh
```

The full correctness gate covers unit tests, real PostgreSQL 17 end-to-end
tests, differential checks, join behavior, concurrency, transaction recovery,
failpoint recovery, and executor architecture. See
[docs/TESTING.md](docs/TESTING.md).

## Releases

Pushing a tag such as `v0.1.0` runs the release workflow. It builds a
PostgreSQL 17 installation package, generates SHA-256 checksums, and publishes
the artifacts to a GitHub Release. The process is described in
[docs/RELEASING.md](docs/RELEASING.md).

## License

Shiba is released under the [PostgreSQL License](LICENSE).
