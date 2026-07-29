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

## How it works

When you declare a Shiba result table, Shiba validates the query, computes the
initial rows, and saves a plan for maintaining them. That happens once.

After that, follow one ordinary source-table commit:

```text
INSERT / UPDATE / DELETE
          │ COMMIT
          ▼
    PostgreSQL WAL
          │
          ▼
  shared change_log
          │
          ▼
 per-result inbox task
          │
          ▼
 saved plan + SQL kernels
          │
          ▼
      result table
```

Each database has one Shiba background process called the Runtime. PostgreSQL
logical decoding has its own walsender; Shiba adds no Router or worker pool.
The Runtime reads committed WAL, saves each source transaction once in the
change log, and gives every affected result table a small inbox task. It then
processes those tasks in turn.

For one task, the Runtime updates operator state, result rows, and progress,
then removes the inbox task—all in one PostgreSQL transaction. If it crashes,
WAL is replayed, the inbox task remains, or the whole update rolls back. It can
therefore retry without rescanning the source table.

The [architecture walkthrough](docs/ARCHITECTURE.md) follows this exact path
step by step and then explains registration, recovery, and limits.

New to Rust? Follow the [guided Rust code tour](docs/LEARNING_RUST.md). It starts
with small round-trip helpers and builds up to binary protocols, state
machines, PostgreSQL FFI, and the Runtime loop.

## Why Shiba?

Traditional materialized views require a refresh that repeatedly reads the
source data. Shiba turns a PostgreSQL result table into an incrementally
maintained projection:

- source writes remain fast because maintenance is asynchronous;
- result updates consume committed deltas instead of rescanning source tables;
- one Shiba worker and one visible queue path make the implementation readable
  end to end.

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

## Development and testing

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
./scripts/test-all.sh
```

The complete correctness and performance gates are documented in
[docs/TESTING.md](docs/TESTING.md).

## Releases

Pushing a tag such as `v0.1.0` runs the release workflow. It builds a
PostgreSQL 17 installation package, generates SHA-256 checksums, and publishes
the artifacts to a GitHub Release. The process is described in
[docs/RELEASING.md](docs/RELEASING.md).

## License

Shiba is released under the [PostgreSQL License](LICENSE).
