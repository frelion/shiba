# Shiba

[![CI](https://github.com/frelion/shiba/actions/workflows/ci.yml/badge.svg)](https://github.com/frelion/shiba/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/frelion/shiba)](https://github.com/frelion/shiba/releases)
[![License: PostgreSQL](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](LICENSE)

Shiba is a PostgreSQL 17 extension that keeps SQL result tables updated from
committed WAL changes, without rescanning the source tables after each write.

> Status: experimental v0.1. Shiba supports a validated SQL subset and is not
> a distributed execution engine.

## Architecture

```mermaid
flowchart LR
    TX["source transaction"] -->|"COMMIT"| WAL["PostgreSQL WAL"]
    WAL --> WS["pgoutput / walsender"]

    subgraph PG["PostgreSQL relations"]
        LOG[("shared change_log")]
        BATCH[("stable input ranges")]
        INBOX[("per-result dag_inbox<br/>batch cursor")]
        PLAN[("PhysicalDagPlan")]
        SCRATCH[("current-batch<br/>UNLOGGED scratch")]
        STATE[("operator state")]
        RESULT[("result table")]
    end

    subgraph R["one Shiba Runtime per active database"]
        DECODE["decode + bounded batching"]
        ROUTE["find affected results"]
        SCHEDULE["round-robin scheduler"]
        APPLY["run the saved SQL plan"]
    end

    WS --> DECODE
    DECODE --> LOG
    LOG --> BATCH
    BATCH -->|"pgoutput Commit is durable"| ROUTE
    ROUTE --> INBOX
    INBOX --> SCHEDULE
    SCHEDULE --> APPLY
    PLAN -->|"read-only"| APPLY
    APPLY --> SCRATCH
    SCRATCH -->|"every batch commits"| STATE
    SCRATCH -->|"every batch commits"| RESULT
    STATE <-->|"read / write"| APPLY
    APPLY -->|"advance batch cursor"| INBOX
```

Ingress normalizes DML into weighted rows: insert is `+1`, delete is `-1`, and
update is `-1 old / +1 new`. It stores a source transaction once in
`change_log` and records stable input ranges as it reads. The Runtime schedules
the saved DAG only after the source `Commit` is durable.

The scheduler reads one stable range per apply transaction. That transaction
updates authoritative operator state, the result table, and the per-result
batch cursor together. Each successful batch is immediately visible; a large
source transaction is deliberately not one result-visibility boundary. The
last range additionally advances apply progress and removes the inbox entry.
If one source transaction affects results A and B, their cursors advance
independently. Replication feedback advances after durable ingress,
including pgoutput `Commit` finalization, independently of result application.

The detailed [architecture](docs/ARCHITECTURE.md) documents the stream model,
operator state, scheduling, recovery, and resource bounds.

The [Rust code tour](docs/LEARNING_RUST.md) traces the implementation from
protocol parsers and state machines to PostgreSQL FFI and the Runtime loop.

## Design constraints

- Result maintenance is asynchronous and does not run inside the source
  transaction.
- PostgreSQL owns durable state and set-oriented SQL execution; Rust owns
  protocol decoding, validation, planning, and scheduling.
- Each active database has one Shiba Runtime. PostgreSQL logical decoding uses
  a separate walsender.
- Per-result source-commit and batch order is strict. Different results are
  scheduled round-robin between ingress batches.
- A running PostgreSQL statement is not preempted. High-fanout Join and large
  Window/TopN work can still block the single Runtime.

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
