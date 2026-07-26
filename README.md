# Shiba

Shiba is a Rust PostgreSQL extension for single-node, asynchronously maintained streaming tables.

`shiba` is a reserved result schema. `CREATE TABLE shiba.<name> AS SELECT ...` declares a Shiba-managed streaming table; source tables belong in other schemas. PostgreSQL's native materialized views are not intercepted or changed.

## Status

The asynchronous engine is installable and verified against a real PostgreSQL
17 cluster. It creates a per-database `pgoutput` logical replication slot and
publication, one WAL Router, and one worker per result DAG. The current
operators include typed Filter/Project, inner and outer equality joins,
semi/anti joins decorrelated from `EXISTS` and `IN`, grouped `COUNT`/`SUM`,
`COUNT(DISTINCT)`, `HAVING`, top-level `DISTINCT`, ordered `LIMIT`/`OFFSET`,
and partitioned windows. It never executes `REFRESH MATERIALIZED VIEW` and
does not scan a source table during incremental maintenance. The exact SQL
contract is specified in [docs/MVP.md](docs/MVP.md).

## Local prerequisites

- Rust toolchain
- PostgreSQL 17 development installation
- `cargo-pgrx 0.19.1`

Configure PostgreSQL so each client backend loads the DDL declaration hook, then reconnect:

```conf
session_preload_libraries = 'shiba'
wal_level = logical
max_replication_slots = 4
```

After installation, run the one-time database activation below. This is required because PostgreSQL forbids creating a logical replication slot inside the `CREATE EXTENSION` transaction; it does not create a streaming table through a function.

```sql
CREATE EXTENSION shiba;
SELECT shiba.activate();
```

The server must have capacity in `max_worker_processes` for one WAL Router plus one executor for each active Shiba result table.

## Development

```bash
cargo pgrx test pg17
./scripts/test-e2e.sh
```

The end-to-end script packages and installs the extension into an actual PostgreSQL 17 installation, starts an isolated database cluster, and runs SQL assertions against it. See [docs/TESTING.md](docs/TESTING.md).

For a code-oriented tour—from PostgreSQL's analyzed query tree through the
durable inbox and operator state—see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
