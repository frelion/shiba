# Catalog contract

## Installation authority

The catalog extension installs one constrained row containing catalog and
protocol version `1`. This is the only Phase-1 durable authority. It belongs to
the PostgreSQL database in which `CREATE EXTENSION` executes, even if that
database hosts multiple application schemas. Names are not identity, and no
legacy catalog table, publication, or change log participates.

## Installation transaction

`CREATE EXTENSION` runs the extension SQL as one PostgreSQL transaction. The
schema therefore contains only operations that can be committed together:
create private/public schemas, create the identity table and constraints, insert
the singleton row, then expose a restricted read-only function. If installation
fails, PostgreSQL rolls back that transaction; the empty-install test must prove
there is no partial `shiba` or `shiba_internal` installation. Extension upgrade,
external side effects, and migrations are out of scope.

The extension does not perform remote calls, create replication resources, or
hold a cross-system lock. Retrying an installation error means first ending the
aborted transaction, then starting a complete new transaction.

**Not proved:** relation binding, DDL invalidation, source catalog rows, or any
multi-writer lifecycle. See [transaction and recovery](TRANSACTION_RECOVERY.md).
