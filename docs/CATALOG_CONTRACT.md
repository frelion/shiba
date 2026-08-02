# Catalog contract

## Installation authority

The catalog extension installs one constrained row containing catalog and
protocol version `1`. This is the only Phase-1 durable authority. It belongs to
the PostgreSQL database in which `CREATE EXTENSION` executes, even if that
database hosts multiple application schemas. Names are not identity, and no
legacy catalog table, publication, or change log participates.

## Installation transaction

`CREATE EXTENSION` runs the extension SQL as one PostgreSQL transaction. It first
creates the schemas, constrained identity, and restricted version function, then
installs the four M2 tables with zero-valued count state/result. If installation
fails, PostgreSQL rolls back the whole installation; the empty-install test must
prove there is no partial `shiba` or `shiba_internal` state. Extension upgrade,
external side effects, and migrations are out of scope.

The extension does not perform remote calls, create replication resources, or
hold a cross-system lock. Retrying an installation error means first ending the
aborted transaction, then starting a complete new transaction.

**Not proved:** relation binding, DDL invalidation, source catalog rows, or any
multi-writer lifecycle. See [transaction and recovery](TRANSACTION_RECOVERY.md).

## M2 execution facts

The extension installs exactly four M2 tables. Three private tables own applied
INSERT causes, deterministic count state, and committed continuation history.
`shiba.count_result` is the SQL-queryable Result Sink projection. These are
separate facts with one logical writer and one commit point, not alternative
authorities for the same decision. PUBLIC receives only result `SELECT`; all
internal access and result mutation remain revoked.

Later tuple slices alter only the existing current-state table. M5.1 adds
`payload_text` with int8/text mutual exclusion; it creates no fifth table,
source registry, change log, alias, or second writer.
