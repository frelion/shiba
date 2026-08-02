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

M5.5 proves the runtime's explicit relation-OID binding behavior without adding
a source catalog table: rename remains valid and same-name drop/recreate fails
closed. Durable binding registration, DDL invalidation, source catalog rows,
and any multi-writer lifecycle remain unproved. See
[transaction and recovery](TRANSACTION_RECOVERY.md).

## M7.1 source ObjectAddress authority

`source_binding` stores one immutable relation ObjectAddress, each live
user-column ObjectAddress, and the current replica-identity index ObjectAddress
when configured. The private registration function inserts that complete set
atomically and is its only logical writer.
`source_invalidation` stores the exact matching address reported by PostgreSQL
event-trigger helpers; one event-trigger function is its only writer for both
`ddl_command_end` and `sql_drop`. Names and rendered object identities are not
stored or compared. Both tables remain inaccessible to ordinary roles.

M7.2 adds test evidence, not catalog state. The `sql_drop` invocation of the
same writer records a registered relation when it is dropped directly or by
schema CASCADE. PostgreSQL transaction rollback removes that fact; a committed
fact continues to name the old OID after a same-name table is recreated.

M7.3 keeps the same table and writer. Relation rows use `objsubid = 0`; column
rows use their positive PostgreSQL attribute number. The invalidation foreign
key therefore proves every durable cause was in the registered exact-address
set. Runtime relation locking explicitly selects the single zero-subid row.

M7.4 adds `binding_kind` because relation and index are both `pg_class` objects
with zero subid. Its closed values are `relation`, `column`, and
`identity_index`; this is not a dynamic kind registry.

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
