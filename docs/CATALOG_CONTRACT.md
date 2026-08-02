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
installs source-row/continuation and operator authority tables without seeded
operators. If installation
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

M7.5 adds no catalog row. Its concurrent gate proves the relation lock orders
the existing Apply transaction before a conflicting DDL transaction; the DDL
transaction remains the sole writer of the resulting invalidation fact.

M8.1 reuses one independent binding set per source. The relation-kind row is
also the source-local processor mutex; registration and DDL invalidation writers
do not change. The count/result singleton is a global union aggregate, not a
second per-source authority.

M8.2 changes no catalog contract. The deterministic concurrency gate proves
each relation-kind row serializes only its owning source while the singleton
count/result remains the one aggregate commit point.

## M9.1 operator authority

The clean-room install contains `operator_definition`, `operator_state`, and
`shiba.operator_result`; it does not create or later drop old count tables.
Definitions freeze compiler version, source ID, operator kind, and nullable or
complete input ObjectAddress according to the kind. The registration adapter is
the sole definition writer and verifies source binding existence/invalidation.
Runtime alone updates state/result. State may be negative because SumInt8 is a
signed aggregate; CountRows enforces non-negativity in its pure evaluator.

Each public result has the same operator ID/kind as its definition through a
foreign key. Internal definition/state have no PUBLIC privileges; the public
sink grants SELECT only. Installation seeds no operator: definition, zero state,
and zero result are created atomically by explicit registration.

Later tuple slices alter only the existing current-state table. M5.1 adds
`payload_text` with int8/text mutual exclusion; it creates no fifth table,
source registry, change log, alias, or second writer.

## M10.4 source-ingress authority

`source_ingress_config` is the sole database-local ingress definition for a
source. Its private configuration writer locks an existing source binding,
rejects source invalidation, resolves the current database and exact
`pg_publication` OID, normalizes the publication's live column list to ordered
attribute numbers, and validates an existing inactive persistent logical
`pgoutput` slot in that database. It inserts the complete definition atomically
and never overwrites a duplicate source.

The publication ObjectAddress is durable identity. Its name, insert/update/
delete/truncate/via-root flags, and normalized attribute numbers are a frozen
semantic snapshot and transport locator, not a name-based identity fallback.
The admitted publication has exactly the bound relation, no row filter, all
required change operations, and no publish-via-root policy. Drift in any frozen
field is fail closed.

`source_ingress_invalidation` is the persistent publication-history fact. The
existing single DDL event writer owns it as well as source invalidation. Exact
drop addresses cover publication removal; on every DDL command end it compares
each configured OID and snapshot against the live catalogs. This scan is
necessary because a real PostgreSQL 17 `ALTER PUBLICATION ... DROP TABLE`
produced no command ObjectAddress. A committed membership/flag/column/filter/
name change, remove-then-add, drop, or same-name recreation therefore cannot
revive the old configuration. Rollback rolls back its invalidation too.

The private slot-rotation writer is a pristine-only generation CAS. It requires
the expected generation, locks the row, rejects invalidated or non-pristine
sources and active/current slots, validates a different existing inactive
`pgoutput` slot in the same database, then changes slot and increments generation
once. It never creates or drops a physical slot. Binding rebuild remains a
separate unimplemented lifecycle.

PostgreSQL `pg_replication_slots` remains physical slot/progress authority.
The catalog deliberately contains no `confirmed_flush_lsn`, receiver PID,
active flag, connection secret, transport status, WAL spool, or cursor mirror.
Ingress config/invalidation are inaccessible to PUBLIC, and ordinary startup is
not a definition writer.

Source Ingress is a transport owner, not another state writer. Governance
revalidates catalog/live publication and slot state before receive, Apply, and
every ACK; an invalidation cannot be bypassed by an empty commit. Ingress cannot
write current rows, operator state/results, or continuation.

## M11.2 bootstrap authority

M11.2 adds exactly one private `source_bootstrap` checkpoint/lifecycle
authority for a pristine source. Its identity is a never-reused `BootstrapId`;
it binds the exact source, slot generation and name, immutable
`consistent_point`, lifecycle, and committed scan progress. It stores neither
the ephemeral `snapshot_name` nor dynamic slot progress and is not a second WAL
cursor, continuation, row log, or EffectStream.

The bootstrap coordinator is its sole writer. Runtime remains the only writer
of current source rows and operator state/result. A batch's row/operator writes
and checkpoint advance share one transaction. Public results must represent
building/unavailable as `result_status = building, value_bigint = NULL`;
complete values require `result_status = active` and become visible with active
lifecycle in one cutover transaction. M11.2 replaces the WAL-cause-shaped
`applied_insert` with the sole key-owned `source_row_state`; no alias or second
current-row table remains.

Before `scan_complete`, a failed hidden attempt may be fully removed only by
the explicit pristine bootstrap owner and restarted with a fresh never-reused
attempt and slot/snapshot. After `scan_complete`, the checkpoint directs
recovery to the existing-slot catch-up phase. Ordinary M10 startup still cannot
create or drop slots. Active/non-pristine reset or generation rebuild is M12,
not an M11 cleanup path.

The implemented schema contains the closed lifecycle, exact ingress foreign
key, immutable consistent point, latest batch ordinal/key/digest, unique writer
fence token, and catch-up fence LSN. It stores no snapshot name, WAL payload, or
moving slot cursor. PG17/18 prove initial reservation, building visibility, two
bounded batch checkpoints, fence activation, WAL-only continuation, and
ordinary live handoff. M11.3 crash/reset/resume and M11.4 million-row
performance remain unproved; M11 and M12 are not complete.
