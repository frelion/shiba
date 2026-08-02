# ADR 0002: M11 consistent snapshot-to-WAL bootstrap

Status: accepted; PG17/18 semantic gate complete, production implementation pending

## Decision

M11 uses only replication-protocol
`CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT`. The returned
`consistent_point` and exported `snapshot_name` form one PostgreSQL-owned
boundary: the imported snapshot shows the database state after which all slot
changes are included. See the PostgreSQL
[17 logical-decoding explanation](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html),
[18 replication protocol](https://www.postgresql.org/docs/18/protocol-replication.html),
and [17 `SET TRANSACTION`](https://www.postgresql.org/docs/17/sql-set-transaction.html).

The exporter stays connected and executes no further command while every
bounded scanner opens a short `REPEATABLE READ READ ONLY` transaction and
imports that exact snapshot before its first query. Repeated imports permit
batch commits without a long Apply transaction. After the atomically committed
final scan checkpoint, the exported snapshot may be released and M10 starts at
the exact `consistent_point`.

Cutover cannot use a sampled LSN or keepalive `wal_end`. M11 will emit one
transactional logical message after `scan_complete` and temporarily request
pgoutput `messages=true`. Only an exact Shiba prefix/content bound to the active
`BootstrapId`, inside an otherwise exact committed transaction, may produce a
`BootstrapFence(end_lsn)`. It is a transport fence, not a source transaction,
continuation entry, or general admission of pgoutput `M`.
The frame and opt-in behavior follow PostgreSQL's
[logical message format](https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html),
[pgoutput `messages` option](https://www.postgresql.org/docs/17/protocol-logical-replication.html),
and [`pg_logical_emit_message`](https://www.postgresql.org/docs/18/functions-admin.html).

## Why this boundary

An independently exported SQL snapshot plus a separately sampled WAL position
has a gap or overlap that Shiba cannot prove away. An existing logical slot
cannot retroactively export its creation snapshot. A snapshot batch also is not
a PostgreSQL source transaction, so assigning it a synthetic commit LSN would
mix bootstrap retry with WAL replay and corrupt continuation semantics.

Accordingly, M11 introduces separate, strongly typed `BootstrapId` and
`BootstrapBatchId` values, a tagged `EffectOrigin`, and one `source_bootstrap`
lifecycle/checkpoint authority. The WAL-shaped columns of the current
`applied_insert` table cannot accept snapshot rows, so M11 replaces that sole
table with key-owned `source_row_state`, without an alias or parallel table.
It introduces no second WAL, cursor mirror, continuation, Effect log, or
decoder. PostgreSQL remains slot authority; M10 remains WAL ingress; Runtime
remains the row/operator/result writer.

## Failure decision

An exported `snapshot_name` is valid only until the exporting replication
connection executes another command or closes. It is therefore ephemeral and
never a durable recovery handle. Before `scan_complete`, loss of that snapshot
requires full deletion of the hidden pristine attempt and explicit cleanup of
its exactly owned slot, followed by a fresh never-reused attempt and new
slot/snapshot. M11 allows this only before first activation. Rebuilding an
active/non-pristine source remains M12.

After `scan_complete`, recovery does not restart the scan: it resumes catch-up
from the same slot. Snapshot row/operator/checkpoint writes are atomic per
batch; public results stay building/unavailable; catch-up uses M10's terminal
authorization and continuation unchanged; active cutover publishes the two
complete results atomically.

## Operational consequences

Ordinary M10 attach still cannot create or drop a slot. Slot creation and
pre-active cleanup are explicit bootstrap operations with exact attempt
ownership. M10 publication ObjectAddress, frozen membership, durable
invalidation, and generation checks guard every bootstrap phase.

Scan uses exactly three connections per source: exporter/replication, scanner,
and Apply. Catch-up/live return to M10's two. Scanner transactions are short
and read-only; Apply transactions are batch-local; neither Apply locks nor an
Apply transaction survive a scan, network wait, or WAL wait. There is no
unbounded queue or persisted WAL spool.

## Not decided or proved here

The test-only libpq gate proves the exported snapshot boundary on PG17 and PG18,
including repeated imports around concurrent DML and expiry on the exporter's
next command. This ADR does not implement catalog tables, scanner, exact fence,
or cutover code. Crash/reset, least privilege, differential correctness,
million-row memory/performance, and WAL-retention bounds remain M11 gates. It
does not authorize M12 binding rebuild, SQL frontend work, or a claim that M11
or V2 is complete.
