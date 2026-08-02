# M11 consistent bootstrap contract

## Scope and boundary

M11 initializes one pristine nullable-`int8` source for the existing CountRows
and SumInt8 operators, then hands the same source to M10 live ingress. It does
not rebuild an active binding. Active/non-pristine rebuild and generation
replacement remain M12.

The only admitted snapshot/WAL boundary is PostgreSQL replication-protocol
`CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT`. Its
`consistent_point` is the earliest position from which the new slot can stream,
and its `snapshot_name` identifies a database snapshot containing exactly the
state before those streamed changes. Shiba must not synthesize either value or
substitute `pg_export_snapshot`, a separately created slot, a wall-clock time,
or a sampled WAL position.

`snapshot_name` is an ephemeral capability, not durable identity. It remains
valid only while the exporting replication connection is idle: that connection
must execute no new command and must remain open until each scanner has imported
the snapshot. Shiba never stores the name as a recoverable catalog fact.

## Identities and authority

`BootstrapId` belongs to a new, never-reused initialization attempt.
`BootstrapBatchId` is the deterministic pair of that attempt and its batch
ordinal. Neither is a `SourceTransactionId`; a snapshot batch has no source
commit LSN, must not fabricate one, and must not write `source_continuation`.
Only real decoded WAL transactions enter the existing M10 continuation domain.
M11 replaces the WAL-only `EffectBatch.source_transaction` field with the
closed `EffectOrigin::Wal(SourceTransactionId)` or
`EffectOrigin::Bootstrap(BootstrapBatchId)` union. Operators still consume only
row effects; Runtime validates the tagged origin before either write path.

One database-local `source_bootstrap` fact is the checkpoint and lifecycle
authority. It binds the bootstrap attempt to the exact source, slot generation,
slot, and immutable `consistent_point`; records only committed scan progress;
and owns the closed `creating`, `scanning`, `scan_complete`, `catching_up`,
`active`, `cleanup_pending`, and `failed` transitions. It is not a WAL cursor
or EffectStream log. PostgreSQL's slot
remains the transport cursor authority, and M10 `source_continuation` remains
the WAL computation/replay authority.

The checkpoint shape is limited to `source_id`, `BootstrapId`, exact slot and
generation, `consistent_point`, closed lifecycle phase, last batch ordinal,
last stable source key, last-batch digest, and one immutable catch-up fence.
It never stores `snapshot_name`, received WAL, decoded changes,
`confirmed_flush_lsn`, or `restart_lsn`.

The existing `applied_insert` has become current-row state but still carries a
WAL-cause-shaped key. M11 must replace it, without an alias or second table,
with the sole `source_row_state` keyed only by exact source and stable row key.
WAL provenance remains in `source_continuation`; bootstrap provenance remains
in `source_bootstrap`. Snapshot rows therefore need no nullable or fabricated
commit LSN/XID fields.

The bootstrap coordinator is the sole lifecycle/checkpoint writer. Runtime
remains the sole writer of current source rows and operator state/result. For
each batch, current-row inserts, both operator updates, and the corresponding
checkpoint advance share one short Apply transaction and commit or roll back
together. Effects remain transaction-local and are never persisted.

## Scan and handoff

One short catalog transaction first reserves a never-reused attempt, exact slot
name, and pre-active generation. `source_ingress_config` remains the sole exact
database/source/publication binding; `source_bootstrap` references it rather
than copying publication identity. The slot name must still be absent, and M10
attach is denied while bootstrap is `creating` or `scanning`. The explicit
bootstrap owner then creates that exact persistent slot and records its returned
boundary before scanning. Slot create/drop cannot share the catalog transaction,
so every retry reconciles exact attempt ownership and uses `cleanup_pending`;
it never discovers, adopts, or drops a slot by name alone.

After slot creation, each bounded scan batch uses a fresh short
`REPEATABLE READ READ ONLY` transaction. Its first transaction command imports
the exact exported snapshot with `SET TRANSACTION SNAPSHOT`; no query may run
first. The exporter remains idle across all imports. Batches scan stable
single-column keys in deterministic order and carry explicit
`BootstrapBatchId`s through the same current-row, EffectBatch, Operator, and
Result Sink contracts used by Runtime. A retry of an exact batch is a no-op or
commits once; it cannot create a second row or contribution.

When the final batch and checkpoint commit atomically, the coordinator records
`scan_complete`. It may then release the exported snapshot and start the
existing bounded M10 receiver at the slot's exact `consistent_point`. Snapshot
time INSERT/UPDATE/DELETE is therefore absent from the snapshot or present in
it according to PostgreSQL MVCC, and every later committed change is consumed
once from the same slot. There is no interval filled by a guessed LSN.

After scan completion, recovery continues catch-up from that existing slot; it
does not recreate the snapshot or reset the build. Catch-up retains M10's one
outstanding transaction, terminal ACK authorization, decoder, continuation,
and direct backpressure. No WAL queue, spool, second decoder, second
continuation, or durable EffectStream is permitted.

The public result has a closed `building | active` status and a nullable bigint:
`building` requires NULL and `active` requires a value. During scan and catch-up
only private `operator_state` changes. Once the receiver has durably processed
through the fixed catch-up fence, one PostgreSQL transaction copies both locked
private states into public results and marks them and the bootstrap lifecycle
active. Later changes continue through the single M10 Result Sink.

The catch-up fence is not a sampled WAL position or keepalive `wal_end`. After
`scan_complete`, the bootstrap owner emits one transactional PostgreSQL logical
message with an exact Shiba prefix and content bound to the `BootstrapId`.
M11 catch-up temporarily requests pgoutput `messages=true`; the existing
assembler must classify only that exact committed `M` transaction as a
`BootstrapFence(end_lsn)` terminal authorization. Unknown, malformed, mixed,
replayed-for-another-attempt, or user-spoofed messages fail closed. The fence is
not a `SourceTransaction`, does not enter Runtime or continuation, and cannot
ACK source WAL by itself; cutover is allowed only after every earlier terminal
outcome was durably handled in stream order and this exact fence arrives. The
first M11 slice admits one bootstrapping source, so cross-slot fence routing is
outside M11.

## Crash and cleanup

Before `scan_complete`, loss of the exporter, a scanner, Shiba, or PostgreSQL
invalidates the attempt's snapshot. The hidden partial attempt is fully reset,
its explicitly owned bootstrap slot is cleaned up, and retry creates a fresh,
never-reused `BootstrapId`, slot, pre-active generation, snapshot, and boundary.
A persisted snapshot name or checkpoint must never be used to resume against a
different snapshot.
This reset is safe only because M11 admits a pristine, never-active source and
no partial result was public.

After `scan_complete`, restart retains the existing slot and resumes bounded
M10 catch-up. A crash before active cutover leaves the result unavailable; a
crash after the cutover transaction observes the complete active result. Slot
creation and pre-active cleanup are explicit bootstrap lifecycle operations,
not ordinary M10 startup behavior. Cleanup must never infer a slot by name or
drop a slot owned by another attempt.

## Identity and resource governance

Every create, batch, catch-up, ACK, cutover, reset, and cleanup step must
revalidate the exact M10 source binding, database, publication OID and frozen
publication semantics, persistent invalidation, slot, and generation. A rename,
membership change, remove/re-add, drop/recreate, or generation mismatch fails
closed. Names remain locators, never identity.

Scanning uses exactly three connections per bootstrapping source: the idle
snapshot-export/replication connection, one short read-only scan connection,
and one short-transaction Apply connection. After scan completion the scan
connection is released and catch-up/live use M10's exact two connections. No
implicit pool or channel is allowed. A scan transaction ends after one bounded
batch; Apply locks and transactions are never held while scanning, waiting for
network input, or waiting for WAL.

## Current evidence boundary

M11.1 freezes this contract from PostgreSQL 17/18 official replication-slot,
exported-snapshot, and `SET TRANSACTION SNAPSHOT` semantics. The paired live
gate proves exact four-field slot creation, repeated short imports around real
INSERT/UPDATE/DELETE, `confirmed_flush_lsn = consistent_point`, snapshot-token
expiry after the exporter's next command, and no Shiba durable write. PG18 also
proved that `snapshot_name` is opaque and may contain hexadecimal letters.
Production bootstrap state, scanning, reset, fence/cutover, permissions, crash
tests, SQL differential tests, million-row boundedness, heap measurements, and
performance gates are not yet implemented or proved. M11 is not complete.
