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
M11.2 replaces the WAL-only `EffectBatch.source_transaction` field with the
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

M11.2 replaces the former WAL-cause-shaped `applied_insert` directly, without
an alias or second table, with the sole `source_row_state`. Stable keyed rows are
unique by exact source and row key; admitted keyless rows use only a generated
internal state identity. WAL provenance remains in `source_continuation`, and
bootstrap provenance remains in `source_bootstrap`. Current rows store no
nullable or fabricated commit LSN, XID, or input sequence.

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

The implemented batch admits at most 10,000 rows and performs one bounded
set-based insert from parallel key/payload arrays. It constructs one
transaction-local `EffectBatch` tagged `EffectOrigin::Bootstrap`, evaluates the
existing CountRows and SumInt8 operators once, and advances the batch digest and
checkpoint in the same transaction. It creates neither per-row SQL round trips
nor retained effects.

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

M11.3 makes that reset explicit. The coordinator first acquires the same
per-source advisory ownership fence, revalidates the exact old bootstrap ID,
slot and generation, and permits replacement only from `creating`, `scanning`,
`cleanup_pending`, or `failed`. It then drops the exact inactive physical slot
through the replication connection. Only after PostgreSQL proves both the old
and requested new slot names absent may the single catalog writer atomically
clear partial `source_row_state`, reset private operator state, retire the exact
old bootstrap/config pair, and reserve a distinct BootstrapId with a strictly
larger generation. The existing reservation writer performs the final live
binding/publication validation and restores public results to building/NULL.
The result's internal active/zero normalization exists only inside this
uncommitted replacement transaction and can never become visible. A slot that
still exists, a continuation, an active result, publication drift, a stale
attempt, or any post-`scan_complete` phase rejects replacement without mutation.

After `scan_complete`, restart retains the existing slot and resumes bounded
M10 catch-up. A crash before active cutover leaves the result unavailable; a
crash after the cutover transaction observes the complete active result. If
cutover commits before fence feedback, `activation_end_lsn` remains the exact
durable authorization: restart replays the same fence, proves the same marker
and terminal end LSN, sends feedback, and never runs Runtime or republishes a
different value. If slot feedback already covers that terminal, restart can
enter live ingress without replay. Slot
creation and pre-active cleanup are explicit bootstrap lifecycle operations,
not ordinary M10 startup behavior. Cleanup must never infer a slot by name or
drop a slot owned by another attempt.

PostgreSQL restart never changes these choices. A pre-scan exported snapshot is
gone and therefore uses exact replacement; a `scan_complete`, `catching_up`, or
active attempt keeps its persistent slot and resumes from PostgreSQL's cursor.
Two bootstrap workers contend on the per-source advisory lock, so at most one
can create, replace, scan, catch up, activate, or acknowledge the source.

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
M11.2 implements the sole `source_bootstrap` authority, strong Bootstrap IDs,
tagged Effect origins, bounded set-based snapshot Apply, committed logical-
message fence, active cutover, and M10 live handoff. On PG17.10 and PG18.4 the
same real gate scans baseline `(1,10),(2,NULL),(3,30)` in batches of two. Private
state reaches CountRows/SumInt8 `3/40` while public values remain
`building/NULL`; one concurrent INSERT/UPDATE/DELETE transaction catches up to
`3/25`; the exact fence atomically publishes `active 3/25`; and a later M10 live
INSERT reaches `4/32`. The final current-row keys and nullable values equal the
source SQL oracle, only the two real WAL transactions create continuations, and
slot feedback covers each durable terminal.

M11.3 is green on PG17.10 and PG18.4. Its recovery gate reconstructs the
durable crash-after-reservation state (`creating`, exact slot absent), persists
`cleanup_pending`, replaces it with a new ID/generation, resets a partial scan,
and rejects stale generation and a foreign replacement slot. It also proves
exact batch replay, overflow rollback, advisory-lock competition,
`scan_complete` plus immediate PostgreSQL restart, catch-up Apply committed
before killed ACK, active cutover committed before killed ACK with exact-fence
replay, and feedback-covered active restart. The final source/current rows and
CountRows/SumInt8 equal the SQL differential `4/50`.

The test does not kill the process at the exact reservation instruction; it
reconstructs the identical committed authority state. An active foreign old-
slot conflict is not directly exercised.

M11.4 is green on PG17.10 and PG18.4 with limits frozen before measurement:
1,000,000 snapshot rows in batches of at most 10,000, scan within 120 seconds
and at least 10,000 rows/s, one exact 10,000-change concurrent WAL transaction
plus activation within 15 seconds, Rust RSS growth at most 256 MiB, exactly
three scan connections, and synchronous delivery with no queue. PG17 scans and
applies 100 batches in 3.098397625 s (322,747.47 rows/s), catches up and
activates in 1.320857542 s, and grows RSS from 10,160 to 13,824 KiB
(+3,664 KiB). PG18 takes 3.136067542 s (318,870.68 rows/s), 1.329330584 s, and
10,160 to 13,824 KiB (+3,664 KiB). SQL differential and ordinary M10 live
handoff remain exact.

Operationally, start bootstrap only for a pristine source with an absent exact
slot and valid publication binding. Treat `building` as unavailable. Monitor
phase, committed batch ordinal and the PostgreSQL slot independently; never
edit a checkpoint or infer ownership from a name. Before `scan_complete`, use
the explicit exact replacement operation after slot cleanup. At or after
`scan_complete`, retain the same slot and resume. Enter ordinary live ingress
only after active cutover and terminal feedback. Budget three connections per
scanning source and no more than one 10,000-row batch or one M10 transaction in
memory.

M11 is complete at this pristine nullable-int8 CountRows/SumInt8 boundary. It
does not complete V2, start M12, prove indefinite concurrent-writer catch-up or
tail latency, or provide a reconnect supervisor.

## M11.5 least-privilege acceptance

PG17.10 and PG18.4 prove the complete bootstrap with no production session run
as superuser or owner. Control/Apply/scanning uses a non-superuser
`NOREPLICATION` identity, transport uses a distinct non-superuser
`REPLICATION` identity, and the result reader can select only the public
result. Real snapshot scan, concurrent WAL catch-up, activation and live
handoff succeed. Swapped identities and missing `EXECUTE`, source `SELECT`, or
checkpoint `UPDATE` fail without partial state, continuation, result
publication, or feedback. Split-role successful `restart_abandoned`,
TLS/password policy, cross-host credential rotation, and column-level grants
remain unproved.

## M12 relationship

M12 reuses this scanner, batch identity, catch-up fence, activation and live
handoff for an active/non-pristine source; it does not weaken M11's pristine
rules. The target binding/config/new generation becomes the sole building
authority in one destructive prepare transaction before scanning. Old rows,
operator state and continuation are retired at that boundary, and old workers
cannot reuse their identities. A new slot's real exported snapshot supplies the
new boundary; no old continuation is copied or relabeled.

Activation only promotes that same target authority from building to active
and publishes its complete private state. There is no candidate binding,
second bootstrap implementation, parallel generation, birth marker, alias or
fallback. The full closed transition and residual physical-slot risk are in
[the M12 rebuild contract](REBUILD_CONTRACT.md). M12.1 freezes the contract;
M12.2 proves the exact admission/retirement transaction and M12.3 now proves
the production snapshot-to-live forward path. Crash recovery remains M12.4.

## M12.2 handoff into the bootstrap lifecycle

M12.2 reuses the sole `source_bootstrap` row and adds the closed
`rebuild_prepared` state. A successful prepare has already installed the target
BootstrapId, binding, publication, slot name and generation as the only catalog
authority, while preserving the exact retired old BootstrapId/slot/generation
for forward reconciliation. Public results are `building/NULL`, old current
rows and continuation are absent, operator private state is zero, and old
invalidations are retired in the same Apply transaction.

No exported snapshot exists at this phase. The old inactive physical slot is
still present and the named target physical slot is absent. M12.3 must first
reconcile those exact slot coordinates, then invoke the existing M11
reservation/exported-snapshot scanner and its bounded batches. It may not reset
to the old authority or synthesize snapshot/WAL identity. The target identity-
index OID is validated explicitly and becomes the fourth exact
`source_binding` row for an M12-produced generation; it adds no bootstrap
checkpoint or second authority. Pre-M12 active state remains the exact
three-row shape. The retired BootstrapId/slot/generation triple persists across
creating, scanning, catch-up, active and recoverable failure phases, so
recovery does not infer identity from current catalogs. Same-OID rename is the
only narrow reconciliation; replacement OID fails closed. The corrected gate
is green on PG17.10 and PG18.4.

## M12.3 proved reuse of the bootstrap path

`scripts/test-m12-rebuild-snapshot-live.sh` is green on PG17.10 and PG18.4.
After prepare, the coordinator retires the exact inactive old slot and obtains
a real exported snapshot from the new slot. It then calls the existing bounded
M11 scan, catch-up, fence, activation and live-handoff path. Concurrent
INSERT/UPDATE/DELETE are supplied only by WAL; no snapshot batch fabricates a
transaction identity and no old continuation is copied.

The public sink stays `building/NULL` until the activation transaction exposes
the complete target result. The same target identity remains installed, and
the retired triple remains after activation. Crash/restart behavior during
these steps remains M12.4 rather than an M12.3 claim.

## M12.4 lost-snapshot recovery

An exported snapshot is ephemeral. If an M12-produced `creating` or `scanning`
attempt cannot prove its original snapshot is usable, recovery cannot resume
that scan. It uses existing `restart_abandoned`, not a parallel bootstrap path:
partial bootstrap state retires transactionally, a fresh BootstrapId and
distinct slot are reserved, and generation advances by exactly one. The new
slot exports a new snapshot for the ordinary bounded M11 scanner.

The old active generation never returns and its continuation is never
relabelled. The same relation/key/payload/identity-index authority is read from
Catalog; marker-null M11 recovery remains unchanged. Readers stay on
`building/NULL` until ordinary catch-up and activation complete.
