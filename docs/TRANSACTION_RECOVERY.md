# Transaction and recovery contract

## Proven transaction owners

PostgreSQL's `CREATE EXTENSION` transaction still owns installation: either the
complete constrained installation exists or none of it does.

M2's processor owns the complete transaction for one committed source input. It
checks continuation history, writes applied INSERT facts, updates private count
state, updates the public result, and writes continuation last before one
`COMMIT`. No intermediate fact is visible and no asynchronous repair exists.

M3.1 decoding precedes that transaction and owns no durable state. A truncated,
unsupported, mismatched, or uncommitted pgoutput buffer yields no
`SourceTransaction`, so `process` cannot run and continuation cannot advance.
Only a fully validated pgoutput COMMIT supplies the stable commit LSN and BEGIN
XID used by M2 identity.

The stable identity is `(source_id, slot_generation, commit_lsn,
ingress_transaction_id)`. The database primary key reserves the first three as
the source coordinate; a different ingress identity at that coordinate fails
closed. Exact identity replay returns `AlreadyApplied` without writes. A new
coordinate must remain in the one admitted source/generation and increase LSN.

## Failure and replay

After an error the complete processor transaction is rolled back. Operator
failure must leave applied facts, state, result, and continuation at their old
values. A test-only trigger terminates the backend after continuation INSERT and
before COMMIT; reconnection must observe the same complete old state. Exact
replay after a successful but unacknowledged commit must not increment again.

M2 promises atomic database-local replay, not external exactly-once effects. It
has no recovery worker, concurrent source writer, slot-generation transition,
or compare-and-swap protocol. Future concurrency/effect contracts must define
those boundaries before code lands. DDL invalidation must use PostgreSQL
`ObjectAddress` semantics rather than name matching.

M3.2 freezes a receiver after it has observed a complete transaction but before
feedback. The processor commits the transaction while `confirmed_flush_lsn`
still names the prior position, then the receiver is killed. Restart replays the
same identity and the processor returns `AlreadyApplied`; result, operator state,
Apply facts, and continuation stay unchanged. Thus retry starts at the PostgreSQL
slot, but Shiba commit visibility remains authoritative. The CLI, publication,
and slot are test infrastructure, not a second Shiba continuation.

## M10 transport recovery boundary

Source Ingress holds no PostgreSQL Apply transaction while receiving WAL. It
assembles one bounded transaction, invokes the existing synchronous Runtime on
a separate connection, and may advance feedback only after Runtime's database
transaction has committed as `Applied` or has proved the exact identity already
committed as `AlreadyApplied`.

```text
slot -> receive -> decode -> Runtime transaction -> commit -> feedback
          X          X              X                X
       replay     replay        rollback         replay/no-op
```

A crash before Apply produces no feedback. A crash after Apply commit but
before feedback produces exact replay and `AlreadyApplied`. A requested
keepalive reports the last durable end position rather than the newest received
WAL position. Decoder, Apply, or Operator failure stops at the failed
transaction. PostgreSQL retains transport history through the slot;
`source_continuation` independently prevents duplicate computation. Neither
authority mirrors or repairs the other.

M10.2 proves each committed window on real PG17 and PG18. Dropping the receiver
after `receive_one` leaves all Shiba state and slot progress old. Restart then
applies once. Dropping it after Runtime commit but before `acknowledge` leaves
the result visible while the slot remains old; restart receives the same
transaction, Runtime returns `AlreadyApplied`, and only that durable token can
advance feedback. Decoder and Operator failures poison the connection, roll
back computation, and retain the old slot position until a clean restart and
successful retry.

The committed feedback coordinate is pgoutput COMMIT `end_lsn`, not
`commit_lsn`, outer `wal_end`, or keepalive `wal_end`. `PQputCopyData` alone did
not advance the live slot in the first failing test; production now explicitly
flushes libpq output, and an independent SQL connection polls the slot to the
exact terminal coordinate. Requested keepalive replies are observed with the
previous durable coordinate in all three walsender status fields.

M10.3 keeps protocol-v2 segments volatile and outside Runtime until a complete
matching stream commit. Stream stop (`E`) is only a segment boundary: it cannot
Apply or ACK. On terminal commit, the existing streamed decoder revalidates the
complete single-XID, key-only INSERT transaction and Runtime uses the same
row/operator/result/continuation transaction and COMMIT `end_lsn` ACK rule as
protocol v1.

A matching terminal abort never enters Runtime and creates no row,
operator/result, or continuation. Protocol-v2 `A` carries no feedback LSN, so
its safe coordinate is the outer XLogData `dataStart` that carries the complete
abort. Only an exact private abort token can report that coordinate. Disconnect
or crash before commit/abort feedback discards the bounded assembly, reports
nothing, and lets the PostgreSQL slot replay. There is no durable spool or
second recovery authority. Corruption, unknown/mismatched XID, wrong terminal,
16 MiB overflow, or more than 10,000 decoded changes fails closed without
Apply or ACK.

An empty protocol-v2 commit also bypasses Runtime, but only for the exact
`S(first=true) E (S(first=false) E)* c` form: at least one complete empty
segment, one nonzero top-level XID, flags zero, valid commit/end coordinates,
no change frame, and no trailing byte. The
receiver exposes a private `EmptyCommitted(end_lsn)` authorization and requires
an explicit empty acknowledgement. It does not create a continuation because
no source transaction exists. This is safe only as a statement about the
selected publication's output, not source identity. M10.4 therefore revalidates
the exact publication OID, frozen semantic snapshot, durable invalidation, slot,
and generation before empty acknowledgement. Any historical or current drift
rejects the token and leaves feedback old.

The empty recognition state remains constant inside the 16 MiB bound. A legal
`R` or `I` makes the commit non-empty and sends its complete bytes to the sole
Runtime decoder; it cannot fall back to the empty path. Every other grammar or
semantic error fails closed.

Thus the only feedback authorizations are Runtime `Applied`, Runtime
`AlreadyApplied`, strict `EmptyCommitted`, and legal terminal `Aborted`. No WAL
position or nonterminal message can substitute for one of those proofs.

## M10.4 lifecycle recovery boundary

Ingress configuration is durable but transport progress is not duplicated.
`source_ingress_config` freezes the current database, publication ObjectAddress
and semantics, exact source binding, existing slot name, and generation.
`source_ingress_invalidation` permanently records committed publication drift.
PostgreSQL's slot owns `confirmed_flush_lsn`; `source_continuation` owns compute
replay. Neither table stores or repairs the other's cursor.

Attach takes two explicit connections. The Apply connection first obtains the
source's session advisory lock, reads catalog and live authorities in a
read-only repeatable-read snapshot, and requires the slot inactive. The separate
replication connection attaches; the Apply connection then revalidates the same
config with that slot active. A failed step drops connections and releases the
session lock without changing config, slot, continuation, or results. Waiting
for WAL holds no Apply transaction or row lock.

Receive, Apply, and each of `acknowledge`, `acknowledge_empty`, and
`acknowledge_abort` revalidate governance. Thus publication drift after attach
but before delivery or feedback cannot advance the slot. In particular, a
structurally valid empty commit is not sufficient after membership removal,
remove/re-add, drop/recreate, or semantic drift. The pending transport token is
not converted into another authority; recovery restarts from the unchanged
slot position after configuration is explicitly rebuilt.

Slot rotation is allowed only while the source has no continuation or current
rows. A row lock and expected-generation comparison serialize contenders; a
stale caller fails, while one successful caller selects a different existing
inactive `pgoutput` slot in the same database and increments generation once.
No slot is created or dropped. Any non-pristine source requires the future
binding-rebuild lifecycle, so old generation computation cannot be relabeled as
new history.

The PG17/18 governed restart gate proves detach releases both active slot and
session ownership, after which the same catalog generation reattaches. It also
receives and durably applies 10,000 streamed changes under split least-privilege
roles. Publication remove/re-add after an empty token is received causes the
subsequent empty ACK to fail; operator result and slot position remain at their
last durable values. Automated reconnect/backoff and cancellation during
Runtime Apply remain outside this synchronous recovery boundary.

M10's cooperative idle shutdown checks a process-local handle between 100 ms
socket-poll intervals. The transport must attempt asynchronous
`PQgetCopyData` before polling: libpq may already hold a complete `CopyData`
even when the socket is no longer readable. After no buffered data remains,
`PQsocketPoll` and `PQconsumeInput` refill libpq. This ordering fixed the first
failure-oriented backlog run without adding a queue or alternate transport.

PG17/18 interrupt idle receive in 42.262/76.950 ms, return only
`ShutdownRequested`, preserve row/operator/result/continuation and slot LSN,
and allow detach/reattach. A transaction already handed to Runtime still obeys
Runtime's atomic commit/rollback rules, but cancellation requested during that
Apply is not implemented. Neither are automatic reconnect or retry backoff.

Relation metadata recovery is scoped to the live replication connection. A
constant-size `PgoutputRelationState` admits omission only after an exact `R`
for the same source was validated on that connection; repeated `R` is always
checked. Restart begins with empty relation state and therefore requires fresh
metadata before changes. No relation frame cache, spool, second decoder, or
durable recovery authority exists.

M4.1 payload presence/value is inserted into the existing Apply row before the
operator and continuation writes. Invalid tuple tags or shapes fail during pure
decode and cannot open a database transaction. A PostgreSQL error after payload
Apply still rolls back payload, count, result, and continuation together under
the unchanged M2 transaction owner.

M4.2 empty rows use the same cause primary key as keyed rows. Allowing a NULL
`source_row_id` does not weaken replay identity: exact replay is still decided
by committed transaction identity, and all empty Apply facts roll back with the
operator, result, and continuation.

M4.3 composite uniqueness is checked by PostgreSQL inside the processor-owned
transaction. A conflicting pair rolls back every fact; exact transaction replay
still short-circuits at continuation before any key write.

M4.4 mutates an existing single-key Apply row before writing continuation. A
missing row fails closed. Backend termination after continuation INSERT proves
that payload, count state, result, and continuation all remain at their prior
committed values. Retry applies the UPDATE once; exact replay then returns
`AlreadyApplied` before mutation. Slot feedback remains outside this authority.

M4.5 uses that unchanged owner and ordering for DELETE. The processor deletes
exactly one current-state row, checks and decrements count without underflow,
publishes the identical public count, then records continuation last before the
single COMMIT. A missing row or underflow aborts the whole transaction, so row,
private count, public result, and continuation all retain their old values.
Invalid `D + K` bytes fail even earlier during pure decode.

A backend termination after continuation INSERT and before COMMIT proves the
same four durable observations remain old. Retrying the same decoded DELETE
then removes and decrements once; replaying that exact committed transaction
returns `AlreadyApplied` without touching row state or counts. Continuation is
therefore the transaction replay authority, while the table later renamed
directly to `source_row_state` is the sole current-row authority; neither a
DELETE log nor a recovery writer exists.

M4.6 adds no transaction or recovery writer. Replica identity and key flags are
validated by the pure decoder before it can return a `SourceTransaction`; a
live FULL-identity `D + O` transaction therefore cannot open the processor
transaction. Apply row, private count, public result, and continuation all stay
at their last committed values, and the pipeline must stop rather than skip the
unadmitted source transaction.

M5.1 treats pgoutput `u` as an update instruction against the existing durable
text row, never as replacement data. The processor verifies that row shape,
retains `payload_text`, writes the unchanged count/result, and inserts
continuation last in its existing transaction. Backend termination after that
continuation INSERT proves the text, count, result, and continuation all remain
old; retry advances once and exact replay is a no-op.

M5.2 applies a complete `t` replacement under the same ordering. A crash after
continuation INSERT exposes the previous text and previous continuation; retry
publishes the replacement once, and replay cannot replace it again. The source
TOAST table never participates in the processor transaction or recovery path.

M5.3 uses the same DELETE ordering for a two-component key. Decode failure
writes nothing; a missing pair or backend termination rolls back pair deletion,
count, result, and continuation. Retry removes the pair once, and exact replay
short-circuits before row lookup.

M5.4 changes no recovery ordering. An admitted index-identity DELETE uses the
same row/count/result/continuation transaction, including crash rollback, retry
once, and exact replay. A RELATION carrying default identity after live drift
fails in the pure decoder, so no processor transaction opens and the prior
continuation remains authoritative.

M5.5 also changes no transaction owner. Rename transactions affect only the
source catalog and a later admitted INSERT commits through the existing Apply
path. After same-name drop/recreate, relation-OID mismatch fails during pure
decode; row state, count/result, and continuation retain their last committed
values. Recovery of a future durable binding registry is not claimed.

M6.1 keeps streamed segments outside PostgreSQL durable Shiba state. Truncation,
abort, or XID mismatch cannot return a transaction, so continuation cannot pass
an incomplete stream. After a complete stream commit, processor crash rollback,
retry once, and exact replay are identical to the existing transaction path.
M6.1 did not yet prove production receiver restart; M10 now does so by dropping
volatile assembly and relying on slot replay. A separate persisted partial-
stream recovery authority remains deliberately absent.

M6.2 observes real streamed segments before source ROLLBACK, then real `A`.
After feedback covers the abort, receiver restart on the same slot emits a new
committed stream; only that transaction can write row/count/result/continuation.
M10.3 later proves pre-feedback abort replay through the production receiver.
Persisted partial-stream recovery remains deliberately absent.

M7.1 orders each non-replay step as relation ObjectAddress lookup, relation
`ACCESS SHARE` lock, exact invalidation check, Apply, result, and continuation.
Conflicting DDL either waits for the processor commit or completes first and
publishes invalidation in its own transaction. A DDL rollback rolls its
invalidation back too. A narrow rename/drop race during lock acquisition may
return a PostgreSQL error rather than `SourceInvalidated`, but still precedes
all Shiba writes. Exact committed replay remains a no-op without relocking.

M7.2 applies the same ownership rule to removal. A pending decoded source
transaction remains applicable after `DROP TABLE` rollback because both the
catalog removal and invalidation roll back. After committed DROP or schema
CASCADE, the exact old ObjectAddress invalidation is durable; pending work fails
before Apply, and same-name recreation cannot change that recovery decision.

M7.3 registers relation and live-column addresses in one transaction. A rolled
back column-type change rolls back invalidation and leaves pending work valid.
A committed type change or column rename writes one exact bound cause; the
processor's any-bound-address check rejects pending work before all Shiba writes.

M7.4 freezes the current replica-identity index in that same registration
transaction. Index rename rollback removes invalidation and retains the OID;
committed rename records the exact index address. Runtime still locks the source
relation and rejects any bound-object invalidation before Apply.

M7.5 observes the live lock order: Apply holds granted `AccessShareLock` while
a conflicting DDL waits for `AccessExclusiveLock`. No result or invalidation is
visible while blocked. Releasing the test-only pause lets Apply commit first,
then DDL commits invalidation; the next pending transaction fails before writes.

M8.1 serializes each source on its relation-binding row. A fast replay probe
preserves the historical no-lock path; a second probe after the mutex closes the
concurrent duplicate race. Continuation ordering and fixed generation are
source-local, while row changes and the global aggregate still commit together.
A backend crash on source 2 leaves source 1 and the aggregate at their old values.

M8.2 starts two exact duplicate calls before either commits. The second waits on
the source mutex, then the post-lock replay probe returns `AlreadyApplied`; no
duplicate row, count, result, or continuation is written. A transaction for a
different source commits while source 1 is paused, proving recovery isolation.

M8.3 rejects a borrowed pgoutput buffer above 16 MiB before parsing and rejects
a 10,001st change before decoding or appending it. Either path returns no
`SourceTransaction`, never opens the processor transaction, and therefore
cannot modify row state, count, result, or continuation. Exactly 10,000 changes
remain inside the existing atomic Apply/replay boundary.

M8.4 applies the same 10,000-change ceiling to every constructor and checks it
again at the processor boundary. Even a forged public value with an exact
already-committed identity returns `TransactionLimitExceeded` before replay
lookup or `Client::transaction`; durable row state, count, result, and
continuation therefore remain unchanged. Admitted work remains synchronous, so
database blocking propagates to the caller rather than accumulating in a
Runtime-owned queue.

M9.1 keeps PostgreSQL as the sole transaction owner while replacing the fixed
calculation. Exact replay still returns before Source Apply. For new work,
Source Apply locks and reads existing rows as needed, writes each mutation once,
and builds a non-durable EffectBatch. Runtime then locks operator state in
ascending operator-ID order, evaluates pure CountRows, publishes state/result,
and inserts continuation last. Any row-image, operator, state/result, database,
or crash failure rolls the row mutation and every operator write back together.

Registration has its own single PostgreSQL transaction: binding lock,
invalidation check, descriptor construction, pure compilation, definition,
zero state, and zero result either all commit or all disappear. A failed or
duplicate registration cannot become a partially executable operator.

M9.2 applies CountRows and SumInt8 to the same transaction-local EffectBatch.
The performance gate observes ascending operator update order with a test-only
trigger: operator 1 is attempted before operator 2 raises. PostgreSQL then
rolls back that audit row, the applied source row, both operator state/result
writes, and continuation together. Exact replay of the successful 10,000-row
transaction performs none of those writes.

The fixed M9.2 scenario separately kills the backend after operator 1's public
result update. Reconnection observes the old current rows, both old operator
states/results, and old continuation; retry applies both operators once. An
overflow in operator 2 has the same rollback boundary. Same-source processors
serialize on the existing binding row and acquire operator states in ascending
ID order; a paused source does not prevent an unrelated source from committing.

## M11.2–M11.3 bootstrap recovery boundary

The only initial-copy boundary is a new logical slot created with
`EXPORT_SNAPSHOT`. PostgreSQL returns an immutable `consistent_point` and an
ephemeral `snapshot_name`; short read-only repeatable-read scanners import the
name before their first query while the exporter remains idle. Snapshot batches
carry `BootstrapId`/`BootstrapBatchId`, never a fabricated source transaction or
commit LSN, and never advance `source_continuation`.

One `source_bootstrap` checkpoint advances in the same short Apply transaction
as the batch's current-row and operator writes. A failure before commit exposes
neither; an exact retry contributes once. Public results remain building/
unavailable, so a committed private batch is not partial publication.

Catch-up cannot declare completion from a keepalive or sampled WAL position.
It must observe the exact committed logical-message `BootstrapFence` for the
current attempt after all earlier terminal outcomes were durably handled. The
fence never enters Runtime or `source_continuation`; malformed, mixed, stale,
foreign, or otherwise unknown `M` input fails closed.

Before `scan_complete`, connection, Shiba, or PostgreSQL loss destroys the
ephemeral snapshot. Recovery fully resets the hidden pristine attempt and its
exactly owned slot, then creates a fresh never-reused attempt/slot/snapshot.
Persisting the snapshot name or resuming its checkpoint under another snapshot
is forbidden. After `scan_complete`, recovery instead retains the existing slot
and resumes M10 catch-up from its `consistent_point`. A crash before cutover
leaves results unavailable; cutover atomically publishes complete operator
results and active lifecycle.

Pre-scan reset has two deliberately separate owners. The coordinator holds the
source advisory fence and uses the replication transport to drop only the exact
inactive physical slot from the exact configured database. It then invokes one
catalog transaction with the old BootstrapId/slot/generation and the distinct
new BootstrapId/slot/larger generation. That writer rejects an existing old or
new slot, continuation, active/non-building result, post-scan phase, stale
identity, or publication drift. On success it deletes hidden partial current
rows, resets private operator state, retires the exact old bootstrap/config,
and calls the existing reservation validator. Any error rolls all catalog,
row, state, and result changes back; it cannot recreate or drop a slot.

The crash windows are closed as follows:

- before slot creation, or after a failed create: the exact creating/
  cleanup-pending attempt has no usable snapshot and is replaced;
- before a scan batch commit: no rows, operator state, or checkpoint from that
  batch exist; after commit, exact batch replay is a no-op;
- after any pre-scan process or PostgreSQL restart: the exported snapshot is
  not recoverable, so partial hidden state is reset under a new attempt;
- after `scan_complete` or during catch-up: retain the same persistent slot and
  resume M10 terminals; never rescan or replace the attempt;
- after active cutover but before fence feedback: exact fence replay matches
  both `catchup_fence_lsn` and `activation_end_lsn`, then feedback advances;
  if feedback already covers the terminal, restart enters live directly;
- competing/repeated starts: only the advisory-lock owner can mutate lifecycle
  or manage the exact slot; losers fail without catalog or slot mutation.

Every phase retains M10's exact binding/publication/generation/invalidation
checks. Scan has three connections and batch-local Apply transactions;
catch-up/live have two. No Apply transaction or lock survives scan/network/WAL
wait, and no WAL spool, queue, second continuation, or persisted EffectStream
exists.

M11.2 now proves the non-crash path on PG17 and PG18. Batches of two commit
snapshot state to private `3/40` with public building/NULL; one real concurrent
WAL transaction advances private state and its sole continuation to `3/25`;
the exact committed fence transaction publishes active `3/25`; and ordinary
M10 live ingress commits and acknowledges `4/32`. No snapshot batch writes a
continuation or duplicates a current row.

M11.3's PG17.10/PG18.4 gate proves the recovery boundary above: reconstructed
durable creating/slot-absent restart, cleanup-pending exact replacement,
partial-scan reset, stale/foreign rejection, exact replay, overflow rollback,
worker conflict, immediate PostgreSQL restart after `scan_complete`, catch-up
Apply commit before killed ACK, active cutover before killed ACK with exact
fence replay, and feedback-covered active restart. Final source/current rows,
CountRows and SumInt8 match the SQL oracle `4/50`. The gate reconstructs rather
than instruction-level kills the post-reservation durable state and does not
directly exercise an active foreign old-slot conflict.

M11.4 proves the bounded recovery path on PG17.10 and PG18.4 with 1,000,000
rows, 100 batches of 10,000, one concurrent 10,000-change WAL transaction,
exact SQL differential, and live handoff. Scan+Apply take
3.098397625/3.136067542 s, catch-up+activation 1.320857542/1.329330584 s, and
Rust RSS grows 3,664/3,664 KiB. These pass the pre-observation 120 s, 10,000 rows/s,
15 s and 256 MiB limits. M11 is complete at this declared recovery boundary;
indefinitely sustained writers/tail latency, reconnect supervision, the two
narrow M11.3 injection gaps above, and M12 active/non-pristine rebuild remain.

M11.5 adds permission-loss recovery evidence on PG17.10 and PG18.4. Missing
bootstrap-function `EXECUTE`, source `SELECT`, or checkpoint `UPDATE`, and
control/transport role reversal, stop without committing row/operator state,
continuation, activation, or feedback. Restoring the exact grant lets the
bounded bootstrap proceed; no cleanup authority or fallback is introduced.
Successful `restart_abandoned` under split roles remains a narrow unproved
operational case.

## M12.1 forward-only rebuild recovery

Before destructive prepare commits, every error rolls back or precedes all
mutation: the old active binding/config/generation, rows, operator state/result,
continuation and slot remain unchanged. Prepare owns one PostgreSQL transaction
under the source fence and exact-old CAS. Its commit installs the target as sole
building authority, makes public results `building/NULL`, retires old rows and
continuation and resets private state. This is the point of no fallback.

Slot drop/create cannot join that transaction. Recovery reads the durable
lifecycle and exact old/new slot coordinates, compares all PostgreSQL-observable
shape, and performs only the next phase's explicit action. Unexpected absence,
presence, active ownership, database/plugin/type/flags, name or generation
fails closed. A crash after prepare therefore resumes target cleanup,
reservation, snapshot scan, catch-up or activation; it never reconstructs old
active state. Old transaction/token/ACK replays fail generation and lifecycle
validation before mutation or feedback.

Snapshot batches and WAL catch-up retain M11/M10 transaction owners and replay
rules. Activation atomically publishes complete target results and promotes the
same authority; a crash before its commit remains building, and a crash after
commit observes active and uses the existing exact terminal feedback recovery.
No second continuation or durable effect log participates.

PostgreSQL 17/18 cannot distinguish an identical slot replacement by a trusted
privileged actor. The `REPLICATION` credential is a control-plane capability,
and its compromise or external slot DDL is outside this correctness model. M12
does not claim database ACL enforcement or add a slot-birth marker. M12.2 now
proves the admission transaction and rollback boundary; instruction-level slot,
snapshot, catch-up and activation recovery remains M12.3--M12.4.

## M12.2 prepare rollback and forward state

PG17.10 and PG18.4 prove that invalid target shape or permission, stale old
BootstrapId/generation, active old slot, occupied target slot, foreign target
binding, mixed operator identity, and losing concurrent preparation all fail
before the destructive commit. Exact snapshots of binding/config/bootstrap,
rows, states/results, continuation and invalidations remain unchanged.

The successful commit is one transaction: target identity becomes sole
`rebuild_prepared` authority; results become `building/NULL`; current rows and
continuation are deleted; private states reset to zero; and obsolete source and
ingress invalidations are cleared. The old inactive slot remains and the target
slot is absent, so a crash at this point is unambiguously forward-recoverable
without pretending slot DDL was transactional. M12.3 owns their exact cleanup/
creation sequence.

Old/foreign receiver tokens are additionally rejected by a receiver-local
authorization capability even when their terminal LSN equals a current value.
The capability is memory-only and cannot replace durable lifecycle/generation
validation. The admission gate does not yet prove kill points after old-slot
drop, new-slot creation, snapshot loss, scan/catch-up, activation or feedback;
those remain M12.3--M12.4.
