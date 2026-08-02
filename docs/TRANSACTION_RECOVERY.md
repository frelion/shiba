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
therefore the transaction replay authority, while `applied_insert` is the sole
current source-row state; neither a DELETE log nor a recovery writer exists.

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
Production receiver restart and persisted partial-stream recovery remain open.

M6.2 observes real streamed segments before source ROLLBACK, then real `A`.
After feedback covers the abort, receiver restart on the same slot emits a new
committed stream; only that transaction can write row/count/result/continuation.
Host crash before abort feedback and persisted partial-stream recovery remain
unproved.

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
