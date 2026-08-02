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
