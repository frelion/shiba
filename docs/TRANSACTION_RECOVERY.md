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
