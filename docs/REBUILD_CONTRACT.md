# M12 active-source rebuild contract

## M12.1 scope

M12 rebuilds an already active, non-pristine nullable-`int8` source with the
existing CountRows and SumInt8 operators. M12.1 freezes the state machine and
failure-first contract only. It does not yet implement admission, destructive
prepare, snapshot scan, recovery, cutover, or live handoff; those proofs belong
to M12.2--M12.6.

Rebuild is deliberately offline. Shiba never computes two generations in
parallel. There is one catalog identity authority and one execution-eligible
generation at every observable point.

## Explicit request and identities

A rebuild request names, without discovery or guessing:

- `source_id` and the exact expected old `BootstrapId`;
- expected old binding ObjectAddresses, publication identity, slot name and
  generation;
- the target relation and column ObjectAddresses, publication identity and
  complete membership/flags, replica-identity ObjectAddress and shape;
- the target compiled operator plan and its ObjectAddress inputs;
- a distinct target slot name and the exact expected new generation.

The new `BootstrapId` is never reused. The new generation is produced by a
strict compare-and-swap from the exact old identity and is increased once; it
is not inferred from a slot, continuation, WAL position, or name. Snapshot
batch identities remain distinct from WAL transaction identities. The target
slot's real `EXPORT_SNAPSHOT` response is the only snapshot/WAL boundary.

## Authority and visibility phases

Before destructive prepare, the old binding, configuration, generation,
bootstrap and result are the sole active authority. All target relation,
publication membership, replica identity, operator plan, permissions and slot
name availability checks are read-only. Any failure leaves old current rows,
operator state/result, continuation, bootstrap, config and slot untouched and
the public result active.

Destructive prepare is one catalog/Apply transaction under the source ownership
fence and exact-old CAS. On commit:

- the target binding, ingress config and new generation become the sole catalog
  identity authority;
- the existing `source_bootstrap` authority records the new rebuild attempt in
  a closed building phase;
- every public operator result becomes `building/NULL`;
- old current rows, private operator state and old continuation are retired in
  that same transaction, with private state initialized for the new build;
- the old generation loses eligibility to receive, Apply, ACK, attach or
  publish.

Identity shape is explicit across the transition. Pre-M12 active state has
exactly relation plus two column bindings. Every M12-produced generation adds
the exact default-primary-key index ObjectAddress as a fourth row. Its non-null
retired BootstrapId/slot/generation triple persists through all lifecycle
phases and distinguishes that shape without inference. Recovery may narrowly
reconcile an index rename only when the OID is unchanged; replacement OID fails
closed and cannot be dynamically adopted.

This commit is the destructive boundary. From it onward recovery is
forward-only. Shiba cannot restore, relabel, adopt or fall back to the old
authority. The target is already the only catalog authority, but is not yet
execution-eligible for ordinary M10 live ingress and has no public value.

Activation does not install or switch binding/configuration again. After the
target snapshot is scanned, its same slot is caught up through the exact fence,
one transaction promotes that same target authority from building to active,
publishes all complete results, records the active lifecycle and enables M10
live ingress. A reader observes either the old complete result before prepare,
`building/NULL` throughout rebuild, or the new complete result after activation.

## Writers, transactions and locks

The rebuild coordinator is the sole lifecycle writer. Catalog registration
remains the sole binding/config definition writer. Runtime remains the sole
writer of current rows and operator state/result. PostgreSQL's logical slot is
the transport cursor authority; `source_continuation` is the one WAL compute
idempotency authority. Rebuild adds neither another continuation nor persisted
WAL/EffectStream.

Admission lock order is fixed: acquire the per-source advisory ownership fence,
lock the exact source binding/config/bootstrap rows, validate the exact old
generation and lifecycle, then lock operator definitions/state/results in
`operator_id` order. The old receiver must be stopped and its slot inactive
before destructive prepare. Live ingress, source DDL and another rebuild use
the same source serialization boundary; a different source retains its own
fence and can proceed independently.

Catalog mutations are owned by short PostgreSQL transactions. Snapshot scans
and WAL Apply reuse M11/M10 short transaction owners. Waiting for a slot,
snapshot, scanner, network input or WAL never holds an Apply transaction or
catalog row lock.

## Physical-slot reconciliation and crashes

Logical slot create/drop is non-transactional with Shiba catalog state. Each
operation therefore has a durable expected phase and exact expected slot name,
generation and observable shape before it runs. Recovery compares that intent
with `pg_replication_slots` and performs only the one phase-appropriate action:

- before destructive prepare, no slot mutation is allowed;
- after prepare, the exact inactive old slot may be retired;
- only the exact absent target slot may be created with `pgoutput` and
  `EXPORT_SNAPSHOT`;
- if creation succeeds before its consistent point is recorded, recovery
  recognizes the exact creating phase and cleans or resumes only according to
  the frozen state transition; it never adopts by name;
- after scan completion, the same target slot is retained for catch-up,
  activation feedback and live handoff;
- old slot cleanup after cutover remains explicit and retryable and cannot
  change the active target authority.

Observable drift always fails closed: preoccupied or unexpectedly missing slot,
active slot, wrong database/plugin/type, temporary/two-phase/failover/synced
shape, wrong lifecycle/generation, or an old/new name inconsistent with the
durable rebuild intent. No observable mismatch is repaired automatically.

PostgreSQL 17/18 expose no immutable logical-slot birth identity or per-slot
ownership ACL. A superuser or any external process holding the trusted
`REPLICATION` credential can drop and recreate a Shiba slot with the same name
and all the same observable coordinates. Shiba cannot distinguish that
replacement. Credential exclusivity, audit and prohibition of external slot
DDL are deployment prerequisites; a compromised trusted control plane is
outside the M12 correctness threat model. This residual risk is not claimed as
a least-privilege database invariant. M12 introduces no slot-birth marker.

## Failure and replay invariants

- A failure before prepare commit preserves the complete old active generation.
- A crash after prepare sees the durable target building authority and resumes
  only forward; it can never expose or reactivate old state.
- Old workers, transactions, continuation values, terminal tokens and ACKs must
  fail their generation/lifecycle validation before mutation or feedback.
- M11 scanner and Runtime accept only the current target authority.
- Snapshot batches are bounded and idempotent; they never manufacture a WAL
  identity or continuation.
- Catch-up uses the target slot and existing M10 decoder, Runtime, result sink,
  ACK authorization and direct backpressure.
- Activation promotes the same authority exactly once. Retry before commit sees
  building; replay after commit observes active and performs no second cutover.
- Invalidation is cleared only when target ObjectAddresses, publication,
  replica identity and operator binding have been strictly validated and the
  target authority is installed by prepare. It is never cleared to revive the
  old identity.

There is no candidate binding/config, dual generation, second bootstrap path,
slot-birth marker, compatibility alias, automatic adoption, repair fallback or
dual write.

## M12.1 evidence boundary

M12.1 supplies failure-first contract tests for the closed state transitions,
exact-old CAS, visibility phases, generation rejection and observable slot
shape classifier. It records PostgreSQL 17.10/18.4 experiments showing that a
same-name, same-shape slot drop/recreate has no observable immutable birth
identity. It does not yet prove the production destructive transaction, real
active-source snapshot-to-live rebuild, crash matrix, DDL/concurrency/roles or
million-row performance. Those are required before M12 is complete.
