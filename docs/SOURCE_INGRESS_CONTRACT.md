# Source Ingress contract

## M16.2 result-schema governance

Ingress remains transport/lifecycle orchestration and does not interpret result
fields or aggregate functions. At startup, resume, bootstrap and rebuild
handoff it validates each terminal's exact canonical schema payload and digest
against the sole OperatorGraph. Rebuild prepare carries result IDs plus opaque
schema payloads/digests; it no longer carries scalar/key/value shape arrays.
Schema drift fails before receive, Apply or ACK. This changes no WAL,
continuation, transaction or feedback authority.

## M14.6 graph ingress cutover

Ingress is now graph-scoped for both one- and two-member graphs. One
`graph_ingress_config` binds the graph digest, database, publication, slot and
generation; `graph_ingress_source` binds the exact ordered members and their
publication columns. One replication stream assembles each PostgreSQL
transaction once. The decoded changes are tagged by SourceId and delivered as
one graph transaction, including transactions that change both members.

Runtime acquires the graph/generation mutex, probes the sole
`graph_continuation`, locks member bindings in SourceId order, applies all
member row changes once, builds one transaction-local `MultiInputBatch`, runs
the canonical `OperatorGraph`, persists generic node state/results, writes the
graph continuation last and commits. Only then can the existing terminal ACK
authorization advance. Ingress carries no concrete node kind, operator ID,
fixed operator count or intermediate DeltaBatch, and it never advances a
member independently.

The source-scoped ingress configuration, invalidation and continuation names in
the historical M10--M13 evidence below are superseded by their graph-scoped
authorities. There is no compatibility path. The directed M14.6 graph Runtime
gate is green on PG17.10/PG18.4: one real pgoutput transaction can carry both
relation changes into one graph Apply; exact replay does not duplicate output,
sink failure rolls back continuation, and exact identity-index replacement
invalidates before mutation. M14.7 re-proves the full receiver/bootstrap/rebuild
release and performance matrix.

The lifecycle closure adds a real two-source governed receiver proof on both PG
versions. After graph activation, `GovernedGraphSession` applies and explicitly
ACKs one complete Join transaction. Dropping the session after durable Apply
but before ACK leaves `confirmed_flush_lsn` old; reattach receives exact replay,
Runtime returns `AlreadyApplied`, continuation cardinality is unchanged, and
the explicit ACK advances the slot to the exact terminal end LSN. Generation-2
live ingress repeats Apply/ACK after whole-graph rebuild.

Every `PgoutputSource` must agree with the durable effective identity index for
its member. Default primary-key and explicit unique replica-identity sources
are admitted; a relation with no effective identity is rejected without a
partial binding. This member rule is checked independently of node kind, so
Ingress never treats the JOIN right side as a special transport source.

After taking the graph/generation lock, Runtime performs the exact replay probe
before current eligibility/invalidation checks. A durable transaction replay
therefore converges even if DDL invalidated the graph after the original commit;
a new transaction still fails eligibility before Source Apply. This ordering
does not authorize ACK before `Applied`/`AlreadyApplied` and never bypasses the
generation comparison.

## M13 lifecycle integration status (historical baseline)

M13.3 changed Runtime input only after a complete transaction has crossed the
existing transport/decoder boundary; ACK authorization and slot authority are
unchanged. The transaction now drives a Catalog-loaded ordered plan set and one
generic scalar/keyed sink. Ingress must never carry a concrete operator kind,
fixed operator ID/count or payload column position.

M13.4 re-proved committed and streamed ingress, bootstrap catch-up, rebuild
handoff, crash/replay and ACK timing with arbitrary plan cardinality and
`ProjectRows` on PG17.10/PG18.4. A failed plan/state decode or keyed sink remains
an Apply failure: no continuation commits and no terminal feedback may advance.

## Ownership

Source Ingress is a transport boundary, not a durable data authority. For each
active source it owns one logical-replication connection, bounded transaction
assembly, the last position safely reported on that connection, and orderly
shutdown. It never writes source-row state, operator state, public results, or
continuation. Runtime remains the sole writer and owns the Apply PostgreSQL
transaction.

The replication connection and Apply connection are distinct. Waiting for WAL
therefore cannot hold an Apply transaction or database lock. Ingress delivers
at most one complete transaction to Runtime at a time and does not read more
WAL until Runtime returns, so a slow Runtime directly backpressures transport.

## Data path

```text
PostgreSQL logical slot (transport cursor authority)
  -> libpq COPY BOTH (complete CopyData payloads)
  -> w/k replication-envelope validation
  -> bounded incremental pgoutput frame assembly (protocol v1 or v2)
  -> existing complete-transaction decoder
  -> Runtime::process (Apply -> EffectBatch -> Operators -> Result Sink)
  -> committed Applied | committed AlreadyApplied
  -> r feedback at the validated terminal end_lsn
```

The frame assembler may retain one transaction of at most 16 MiB. It accepts
arbitrary transport chunk boundaries, but it does not decode tuple values or
source identity and cannot produce a Runtime transaction. Only the existing
semantic decoder can do that. The decoder's 10,000-change limit remains
authoritative. A partial, corrupt, unknown, or excessive transaction is never
Apply input.

## ACK and recovery invariants

Feedback requires one of exactly four terminal authorizations:

1. `Applied(end_lsn)`: Runtime durably committed the decoded transaction.
2. `AlreadyApplied(end_lsn)`: Runtime proved exact continuation replay.
3. `EmptyCommitted(end_lsn)`: protocol v2 contained exactly
   `S(first=true) E (S(first=false) E)* c` for the same nonzero top-level XID,
   commit flags were zero, commit/end coordinates were valid, at least one
   complete empty segment existed, and there was no other frame or trailing
   byte. It requires an explicit `acknowledge_empty` call.
4. `Aborted(ack_lsn)`: a complete, legal top-level terminal `A` closed the
   matching stream and requires an explicit `acknowledge_abort` call.

These are exact terminal authorizations, not interchangeable LSN observations.
Outer `wal_end`, keepalive progress, a partial frame, and `E` alone authorize
nothing.

- Receive before Apply crash: no feedback; the slot replays after restart.
- Apply commit before feedback crash: replay reaches Runtime, which returns
  `AlreadyApplied`; only then may feedback advance.
- Feedback after durable Apply: restart begins at PostgreSQL's slot position and
  cannot require re-executing acknowledged history.
- Decode, Apply, or Operator error: no computation state and no feedback advance.
- Keepalive with reply requested: reply immediately with the last durable Apply
  position, never the newest received `wal_end`.
- Protocol-v2 stream stop (`E`) is not terminal. Neither a partial segment nor
  `E` can be decoded, applied, or acknowledged. Only a matching stream commit
  (`c`) can enter the existing Runtime streamed decoder.
- A matching stream abort (`A`) discards the volatile assembly without entering
  Runtime or creating a continuation. Its feedback boundary is the outer
  XLogData `dataStart` that carried the complete abort, not a pgoutput field
  (protocol v2's abort message has no LSN field).

M10.2 implements ACK as an explicit state transition. `receive_one` yields a
non-constructible volatile input and blocks any second receive. `apply_received`
calls Runtime and yields a non-constructible durable token only after
`Applied` or `AlreadyApplied`. `acknowledge` accepts only the receiver's exact
pending terminal `end_lsn`, sends write/flush/apply at that same coordinate,
flushes libpq output, and only then advances its in-memory last-ACK value.
Decoder or Runtime failure poisons the receiver: it cannot skip the failed
transaction and must restart from the slot.

```mermaid
sequenceDiagram
    participant PG as PostgreSQL slot
    participant I as Source Ingress
    participant R as Runtime
    participant DB as Shiba transaction
    PG->>I: XLogData through terminal COMMIT(end_lsn)
    I->>R: one decoded SourceTransaction
    R->>DB: Apply + Operators + Result + continuation
    DB-->>R: COMMIT
    R-->>I: Applied or AlreadyApplied
    I->>PG: status write=flush=apply=end_lsn
    Note over I,PG: crash before status means slot replay; Runtime returns AlreadyApplied
```

A requested keepalive follows the same rule but reports only the previous
durable ACK, never the keepalive's `wal_end`. PG17 and PG18 tests use a 2-second
walsender timeout and observe all three coordinates in `pg_stat_replication`
before any source transaction exists.

The receiver never persists a partial transaction or a second WAL spool. A
crash loses only volatile assembly and PostgreSQL replays from the slot. This
is bounded-memory recovery, not persisted partial-stream recovery.

## Protocol-v2 streaming boundary

Streaming mode is selected explicitly at replication startup and requests
pgoutput protocol version 2 with streaming enabled. Production assembly accepts
arbitrary transport chunk boundaries: a pgoutput frame may span XLogData
payloads, and one payload may contain several frames. It retains at most one
single-XID transaction within the same 16 MiB wire bound; the existing Runtime
decoder independently enforces the 10,000-change bound.

The admitted semantic shape remains M6's top-level, single-XID, key-only
`INSERT`: `S ... (R/I ... E)+ ... c`. Streaming assembly adds no nullable
payload, UPDATE, DELETE, subtransaction, or interleaved-XID language. It scans
frame structure and XID consistency only; the unchanged Runtime
`decode_streamed_changes` remains the sole semantic relation/tuple decoder and
revalidates the complete terminal commit before Apply.

Partial segments and stream stop never produce an Apply token. A terminal abort
produces a private transport token only after the matching complete `A`; it is
not sent to Runtime, writes no Shiba state, and may be acknowledged only at that
abort frame's outer XLogData `dataStart`. A crash before either commit feedback
or abort feedback drops the volatile bytes and lets the PostgreSQL slot replay
them. Corruption, an unknown/mismatched XID, an invalid terminal, or either
admission limit poisons the receiver and advances no feedback.

A strictly empty committed stream is the one exception to Runtime delivery. It
has the exact `S(first=true) E (S(first=false) E)* c` structure above and yields a private
`EmptyCommitted(end_lsn)` token; it never manufactures an empty
`SourceTransaction` or continuation. This proves only that the configured
publication emitted no changes for that committed transaction. It is safe to
acknowledge only while M10.4 governance also proves the exact publication
identity and its frozen membership snapshot remain valid. `ALTER/DROP
PUBLICATION`, remove/re-add, same-name recreation, flag changes, column-list
changes, and row-filter changes persistently invalidate the source. Every empty
acknowledgement revalidates that fact first.

> A strictly validated publication-empty terminal commit may authorize feedback whether it contains one or multiple complete empty stream segments.

The empty recognizer uses constant structural state inside the existing 16 MiB
transaction bound; it does not retain a segment list. Any legal `R` or `I`
frame makes the transaction non-empty and routes the complete commit through
the sole Runtime streamed decoder. Any other frame, ordering, XID, terminal,
flag, coordinate, or trailing-byte shape fails closed. There is deliberately no
rule that authorizes feedback merely because decoding yielded no rows.

## Lifecycle boundary

M10.4 adds one database-local `source_ingress_config` authority per source. It
binds the current database OID, exact `pg_publication` ObjectAddress, frozen
publication name/semantic flags/normalized published attribute numbers, the
existing source relation binding, slot name, and slot generation. Publication
OID is identity; the frozen name is only a transport locator and drift signal.
Neither schema/table names nor a newly created publication with the old name can
resume a source.

The single event-trigger writer records persistent
`source_ingress_invalidation` when the exact publication is dropped or its live
snapshot differs. Snapshot comparison is required because PostgreSQL 17 emits
no command-address row for some publication-membership changes. Once recorded,
remove-then-add cannot revive the binding. Governance revalidates the database,
source binding and invalidation, publication OID/snapshot, generation, and live
slot immediately before every receive, Apply, and terminal ACK. Consequently an
invalidated publication cannot authorize `EmptyCommitted` or `Aborted` feedback.

`pg_replication_slots` is the physical slot and transport-progress authority.
Shiba does not mirror `confirmed_flush_lsn`, receiver PID/status, connection
secrets, or dynamic slot progress. Attach accepts only a pre-existing,
persistent, inactive logical `pgoutput` slot in the configured database; after
COPY BOTH starts it revalidates that the same slot is active. Startup never
creates, drops, replaces, or silently discovers a slot.

Slot replacement is a private compare-and-swap operation. It locks the config,
requires the expected generation and a different pre-existing inactive
`pgoutput` slot in the same database, and increments generation exactly once.
It rejects active slots, stale callers, publication invalidation, and any source
with current rows or continuation. The latter requires the explicit M12 rebuild
lifecycle rather than pretending old computation belongs to new history.

## Connection and shutdown budget

Each active source has exactly two connections: one replication connection and
one synchronous Apply connection. The process hard cap is 32 active sources
(64 connections); there is no implicit pool or unbounded connection creation.
Both conninfo strings must name the same explicit database and set a positive
`connect_timeout`; Apply also receives an explicit positive
`statement_timeout`.

A process-local permit enforces the source cap. A session advisory lock plus
the slot's active state excludes a second receiver for the same source. The
advisory lock is lifecycle coordination, not source or cursor authority.
Waiting for WAL retains that session lock but no Apply transaction, row lock,
or in-flight query. Detach is explicit between receives: it closes the
replication connection, releases the advisory lock, and leaves the slot and
catalog untouched. An in-flight blocking receive, transport interruption/TLS
policy, and cross-process graceful-stop orchestration remain operational proof
outside the M10.4 catalog contract.

The production transport uses cooperative idle-receive shutdown. It first
drains any `CopyData` already buffered by libpq with asynchronous
`PQgetCopyData`; only when none is buffered does it wait through
`PQsocketPoll`, call `PQconsumeInput`, and retry. A shutdown handle is checked
on each bounded poll cycle. This ordering is a correctness requirement: the
first failure-oriented performance run showed that polling the socket before
draining libpq could sleep while a complete transaction was already buffered.

PG17 and PG18 idle gates complete in 42.262 ms and 76.950 ms respectively,
below the frozen 1 s limit. Shutdown returns no transaction or ACK, changes no
row/operator/result/continuation state or slot LSN, then permits explicit
detach/reattach. Shutdown requested during Runtime Apply, automatic reconnect/
backoff, and process orchestration remain outside this cooperative boundary.

## Production roles

PG17 and PG18 gates prove split, non-superuser roles. The synchronous Apply role
is `NOREPLICATION`; it receives schema usage and the narrow internal
SELECT/INSERT/UPDATE/DELETE grants needed by governance and Runtime. Its source
schema `USAGE` and source-table `SELECT` are required solely because Runtime
preflight acquires `ACCESS SHARE`; Source Apply never queries the source table.
Its `UPDATE` on `source_continuation` is required by the latest-continuation
`SELECT ... FOR UPDATE`, not by a second continuation writer.

The proved Apply grants are:

- schema `USAGE` on `shiba_internal`, `shiba`, and the bound source schema;
- `SELECT, UPDATE` on `source_binding` for preflight and its source mutex;
- `SELECT` on source/ingress invalidations, ingress config, and
  `operator_definition`;
- `SELECT, INSERT, UPDATE` on `source_continuation`;
- `SELECT, INSERT, UPDATE, DELETE` on the current-row table `source_row_state`;
- `SELECT, UPDATE` on `operator_state` and public `operator_result`;
- `SELECT` on the bound source table solely for the relation lock above.

The receiver role has PostgreSQL `REPLICATION`, source schema `USAGE`, and
published source table `SELECT`. It has no authority to write Shiba internal
state. The Apply role cannot open the replication connection, the receiver role
cannot perform governed Apply, and neither role is superuser. These grants prove
the current single-database topology; secret distribution, TLS policy, and role
rotation remain deployment responsibilities.

## Relation metadata and strict resource bounds

pgoutput relation metadata is connection-scoped and need not be repeated in
every transaction. The receiver therefore owns one constant-size
`PgoutputRelationState`: the first `R` must validate the exact configured
source; every repeated `R` is revalidated; only a later transaction on that
same connection and exact source may omit it. A missing first descriptor,
changed source, or mismatching repeated descriptor fails closed. This is no
frame cache, second decoder, durable authority, or cross-connection fallback.

Rust-owned memory has explicit structural bounds:

- one owned `CopyData` vector is at most 16 MiB plus its 25-byte replication
  envelope;
- transaction assembly is at most 16 MiB;
- semantic decode owns at most 10,000 changes;
- synchronous delivery permits one outstanding transaction and no queue.

These are code-enforced allocation bounds, not a claim about allocator
overhead, process RSS, PostgreSQL memory, or cross-host soak behavior.

## Frozen PG17/18 performance evidence

The 10,000-change measurement starts before the source INSERT commit and ends
only after production receive and durable Runtime Apply. It is true
source-commit-to-durable-Apply E2E: 860.865 ms on PG17 and 867.479 ms on PG18.
Exact replay is 29.350 ms and 31.085 ms.

The sustained sample has a different meaning: 100 ten-row transactions are
precommitted before timing, so its percentiles measure receiver service latency
against a ready backlog, not source commit latency. PG17 services the backlog in
622.987 ms at 160.52 tx/s with p50/p95/p99 of
6.216/6.355/6.533 ms. PG18 takes 739.298 ms at 135.26 tx/s with
7.375/7.585/7.776 ms. Slow Apply lasts 357.969/358.370 ms and an attempted
second receive is rejected in 1.393/1.836 ms, proving direct backpressure rather
than queue growth. Allocator/RSS peaks and cross-host sustained soak remain
unproved.

## M10 admission boundary

M10 reuses the already admitted pgoutput v1 and v2 transaction shapes. It does
not add SQL parsing, source discovery, new tuple shapes, a second decoder,
receiver-written results, automatic slot administration, persisted WAL, or
binding rebuild. Production receiver/feedback, streaming assembly, governed
lifecycle, split-role permissions, bounded idle shutdown, and the frozen local
performance gate have PG17/18 evidence. M10 is complete at this declared scope.
TLS/disconnect behavior, shutdown during Apply, reconnect/backoff orchestration,
allocator/RSS measurement, and cross-host soak remain future work.

## M11.2 bootstrap handoff

M11 does not infer an initial boundary from M10 feedback. Its explicit
pre-active lifecycle creates a new `pgoutput` slot with `EXPORT_SNAPSHOT` and
uses only PostgreSQL's returned `consistent_point` and ephemeral
`snapshot_name`. While the exporter remains idle, each bounded short scanner
imports that snapshot in a read-only repeatable-read transaction before its
first query. The snapshot name is neither persisted nor recoverable.

Snapshot batches have separate bootstrap identities and do not enter the M10
receiver, decoder, `SourceTransaction`, terminal-ACK, or continuation domains.
After the final scan checkpoint commits, M10 starts at the exact
`consistent_point` and consumes accumulated WAL with its existing bounded
transport and authorization rules. Catch-up adds no queue or spool. Exact
source/publication ObjectAddress, frozen semantics, durable invalidation, slot,
and generation must be valid before every scan, Apply, catch-up, ACK, and
cutover step.

Cutover requires an exact transactional logical-message fence bound to the
active BootstrapId. M11 temporarily enables pgoutput messages and admits only
that strict `BootstrapFence(end_lsn)` terminal; sampled/keepalive LSNs, unknown
`M`, mixed source changes, and foreign/stale attempts cannot authorize cutover.

Scan uses exactly three connections: exporter/replication, scanner, and Apply.
It releases the scanner after `scan_complete`; catch-up/live use the existing
two. Before scan completion, loss of the exported snapshot resets the entire
hidden pristine attempt and creates a fresh slot/boundary. After scan completion
the same slot is retained for recovery. The M11.1 PG17/18 gate proves the
exported-snapshot lifetime and exact consistent-point relation.

M11.2 implements the scanner, strict logical-message fence, catch-up, cutover,
and live conversion without adding another receiver or queue. Both PG17 and
PG18 scan batches of two to private `3/40` while public results remain
building/NULL, consume one concurrent source transaction to private `3/25`,
activate public `3/25` only at the exact fence, and then reach `4/32` through an
ordinary M10 live transaction. Slot feedback covers the durable catch-up and
live terminals; snapshot batches create no continuation.

M11.3 recovery is described below; M11.4 bounded production evidence follows.

## M11.3 recovery ingress

Pre-scan recovery is an explicit bootstrap operation, never ordinary M10
startup. Holding the same per-source advisory lock, it verifies the exact old
BootstrapId, slot and generation, drops only that inactive physical slot via
the replication transport, and calls the single catalog replacement writer
only after old and new slot names are absent. The replacement advances both
attempt identity and generation and reuses the existing reservation validator;
it does not auto-discover, adopt, create, drop, or rotate any other slot.

After `scan_complete`, recovery takes the opposite path: the persistent slot is
the transport cursor authority, so the coordinator reattaches it and resumes
the existing bounded M10 receiver. Catch-up continues to revalidate exact
source/publication/generation before receive, Apply, activation, and ACK. An
active cutover with feedback still pending can authorize only an exact replay
of its stored fence marker and terminal `activation_end_lsn`; a slot already
confirmed through that end enters live without replaying Runtime. PostgreSQL
restart changes neither rule, and advisory-lock competition permits only one
active coordinator per source.

The M11.3 gate is green on PG17.10 and PG18.4. It proves reconstructed durable
creating/slot-absent restart and exact replacement, partial-scan reset,
stale/foreign rejection, batch replay and rollback, worker competition,
same-slot resume across immediate PostgreSQL restart, killed feedback after
catch-up Apply and after active cutover, exact-fence replay, and a no-op restart
once feedback covers the active terminal. Final SQL differential is `4/50`.
The gate does not directly kill at the reservation instruction or exercise an
active foreign old-slot conflict.

## M11.4 bounded bootstrap ingress

The frozen gate admits one million snapshot rows in exactly 100 batches of at
most 10,000 and one exact concurrent 10,000-change WAL transaction. It retains
three scan connections, synchronous batches, direct M10 catch-up backpressure,
and no channel or queue. Pre-observation limits are scan <=120 s and >=10,000
rows/s, catch-up+activation <=15 s, and Rust RSS growth <=256 MiB.

PG17.10 records 3.098397625 s / 322,747.47 rows/s, 1.320857542 s catch-up, and
10,160→13,824 KiB RSS (+3,664). PG18.4 records 3.136067542 s /
318,870.68 rows/s, 1.329330584 s, and 10,160→13,824 KiB (+3,664). Both pass the SQL differential
after concurrent UPDATE/DELETE/INSERT and the ordinary M10 live handoff.

Operators should budget exactly three connections while scanning and two after
handoff, never expose `building`, and choose recovery by phase: exact
slot cleanup/replacement only before `scan_complete`; same-slot reattach at or
after it. M11 is complete at this declared source shape. Indefinite concurrent-
writer catch-up, tail latency, and reconnect supervision remain outside its
evidence; M12 subsequently proves active/non-pristine rebuild by reusing it.

M11.5 proves the same ingress under split least privilege on PG17.10 and
PG18.4: a non-superuser `NOREPLICATION` control/Apply/scanner connection, a
separate non-superuser `REPLICATION` transport connection, and a public-result-
only reader. The real path reaches live ingress and matches the SQL oracle.
Role swapping and revoked `EXECUTE`, source `SELECT`, or checkpoint `UPDATE`
fail before unauthorized Apply or feedback; no production session uses
superuser or inherited role membership.

## M12.1 rebuild ingress boundary

Rebuild first stops ordinary live ingress under the same per-source ownership
fence and requires the old physical slot inactive. A failure during side-effect-
free target preflight leaves that receiver, old authority and result unchanged.
Once destructive prepare commits, only the target building config/generation
may be used: old-generation receive, Apply, pending terminal authorization,
feedback and attach all fail closed.

The target slot must be created explicitly with `pgoutput EXPORT_SNAPSHOT`.
M11's bounded scanner imports its real snapshot; the same target slot then
feeds M10 catch-up, exact fence, activation and live handoff. Waiting for any
of these transport operations holds no Apply transaction or catalog lock. No
new receiver, decoder, queue, WAL spool or continuation is introduced.

Ingress rejects every observable slot drift: unexpected presence/absence,
active state, database, plugin/type, temporary/two-phase/failover/synced shape,
name, lifecycle or generation mismatch. It never adopts or repairs a slot by
name. PostgreSQL cannot reveal a same-name/same-shape replacement performed by
a superuser or holder of the trusted `REPLICATION` credential. Credential
exclusivity and no external slot DDL are deployment prerequisites, not a
per-slot ACL proof. M12.1 defines these rules; M12.2 now enforces the admission
and destructive catalog boundary, while M12.3--M12.6 retain the slot/data-path
and recovery obligations.

## M12.2 rebuild admission ingress

`PreparedRebuild::prepare` accepts only explicit old and target BootstrapId,
relation OID, identity-index OID, publication OID, slot name and generation.
The operator definition authority supplies the complete ordered plan set; the
request does not carry fixed operator IDs, a plan count or column positions.
It acquires normal per-source ownership,
keeps the old slot inactive, validates replication/apply database agreement,
and performs relation, publication, replica-identity, operator, permission and
target-slot preflight before destructive SQL. It never guesses an object from
its name, adopts a slot, or creates a candidate config.

On commit the target config is the only ingress config but is not execution
eligible: phase `rebuild_prepared` forbids ordinary M10 attach/receive/Apply/
ACK. The old inactive slot remains for the M12.3 retirement step; the target
slot is still absent. M11 scanner and Runtime can later resolve only the target
authority. The default bigint primary-key index OID travels as an exact CAS
coordinate. Pre-M12 active state uses the frozen three binding rows; every
M12-produced generation also has the exact fourth identity-index binding. Its
persistent retired identity triple selects that interpretation throughout
recovery. Same-OID rename permits narrow reconciliation; replacement OID fails
closed and cannot be dynamically substituted.

All received terminal tokens also include a private authorization minted by
their receiver instance. Apply and feedback reject a stale/foreign receiver's
token before it can exploit an equal LSN. This is volatile capability checking,
not a stored cursor, generation, continuation or slot identity; catalog
lifecycle/generation validation remains mandatory. The earlier PG17.10/PG18.4
admission gate predates the identity-authority correction. Its new
failure-first run exposed invalid unparenthesized PL/pgSQL `IF CASE` syntax on
PG17; the corrected gate is green on PG17.10 and PG18.4.

## M12.3 rebuild transport handoff

PG17.10 and PG18.4 `scripts/test-m12-rebuild-snapshot-live.sh` prove the exact
handoff: drop the inactive generation-2 slot, create the absent generation-3
slot with real `EXPORT_SNAPSHOT`, scan in bounded M11 batches, consume
snapshot-concurrent INSERT/UPDATE/DELETE through M10, cross the exact fence,
activate, then process and ACK an ordinary live transaction. Backpressure stays
synchronous and no WAL spool, queue or second receiver path is added.

An old token and old-generation attach are rejected. Results remain
`building/NULL` until activation, and feedback derives only from durable WAL
Apply after the real snapshot boundary. M12.4 subsequently proved restart
behavior across the non-transactional slot and handoff crash windows.

After locking the sole binding, Runtime validates exact ingress config and
bootstrap generation before replay or Apply. With a lifecycle row present,
ordinary WAL is admitted only in `catching_up` or `active`: retired generation
2 rejects before and after activation, and target generation 3 cannot enter the
ordinary path during `rebuild_prepared`, `creating`, `scanning`, or
`scan_complete`.

## M12.4 crash and resume ingress boundary

Each durable phase has one forward action. `rebuild_prepared` resumes exact
old-slot retirement/new-slot creation. If a M12 `creating` or `scanning`
attempt loses its exported snapshot, ingress does not import it again: the
existing abandoned-attempt replacement path obtains a fresh BootstrapId,
distinct slot and exact successor generation, then runs the ordinary M11 scan
and M10 catch-up path. `scan_complete`, `catching_up` and `active` retain their
existing M11/M10 resume rules.

Runtime validates the one current target binding/config/generation (including
the durable identity-index OID) at each boundary. Old-generation attach,
Apply, token authorization and ACK fail closed. Replacement scan work never
manufactures WAL identity or an ACK position.

## M12.5 governed rebuild ingress

The PG17.10/PG18.4 `scripts/test-m12-rebuild-governance.sh` gate proves that
ingress does not treat a successful transport connection as target authority.
Before destructive prepare, the dedicated transport credential completes
`IDENTIFY_SYSTEM` for the configured database and the control caller has
target-relation `SELECT`. Control/Apply/scanner is `NOREPLICATION`; only the
separate transport identity carries the trusted `REPLICATION` capability plus
target `SELECT`; the reader has no ingress or internal-table capability.

At receive, catch-up and activation boundaries, ingress revalidates one durable
relation/publication/column/identity-index/plan authority. Publication
remove/re-add or OID drift, replica-identity/index/column drift and committed
post-prepare invalidation cannot be repaired by a same name: they stop the
building generation before Apply or ACK. The common ownership fence permits one
same-source rebuild/live transition while another source progresses normally.
M12.6, not this gate, owns final performance and release evidence.

## M12.6 bounded rebuild ingress acceptance

The final M12 ingress gate runs an active, non-pristine one-million-row source
through the existing receiver and scanner while one real 10,000-change source
transaction accumulates behind the exported-snapshot boundary. Backpressure
remains synchronous: one bounded scan batch or one complete bounded WAL
transaction is handed forward at a time, with no channel, spool or per-row
network round trip. The test records scan, catch-up, activation and total time,
RSS growth and retained WAL.

Thresholds were frozen before observation: scan <= 12 s, catch-up <= 8 s,
activation <= 2 s, total <= 25 s, RSS growth <= 128 MiB and retained WAL
<= 256 MiB. M11's comparison evidence is approximately 3.1 s scan, 1.3 s
catch-up and 3.6 MiB RSS growth. PG17.10/PG18.4 observed scan
4.357951916/4.429333333 s, catch-up 1.946769416/1.907849875 s, activation
9.755875/9.981958 ms, total 6.343139667/6.375927458 s, RSS +4,272/+4,320 KiB,
and retained WAL 252,864,952/252,898,072 bytes. The release wrapper ran 48
unique scripts and 96 PG invocations without skipping an old gate.

Even after acceptance, TLS, remote-network failure policy, automatic reconnect
and backoff, cross-host soak, slot failover and indefinite concurrent-writer
tail latency remain outside the proved ingress boundary.
