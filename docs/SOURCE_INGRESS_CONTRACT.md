# Source Ingress contract

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
with current rows or continuation. The latter requires an explicit future
binding rebuild rather than pretending old computation belongs to new history.

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
after it. M11 is complete at this declared source shape, but indefinite
concurrent-writer catch-up, tail latency, reconnect supervision, and M12 remain
outside the evidence.

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
per-slot ACL proof. M12.1 defines these rules; production ingress enforcement
is pending M12.2--M12.6.
