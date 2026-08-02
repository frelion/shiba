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
`SourceTransaction` or continuation. This proves only that the currently
selected publication emitted no changes for that committed transaction. It
does not prove source identity or that publication membership has not drifted.
Until M10.4 validates durable publication identity, an invalidated publication
must never authorize empty feedback. `ALTER/DROP PUBLICATION`, remove/re-add,
and same-name recreation must be prevented by role permissions or durably
invalidate configuration before any later `EmptyCommitted` ACK.

> A strictly validated publication-empty terminal commit may authorize feedback whether it contains one or multiple complete empty stream segments.

The empty recognizer uses constant structural state inside the existing 16 MiB
transaction bound; it does not retain a segment list. Any legal `R` or `I`
frame makes the transaction non-empty and routes the complete commit through
the sole Runtime streamed decoder. Any other frame, ordering, XID, terminal,
flag, coordinate, or trailing-byte shape fails closed. There is deliberately no
rule that authorizes feedback merely because decoding yielded no rows.

## Lifecycle boundary

M10.1–M10.2 use one explicit receiver configuration: connection target,
`source_id`, exact slot name, publication name, and slot generation. There is no
environment/table fallback and no discovery by schema or relation name.
M10.4 must replace this process-only lifecycle input with one catalog authority,
validate the live database, slot, publication, binding, and generation, and
exclude a second active receiver. Ordinary startup never creates, drops, or
replaces a slot.

The catalog must not store connection secrets, receiver PID/status, dynamic slot
progress, or `confirmed_flush_lsn`. PostgreSQL owns the physical slot and its
progress. A future generation rotation is a compare-and-swap operation; until
binding rebuild is implemented, a non-pristine source cannot rotate safely.

## Connection and shutdown budget

Each active source has exactly two connections: one replication connection and
one synchronous Apply connection. The process configuration must state a finite
maximum active-source count and connection timeout. No implicit per-source pool
is permitted. Shutdown stops accepting new WAL, finishes or rolls back the one
in-flight Apply, sends feedback only for a committed result, and then detaches
without creating or dropping the slot.

## M10 admission boundary

M10 reuses the already admitted pgoutput v1 and v2 transaction shapes. It does
not add SQL parsing, source discovery, new tuple shapes, a second decoder,
receiver-written results, automatic slot administration, persisted WAL, or
binding rebuild. Production receiver/feedback, streaming assembly, lifecycle,
permissions, and performance become proven only when their PG17/18 gates pass.
