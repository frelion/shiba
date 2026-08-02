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
  -> bounded incremental pgoutput frame assembly
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

- Receive before Apply crash: no feedback; the slot replays after restart.
- Apply commit before feedback crash: replay reaches Runtime, which returns
  `AlreadyApplied`; only then may feedback advance.
- Feedback after durable Apply: restart begins at PostgreSQL's slot position and
  cannot require re-executing acknowledged history.
- Decode, Apply, or Operator error: no computation state and no feedback advance.
- Keepalive with reply requested: reply immediately with the last durable Apply
  position, never the newest received `wal_end`.
- Protocol-v2 stream stop is not terminal. No segment is decoded, applied, or
  acknowledged before a matching stream commit. A matching abort discards the
  volatile assembly; its safe feedback boundary requires PG17/18 proof.

The receiver never persists a partial transaction or a second WAL spool. A
crash loses only volatile assembly and PostgreSQL replays from the slot. This
is bounded-memory recovery, not persisted partial-stream recovery.

## Lifecycle boundary

M10.1 starts with one explicit receiver configuration: connection target,
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
