# V2 replication ingress design

Status: implementation contract

Target: PostgreSQL 17

## Decision

Shiba has one DAG execution process, `shiba runtime`. Logical decoding runs in
the PostgreSQL walsender created for the Runtime's replication-protocol
connection.

```text
source transactions
  -> WAL
  -> PostgreSQL walsender / pgoutput v2
  -> replication CopyData
  -> Shiba Runtime bounded ingress transaction
  -> durable change log
  -> DAG scheduler and work quanta
```

The walsender is not a Shiba worker and never executes DAG work. DAG instances
remain in-process scheduler objects and do not own processes, threads,
connections, pools, or CPUs.

## Why two backend sessions are required

PostgreSQL 17 marks a backend that owns an out-of-transaction persistent
logical-decoding context with `PROC_IN_LOGICAL_DECODING`. Snapshot construction
then deliberately ignores that backend's XID. The flag is cleared only when
the replication slot is released. Consequently, retaining the context while
opening an ordinary SPI transaction in that same backend is not safe.

The replication connection moves that context to a walsender. The Runtime's
SPI backend remains an ordinary transaction participant.

## Connection contract

The Runtime owns exactly one replication client connection for its active
database. The connection:

- uses `replication=database`;
- starts `pgoutput` with `proto_version '2'`,
  `publication_names 'shiba_publication'`, and `streaming 'on'`;
- is nonblocking after connection establishment;
- is polled only outside an SPI transaction;
- reconnects from durable ingress progress after errors;
- treats keepalive reply requests as an obligation to send feedback promptly.

Authentication is deployment configuration. Inline passwords MUST NOT be
stored in Shiba tables. A passfile, certificate, peer-authenticated local
socket, or an equivalent PostgreSQL mechanism supplies credentials.

## Durable authority

The replication connection, its receive buffer, relation metadata cache, and
all parser state are disposable. LOGGED PostgreSQL relations are authoritative.

An open source transaction is identified by:

```text
(slot_generation, xid, identity_lsn)
```

One decoded row image is identified by:

```text
(ingress_txn_id, change_lsn, change_ordinal, image_ordinal)
```

Replay of the same identity and payload is a no-op. Replay of the same identity
with a different source, weight, or payload is corruption.

Open and committed transaction payload uses the same physical durable event
rows. Receiving `Stream Commit` updates only the transaction header and creates
a routing task; it does not rewrite every payload row.

## Batch and transaction boundary

The Runtime receives complete protocol messages until one of these conditions:

- ingress row budget;
- ingress byte budget;
- `Stream Stop`;
- ordinary Commit;
- `Stream Commit`;
- `Stream Abort`;
- scheduler fairness deadline.

It then opens one SPI transaction and atomically:

1. locks or creates the source transaction header;
2. inserts events idempotently in wire order;
3. records the replication batch and its ending WAL position;
4. advances durable counters and input sequence;
5. records commit or abort control state when present.

No replication socket read or wait occurs inside that SPI transaction.

After commit, the Runtime may send a Standby Status Update. The reported flush
and apply LSN MUST NOT exceed the greatest committed ingress batch. Reporting
an LSN before the corresponding LOGGED commit is data loss.

A crash after ingress commit but before feedback causes replay and
deduplication. A crash after feedback can still replay from an older
checkpoint, so event identities remain retained behind the conservative
`replay_safe_lsn` GC fence.

## Scheduling

One Runtime turn performs at most:

1. one bounded ingress transaction, if a complete message batch is ready;
2. one bounded DAG work quantum selected fairly;
3. bounded ingress, Stage, and task GC when due.

The Runtime does not wait indefinitely for replication input. When the socket
has no complete message, DAG work remains eligible immediately.

## Delivery stages

1. Replication transport and protocol-v2 parser.
2. Durable provisional ingress and replay-safe feedback.
3. Bounded commit finalization and routing.
4. Existing DAG reader migration to the new transaction-header/event schema.
5. Large-transaction, interleaving, crash, memory, and performance gates.

The old protocol-v1 SQL SRF router remains available only until stages 2–4 are
complete. V2 MUST NOT claim bounded large-transaction ingress while that path
is active.
