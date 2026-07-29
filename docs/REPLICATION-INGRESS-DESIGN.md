# Durable replication ingress design

Status: current implementation contract

Target: PostgreSQL 17

## Decision

Shiba has one DAG execution process, `shiba runtime`. Logical decoding runs in
the PostgreSQL walsender created for the Runtime's replication-protocol
connection.

```text
source transactions
  -> WAL
  -> PostgreSQL walsender / pgoutput protocol v2
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
  `publication_names 'shiba_publication'`, and `streaming 'off'`;
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

With transaction streaming disabled, PostgreSQL owns savepoint,
subtransaction rollback, and top-level abort semantics. Its logical-decoding
reorder buffer spills beyond `logical_decoding_work_mem` and emits only the
final committed transaction. Shiba still receives that output as individual
CopyData messages and persists bounded batches; it never materializes the
whole transaction in Runtime memory.

The authoritative catalog is:

- `ingress_replay_state`: slot generation and the persisted, confirmed, and
  replay-safe LSN fences;
- `ingress_transactions`: one header per source transaction;
- `change_log`: payload stored once regardless of DAG fan-out;
- `routing_tasks`: resumable subscriber fan-out;
- `dag_inbox`: one `(result_oid, ingress_txn_id)` work reference.

An open source transaction is identified by:

```text
(slot_generation, xid, final_lsn)
```

One decoded row image is identified by:

```text
(ingress_txn_id, change_lsn, change_ordinal, image_ordinal)
```

Replay of the same identity and payload is a no-op. Replay of the same identity
with a different source, weight, or payload is corruption.

Open and committed transaction payload uses the same physical durable event
rows. Receiving the ordinary Commit message updates only the transaction
header and creates a routing task; it does not rewrite every payload row.

## Batch and transaction boundary

The Runtime receives complete protocol messages until one of these conditions:

- ingress row budget;
- ingress byte budget;
- ordinary Commit;
- scheduler fairness deadline.

It then opens one SPI transaction and atomically:

1. locks or creates the source transaction header;
2. inserts events idempotently in wire order;
3. records the replication batch and its ending WAL position;
4. advances durable counters and input sequence;
5. records commit control state when present.

No replication socket read or wait occurs inside that SPI transaction.

Prefix batches of an ordinary source transaction never advance
`persisted_lsn` and never produce replication feedback. Only the batch
containing Commit may atomically advance durable progress to `Commit.end_lsn`.
After that database commit, the Runtime may send a Standby Status Update.
Reporting an LSN before the entire source transaction is durably finalized is
data loss.

A crash after ingress commit but before feedback causes replay and
deduplication. A crash after feedback can still replay from an older
checkpoint, so event identities remain retained behind the conservative
`replay_safe_lsn` GC fence.

## Scheduling and apply

One Runtime loop interleaves:

1. one bounded ingress transaction when a complete message batch is ready;
2. one bounded routing page;
3. one complete source transaction for one DAG selected round-robin;
4. bounded ingress, Stage, and task GC.

The Runtime does not wait indefinitely for replication input. When the socket
has no complete message, DAG work remains eligible immediately.

Applying a DAG commit is atomic today: operator state, result rows,
`view_progress`, and deletion of the exact inbox identity commit together.
Ingress is bounded independently, so an arbitrarily large source transaction
does not need to fit in Runtime memory. Socket backpressure pauses the
walsender while the Runtime commits a batch. A highly expanding operator can still
make its apply transaction large; resumable operator output is a separate
execution feature, not part of ingress correctness.

## Removed architecture

There is no protocol-v1 SQL decoding fallback, slot-peek loop,
`routed_transactions` catalog, per-DAG background worker, or worker pool.
Activation fails closed unless `shiba.replication_conninfo` is configured.
