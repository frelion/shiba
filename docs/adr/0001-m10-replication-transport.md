# ADR 0001: M10 logical-replication transport

Status: accepted for M10 implementation

## Decision

M10 uses the safe Rust `libpq` binding over PostgreSQL's maintained `libpq`
implementation. The replication connection starts with
`replication=database`, issues `START_REPLICATION ... LOGICAL` through the
simple-query protocol, receives server `CopyData` with `PQgetCopyData`, and
sends standby status `CopyData` with `PQputCopyData`.

This is not a Shiba implementation of the frontend/backend wire protocol.
`libpq` owns startup, authentication, TLS, message framing, error handling, and
COPY BOTH. Shiba owns only the documented replication payloads inside complete
`CopyData` values: XLogData (`w`), primary keepalive (`k`), and standby status
update (`r`).

The production receiver is synchronous and bounded. It reads until it has one
complete admitted source transaction, stops reading while the existing Runtime
processes that transaction, and resumes only after the result is known. There
is no channel, worker queue, or receiver-side connection pool. Each active
source uses exactly one replication connection and one Apply connection.

## Evidence and rejected alternatives

PostgreSQL 17 and 18 specify that logical replication uses
`replication=database`, simple-query commands, a COPY BOTH response to
`START_REPLICATION`, server `w`/`k` payloads, and client `r` feedback. See the
[PostgreSQL 18 streaming replication protocol](https://www.postgresql.org/docs/18/protocol-replication.html)
and the corresponding [PostgreSQL 17 protocol](https://www.postgresql.org/docs/17/protocol-replication.html).
The selected binding exposes `PQgetCopyData` and `PQputCopyData`; PG17 and PG18
linking and real duplex behavior remain mandatory integration gates.

The existing `postgres`/`tokio-postgres` dependency has no supported COPY BOTH
replication API. `pgwire-replication` and `pg_walstream` were rejected because
their current public paths parse pgoutput themselves and/or insert worker
queues. Adopting either would create a second decoder or weaken the direct
backpressure proof. `pg_recvlogical` remains test oracle infrastructure only.

## Framing boundary

One `CopyData` value is not assumed to equal one pgoutput message. A new small,
pure frame scanner identifies complete pgoutput frames across arbitrary
XLogData payload boundaries and a bounded assembler retains at most one
transaction. It does not interpret relation identity or tuples. The existing
Runtime decoder remains the only semantic pgoutput decoder and revalidates the
complete assembled transaction, including the 16 MiB wire and 10,000-change
limits.

Protocol-v2 uses the same boundary. The scanner recognizes streamed frame
lengths and XIDs across arbitrary XLogData chunks, but only a complete stream
commit enters the existing Runtime streamed decoder. Partial segments and `E`
are nonterminal. A matching abort bypasses Runtime; its safe feedback coordinate
is the enclosing XLogData `dataStart`, because protocol-v2 `A` contains no LSN.
No partial stream is persisted: slot replay is the recovery mechanism and no
second spool authority exists.

Feedback authorization is a closed set: Runtime `Applied`, Runtime
`AlreadyApplied`, strict `EmptyCommitted`, or legal top-level `Aborted`.
`EmptyCommitted` requires exactly
`S(first=true) E (S(first=false) E)* c`: at least one complete empty segment,
the same nonzero XID, zero flags, valid commit/end positions, and no other frame
or trailing byte. Recognition uses constant state within the 16 MiB bound. A
legal `R/I` selects the non-empty path through the sole Runtime decoder; every
other form fails closed. Empty commit requires an explicit ACK and creates no
continuation.
This proves only empty output for the process-selected publication. M10.4 must
bind and validate publication identity so mutation, removal/re-add, drop, or
same-name recreation cannot be mistaken for a legitimate empty transaction.

## Feedback decision

The safe committed coordinate is the pgoutput terminal transaction `end_lsn`,
not its `commit_lsn`, an XLogData `wal_end`, or a keepalive `wal_end`.
PostgreSQL defines `end_lsn` as the transaction end position and standby status
positions as the last durable WAL byte plus one. M10 will verify this mapping
differentially on PG17 and PG18 before feedback code is accepted. M10.2 has now
passed that gate on both majors; the first failing test also established that
`PQputCopyData` must be followed by an explicit libpq flush.

For a commit, the receiver may advance its in-memory acknowledged position only
after complete decode and after Runtime returns `Applied` or `AlreadyApplied`
from a committed PostgreSQL transaction. A requested keepalive reply reports
only that last acknowledged position. Decode, Apply, or Operator failure stops
the receiver without advancing feedback.

PostgreSQL's replication slot is the transport cursor authority.
`source_continuation` remains the computation/replay authority. Shiba does not
mirror `confirmed_flush_lsn` in its catalog.

## Consequences and open proof

This choice adds one production dependency with a narrow boundary and a system
`libpq` requirement. The binding exposes a defective raw-handle `Clone`; Shiba
therefore keeps the connection private, exclusive, and never clones it. M10
must still prove disconnect behavior, async server
errors during COPY, graceful shutdown, slot and
publication validation, least privilege, buffer peaks, and PG17/18 parity.
Failure of those gates reopens this ADR; it does not authorize a CLI receiver,
raw wire implementation, second decoder, or compatibility path.
