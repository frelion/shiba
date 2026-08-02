# M3 pgoutput contract

## M3.1 accepted input

M3.1 accepts exactly one complete, non-streaming pgoutput protocol-version-1
transaction with this shape:

```text
BEGIN -> target RELATION -> one or more INSERT -> COMMIT
```

The admitted relation is identified by its live PostgreSQL relation OID and has
one published built-in `int8` column. INSERT tuple data must be text format and
must contain a canonical `i64`; NULL, unchanged TOAST, binary values, extra
columns/relations/messages, partial transactions, and trailing bytes fail
closed. RELATION does not encode `NOT NULL`; M3.1 enforces the narrower runtime
boundary by rejecting NULL while the live test table owns its SQL constraint.

## Identity and ownership

`PgoutputSource` supplies the admitted `SourceId`, `SlotGeneration`, and relation
OID. Names are not identity. BEGIN XID becomes `IngressTransactionId`; COMMIT
commit LSN becomes the durable `SourceTransactionId` coordinate. BEGIN/COMMIT
LSN and timestamp fields must agree before the decoder returns a value.

The decoder is pure and owns no authority, connection, slot, publication, or
transaction. The existing M2 processor remains the sole writer and PostgreSQL
transaction owner. A decode error cannot call Apply and cannot advance
continuation.

## M3.2 acknowledgement crash point

The recovery gate deliberately separates database visibility from replication
feedback. A receiver with periodic status disabled observes a complete second
transaction, is stopped before feedback, and the M2 processor commits its Apply,
count result, and continuation while the slot's `confirmed_flush_lsn` remains at
the previous transaction. The receiver is then killed. Restarting the same slot
re-emits the identical `SourceTransaction`; processing returns `AlreadyApplied`
and all four durable facts remain unchanged. A later acknowledgement is therefore
an optimization for WAL retention, not a second Shiba authority.

## Evidence and deferred boundary

The wire layout follows the PostgreSQL 17/18 logical replication protocol and
message-format documentation. The test uses each major's own `pg_recvlogical`
and a disposable publication/slot. PostgreSQL's CLI appends a newline after each
XLogData payload; a test-only framing helper removes exactly those delimiters
before the production decoder sees the bytes.

M3.1 proves live decoding and clean capture restart. M3.2 proves the abnormal
post-result/pre-ack crash window on PG17 and PG18 using the slot's own
`confirmed_flush_lsn`. Production transport and slot lifecycle remain unproved;
they cannot introduce another continuation or writer. Tuple shapes beyond the
single non-null `int8` INSERT remain M4 work, and streaming remains out of scope.
