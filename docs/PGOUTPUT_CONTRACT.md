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

## Evidence and deferred boundary

The wire layout follows the PostgreSQL 17/18 logical replication protocol and
message-format documentation. The test uses each major's own `pg_recvlogical`
and a disposable publication/slot. PostgreSQL's CLI appends a newline after each
XLogData payload; a test-only framing helper removes exactly those delimiters
before the production decoder sees the bytes.

M3.1 proves a clean capture stop/restart and a subsequent transaction. Production
transport ownership, slot admission, feedback/acknowledgement recovery, and an
abnormally terminated capture remain M3.2 work. Streaming remains out of scope.
