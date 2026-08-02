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

## M4.1 nullable payload extension

`PgoutputSource::with_nullable_int8_payload` admits exactly two columns: a
non-null canonical text `int8` key followed by a nullable text `int8` payload.
The pgoutput `n` tag becomes `SourcePayload::Null`; canonical text becomes
`Int8`. Key NULL and `u`/`b` tuple tags fail closed. The original constructor
retains the exact M3 single-key shape and produces `SourcePayload::Absent`.

## M4.2 empty tuple extension

`PgoutputSource::empty` admits only a zero-column RELATION and zero-column new
tuple. Each INSERT becomes a cause-scoped `SourceInsert::empty`; any advertised
or encoded column fails closed. No synthetic key is derived from names, order,
or WAL position.

## M4.3 composite identity extension

`PgoutputSource::composite_int8` admits exactly two built-in `int8` key columns.
Both tuple values must be canonical text and non-NULL; the decoder returns one
two-part row identity. This mode is explicit and cannot be confused with the
same-width nullable-payload mode.

## M4.4 unchanged-key UPDATE extension

Only nullable-payload mode admits UPDATE. Its exact shape is relation OID,
`N`, two columns: canonical non-NULL text `int8` key followed by SQL NULL or
canonical text `int8` payload. The decoder preserves message order in one
`SourceTransaction`. `K`/`O` old tuples, changed keys, UPDATE for other modes,
unchanged-TOAST, binary, and any other shape fail closed. M4.4 does not admit an
INSERT and UPDATE of the same row in one transaction.

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
they cannot introduce another continuation or writer. M4.1–M4.3 prove nullable,
empty, and fixed composite INSERT shapes. M4.4 proves unchanged-key nullable
payload UPDATE; DELETE, key-changing/old-tuple UPDATE, TOAST, and streaming
remain out of scope.
