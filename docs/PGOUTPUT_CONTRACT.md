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

## M4.5 stable-key DELETE extension

Only key-only mode admits DELETE. The exact protocol-version-1 wire shape is
relation OID followed by tuple selector `K`, column count `1`, and one `t`
column containing canonical decimal text for an `i64`. The decoder returns a
DELETE change only after checking the admitted relation OID, selector, count,
tuple tag, length, canonical key, transaction envelope, and trailing boundary.
An invalid selector or tuple tag fails during pure decode, before the processor
can write.

This is deliberately only PostgreSQL's default-replica-identity `D + K` path
for a stable, single-column `int8` key. `D + O`, replica identity `FULL`,
composite DELETE, key-changing UPDATE, UPDATE old tuples, TOAST, streaming,
generation changes, and multiple sources remain outside the admitted language.
The wire semantics are grounded independently in the PostgreSQL
[17 logical replication message formats](https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html)
and [18 logical replication message formats](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html).

## M4.6 replica identity admission

The RELATION message is admitted only when its replica identity byte is
PostgreSQL default (`d`) and its column key flags exactly match the selected
frozen shape: `[]`, `[1]`, `[1, 0]`, or `[1, 1]`. The decoder does not infer a
key from a name, column order, or tuple contents. `n`, `f`, `i`, an unknown
identity byte, or a key-flag mismatch fails as relation shape before a
`SourceTransaction` exists.

A live `ALTER TABLE ... REPLICA IDENTITY FULL` followed by DELETE emits
`RELATION f` and `D + O`; M4.6 observes that real boundary but deliberately
rejects it. It does not decode `O`, add a FULL-row identity, or advance the
existing continuation past the rejected transaction.

## M5.1 unchanged TOAST extension

`PgoutputSource::with_text_payload` admits one default-identity relation with
key flags `[1, 0]` and built-in type OIDs `[int8, text]`. INSERT requires a
canonical text-format int8 key and a present text-format UTF-8 payload. UPDATE
requires the same key followed by exactly `u`; the token means retain the
previously applied text value and carries no replacement bytes.

`u` on INSERT, `n`/`t`/`b` as the UPDATE payload, invalid UTF-8, old/key tuples,
and other relation shapes fail during pure decode. M5.1 does not admit a new
text value in UPDATE or infer data from the source table outside pgoutput.

## M5.2 incompressible TOAST replacement

Text-payload UPDATE additionally admits a present `t` value. The decoder owns
its complete UTF-8 bytes and the processor replaces the existing durable text;
`u` continues to mean retain the old value. `n`, `b`, invalid UTF-8, and a
replacement for a non-text row fail closed. PostgreSQL storage/compression is
evidence about the source value, not a second wire format or Shiba authority.

## M5.3 composite-key DELETE

The existing composite-int8 shape additionally admits DELETE with relation OID,
selector `K`, column count `2`, and two canonical non-NULL text int8 values.
Both components become the existing row identity pair. A bad second tag,
partial/extra pair, `O`, or DELETE for another shape fails before writes.

## M5.4 replica identity index admission

`PgoutputSource::with_replica_index` explicitly admits the existing single
`int8` key shape when RELATION advertises replica identity index (`i`) and the
sole column has key flag `1`. The default constructor continues to require
`d`; neither path infers identity from relation or index names.

The index-bound path uses the unchanged INSERT and `D + K` tuple languages and
the existing processor transaction. A live switch back to default identity is
therefore rejected as relation drift before a `SourceTransaction` or durable
write exists. M5.4 does not add persistent source binding, general identity
configuration, composite replica indexes, `FULL`, `NOTHING`, or `D + O`.

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
payload UPDATE, including non-NULL canonical text and a valid UPDATE whose
missing target rolls back mutation and continuation. M4.5 proves real `D + K`,
exact key decoding, atomic current-state deletion and count publication,
failure rollback, crash/retry, and exact replay on both supported PostgreSQL
majors. M4.6 proves default identity/key-flag admission and live FULL drift
rejection before writes. M5.1 proves exact text INSERT plus a real
unchanged-TOAST `u` UPDATE. M5.2 proves a real default-EXTENDED, out-of-line,
uncompressed `t` replacement. Both cover crash/retry/replay with exact durable
text. Broader identity, NULL/binary text, TOAST keys, and streaming remain out
of scope. M5.3 additionally proves exact composite `D + K`, pair isolation,
missing-row rollback, crash/retry, and replay. M5.4 proves explicit
single-column replica-index admission, atomic INSERT/DELETE recovery, and live
default-identity drift rejection without adding a catalog authority.
