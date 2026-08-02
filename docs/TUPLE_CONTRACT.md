# Tuple contract

## M4.1 admitted shape

M4.1 accepts exactly two INSERT relation shapes:

```text
(key int8 NOT NULL)
(key int8 NOT NULL, payload int8 NULL)
```

Both columns must be advertised as built-in `int8`. The key must use canonical
text encoding. A present payload may be canonical text or pgoutput `n` for SQL
NULL. Binary, unchanged-TOAST, NULL key, extra columns, and other types fail
closed. PostgreSQL RELATION messages do not carry nullability, so the source
table owns `NOT NULL` and the decoder independently rejects a NULL key.

## Apply representation and ownership

`SourcePayload::Absent` means the admitted relation has no payload column;
`Null` and `Int8` mean the column is present. The existing
`shiba_internal.applied_insert` row stores this as `payload_present` plus
`payload_int8`, preserving the distinction between absent and SQL NULL.

The M2 processor remains the only writer and transaction owner. Payload Apply,
count operator state, public result, and continuation commit or roll back
together. Replay uses the unchanged transaction identity and never re-applies a
payload.

## Deferred boundary

Empty tuples, composite identity, UPDATE, DELETE, replica identity selection,
TOAST, binary transfer, and schema drift are not admitted by M4.1.
