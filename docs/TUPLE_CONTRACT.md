# Tuple contract

## Admitted shapes through M4.4

M4 accepts exactly four INSERT relation shapes:

```text
()
(key int8 NOT NULL)
(key int8 NOT NULL, payload int8 NULL)
(key1 int8 NOT NULL, key2 int8 NOT NULL)
```

Both columns must be advertised as built-in `int8`. The key must use canonical
text encoding. A present payload may be canonical text or pgoutput `n` for SQL
NULL. Binary, unchanged-TOAST, NULL key, extra columns, and other types fail
closed. PostgreSQL RELATION messages do not carry nullability, so the source
table owns `NOT NULL` and the decoder independently rejects a NULL key.
The zero-column shape has no source row key; it is identified only by the
transaction identity and input sequence of its INSERT cause.

M4.4 additionally admits `UPDATE (key, payload)` only for the nullable-payload
shape when PostgreSQL emits a new tuple and the key is unchanged. Its encoding
rules are identical to INSERT. Old/key tuples and key mutation are excluded.

## Apply representation and ownership

`SourcePayload::Absent` means the admitted relation has no payload column;
`Null` and `Int8` mean the column is present. The existing
`shiba_internal.applied_insert` row stores this as `payload_present` plus
`payload_int8`, preserving the distinction between absent and SQL NULL.
For an empty tuple, `source_row_id` is NULL and payload is `Absent`. Multiple
empty rows remain distinct through the Apply fact's cause primary key; no
synthetic row identity is created.
For a composite identity, `source_row_id` and `source_row_sub_id` store the two
components. Keyed-row uniqueness covers the pair; empty tuples are excluded.

The M2 processor remains the only writer and transaction owner. Payload Apply,
count operator state, public result, and continuation commit or roll back
together. Replay uses the unchanged transaction identity and never re-applies a
payload.

For UPDATE the existing Apply row is current source state: only its payload is
mutated. The original INSERT cause remains stable, continuation records the
UPDATE transaction, and count state/result do not change. All remain under the
same writer and PostgreSQL transaction.

## Deferred boundary

DELETE, key-changing UPDATE, old/key tuples, replica identity changes, TOAST,
binary transfer, and schema drift are not admitted through M4.4. Composite
identities beyond two built-in `int8` columns are also excluded.
