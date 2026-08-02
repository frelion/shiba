# Tuple contract

## Admitted shapes through M5.2

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

M4.5 additionally admits `DELETE (key)` only for the single-key shape. The
pgoutput DELETE must select a `K` tuple with exactly one column containing a
canonical text `int8`. The relation OID and tuple selector are validated; NULL,
unchanged-TOAST, binary, `O`, extra columns, and non-canonical text fail closed.

M4.6 requires the enclosing RELATION to advertise default replica identity and
the exact key flags of the admitted shape. It observes but rejects a live FULL
identity relation and its `D + O` tuple before tuple decoding or Apply. No old
row representation is added.

M5.1 additionally admits `(key int8 NOT NULL, payload text NOT NULL)`. INSERT
owns the complete UTF-8 text bytes from a `t` tuple value. UPDATE admits only
`(key=t, payload=u)`: `u` is an instruction to preserve the existing durable
text, not a payload value and not NULL. Other payload tags fail closed.

M5.2 also admits `t` in text-payload UPDATE as a complete replacement UTF-8
value. It replaces only an existing text row. Source TOAST compression and
out-of-line storage do not change the admitted tuple representation.

## Apply representation and ownership

`SourcePayload::Absent` means the admitted relation has no payload column;
`Null`, `Int8`, and `Text` mean the column is present. The existing
`shiba_internal.applied_insert` row stores this as `payload_present` plus
mutually exclusive `payload_int8`/`payload_text`, preserving absent, SQL NULL,
int8, and text without a second row-state authority.
For an empty tuple, `source_row_id` is NULL and payload is `Absent`. Multiple
empty rows remain distinct through the Apply fact's cause primary key; no
synthetic row identity is created.
For a composite identity, `source_row_id` and `source_row_sub_id` store the two
components. Keyed-row uniqueness covers the pair; empty tuples are excluded.

The M2 processor remains the only writer and transaction owner. Payload Apply,
count operator state, public result, and continuation commit or roll back
together. Replay uses the unchanged transaction identity and never re-applies a
payload.

The existing `applied_insert` row is current source state: INSERT creates it,
UPDATE mutates only its payload, and DELETE removes it. The original INSERT
cause remains stable while the row exists. Continuation is the transaction
replay authority, not a change log or a second row-state table. DELETE removes
exactly the matching single-key row, decrements private count and public result
once, and records continuation under the same writer and PostgreSQL transaction.
The table name is known debt; M4.5 adds no alias, compatibility view, or second
authority.

## Deferred boundary

Admission for `D + O`, replica identity `FULL`, composite DELETE, key-changing
UPDATE, UPDATE old tuples, NULL text, TOAST keys, binary transfer, streaming
transactions, multiple sources, generation changes, and broader schema drift
is not admitted through M5.2. Composite
identities beyond the existing fixed INSERT shape are also excluded.
