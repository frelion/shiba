# Architecture boundary

## Direction

The intended dependency line is:

```text
Protocol -> Catalog -> Compiler -> Source Ingress -> Source Apply
         -> EffectStream -> Runtime -> minimal Operator -> Result Sink
```

Phase 1 proved `Protocol -> Catalog`. M2 implements only the narrow path from a
test-constructed committed source transaction through Apply, deterministic
count, Result Sink, and continuation. A right-hand component may depend on the
contracts to its left; a left-hand component must not import execution,
registration, or result code. PostgreSQL is the durable authority; Rust values
are not a second authority.

## Authority and schemas

`shiba_internal.catalog_identity` is the one Phase-1 writer-owned catalog fact.
Its constraints make the singleton and frozen versions explicit. The private
schema is not a public table contract. `shiba.versions()` is a read-only public
view of that fact. Neither user schemas nor legacy schemas are queried or
mirrored. The function pins `search_path` so an invoker cannot redirect catalog
lookup by creating a same-named object.

M2 adds four purpose-specific facts: `applied_insert`, `count_state`,
`source_continuation`, and public `shiba.count_result`. They have one logical
writer, the M2 processor, and are never independently repaired or mirrored.

M3.1 adds a pure Source Ingress decoder inside `shiba-runtime`. Its admitted
relation OID, source ID, and slot generation are explicit input configuration;
schema and relation names are decoded only for bounds checking and are not
identity. The decoder owns no connection and no durable state. It produces an
M2 `SourceTransaction` only after validating COMMIT, so the processor remains
the only transaction owner and writer.

M3.2 adds no production state. PostgreSQL's slot may lag the committed Shiba
continuation and replay an already visible transaction; the processor's durable
identity check makes that replay a no-op. Slot feedback never writes Shiba state
and cannot make a result visible.

M4.1 extends each existing Apply fact with `payload_present` and nullable
`payload_int8`. This distinguishes a relation with no payload column from a
present SQL NULL without creating another Apply table or authority. The decoder
admits only key-only or key-plus-nullable-int8 shapes; the same processor writes
the payload, count state, result, and continuation in one transaction.

M4.2 represents a zero-column INSERT with no `source_row_id`. Its durable cause
identity remains transaction plus input sequence, so multiple empty rows in one
transaction are distinct without a synthetic key. The existing Apply table and
writer remain unchanged; keyed paths continue to require a non-null key.

M4.3 stores a fixed second `int8` key component in the same Apply fact. A
partial `NULLS NOT DISTINCT` unique index covers only keyed rows, preserving
single-key uniqueness, composite-pair uniqueness, and multiple cause-identified
empty rows. The processor is still the sole writer and transaction owner.

M4.4 updates the nullable payload in that existing Apply row; it creates no
UPDATE log or second authority. The original INSERT cause remains attached to
the row, while `source_continuation` durably records each committed source
transaction. UPDATE does not advance count. Apply mutation, unchanged count
state/result, and continuation are owned by the same processor transaction.

M4.5 removes that same Apply row for a pgoutput `D + K` change with one stable
`int8` key. `applied_insert` has therefore evolved into the sole current
source-row-state authority: INSERT creates, UPDATE mutates, and DELETE removes.
Its name is recorded debt; renaming it is deferred because doing so would expand
this slice, and no alias, compatibility view, or second state table is allowed.
The processor remains the sole writer and PostgreSQL transaction owner. It
deletes exactly one row, decrements private count without underflow, publishes
the matching result, and records continuation in the same transaction.

M4.6 tightens the pure decoder's existing relation-shape check. It admits only
PostgreSQL default replica identity (`d`) and exact key flags for each frozen
shape: none for empty, key for single-key, key/non-key for nullable payload,
and two keys for composite identity. This adds no durable state or writer. A
live `FULL` relation is rejected before the processor owns a transaction.

M5.1 extends the same `applied_insert` row with `payload_text`; it does not add
a table or authority. A text INSERT writes the exact UTF-8 value. An
unchanged-TOAST UPDATE carries only a `u` token, so the processor validates that
the target is an existing text row and retains its durable value. Row state,
unchanged count/result, and continuation still share one processor transaction.

M5.2 uses the same text row and writer for a complete replacement value. The
wire owns the new UTF-8 bytes; the processor overwrites only `payload_text`,
keeps count/result stable, and records continuation in the same transaction.
No TOAST store, fetcher, or alternate value authority is introduced.

M5.3 carries the existing composite pair into DELETE without introducing a
general row-identity layer. The same current-state row stores both components;
the processor matches both (including NULL-safe single-key handling), deletes
exactly one row, decrements count/result, and commits continuation atomically.

## Phase gates

Every later module must name: its durable authority, sole writer, transaction
owner, input identity, retry boundary, recovery proof, and deletion/DDL policy.
No later code may use an old authority as a fallback. An implementation is
accepted only after its clean-room tests prove its new contract; legacy tests are
evidence inputs, not implementation dependencies.

**Unproved:** production replication transport and slot ownership, admission
for `D + O` or replica identity `FULL`, key-changing/composite UPDATE and old
tuples, NULL text, binary payloads, TOAST keys,
streaming transactions, catalog binding inspection, concurrency,
generation changes, multiple sources, general operators, and recovery workers
remain.
