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

## Phase gates

Every later module must name: its durable authority, sole writer, transaction
owner, input identity, retry boundary, recovery proof, and deletion/DDL policy.
No later code may use an old authority as a fallback. An implementation is
accepted only after its clean-room tests prove its new contract; legacy tests are
evidence inputs, not implementation dependencies.

**Unproved:** production replication transport and slot ownership, empty and
composite identities, UPDATE/DELETE, TOAST, catalog binding inspection,
concurrency, generation changes, general operators, and recovery workers remain.
