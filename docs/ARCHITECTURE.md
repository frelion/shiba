# Architecture boundary

## Direction

The intended dependency line is:

```text
Protocol -> Catalog -> Compiler -> Source Ingress -> Source Apply
         -> EffectStream -> Runtime -> minimal Operator -> Result Sink
```

Only `Protocol -> Catalog` exists in Phase 1. A right-hand component may depend
on the contracts to its left; a left-hand component must not import execution,
registration, or result code. PostgreSQL is the durable authority for catalog
facts; Rust values are not a second authority.

## Authority and schemas

`shiba_internal.catalog_identity` is the one Phase-1 writer-owned catalog fact.
Its constraints make the singleton and frozen versions explicit. The private
schema is not a public table contract. `shiba.versions()` is a read-only public
view of that fact. Neither user schemas nor legacy schemas are queried or
mirrored. The function pins `search_path` so an invoker cannot redirect catalog
lookup by creating a same-named object.

## Phase gates

Every later module must name: its durable authority, sole writer, transaction
owner, input identity, retry boundary, recovery proof, and deletion/DDL policy.
No later code may use an old authority as a fallback. An implementation is
accepted only after its clean-room tests prove its new contract; legacy tests are
evidence inputs, not implementation dependencies.

**Unproved:** catalog binding inspection, identity admission, compilation and
all execution/recovery paths remain future work.
