# Protocol contract

## Phase-1 contract

Protocol is a pure value boundary: explicit versioned messages, canonical
serialization/digest rules, and validation that rejects malformed or ambiguous
input. It owns no PostgreSQL connection, schema mutation, source registration,
or runtime state. The Protocol crate must be usable by Catalog without creating
a reverse dependency.

The version-1 canonical envelope is compact JSON with a fixed field order and a
closed message kind. Its digest is SHA-256 over the exact canonical bytes after
the domain separator `shiba.protocol.wire.v1\0`. It is not calculated over map
iteration order, display formatting, SQL text, or transport framing. Unknown
fields, aliases, trailing JSON, zero versions, and unsupported versions are
rejected. A version change is a new contract and cannot silently reinterpret old
bytes.

**Fact:** `tests/fixtures/protocol/canonical-v1.json` is the attributed,
data-only canonical JSON/digest vector. `scripts/test-l0.sh` independently checks
its exact bytes and digest. Protocol's directed tests generate the same bytes
and digest and re-prove semantic sensitivity, round-trip decoding, closed
variants, and strict rejection in the clean-room crate. The adjacent
`canonical-v1.provenance.md` names the evidence source and both proof commands.

**Not proved:** the Phase-1 fixture and unit tests do not prove inter-process
transport compatibility, pgoutput or streaming-transaction semantics, or a
Compiler, Ingress, Apply, EffectStream, Runtime, Operator, or Sink consumer.
