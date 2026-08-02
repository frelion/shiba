# Canonical Protocol V1 vector provenance

- **Classification:** A — contract and test evidence. This is data, not copied
  executable implementation.
- **Clean-room fixture:** `canonical-v1.json`, whose trailing LF is not part of
  the canonical JSON bytes. The L0 gate strips that one LF before verification.
- **Expected digest:** `82b80d7d38e26756d89e9d390525b1f57189f4a002d75603892d8f0d7c382b39`
  computed as `SHA-256(b"shiba.protocol.wire.v1\\0" || canonical_json)`.
- **Old evidence source:**
  `/Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/lib/tests.rs` and
  `/Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/primitive.rs`, at
  commit `6af593c`.
- **Old evidence command:**
  `cargo test -p shiba-protocol canonical`
- **Clean-room proof command:**
  `PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config ./scripts/test-l0.sh`
  (the protocol crate's `canonical_wire_vector_is_stable_and_roundtrips` test
  proves parse, re-encode, and digest equivalence; the script independently
  checks these exact fixture bytes and digest).

Unproved boundary: this vector has not yet been independently reproduced by a
non-Rust implementation or a PostgreSQL reference query.
