# Transaction and recovery contract

## Current proof boundary

Phase 1 has one transaction-owned operation: extension installation. Its owner
is PostgreSQL's `CREATE EXTENSION` transaction. Its durable result is either the
complete constrained installation identity or no installation. An empty
PostgreSQL 17/18 cluster test is the required proof of both successful install
and rollback-after-failure cleanup.

No component may publish a partial authority then repair it asynchronously.
After an error, the client must roll back the failed transaction before reuse;
retry happens at the complete installation transaction boundary. The catalog
does not promise a recovery worker, exactly-once effects, continuation storage,
CAS, crash replay, or concurrent source processing.

## Deferred contracts

Future contracts for continuations/CAS, source application, effects, and runtime
must define a unique writer, compare-and-swap key/version, commit order,
visibility point, crash state machine, and rollback behavior before code lands.
DDL invalidation must use PostgreSQL `ObjectAddress` semantics rather than name
matching. Until their tests are imported and re-proved, legacy failure cases are
reference evidence only.
