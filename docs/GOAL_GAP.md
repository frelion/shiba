# V2 goal gap after M8

M1–M8 are the reference correctness kernel, not a complete Shiba V2. They
prove the transaction and recovery semantics that later architecture must
preserve while replacing the fixed count path with explicit compiler,
operator, effect, and sink contracts.

| Original link | State after M8 | Remaining gap |
|---|---|---|
| Protocol | Strong IDs, canonical JSON/digest, strict pgoutput values | Operator IR and compiled-plan contracts |
| Catalog | Database-local version, source bindings and exact DDL invalidation | Operator definitions/state and binding rebuild lifecycle |
| Compiler | Not present | Strict declarative IR to ObjectAddress-bound plan |
| Source Ingress | Real protocol-v1/v2 pgoutput decode in PG17/18 tests | Production receiver, feedback and slot lifecycle |
| Source Apply | One current-row authority with INSERT/UPDATE/DELETE/TOAST | Transaction-local row effects for multiple operators |
| EffectStream | Not present | Non-durable before/after batch; no effect log |
| Runtime | Replay, crash, CAS, DDL and synchronous concurrency semantics | General operator execution and production ingress lifecycle |
| Operator | Fixed count calculation embedded in Runtime | Database-free shared contract and a second distinct operator |
| Result Sink | Fixed `count_result` table | Operator-keyed state and public result sink |

## Proven reference boundary

The synchronous Runtime has one PostgreSQL transaction owner and proves atomic
source-row mutation, count/result publication, continuation, replay, crash
rollback, same-source CAS, independent-source progress, DDL fail-closed
admission, a 16 MiB/10,000-change input boundary, and PG17/18 reference latency
ceilings. These facts are constraints on M9, not a reason to preserve the fixed
count authority.

## Still unproved

Production replication receiver/feedback/slot restart, persisted partial-stream
recovery, source binding rebuild and generation lifecycle, a compiler, general
operators, a general result sink, sustained throughput, empirical heap peak and
contention tail latency remain outside M8. M9 addresses only the compiler,
operator, transaction-local EffectStream, and result-sink gaps.
