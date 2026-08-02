# V2 goal gap after M9.2

M1–M8 are the reference correctness kernel, not a complete Shiba V2. They
prove the transaction and recovery semantics that later architecture must
preserve while replacing the fixed count path with explicit compiler,
operator, effect, and sink contracts.

| Original link | State after M9.2 | Remaining gap |
|---|---|---|
| Protocol | Strong IDs, canonical JSON/digest, strict pgoutput values | Broader cross-process plan/wire contracts |
| Catalog | Version, source bindings/invalidation, operator definition/state/result | Binding rebuild lifecycle |
| Compiler | Strict V1 IR to ObjectAddress-bound plan | SQL frontend and broader plan language |
| Source Ingress | Production protocol-v1 COPY BOTH plus bounded assembly; protocol-v2 decoder evidence | Durable feedback/restart, production streaming assembly and slot lifecycle |
| Source Apply | Current-row authority plus transaction-local before/after effects | Broader row shapes and non-aggregate effects |
| EffectStream | Non-durable transaction-local EffectBatch | Persisted effects intentionally absent |
| Runtime | Replay/recovery plus ordered registered-operator execution | Production ingress lifecycle and broader operators |
| Operator | Database-free CountRows/SumInt8 contract; both integrated atomically | Add non-aggregate kinds |
| Result Sink | Operator-keyed private state and public result | Non-bigint result shapes |

## Proven reference boundary

The synchronous Runtime has one PostgreSQL transaction owner and proves atomic
source-row mutation, count/result publication, continuation, replay, crash
rollback, same-source CAS, independent-source progress, DDL fail-closed
admission, a 16 MiB/10,000-change input boundary, and PG17/18 reference latency
ceilings. These facts constrain future slices; the fixed count authority itself
has been removed.

## Still unproved

Production replication feedback/slot restart, production streamed assembly,
persisted partial-stream
recovery, source binding rebuild and generation lifecycle, SQL frontend,
non-aggregate operators, non-bigint result shapes, sustained throughput,
empirical heap peak, and contention tail latency remain outside M9.2.
