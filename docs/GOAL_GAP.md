# V2 goal gap after M10

M1–M9 are the reference correctness kernel, not a complete Shiba V2. They
prove the transaction and recovery semantics that later architecture must
preserve while replacing the fixed count path with explicit compiler,
operator, effect, and sink contracts.

| Original link | Current state | Remaining gap |
|---|---|---|
| Protocol | Strong IDs, canonical JSON/digest, strict pgoutput values | Broader cross-process plan/wire contracts |
| Catalog | Version, source and publication binding/invalidation, ingress config/generation, operator definition/state/result | Non-pristine binding rebuild lifecycle |
| Compiler | Strict V1 IR to ObjectAddress-bound plan | SQL frontend and broader plan language |
| Source Ingress | M10 production protocol-v1/v2 COPY BOTH, bounded assembly, feedback/crash restart, governed slot/publication lifecycle, split roles, idle shutdown, and frozen local performance | TLS/disconnect policy, Apply-time shutdown, reconnect/backoff, allocator/RSS and cross-host soak |
| Source Apply | Current-row authority plus transaction-local before/after effects | Broader row shapes and non-aggregate effects |
| EffectStream | Non-durable transaction-local EffectBatch | Persisted effects intentionally absent |
| Runtime | Replay/recovery plus ordered registered-operator execution integrated with M10 ingress | Broader operators and production orchestration |
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

Non-pristine source binding rebuild, SQL frontend, non-aggregate operators,
non-bigint result shapes, cross-host sustained soak, empirical heap peak, and
contention tail latency remain outside the proved boundary.

M10.3 deliberately does not add persisted partial-stream recovery: partial
stream bytes are volatile and PostgreSQL's replication slot replays them after
restart. Its proven gate includes strict `Applied`, `AlreadyApplied`,
`EmptyCommitted`, and `Aborted` terminal authorization. Empty-stream structure
proves only empty output from the selected publication. The committed M10.4
catalog slice now binds exact publication OID plus frozen semantics, persists
membership/drop/recreate invalidation, and provides pristine-only slot-
generation CAS without mirroring progress. PG17/18 now also prove governed
receive/Apply/ACK, single-receiver exclusion, detach/reattach, exact two-
connection ownership, split least-privilege roles, bounded idle receive
shutdown, and local latency/throughput/backpressure limits. M10 is complete at
its declared production-ingress boundary. TLS/disconnect policy, shutdown
during Apply, reconnect daemon/backoff, allocator/RSS peaks, and cross-host soak
remain future ingress work. The complete V2 is not finished: non-pristine
binding rebuild, SQL frontend, broader operators/results, and production
orchestration remain.
