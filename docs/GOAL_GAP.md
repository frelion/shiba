# V2 goal gap during M12.1

M1–M9 are the reference correctness kernel, not a complete Shiba V2. They
prove the transaction and recovery semantics that later architecture must
preserve while replacing the fixed count path with explicit compiler,
operator, effect, and sink contracts.

| Original link | Current state | Remaining gap |
|---|---|---|
| Protocol | Strong IDs, canonical JSON/digest, strict pgoutput values | Broader cross-process plan/wire contracts |
| Catalog | Version, source/publication authority, operator state/result, sole `source_row_state`, one bootstrap lifecycle, M12.2--M12.5 admission/identity/recovery/governance | M12.6 performance and final release gate remain |
| Compiler | Strict V1 IR to ObjectAddress-bound plan | SQL frontend and broader plan language |
| Source Ingress | M10 production COPY BOTH plus complete M11 consistent snapshot, recovery, bounded million-row catch-up and live handoff | TLS/disconnect policy, Apply-time shutdown, reconnect/backoff, indefinite-writer tail latency and cross-host soak |
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

SQL frontend, non-aggregate operators, non-bigint result shapes, cross-host
sustained soak, empirical heap peak, contention tail latency, and M12.6 active-
source rebuild performance/release evidence remain outside the proved boundary.

M11.1 defined the initialization contract. A new slot's
`EXPORT_SNAPSHOT` result is the sole snapshot/WAL boundary; the snapshot name is
ephemeral, bootstrap IDs cannot be confused with source transactions, partial
results remain unavailable, and pre-scan-complete loss resets the entire hidden
pristine attempt. After scan completion, recovery retains the slot for M10
catch-up. M11.2 now implements the single checkpoint, strong bootstrap identity,
tagged transaction-local effects, bounded set-based batches, fence cutover, and
M10 live conversion. PG17/18 prove private `3/40`, concurrent-WAL `3/25`, active
`3/25`, then live `4/32`, with building/NULL visibility and SQL differential
equality.

M11.3 proves exact pre-scan reset/re-reservation, post-scan same-slot
resume, and active-before-feedback recovery without a second cursor or
continuation on PG17.10 and PG18.4. The matrix covers partial reset/replay/
rollback, worker competition, immediate PostgreSQL restart, killed ACK windows,
exact-fence replay, feedback-covered restart and SQL differential `4/50`.
Instruction-level kill at reservation and an active foreign old-slot conflict
remain narrower unproved cases. M11.4 proves one million rows in 100 bounded
10,000-row batches: PG17/PG18 scan+Apply are
3.098397625/3.136067542 s (322,747.47/318,870.68 rows/s), catch-up+activation are
1.320857542/1.329330584 s, and RSS growth is 3,664/3,664 KiB against thresholds
frozen before observation. SQL differential and live handoff are green.

M11 is complete at its pristine nullable-int8 CountRows/SumInt8 scope. V2 is
not complete. M12.1 freezes an offline rebuild: target identity becomes sole
building authority at destructive prepare, old computation is retired, and
activation promotes the same authority. It adds no candidate or parallel path.
M12.2 now proves the production admission transaction: all preflight failures
preserve old active authority, exact-old CAS installs target as the sole
`rebuild_prepared` building authority, results become `building/NULL`, old
rows/continuation/invalidations retire and private state resets to zero. The
old inactive slot remains and the target slot is absent for forward recovery.
The identity-index OID is an explicit CAS coordinate. Pre-M12 active state has
the proved three-row binding; every M12-produced generation has a fourth exact
identity-index binding selected by its persistent retired identity marker.
Recovery cannot dynamically substitute a replacement index: same-OID rename is
the narrow reconciliation case and a new OID fails closed. The failure-first
PG17 gate exposed invalid unparenthesized PL/pgSQL `IF CASE`. After correction,
PG17.10 and PG18.4 independently pass
`scripts/test-m12-rebuild-identity-authority.sh`: exact-four persistence,
catalog-only resume, same-OID rename, unrelated DDL isolation, malformed binding
rejection, repeated rebuild CAS, and replacement-OID rejection are proved.
Receiver-local token capability prevents a foreign old receiver from
authorizing Apply/ACK. PG17.10 and PG18.4 now pass
`scripts/test-m12-rebuild-snapshot-live.sh`: generation 2 rebuilds to 3 through
exact old-slot retirement, real exported snapshot, bounded scan, concurrent WAL
catch-up, fence, atomic activation and ordinary M10 live ACK without copying old
continuation or switching identity twice. M12.4 recovery and M12.5 DDL/role
governance are recorded below; M12.6 performance/release remains work.

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

M11.5 additionally proves on PG17.10 and PG18.4 that full bootstrap does not
depend on superuser execution: control/Apply/scanning, replication transport,
and result reading use three separated least-privilege identities. Role
swapping and missing function, source, or checkpoint privileges fail closed.
TLS/password policy, cross-host credential rotation, column-level grants, and
split-role successful abandoned-attempt replacement remain operational gaps,
not M11 data-correctness gaps.

M12 also records a PostgreSQL boundary: `pg_replication_slots` has no immutable
slot birth identity or per-slot ACL. All observable drift must fail closed, but
a same-name/same-shape replacement by a superuser or holder of the trusted
`REPLICATION` credential cannot be detected and is outside the correctness
threat model. Credential exclusivity and audit are deployment prerequisites;
no slot-birth marker is introduced in M12.1.

M12.4 adds the forward recovery contract, not a second bootstrap: durable
`rebuild_prepared` resumes its exact handoff; a lost M12 `creating`/`scanning`
snapshot is abandoned and replaced with a fresh BootstrapId, distinct slot and
exact successor generation. It cannot return to generation 2 or reuse its
continuation. `scripts/test-m12-rebuild-recovery.sh` is the focused PG17.10/
PG18.4 evidence gate. M12.5 now adds PG17.10/PG18.4
`scripts/test-m12-rebuild-governance.sh`: exact relation/publication/primary-
index/replica-identity/column/operator ObjectAddress governance, post-prepare
invalidation, same-source one-winner/different-source progress, and split
least-privilege roles. It also proves side-effect-free transport
`IDENTIFY_SYSTEM`+database and control target-`SELECT` preflight, exact-index
`AccessShareLock` validation through `pg_relation_size`, and fail-closed role
or grant loss. M12.6 performance/release remains unfinished.
