# V2 goal gap after M14

## M15.1--M15.4 SQL frontend status

The bounded SQL `SELECT` parser emits an ephemeral `UnboundQuery`. M15.4 now
implements a pure Binder for the first single-source projection/filter/compute
shape and a short PostgreSQL registration adapter. It resolves only a uniquely
registered SourceId and exact relation/column/effective-identity ObjectAddresses
inside the registration transaction, lowers canonical QuerySpec, and delegates
durable writes to Runtime's existing transaction-local writer. Raw SQL,
`sqlparser` AST and PostgreSQL server parse/optimizer trees are not authority.
Rebuild still rebinds the durable QuerySpec against explicit target descriptors
and generation-specific effective identity.

The contract fixes projection/filter/compute/grouped Count/Sum and narrow
two-source INNER JOIN syntax, explicit rejections, identifiers/aliases,
three-valued NULL semantics, stable spans/errors, DDL locking and hard bounds.
M15.3 pins `sqlparser` 0.62.0 with default features disabled and only `std`, so
the frontend dependency graph has no C-toolchain, PostgreSQL or Runtime
dependency. Twenty-two pure tests prove the accepted/rejected AST allowlist,
limits, exact spans, closed snake_case errors, canonical equivalence and
fail-closed iterative validation. A failure-first case found that admitting a
4,096-token left-deep tree could overflow during recursive AST drop; the parser
now applies a conservative structural ceiling before constructing that tree.
Runtime, Ingress and Operator remain parser-free and all durable authority is
unchanged.

M15.2 replaced GraphSpec recipes with generic QuerySpec
nodes/results through compiler version 2, registration and rebuild compilation.
It did not add the SQL parser. Ten pure Compiler tests prove strict bounds,
canonical digest/topology, generic M14-shape equivalence, exact catalog type
coordinates and corrupt/old declaration rejection. The complete PG17.10/PG18.4
release matrix passed 52 scripts and 104 invocations, including registration,
bootstrap, rebuild, recovery and two-source lifecycle gates. M15.2 is green;
M15 as a whole is not complete.

M15.4 directed PG17.10/PG18.4 runs prove the quoted single-source SQL example
through atomic registration/rollback, full-keyed SQL differential, deterministic
DDL-first drift rejection, exported-snapshot bootstrap, concurrent I/U/D
catch-up, production receiver Apply/ACK, Apply-before-ACK crash replay, and rebuild to
changed target ObjectAddresses followed by live Apply/ACK. QuerySpec and
OperatorGraph authority, continuation, transaction ownership and ACK semantics
are unchanged. The new script is enrolled as gate 53, but the complete
53-script/106-invocation release matrix has not been run as M15.4 evidence.

Still unproved are aggregate SQL (M15.5), two-table Join SQL (M15.6),
least-privilege SQL registration, the frozen 10,000-query and registration
performance/heap gates, and the final full matrix (M15.7). M15.4 is not M15
completion.

## M14.7 completion status

M14 is complete at the declared Operator Development boundary. Commit
`206f085` preserves the prior 51-script/102-invocation PG17.10/PG18.4 release
matrix. The independently green two-version `test-m14-join-lifecycle.sh` closes
the remaining real lifecycle evidence. The final runner passed 52 unique
scripts and 104 PostgreSQL invocations.

That gate proves a cross-schema two-source Join through one exported snapshot,
both-member scan, concurrent two-source WAL catch-up, activation, governed live
Apply and explicit ACK; durable-Apply/before-ACK session loss converges through
`AlreadyApplied` and exact feedback. It then rebuilds the same non-pristine
graph from generation 1 to 2, catches up concurrent WAL, activates once, and
continues live with a full keyed SQL oracle and exact continuation/slot checks.

Five-run same-scene Apply medians are 771.019625 ms on PG17 (-1.44% versus
782.302750 ms baseline) and 821.920250 ms on PG18 (+4.42% versus 787.157125 ms
baseline), below 899.648163/905.230694 ms ceilings. Million-row rebuild totals
are 7.588033917/7.703419083 s, RSS +6,224/+6,272 KiB and retained WAL
252,905,752/252,938,872 bytes, below 268,435,456 bytes.

M14 completion is not V2 completion. Broader SQL frontend, outer/three-table joins,
windows, DISTINCT, Min/Max/Avg, broader types/results, plugins, scheduler,
cross-host failover and long-running production soak remain unproved.

## M14.6 production cutover status

The production schema and execution/lifecycle code have been cut over to the
same canonical `OperatorGraph` proved purely in M14.5. Single-source execution
is a one-member graph; the admitted JOIN is a two-member graph. Both use one
graph ingress configuration/publication/slot/generation, one graph transaction,
one `graph_continuation`, generic node state/results, one exported-snapshot
bootstrap and graph-wide rebuild. Ordered member checkpoints are subordinate to
that one bootstrap lifecycle.

The old per-operator and per-source execution authorities are removed rather
than adapted. Runtime, Ingress, Bootstrap and Rebuild consume graph identity and
contracts only; they do not know CountRows, SumInt8, Filter, grouped nodes,
Join, Materialize, fixed node IDs or node counts. Intermediate DeltaBatch values
remain transaction-local.

The directed M14.6 Runtime gate is green on PG17.10/PG18.4. It proves
cross-schema keyed results, an atomic transaction changing both sides, right
fan-out/retraction, join-key changes, rollback/retry/exact replay and exact
right-PK replacement invalidation. Source registration durably binds either a
default PK or explicit replica-identity index and rejects a relation with none.
Strict Compiler tests cover ComputedProject and filtered grouped Count/Sum
pipelines; Runtime consumes only generic result contracts.

M14.7 and the lifecycle evidence closure prove receiver/bootstrap/rebuild,
least-privilege, release and performance behavior without a second authority.

## M14.5 pure JOIN kernel status

M14.4 freezes the two-source JOIN authority. M14.5 implements its pure
Compiler/Operator half: GraphId, ordered SourcePorts, exact effective right
replica-identity binding, generic partition state and pre-to-final multi-input
evaluation. The 300-step relational differential, 20,000/20,001 fan-out bound
and `O(n^2)` to `O(n log n)` affected-row correction are proved. M14.6 supplies
the Runtime/Catalog/lifecycle cutover and directed PostgreSQL Runtime evidence.
Exactly two sources share
one database/publication/slot/generation, graph continuation and ACK,
exported-snapshot bootstrap and graph-wide rebuild. Admission binds exact
relation/column ObjectAddresses and the exact right PK/UK index. See
[JOIN_AUTHORITY_CONTRACT.md](JOIN_AUTHORITY_CONTRACT.md) and
[ADR 0006](adr/0006-m14-two-source-join-authority.md).

The complete graph bootstrap/rebuild/least-privilege and frozen performance
boundaries are proved. No per-source continuation, second Runtime, persisted
DeltaBatch or adapter was introduced.

## M14.3 implementation status

Typed values/rows/deltas, strict expressions, Filter, Compute, Project and
Materialize now run in the database-free kernel. The compiler resolves names
once to exact ObjectAddress slots in the complete binding-ordered source
layout. Source Apply and bootstrap construct one transaction-local DeltaBatch;
Runtime performs generic plan/state/result persistence without matching node or
operator names. The former ProjectRows declaration and execution variant no
longer exists in production; its behavior is Project followed by Materialize.

M14.3 added the sole generic keyed node-state authority, KeyBy, GroupedCount and
GroupedSumInt8. PG17.10/PG18.4 full-row SQL differentials prove I/U/D, NULL and
all-NULL groups, key changes, empty-group deletion, whole-transaction rollback,
retry and replay with set-based persistence. M14.4--M14.7 subsequently added
two-input Join, graph-wide lifecycle/bootstrap/rebuild and the full release/
performance matrix. M14.3 alone was not M14 completion.

## M14.1 contract status

M14.1 freezes one canonical typed OperatorGraph, graph-wide transaction and
continuation authority, generic keyed state, explicit terminal materialization,
bounded two-source INNER JOIN, and the SourceId/row/node lock order. M14.2 adds
the stateless single-input slice and M14.3 adds grouped keyed state. Later M14
stages must add Join, cut over lifecycle authority, and re-prove the complete
PG17/18 matrix without a second Runtime or continuation.

## M13 completion status

M1–M9 are the reference correctness kernel, not a complete Shiba V2. They
prove the transaction and recovery semantics that later architecture must
preserve while replacing the fixed count path with explicit compiler,
operator, effect, and sink contracts.

| Original link | Current state | Remaining gap |
|---|---|---|
| Protocol | Strong IDs, canonical JSON/digest, strict pgoutput values | Broader cross-process plan/wire contracts |
| Catalog | Version, source facts/effective identity, sole canonical graph plan/membership, graph ingress/continuation/bootstrap, generic graph node state/results, sole `source_row_state`, complete M14 evidence | Broader graph shapes/types |
| Compiler | Strict IR plus M15.4 pure binding of one SQL projection/filter/compute shape to an ObjectAddress-bound graph | Aggregate/Join SQL lowering and broader plan language |
| Source Ingress | M10 production COPY BOTH plus complete M11 consistent snapshot, recovery, bounded million-row catch-up and live handoff | TLS/disconnect policy, Apply-time shutdown, reconnect/backoff, indefinite-writer tail latency and cross-host soak |
| Source Apply | SourceId-tagged current rows plus transaction-local before/after effects for every graph member | Broader row shapes and identities |
| EffectStream | Non-durable transaction-local DeltaBatch/MultiInputBatch | Persisted effects intentionally absent |
| Runtime | Generic graph decode/topology, ordered multi-input dispatch and atomic scalar/keyed persistence with complete M14 recovery/performance evidence | Cross-host/daemon/soak boundaries |
| Operator | Database-free stateless/grouped/computed nodes and bounded two-source INNER JOIN on one kernel | Broader operator families remain unproved |
| Result Sink | Graph-terminal visibility headers, scalar bigint arm and active-only nullable keyed rows | Non-bigint result types |

M13.2 implemented the pure generic plan/state/transition contract. M13.3 made it
the sole Catalog/Runtime path and proved CountRows, SumInt8 and ProjectRows
atomically against PG17.10/18.4 SQL oracles. M13.4 removed fixed operator
IDs/counts/column positions from ingress, bootstrap and rebuild and re-proved
the directed M10--M12 matrix. M13.5 closed the complete PG17.10/PG18.4 release
matrix, five-run performance comparison, forbidden-specialization scan and
documentation evidence.

## Proven reference boundary

The synchronous Runtime has one PostgreSQL transaction owner and proves atomic
source-row mutation, generic scalar/keyed result publication, continuation,
replay, crash rollback, same-source CAS, independent-source progress, DDL
fail-closed admission, a 16 MiB/10,000-change input boundary, and PG17/18
reference latency ceilings. These facts constrain future slices; the fixed
count authority itself has been removed.

## Still unproved

Aggregate/Join SQL frontend completion, additional operator families,
non-bigint result shapes, cross-host
sustained soak, empirical heap peak, contention tail latency, automatic
receiver supervision/reconnect, and broader binding/operator lifecycles remain
outside the proved boundary.

## M13.5 release evidence

The fixed-order release runner passed 49 unique PostgreSQL scripts on both
PG17.10 and PG18.4: 98 successful invocations. The required production scan for
fixed CountRows/SumInt8 IDs, kinds and cardinality assumptions is empty. Five
post-change same-machine CountRows+SumInt8 runs measured Apply medians of
782.302750 ms on PG17 and 787.157125 ms on PG18, versus frozen pre-change
medians of 768.727625/770.174125 ms: regressions of about 1.8%/2.2%, below the
15% stop lines.

The historical million-row M12 performance scenario remains the frozen
CountRows+SumInt8 comparison rather than adding one million keyed projection
rows. The final matrix observed PG17/PG18 scan 4.525961791/4.449666292 s,
catch-up 1.974170791/2.101600625 s, activation 11.072250/12.180917 ms, total
6.556642834/6.609238167 s, RSS +5,184/+5,152 KiB and retained WAL
252,876,464/252,917,880 bytes. ProjectRows bootstrap, catch-up, rebuild and
recovery correctness remain independently covered by their full keyed SQL
oracles; no assertion or frozen threshold was removed.

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
M12.2 proved the production admission transaction: all preflight failures
preserve old active authority, exact-old CAS installs target as the sole
`rebuild_prepared` building authority, results become `building/NULL`, old
rows/continuation/invalidations retire, and the then aggregate-shaped private
state was reset. M13.4 re-proved the same atomic boundary using generic
plan-derived initial state/output, including partial keyed recovery. The
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
governance are recorded below; M12.6 closes the frozen performance/release gate.

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
remain future ingress work. The complete V2 is not finished: broader SQL frontend,
broader source/operator/result shapes, automated lifecycle orchestration, and
production supervision remain. Active/non-pristine rebuild for the declared
nullable-`int8` CountRows/SumInt8 shape is proved by M12.

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
or grant loss. M12.6 then closes the bounded performance/release matrix.

M12.6 freezes its acceptance limits before measurement: one-million-row scan
<= 12 s, real 10,000-change catch-up <= 8 s, activation <= 2 s, complete
rebuild <= 25 s, RSS growth <= 128 MiB and retained WAL <= 256 MiB. The M11
comparison baseline is approximately 3.1 s scan, 1.3 s catch-up and 3.6 MiB RSS
growth. PG17.10/PG18.4 observed scan 4.357951916/4.429333333 s, catch-up
1.946769416/1.907849875 s, activation 9.755875/9.981958 ms, total
6.343139667/6.375927458 s, RSS +4,272/+4,320 KiB and retained WAL
252,864,952/252,898,072 bytes. The fixed-order runner reports 48 unique scripts
and 96 successful PG invocations.

That green gate closes the declared active nullable-`int8` CountRows/SumInt8
rebuild lifecycle, not V2. M15.4 later proves the first single-source SQL
vertical; remaining product gaps include aggregate/Join SQL, additional
source/operator/result shapes, TLS and credential
rotation, automatic receiver supervision/reconnect, cross-host and failover
operation, sustained soak/heap/tail-latency evidence, and defense against a
privileged indistinguishable same-name/same-shape slot replacement.
