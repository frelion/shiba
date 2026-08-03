# Shiba V2 clean-room

This worktree starts a new V2 on branch `codex/v2-cleanroom`. It is not a refactor
of the previous V1 or V2 code. Phase 1 established Protocol and Catalog; M2 adds
one transactional INSERT/count vertical path; M3.1 replaces its synthetic input
with a strict decoder fed by live PostgreSQL `pgoutput` version 1. M3.2 proves
safe slot replay across the post-result/pre-ack crash window. M4.1 adds a fixed
nullable `int8` payload while retaining the stable non-null `int8` row key.
M4.2 admits zero-column INSERT tuples without inventing a row identity.
M4.3 adds a fixed two-`int8` composite row identity. M4.4 admits a
single-key UPDATE that changes only the nullable payload. M4.5 admits DELETE
only for a stable, single-column `int8` key emitted as pgoutput `D + K`.

## Scope decision

**Facts.** The legacy repository at `/Users/zzhang/Documents/Shiba` remains an
oracle and evidence archive. PostgreSQL 17 and 18 are supported test targets.

**Decisions.** Phase 1 establishes a single database-local catalog authority,
protocol value contracts, static L0 gates, and empty-cluster installation
checks. `shiba_internal` owns private state; `shiba` exposes only a read-only
SQL surface. A database may contain many schemas; the catalog
authority is per installed database, not per application schema or client.

M2 accepts one test-driven, ingress-independent committed `SourceTransaction`.
One runtime-owned PostgreSQL transaction atomically applies INSERT facts,
advances a deterministic count, publishes `shiba.count_result`, and records its
continuation last. Exact replay is a no-op.

M3.1 decodes a complete live `BEGIN → RELATION → INSERT+ → COMMIT` transaction
for one admitted `int8` relation before invoking the unchanged M2 processor.
Decode failures therefore cannot expose Apply, result, or continuation state.

M4.4 treats the existing Apply row as current source state. An unchanged-key
UPDATE mutates its payload in the same processor transaction, leaves count at
the number of inserted rows, and advances continuation only with that mutation.

M4.5 makes that current-state meaning explicit: `applied_insert` is the sole
source-row-state table, despite its now-incomplete name. INSERT creates a row,
UPDATE mutates it in place, and DELETE removes it. DELETE, private count, public
result, and continuation commit in the same processor-owned PostgreSQL
transaction. Missing-row, decode, and backend-crash failures expose none of
those writes; retry applies once and exact transaction replay is a no-op.

M4.6 makes replica identity part of relation admission instead of silently
accepting any PostgreSQL setting. The four frozen shapes require default
replica identity and their exact key-column flags. A live change to `FULL`
therefore fails during decode before Apply can write or advance continuation.

M5.1 adds one real text-payload path to that same current-state authority. A
text INSERT stores the exact value; an UPDATE whose pgoutput payload is `u`
retains that committed value while count/result remain unchanged and
continuation advances in the same processor transaction.

M5.2 admits a present text-format replacement on that same UPDATE path. A real
default-EXTENDED, out-of-line and uncompressed payload is replaced exactly;
row state, unchanged count/result, and continuation still commit together.

M5.3 extends the already admitted two-int8 primary-key shape through DELETE.
Both key components identify one current-state row; deleting one pair leaves a
row sharing key1 untouched and atomically decrements count/result.

M5.4 explicitly admits the existing single-int8-key shape for a live replica
identity index. The default and index constructors require `d` and `i`
respectively, so a live identity change fails before Apply. This is decoder
configuration, not a second durable source-binding authority.

M5.5 proves the pgoutput decoder binding is independent of names and catalog
scan order. M7.1 supersedes rename admission at the processor boundary: the OID
is stable, but committed source DDL records invalidation before later Apply.

M6.1 admits one complete, non-interleaved streamed key-only INSERT transaction.
No segment is visible before stream commit; the assembled transaction uses the
existing Apply/count/result/continuation commit and replay boundary.

M6.2 proves a real streamed abort is never visible and that restarting the same
slot can subsequently deliver an independent streamed commit exactly once.

M7.1 requires every source to be registered by exact PostgreSQL ObjectAddress.
For each new transaction the processor locks the bound relation, checks the
DDL-owned invalidation fact, and only then applies. Rename rollback leaves the
binding valid; committed rename invalidates before any later result is visible.

M7.2 proves the same authority for object removal without adding production
code. Direct DROP rollback removes its invalidation atomically; committed DROP,
schema CASCADE, and same-name recreation preserve the old exact ObjectAddress
invalidation and cannot revive the source binding.

M7.3 extends that same immutable binding row set with each live user-column
ObjectAddress. Column-type rollback remains applicable; committed type change
invalidates the relation address, while column rename records its exact positive
attribute number. Either cause rejects pending work before Apply.

M7.4 adds the selected replica-identity index ObjectAddress to that same frozen
set. Index rename rollback leaves pending work valid; committed rename records
the exact stable index OID and rejects later work before Apply. Unrelated index
DDL remains isolated.

M7.5 proves the transaction race with observed PostgreSQL locks. Apply holds
relation `AccessShareLock` through its commit, so conflicting DDL waits; after
Apply commits, DDL commits invalidation and the next pending transaction fails
before any Shiba write.

M8.1 admits multiple registered sources. Each source has its own immutable
binding lock, fixed slot generation, continuation order, and replay identity;
the existing singleton count state and public result intentionally count the
union of all admitted source rows. A crash in one source transaction rolls back
only that transaction and the shared aggregate update.

M8.2 proves the mutex/CAS behavior under real concurrency. Two calls for one
source transaction yield exactly one Apply and one replay no-op; while source 1
is paused after its mutex, source 2 can commit independently and publish the
correct union result.

M8.3 bounds both admitted decoder paths before Apply. A borrowed pgoutput input
may be at most 16 MiB and one decoded transaction may contain at most 10,000
changes. Exact-limit committed and streamed transactions remain admitted; an
oversized input or 10,001st change returns `LimitExceeded` without constructing
a transaction or changing durable state. Runtime owns no ingress queue.

M8.4 makes 10,000 changes the single transaction workload limit, including
direct constructors and a defensive processor check before a database
transaction opens. The synchronous processor owns no queue or worker, so lock
and commit latency propagate to its caller. A real 10,000-change PG17/18 gate
freezes regression ceilings of 2 seconds decode, 10 seconds first Apply, and 2
seconds exact replay; current evidence is about 8 ms, 0.8 seconds, and 0.2 ms.

M9.1 replaces the fixed count implementation with a database-free Operator and
Compiler contract. Source Apply now emits a transaction-local, non-durable
before/after `EffectBatch`; registered `CountRows` operators update
operator-keyed private state and public results before continuation in the same
processor transaction. Operator specifications are strict version-1 JSON;
column names are resolved once and only ObjectAddress identity is durable. The
old `count_state` and `count_result` authorities no longer exist.

M9.2 proves the second compiled operator through that same path. One real
10,000-row nullable-`int8` transaction uses IDs 1–10,000 and payload `NULL`
when the ID is divisible by four, otherwise `2`; the same EffectBatch advances
CountRows to 10,000 and SumInt8 to 15,000 in one processor transaction. The
PG17 reference measured 9.56 ms decode, 836.72 ms Apply, and 0.171 ms replay,
within the unchanged 2 s / 10 s / 2 s ceilings. An ordered test trigger observes
operator 1 before failing operator 2; its audit, source row, both operator
writes, result writes, and continuation all roll back.
M9.2 is a bounded aggregate execution proof, not a complete V2 runtime.

M11 completes consistent pristine initialization. A new logical slot created
with `EXPORT_SNAPSHOT` supplies the sole `consistent_point`/snapshot boundary;
bounded batches, crash recovery, catch-up, atomic activation, million-row
boundedness and split least-privilege execution are proved on PG17.10/PG18.4.
Snapshot batches never fabricate WAL identities or continuation.

M12.1 now freezes the contract for an offline, forward-only rebuild of an
active/non-pristine source. Before destructive prepare the old generation is
sole active authority. Prepare atomically installs the target as sole building
authority, hides results as `building/NULL`, and retires old computation.
Activation promotes that same authority; it does not perform a second binding
switch. M12.2--M12.6 implement and prove that production rebuild path on
PG17.10/PG18.4, including recovery, governance, least privilege, bounded
million-row performance, and the complete release matrix.

M13 replaces the aggregate-shaped API with a canonical compiled plan, opaque
state codec, typed scalar/keyed transition and generic Result Sink. Concrete
operator dispatch is confined to the database-free Operator crate. Runtime,
Ingress, Bootstrap and Rebuild consume only the complete ordered durable plan
set; CountRows, SumInt8 and non-aggregate ProjectRows use that sole path.

**Not proved.** Persisted partial-stream recovery is intentionally absent.
Admission for `D + O`, replica identity `FULL`, composite UPDATE, UPDATE old
tuples, NULL text, binary payloads, TOAST keys, composite replica indexes and
streamed interleaving/subtransactions remains outside the declared shape. SQL
frontend, additional operator families, broader result types, external effects,
automatic receiver supervision/reconnect, cross-host/failover operation,
sustained soak, heap peak and contention tail latency also remain unproved.
There is no compatibility path, alias, fallback or dual write.

Read [architecture](ARCHITECTURE.md), [protocol contract](PROTOCOL_CONTRACT.md),
[catalog contract](CATALOG_CONTRACT.md), the
[Source Ingress contract](SOURCE_INGRESS_CONTRACT.md), the
[transport ADR](adr/0001-m10-replication-transport.md), and the
[bootstrap contract](BOOTSTRAP_CONTRACT.md), the
[bootstrap ADR](adr/0002-m11-consistent-bootstrap.md), and the
[rebuild contract](REBUILD_CONTRACT.md), the
[offline rebuild ADR](adr/0003-m12-offline-rebuild.md), and the
[reuse manifest](contracts/REUSE_MANIFEST.md) before extending the workspace.
Ingress work must also follow the
[pgoutput contract](PGOUTPUT_CONTRACT.md).
Nullable tuple work is bounded by the [tuple contract](TUPLE_CONTRACT.md).
Compiler, EffectBatch, Operator, and sink work must follow the
[operator contract](OPERATOR_CONTRACT.md) and the
[M13 Operator Kernel contract](OPERATOR_KERNEL_CONTRACT.md). The design
decision is recorded in [ADR 0004](adr/0004-m13-generic-operator-kernel.md).
M14 operator development follows the
[typed Operator Graph contract](OPERATOR_GRAPH_CONTRACT.md) and
[ADR 0005](adr/0005-m14-operator-graph.md); it does not introduce a SQL parser.
M14.2 implements typed expressions plus Filter/Compute/Project/Materialize on
one binding-ordered transaction-local DeltaBatch and removes the production
ProjectRows execution variant. M14.3 adds the sole generic keyed-state
authority plus KeyBy, GroupedCount and GroupedSumInt8, proved with complete
PG17.10/PG18.4 keyed SQL differentials. Later slices added Join and the unified
graph lifecycle; M14.7 closes their release/performance matrix.
M14.4's accepted two-source authority is documented in
[JOIN_AUTHORITY_CONTRACT.md](JOIN_AUTHORITY_CONTRACT.md) and
[ADR 0006](adr/0006-m14-two-source-join-authority.md). It
requires exactly two explicit SourceIds to share one slot/generation,
transaction assembly, graph continuation/ACK, exported snapshot and graph-wide
rebuild, with an exact effective right PK/UK identity binding. M14.5 implements
the pure GraphId/ordered-SourcePort Compiler and partition-state INNER JOIN
kernel, including a 300-step differential and exact 20,000/20,001 fan-out
gates. M14.6 replaces source/operator execution authority with one canonical
graph definition, ordered one/two-source membership, graph ingress/continuation,
generic graph state/results and a graph-wide bootstrap/rebuild lifecycle. The
directed PG17.10/PG18.4 graph Runtime gate proves singleton and cross-schema
JOIN execution, both-side atomicity, fan-out/retraction, rollback/replay and
exact effective-identity invalidation. Compiler also admits strict computed
projection and filtered grouped pipelines without Runtime kind dispatch.
M14.7's matrix at `206f085` preserved 51 unique scripts and 102 successful
PG17.10/PG18.4 invocations. The subsequent two-version Join lifecycle gate
proves one exported snapshot across both sources, catch-up, governed live
ACK/crash replay and whole-graph generation rebuild. The final matrix passed
52 scripts and 104 invocations. Five-run
same-scene Apply medians are 771.019625/821.920250 ms, below the frozen
899.648163/905.230694 ms ceilings. M14 is complete; this does not claim a SQL
frontend, outer/three-table joins, windows, additional aggregates, plugins,
scheduler, or complete V2.

M15 SQL frontend work is governed by
[SQL_FRONTEND_CONTRACT.md](SQL_FRONTEND_CONTRACT.md) and
[ADR 0007](adr/0007-m15-sql-frontend.md). M15.1 freezes a narrow bounded
`SELECT` subset, canonical QuerySpec authority, exact ObjectAddress binding,
DDL-safe registration/rebuild rebind and stable diagnostics. It selects
`sqlparser` 0.62.0 `PostgreSqlDialect` as the parser candidate. That M15.1
contract-only slice added no production parser or V2 completion claim.

M15.2 is the completed generic declaration cutover. It
replaces GraphSpec recipes with canonical QuerySpec nodes/results in Compiler,
registration and rebuild while leaving OperatorGraph/Runtime/ACK authority
unchanged. Ten pure Compiler tests and the full PG17.10/PG18.4 52-script,
104-invocation release matrix are green.

M15.3 implements the separate, database-independent SQL parser and
`UnboundQuery` normalization boundary. It pins `sqlparser` 0.62.0 with only
`std`, enforces the frozen byte/token/AST/expression limits, emits stable
half-open byte spans and closed snake_case errors, and validates public ASTs
iteratively before canonical encoding. Twenty-two pure tests plus doc-tests,
fmt/check/clippy and static dependency-isolation gates are green. It adds no
parser dependency to Runtime, Ingress or Operator and changes no durable
authority. Binder/type checking, SourceId/ObjectAddress resolution, QuerySpec
lowering, registration, PG17/18 SQL differential/lifecycle evidence and the
frozen 10,000-query performance gate remain unproved; M15 is not complete.
