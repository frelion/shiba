# Testing strategy

## M16 admission-hardening gates

The admission tests are failure-first. QuerySpec, SQL frontend, Compiler and
OperatorGraph reject HAVING trees that exceed the shared node/depth/boolean
limits before recursive lowering; ordinal zero, out-of-range ordinals and
empty call lists are rejected consistently by validation, evaluation and graph
admission. Graph construction rejects Aggregate-to-Aggregate edges, aggregate
fan-out and aggregates without exactly one direct Materialize, so unsupported
topologies never reach Runtime.

Typed-layout tests build the same node with equal value types and different
nullable bits and require different derived identities for Source-derived
layouts and Materialize/Project/Compute/KeyBy/Aggregate outputs. Direct graph
construction is covered independently of SQL frontend validation.

Graph-budget tests charge multiple Aggregate nodes cumulatively and exercise
the inclusive limit and limit-plus-one cases for touched groups, exact state
keys, partition entries, state mutations, result mutations and estimated work
bytes. They also cover extrema and multi-result transitions. A budget error
must return no partial transition; Runtime must therefore write no state or
result, advance no continuation, and authorize no ACK. These tests are
database-free in the kernel and are complemented by Runtime rollback/retry
coverage.

## M16.3 generic Aggregate gates

The production `Aggregate` kernel replaces the four concrete Count/Sum node
families. A shared database-free harness drives CountStar, Count(nullable
`int8`) and SumInt8 through 1,000 fixed-seed INSERT/UPDATE/DELETE transitions
against an independent row model. It also proves exact function/state version
rejection, normalized transaction deltas, exact `i64::MIN` retraction, checked
overflow and complete state removal when a group becomes empty. Compiler tests
bind declaration bounds directly to the Operator ABI constants.

`scripts/check-m16-aggregate-contract.sh` additionally rejects concrete
function dispatch outside `shiba-operator` aggregate modules and any function
knowledge in Runtime, Ingress or Catalog. Directed PG17.10/PG18.4 gates for M9
Count/Sum, M13 generic kernel, M14 grouped execution and M15 aggregate SQL prove
the migrated scalar/grouped lifecycle and full SQL oracles. The final release
matrix remains 57 uniquely enrolled scripts and 114 versioned invocations;
M16.4 adds multi-call SQL and Count(expr), M16.5 adds exact MIN/MAX, and M16.6
adds restricted grouped HAVING visibility transitions.

## M16.4 multi-call SQL gates

The M16.4 Binder tests prove one generic Aggregate node for multiple calls,
stable ordinals and result slots, `COUNT(expr)` nullable-input semantics,
duplicate output rejection, and the 16-call bound. The directed SQL aggregate
gate runs on PostgreSQL 17.10 and 18.4 and compares the complete
`count(*)`/`count(payload)`/`sum(payload)` row with SQL after snapshot
bootstrap, live I/U/D, explicit feedback, exact replay, and changed-target
rebuild plus post-rebuild live Apply. M16.5 extends this production gate with
`min(payload)`/`max(payload)`, duplicate-extrema and NULL transitions, and
complete PG17.10/PG18.4 SQL row comparison. M16.6 adds complete-row HAVING
transitions; M16.7 records the final performance and extensibility evidence
without changing these semantics.

M16.7 closes the frozen release and extensibility gates; after M16.8 is
enrolled, the exact-enrollment runner passes 58 scripts and 116 PostgreSQL
invocations on PG17.10/PG18.4.
The static audit proves function dispatch remains confined to operator aggregate
modules. The full PG17 matrix previously exposed the existing two-second walsender timeout while a
10,000-row generic Aggregate retry was synchronously applying. The receiver now
refreshes only the prior durable coordinate before and during Apply. The M10 streaming
gate proves that the retry still advances slot feedback only after Runtime
commit; Runtime failure continues to leave slot progress, state, result and
continuation unchanged. The same directed gate is run on PG17 and PG18.

## M16.8 IndexedState gates

The database-free state contract tests cover signed Int8 order-key vectors,
version and stale-key rejection, exact/range overlap, per-range limit and
limit-plus-one rejection, duplicate or malformed
entries, range direction and limit bounds. The aggregate reference harness
continues to compare I/U/D, NULL, duplicate extrema, current-extreme deletion,
key changes, empty groups, multiplicity underflow and checked failures against
an independent model.

`scripts/test-m16-indexed-state.sh` is a real PG17.10/PG18.4 gate. It creates
100,000 distinct extrema values, registers the normal SQL aggregate, runs the
production snapshot-to-live path, deletes the current minimum, checks the full
SQL result and ACK, and inspects `EXPLAIN (ANALYZE)` for the exact production
`WITH ranges`/`CROSS JOIN LATERAL`/dynamic `LIMIT`/`FOR UPDATE` shape, ordered
B-tree and two-row candidate window. It prints batch count, bootstrap/live
latency and durable state-row count. The gate must not be replaced by a small
fixture or a full-partition scan. M16.8.2 also exercises two same-direction
ranges on distinct partitions and checks that the production query explicitly
orders by `range_index` and then `item_order_key` in the requested direction;
range identity rejects a second direction for the same base coordinate.

## M16.2 wide-result gates

Markdown/diff review freezes the aggregate descriptor ABI, call identity,
result-schema/row codecs, generic ordered-state read contract, HAVING old/new
transition table, transaction boundary and hard resource and performance
thresholds. `scripts/check-m16-aggregate-contract.sh` runs 12 database-free,
test-only reference-model cases covering CountStar/Count/Sum/Min/Max, exact
multiplicity, grouped create/delete/key change, HAVING visibility, fixed-seed
randomized I/U/D, codecs/corruption and exact bound/bound+1 behavior. This is
reference evidence, not production or PostgreSQL evidence.

M16.2 adds strict production schema/row/key codec tests, one-field scalar and
two-field keyed/wide rows, NULL/type/arity/version/digest rejection, generic
sink failure rollback/retry/replay and full-row decoding through the public
active-only API. Existing M15 registration, aggregate, Join, bootstrap and
rebuild gates are rerun on PG17.10 and PG18.4 after replacing their fixed-column
oracles with complete canonical row oracles. At that boundary these tests did
not count as Aggregate ABI, MIN/MAX, multi-call or HAVING evidence; later M16
stages now supply those proofs.

The M16.2 release run is green on PostgreSQL 17.10 and 18.4: 57 uniquely
enrolled scripts and 114 successful versioned invocations, including the new
`scripts/test-m16-wide-results.sh` gate. The inherited million-row rebuild
remained below its frozen limits: PG17/PG18 total time was 8.418541416/
7.764510792 seconds, RSS growth 5,984/6,224 KiB, and retained WAL
253,008,000/253,084,256 bytes (limit 268,435,456). This closes only the
canonical wide-result cutover. M16.3 subsequently supplies generic
Count/CountStar/Sum execution; later M16 stages add multi-call, MIN/MAX and
grouped HAVING.

## M15.1 SQL frontend contract gates

M15.1 is docs-only. Static review freezes raw SQL -> canonical QuerySpec ->
ObjectAddress-bound OperatorGraph authority, accepted/rejected SQL, identifiers,
aliases, NULL/types, deterministic NodeIds/digests, DDL-safe registration,
rebuild rebind, stable spans/errors and hard resource/performance bounds. ADR
0007 freezes `sqlparser` 0.62.0 `PostgreSqlDialect` as the minimized-feature
candidate; later tests must layer its recursion limit with Shiba bounds and
normalize `Spanned`/`TokenWithSpan` locations to stable UTF-8 byte spans.

M15.2 adds pure canonical/limit tests and reuses every enrolled M1--M14
PG17.10/PG18.4 gate for the declaration cutover. Later SQL slices must add
golden/error/span, SQL-oracle, DDL-race, least-privilege and rebuild-rebind
frontend gates. Live Apply must contain no parser call. M15.1 itself claimed no
implementation evidence.

## M15.3 pure parser evidence

`shiba-sql-frontend` has 22 pure tests plus doc-tests. They cover the accepted
single/two-source subset, explicit unsupported families, identifier and alias
normalization, exact half-open UTF-8 error spans, closed snake_case error codes,
the 64 KiB/4,096-token/2,048-AST-node/256-expression-node/32-depth bounds,
canonical golden/metamorphic behavior, and fail-closed validation of manually
constructed public ASTs. Fixed malformed inputs prove no panic. Failure-first
testing exposed a 4,096-token left-deep AST whose recursive destruction could
overflow the stack; a pre-parser structural ceiling now rejects it before AST
construction, while canonical validation/encoding walks admitted public ASTs
iteratively.

`cargo fmt --all -- --check`, workspace check, frontend tests, frontend clippy
with `-D warnings`, `scripts/check-m15-contract.sh` and
`scripts/check-m15-parser.sh` are the M15.3 gates. The static parser gate proves
the exact `sqlparser = 0.62.0` pin, disabled default features, production file
limits, forbidden unsafe and that Runtime/Ingress/Operator remain
parser-independent. The manifest and resolved dependency-tree audit prove that
only `std` is enabled and no C-toolchain or PostgreSQL runtime is pulled in. No
PostgreSQL gate is evidence for this pure slice.

M15.3 does not prove Binder types, SourceId/ObjectAddress lookup, QuerySpec
lowering, registration rollback, PG17/18 SQL differential, DDL/lifecycle races
or the 10,000-query performance thresholds. Those remain later M15 gates.

## M15.4 Binder, registration and first SQL lifecycle evidence

Pure Binder tests cover the admitted single-source keyed projection, checked
bigint expression, predicate typing, quoted column lookup, exact identity,
missing/wrong-type inputs and stable binding diagnostics. The independent
`shiba-sql-registration` tests prove parser diagnostics retain their class/code/
span and that control-plane diagnostics use the binding class. Static
`scripts/check-m15-registration.sh` proves only that this control-plane crate may
join the pure frontend to PostgreSQL/Runtime, Runtime and Ingress remain parser-
free, the transaction-local registration entry exists, raw SQL is not a Catalog
field, unsafe is absent and production files respect the frozen limits.

Run `scripts/test-m15-sql-vertical.sh` separately with the absolute PG17.10 and
PG18.4 `pg_config`. Both directed runs are green. They execute:

- quoted, schema-qualified `SELECT e."Id", e."Payload" + 1 ... WHERE
  e."Payload" > 0` binding to one already registered SourceId;
- injected result-registration failure with zero definition/member/result rows,
  followed by atomic successful registration and canonical graph verification;
- an observed DDL-first lock race that replaces the payload ObjectAddress,
  returns `ddl_drift`, persists invalidation and leaves no graph authority;
- exported-snapshot bootstrap with concurrent INSERT/UPDATE/DELETE, building
  visibility, WAL catch-up and complete key/value SQL oracle;
- production live receiver Apply and explicit ACK, NULL/predicate/key-change
  transitions, Apply-before-ACK session loss, exact `AlreadyApplied` replay and
  confirmed-flush advancement only after ACK;
- rebuild to a different relation, column and identity-index ObjectAddress using
  the durable QuerySpec, concurrent target WAL catch-up, atomic activation and
  post-cutover live Apply/ACK with the full SQL oracle.

`test-m15-sql-vertical.sh` is enrolled exactly once, so the current release
runner now contains 53 PostgreSQL scripts (106 versioned invocations). M15.4
acceptance uses the two directed invocations, not a claimed complete 53×2 run.
The following M15.5 section records its later directed aggregate evidence.
At the M15.4 boundary, M15.6 Join SQL and M15.7 performance/release evidence
were unproved; the later sections record their closure.

## M15.5 aggregate SQL lifecycle evidence

Pure Binder tests cover canonical generic lowering for scalar `COUNT(*)`,
nullable scalar `SUM(bigint)`, filtered grouped Count and grouped Sum, including
alias/parenthesis equivalence, exact ObjectAddress binding and missing,
duplicate, wrong-type and topology rejection. Operator tests prove scalar SUM's
empty/all-NULL/value/delete-last-value transitions, checked overflow, corrupt
state rejection and the exact two-key read set: checked sum plus non-NULL count
inside the same `graph_node_state` authority. Scalar Count remains explicitly
non-nullable.

Run `scripts/test-m15-sql-aggregates.sh` separately with the absolute PG17.10
and PG18.4 `pg_config`. Both directed runs are green. They execute:

- four independent SQL registrations and canonical graph/result-contract
  checks for scalar count, scalar nullable sum, filtered grouped count and
  grouped sum;
- exported-snapshot bootstrap, WAL catch-up, activation and production live
  receiver Apply with explicit ACK for all four graphs;
- complete scalar/keyed SQL differential, including empty and all-NULL SUM,
  false/NULL/true predicate changes, group-key changes and empty-group removal;
- injected grouped-SUM overflow with source rows, node state, public results and
  continuation unchanged and replication feedback not advanced;
- restored-state retry applied once, Apply-before-ACK restart converging through
  `AlreadyApplied`, followed by explicit feedback;
- changed-ObjectAddress grouped-SUM rebuild, catch-up, atomic activation and
  post-rebuild live Apply/ACK.

The first failure-first test exposed that sum-only private state cannot
distinguish numeric zero from no non-NULL input; the fix adds a second generic
StateKey under the existing state authority. The second exposed that the generic
scalar sink/catalog rejected typed NULL active output; the fix propagates
explicit scalar nullability through QuerySpec, compiled output contract and the
same sink. Neither fix adds a writer, table or execution path, and no legacy
implementation or SQL workflow was reused.

These are directed PG17.10/PG18.4 M15.5 results. They did not by themselves
claim the complete release matrix; M15.6 and M15.7 subsequently close Join,
least-privilege, frontend/registration performance and release enrollment.

## M15.6 two-source SQL Join lifecycle evidence

Pure `binder_join` tests prove the exact cross-schema SQL shape lowers to the
generic M14 InnerJoin contract. They use left SourceId 20 and right SourceId 10
to distinguish canonical sorted QuerySpec membership from semantic node input
order, and cover reversed equality/aliases, quoted identifiers, exact fields,
right effective identity, wrong type/identity/missing columns and rejected
unproved shapes. Compiler's existing exact-right-identity test remains green.

The declaration intentionally references the InnerJoin node directly as its
keyed result. The M14 node already emits `[left.id, right.payload]`; Compiler
adds the same generic Materialize terminal used elsewhere. Static
`scripts/check-m15-join.sh` rejects Join recipe names and proves Runtime/Ingress
remain SQL/Binder independent.

Run `scripts/test-m15-sql-join.sh` separately with absolute PG17.10 and PG18.4
`pg_config`. Both directed runs are green. They prove:

- a NOSUPERUSER/NOREPLICATION control role atomically registers the SQL graph,
  an independent NOSUPERUSER/REPLICATION role owns transport, and a third role
  can only read public results;
- missing graph INSERT and bootstrap-writer EXECUTE fail before any graph,
  bootstrap, slot or source-row authority is left behind;
- one exported snapshot scans both cross-schema members, hides building rows,
  catches up concurrent two-side WAL and activates one complete result;
- one PostgreSQL transaction modifying both tables is one graph Apply, followed
  by explicit ACK and a complete keyed nullable SQL oracle;
- Apply-before-ACK restart returns exact `AlreadyApplied` before feedback;
- dropping/recreating the right effective PK gives a new OID, invalidates the
  old exact binding, and leaves source rows, Join state/results, continuation
  and confirmed flush LSN unchanged on rejected Apply;
- whole-graph rebuild recompiles the durable QuerySpec against changed relation,
  column and identity ObjectAddresses, uses generation 2, then continues live
  Apply/ACK with the complete SQL oracle.

This reuses M10--M14 production authority and changes no transaction or ACK
rule. M15.7 re-enrolls it in the complete release matrix.

## M15.7 performance and final release evidence

The exact-enrollment runner passed 56 unique scripts on PostgreSQL 17.10 and
18.4: 112 successful PostgreSQL invocations, followed by fmt, workspace check
and tests, clippy with `-D warnings`, L0 and every M15 static contract gate.
No M1--M14 threshold, workload or assertion was relaxed.

The frozen frontend workload used 10,000 representative accepted queries. On
PG17 its median/p95 latency was 6.833/11.958 us, the admitted 64 KiB case took
170.959 us, modeled heap was 3,342,336 bytes and observed RSS growth was
96 KiB. PG18 measured 8.125/13.792 us, 216.500 us, the same modeled heap and
80 KiB RSS growth. The registration workload used 200 measured samples after
10 warmups: PG17 median/p95 was 1.546625/1.623708 ms with 112 KiB RSS growth;
PG18 was 1.687958/1.782958 ms with 64 KiB RSS growth. These are below the
predeclared 1 ms median, 5 ms p95, 20 ms maximum-input, 4 MiB modeled-heap and
25 ms registration-p95 limits.

The inherited M12 million-row gate also remained green. PG17 prepare/handoff/
scan/catch-up/activation/total was 16.071625 ms, 22.582292 ms, 5.610511417 s,
2.220375833 s, 18.177958 ms and 7.898309 s; scan rate was 178,236.87 rows/s,
RSS grew 6,240 KiB and retained WAL peaked at 252,926,624 bytes. PG18 measured
20.439041 ms, 26.418334 ms, 5.635338791 s, 2.388243375 s, 18.448541 ms and
8.098652667 s; scan rate was 177,451.62 rows/s, RSS grew 6,368 KiB and retained
WAL peaked at 252,959,792 bytes. Both WAL peaks remain below 256 MiB.

M15 is complete at the declared bounded SQL frontend and QuerySpec scope. The
remaining general-SQL, broader-operator/result, cross-host, supervision and
long-running production boundaries are not covered by this matrix.

## M15.2 QuerySpec cutover evidence status

The green cutover deletes complete-query GraphOutputSpec recipes, changes the
Catalog compiler version to 2, stores canonical `QuerySpecV1`, and makes
registration/rebuild use `compile_query`. Ten pure Compiler tests cover strict
codec/digest/topology, hard bounds, exact catalog error coordinates and all
former Count/Sum/Project/Compute/Filter/Grouped/Join shapes. Workspace tests,
clippy and L0 pass. The complete release runner passed 52 scripts and 104
PG17.10/PG18.4 invocations, including Runtime registration/results,
bootstrap/rebuild/recovery and Join lifecycle. SQL parsing and its diagnostics
were not M15.2 evidence; M15.3 subsequently proves the pure parser and
diagnostic boundary described above, without changing the M15.2 PG evidence.

## M14.7 release evidence

Commit `206f085` records the previous green release matrix: 51 enrolled scripts
on PostgreSQL 17.10 and PostgreSQL 18.4, or 102 successful invocations. With
`test-m14-join-lifecycle.sh` enrolled exactly once, the final release runner
passed 52 scripts and 104 PG invocations. An unlisted `test-*.sh` still fails
before databases start.

The frozen same-scene five-run M13 Apply comparison reports PG17 median
771.019625 ms versus 782.302750 ms baseline (-1.44%) and PG18 median
821.920250 ms versus 787.157125 ms baseline (+4.42%). Both remain below the
fixed M14 ceilings of 899.648163/905.230694 ms.

The final million-row M12 rebuild regression reports PG17 total 7.434870458 s,
RSS +6,256 KiB and retained WAL 252,905,752 bytes; PG18 total 7.151174542 s,
RSS +6,224 KiB and retained WAL 252,938,872 bytes. Both stay below the unchanged
268,435,456-byte retained-WAL limit and preserve the prior time/RSS thresholds.
No workload, assertion or threshold was relaxed after observation.

Failure-first static/directed gates additionally freeze three boundaries:

- only a zero-column singleton CountRows layout may omit identity;
- composite identities remain admitted for proven one-member graphs, while the
  JOIN right side is exactly one non-null bigint PK/UK effective identity;
- after graph/generation locking, exact replay is probed before current
  eligibility/invalidation, while all new work still fails on invalidation.

M14 is complete at the declared Operator Graph scope. The final release matrix
retains the 52/104 enrollment result; this does not claim complete V2.

## M14 lifecycle evidence closure

`scripts/test-m14-join-lifecycle.sh` is independently green on PostgreSQL 17.10
and 18.4. One two-source cross-schema Join graph is registered, then one real
exported snapshot scans both members while a transaction changing both sources
accumulates in the same slot. Catch-up applies that WAL, activation publishes
the complete keyed result, and `GovernedGraphSession` continues live on the
same graph generation.

The gate then proves explicit feedback and both crash sides. A live graph
transaction is durably applied and explicitly ACKed to its exact end LSN. A
second transaction commits in Shiba but the governed session is dropped before
ACK; restart receives the same transaction as `AlreadyApplied`, leaves graph
continuation cardinality unchanged, and only then advances
`confirmed_flush_lsn` to the exact terminal coordinate.

Finally, the non-pristine same-binding graph rebuild moves generation 1 to 2 as
one unit. A new exported snapshot scans both members, concurrent two-source WAL
is caught up, activation changes the whole graph generation, and a post-cutover
live transaction is applied and ACKed on the new slot. Every phase compares all
keyed Join rows with an independent SQL oracle and checks exact graph
continuation cardinality/generation. No production code or authority changed;
mechanical responsibility splits leave every production file below 300 lines
without changing public APIs, transaction ownership or ACK semantics.

## M14.6 graph lifecycle cutover gates

The production cutover under test has one canonical `OperatorGraph`, ordered
one/two-source membership, one graph ingress configuration and slot generation,
one graph continuation, generic graph node state/results, one graph bootstrap
lifecycle with subordinate member checkpoints, and graph-wide rebuild. The
single-source case is a one-member graph; there is no old Runtime, per-source
continuation, operator table, adapter or dual write.

Static and Catalog tests must prove the superseded source/operator execution
authorities are absent and Runtime/Ingress/Bootstrap/Rebuild do not mention
concrete nodes. Runtime tests must prove one PostgreSQL transaction changing
either or both members constructs one multi-input batch, locks sources and state
canonically, persists all deltas and writes continuation last. Failure at any
point must leave all members, state/results and continuation old; ACK must remain
unauthorized until commit or exact graph replay.

`scripts/test-m14-graph-runtime.sh` is green on PG17.10 and PG18.4. It proves a
one-member Count graph and two-member cross-schema JOIN use the same Runtime;
both relations enter one decoded PostgreSQL transaction; right UPDATE/DELETE
fan-out, both join-key changes, full keyed rows, retry/exact replay, injected
sink rollback and exact right-PK replacement behave atomically. It also proves
default primary-key binding, an explicit unique replica-identity binding, and
failure without any durable binding for a relation with no effective identity.

Pure Compiler tests additionally prove strict `ComputedProject`,
`FilteredGroupedCount` and `FilteredGroupedSumInt8` pipelines and graph terminal
result contracts. Runtime persists those contracts generically and contains no
node-kind dispatch. Failure-first tests caught canonical keyed value payload,
the rebuild writer's 22nd value-nullability parameter, member-trigger COALESCE
syntax, and old-versus-post-prepare invalidation semantics; each now fails
closed at its owning boundary.

M14.7 closes full receiver/bootstrap/rebuild enrollment, regression,
performance and release evidence while preserving the frozen M8--M13 thresholds.

## M14.5 pure two-source JOIN gates

Pure Compiler/Operator tests prove GraphId ownership, ordered SourcePorts,
`SourcePort(SourceId)` inputs, exact effective right replica-identity binding,
partition state and pre-to-final mixed-input semantics. A fixed-seed 300-step
relational differential is green. Fan-out 20,000 succeeds and 20,001 fails;
ordered affected-row indexes correct the initial `O(n^2)` scan to `O(n log n)`.
M14.6 supplies Runtime/Catalog persistence and the graph lifecycle cutover; its
directed PG17/18 graph Runtime evidence is green. M14.7 closes full
bootstrap/rebuild and release evidence.

## Directed M14.6 PostgreSQL JOIN gates

The accepted authority is in
[JOIN_AUTHORITY_CONTRACT.md](JOIN_AUTHORITY_CONTRACT.md). The M14.4 contract is
accepted and implemented by the graph cutover. The following directed Runtime
portion passes independently on PG17.10 and PG18.4:

- registration rejects missing effective identity and durably binds default PK
  or explicit replica-identity index; exact replacement invalidates the graph;
- complete expected keyed rows cover left-only, right-only and one PostgreSQL
  transaction changing both sides, including right UPDATE/DELETE fan-out;
- one graph transaction atomically rolls back source rows, node state, results
  and graph continuation on injected sink failure, then retry applies once;
- exact replay returns a no-op without duplicating rows or continuation;
- relation/index drift fails closed by exact bound identity, including
  same-shape right-index replacement.

M14.7 re-proves the complete one-snapshot bootstrap, graph rebuild/recovery,
split-role, publication/column drift, performance and release portions of the
matrix; those are not inferred from this directed Runtime test.

The runner statically rejects a per-source continuation, second
Runtime, persisted DeltaBatch/EffectStream, adapter, fallback or dual write.
These static M14.6 requirements are current evidence and remain enrolled in the
M14.7 release matrix.

## M14.3 generic grouped-state gates

`scripts/test-m14-grouped.sh` is green on PG17.10 and PG18.4. It compares every
GroupedCount/GroupedSumInt8 keyed row with an independent SQL oracle across
INSERT/UPDATE/DELETE, NULL and all-NULL groups, key changes, empty-group
deletion, replay and retry. Injected overflow and corrupt keyed state prove
source rows, all node state/results and continuation roll back together.
Permission and static gates prove private node state and set-based load/delete/
upsert rather than per-row SQL. This directed gate does not prove Join,
graph-wide bootstrap/rebuild/lifecycle or the final M14 release/performance
matrix.

## M14.2 typed stateless graph gates

M14.2 implements the database-free `TypedValue`/`TypedRow`/`DeltaBatch` and
canonical `OperatorGraph` codecs, SQL-three-valued expressions, checked bigint
arithmetic, Filter, Compute, Project and Materialize. Pure tests cover exact
false/NULL filtering, key-changing retract/upsert, absent/type/layout/plan
corruption, deterministic canonical digests, fixed-seed keyed reference-model
differential and the 10,000-row/20,000-node-output/200,000-total-delta/64 MiB
hard bounds. Runtime constructs one binding-ordered batch and invokes every
plan with it; no concrete operator kind is matched outside `shiba-operator`.
PG17/PG18 M13 keyed differential remains the vertical regression gate for the
Project-to-Materialize replacement.

## M14.1 frozen graph gates

M14.1 freezes the typed SDK, single/multi-input graph authority, graph-scoped
continuation, canonical lock order, complete-transaction retry and hard work
bounds in `OPERATOR_GRAPH_CONTRACT.md`. Pure gates use independent fixed-seed
reference models for every expression/node. PostgreSQL gates compare every
grouped and joined row with SQL oracles on PG17.10 and PG18.4; count or digest
comparison alone is insufficient.

The current 49-script/98-invocation M13 matrix remains the regression floor.
New M14 scripts must enter the release runner's exact enrollment before their
stage can close. The unchanged CountRows/SumInt8 five-run Apply medians are
782.302750/787.157125 ms; M14 stops at 899.648163/905.230694 ms. All M8--M13
absolute thresholds remain fixed, including 10,000 changes, 16 MiB assembly
and M12 retained WAL <=256 MiB.

## M13 gates and current evidence

M13.1 records the pre-change five-run medians in
`OPERATOR_KERNEL_CONTRACT.md`. PG17/PG18 Apply medians are 768.727625 and
770.174125 ms, so the unchanged-scenario 15% regression ceilings are
884.036769 and 885.700244 ms. Existing absolute decode, Apply, replay,
bootstrap, ingress, rebuild, RSS and retained-WAL limits remain in force.

M13.2 pure codec/model/randomized gates are green. M13.3 is green on PG17.10
and PG18.4 through `scripts/test-m13-operator-kernel.sh`: CountRows, SumInt8 and
ProjectRows consume one EffectBatch; INSERT/UPDATE/key-changing UPDATE/DELETE,
NULL, exact replay, corrupt plan/state and keyed-sink failure are compared with
full SQL rows, while state/results/source rows/continuation roll back together.
The migrated `test-m9-registration.sh` and `test-m9-count-sum.sh` are also green
on both versions. Catalog unit/clippy plus PG17/18 empty-install gates prove the
new strict plan/state/output schema and transactional installation.

M13.4 is green on PG17.10/18.4 across committed/streaming ingress, bootstrap,
bootstrap recovery/roles, and rebuild contract/admission/snapshot/recovery/
identity/governance. It includes full ProjectRows keyed SQL oracles and
pre-destructive plan-digest drift rejection with zero production
operator-kind/fixed-ID/fixed-count/column-position knowledge. M13.5 passed the
one-click PG17.10/PG18.4 release matrix: 49 unique scripts and 98 PostgreSQL
invocations. The forbidden-specialization scan is empty. ProjectRows compares
every key and nullable value with an independent SQL oracle; a count or digest
alone is insufficient.

Five post-change same-machine CountRows+SumInt8 runs measured Apply medians of
782.302750 ms on PG17 and 787.157125 ms on PG18. Relative to the frozen
768.727625/770.174125 ms baselines, this is about +1.8%/+2.2%, below the 15%
stop lines. The final historical M12 CountRows+SumInt8 performance gate observed
retained WAL of 252,876,464/252,917,880 bytes, still below 256 MiB. Its plan set
is intentionally frozen; ProjectRows lifecycle correctness is proved by the
separate full-row M13 bootstrap/rebuild differential gates.

## Milestone gates

Run from `/Users/zzhang/Documents/Shiba-v2-cleanroom`:

```bash
PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config ./scripts/test-l0.sh
PG_CONFIG=/opt/homebrew/opt/postgresql@18/bin/pg_config ./scripts/test-l0.sh
./scripts/test-empty-install.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-empty-install.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m2.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m2.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m3.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m3.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-empty.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-empty.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-composite.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-composite.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-update.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-update.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-delete.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-delete.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-replica-identity.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-replica-identity.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m5-toast.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m5-toast.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m5-incompressible-toast.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m5-incompressible-toast.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m5-composite-delete.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m5-composite-delete.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-committed-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-committed-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-streaming-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-streaming-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-catalog-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-catalog-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-governed-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-governed-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-performance-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-performance-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-shutdown-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-shutdown-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
```

`test-l0.sh` selects the matching `pg17` or `pg18` feature, then runs formatting,
Protocol/Catalog/Runtime checks and tests, clippy with warnings denied, `git diff
--check`, forbidden-surface scans, the canonical fixture byte/digest check, and
the deferred-evidence manifest completeness check.

Each `test-empty-install.sh` invocation creates an isolated empty cluster. It
proves complete rollback after a forced failure in the `CREATE EXTENSION`
transaction; successful install and the single `1|1` version result; public API
access and private table denial for an ordinary role; and clean extension drop.
This re-proves only the Phase-1 installation rollback boundary, not later
component recovery.

`test-m2.sh` creates another isolated cluster and packages the extension. Its
test-only source schema commits two ordinary rows before constructing the Rust
input. It proves result `2`, cross-schema placement, exact replay, identity
conflict, operator-error rollback, backend-termination rollback after the
continuation write, reconnect/replay, and ordinary-role result-only access.
Failure triggers are test objects and never ship in extension SQL.

`test-m3.sh` enables logical decoding in an isolated cluster, creates one
test-only publication and `pgoutput` slot, and captures two real committed
transactions. The integration test removes only that client's per-XLogData
delimiter, decodes pure pgoutput, applies through M2, and proves result `2`,
exact replay `2`, and corrupt/truncated input state unchanged. For the second
transaction it disables periodic feedback, stops the receiver after complete
COMMIT, applies result `3` while `confirmed_flush_lsn` is unchanged, kills the
receiver, then proves slot replay is the identical transaction and is a no-op.
PG17 and PG18 run the same gate.

`test-m4.sh` captures a real two-column relation containing SQL NULL and an
`int8` value. It proves operator-error rollback after payload Apply, exact Apply
facts, count result, replay no-op, and a precisely corrupted key tuple tag
failing before any durable state appears.

`test-m4-empty.sh` captures two zero-column INSERTs committed together. It
proves two NULL-key/absent-payload Apply facts, count `2`, replay no-op, and a
corrupt nonzero tuple column count failing with zero durable state.

`test-m4-composite.sh` captures two rows sharing key-1 but differing in key-2.
It proves exact two-part Apply facts, count `2`, replay no-op, and a precisely
corrupted second-key tag failing with zero durable state.

`test-m4-update.sh` applies a real INSERT and then captures an unchanged-key
UPDATE through both non-NULL canonical text and SQL NULL paths. It proves count
stays `1`, the Apply payload changes exactly once, a corrupt key tag fails before
writes, and a valid UPDATE for a missing row rolls mutation and continuation
back. Backend termination after continuation INSERT rolls payload, count,
result, and continuation back together before successful retry and replay no-op.

`test-m4-delete.sh` first applies one real INSERT transaction containing two
single-key rows, then captures one row's real DELETE as pgoutput protocol-v1
`D + K`. It proves relation OID, selector,
column count, and canonical text key decoding; only the target current-state row
is removed; another row is unchanged; private count and public result decrement
exactly once; and continuation commits with them. It separately proves invalid
tuple tag rejection before writes, missing-row and count-underflow rollback,
backend-crash rollback of row/count/result/continuation, successful retry, exact
replay no-op, and the same behavior on PostgreSQL 17 and 18.

`test-m4-replica-identity.sh` first captures and applies a default-identity
single-key INSERT, proving RELATION `d`, key flag `1`, result visibility, and
exact replay. It then changes the live source to replica identity FULL and
captures a real `RELATION f` plus `D + O` DELETE. The decoder rejects before
Apply, leaving the existing row, private count, public result, and continuation
unchanged on PostgreSQL 17 and 18.

`test-m5-toast.sh` stores a deterministic 64 KiB UTF-8 value in a source text
column forced to `STORAGE EXTERNAL`, verifies its TOAST relation has storage,
and applies the real INSERT into `payload_text`. A no-key-change UPDATE is
captured as `U + N` with canonical key `t` and payload `u`. The gate proves bad
payload-tag rejection before writes, continuation-after-insert crash rollback,
exact text retention, retry once, replay no-op, and unchanged private/public
count on PostgreSQL 17 and 18.

`test-m5-incompressible-toast.sh` uses two seeded, dependency-free 64 KiB
high-entropy ASCII values under default `EXTENDED` storage. It proves both
source values are out-of-line and uncompressed, then verifies the replacement
UPDATE carries exact `t` bytes and atomically replaces `payload_text` while
count/result stay `1`. A binary-tag corruption, post-continuation crash, retry,
and exact replay prove the failure and recovery boundaries on PG17/18.

`test-m5-composite-delete.sh` applies two composite-key rows sharing key1,
captures one real `D + K` with two canonical int8 fields, and proves only the
exact pair is removed while count/result decrement once. It also proves bad
second-key rejection, a valid missing-pair rollback, continuation-after-insert
crash rollback, retry, and replay on PostgreSQL 17 and 18.

`test-m5-replica-index.sh` captures a real single-column relation under
`REPLICA IDENTITY USING INDEX`, proves RELATION `i`, exact key flag and `D + K`,
then applies INSERT and DELETE through the existing current-state/count/result/
continuation transaction. It proves crash rollback, retry once, replay no-op,
and that a live switch back to default identity is rejected before writes on
PostgreSQL 17 and 18.

`test-m7-concurrent-ddl.sh` uses bounded wait-event polling, not timing guesses,
to prove Apply's granted relation lock, DDL's waiting exclusive lock, zero
blocked-state writes, Apply-before-DDL commit order, and subsequent fail-closed
processing on PostgreSQL 17 and 18.

`test-m8-multi-source.sh` captures two real sources through independent slots
and publications. It proves source-local continuation/replay, the shared union
count/result, source-2 crash isolation and retry, and generation mismatch
rollback on PostgreSQL 17 and 18.

`test-m8-concurrent-sources.sh` uses real captures, wait-event polling, and
bounded channel receives to prove one-Apply duplicate CAS and independent
source progress with exact global/per-source state on PostgreSQL 17 and 18.

`test-m8-bounded-decode.sh` captures a real 10,000-change committed transaction,
decodes/applies/replays it, then captures 10,001 changes and requires explicit
limit rejection with unchanged durable state on PostgreSQL 17 and 18. A pure
test also proves both committed and streamed decoders reject input above 16 MiB.
This is decoder admission evidence, not a production receiver or throughput
claim.

`test-m8-performance.sh` freezes a real 10,000-change PG17/18 regression budget:
decode at most 2 seconds, first Apply at most 10 seconds, and exact replay at
most 2 seconds. The originating clean-room runs measured approximately 8 ms,
0.8 seconds, and 0.2 ms respectively. It also proves constructors reject 10,001
changes and a forged public value fails before database state or replay can be
reached. The threshold is a correctness regression gate, not a sustained-load,
tail-latency, or cross-hardware benchmark.

`test-m9-registration.sh` installs the operator authority, proves CountRows has
no input address, resolves a live int8 column name to its exact ObjectAddress,
and proves missing source/column, wrong type, and duplicate ID leave no partial
definition/state/result. The migrated M2–M8 gates explicitly register one
CountRows per source and query operator-keyed state/result; multi-source tests
sum those independent public facts only to retain the historical union
observation.

`test-m9-operator-performance.sh` captures one real 10,000-row nullable-int8
transaction: every fourth payload is NULL and every other payload is 2. One
EffectBatch must publish CountRows=10,000 and SumInt8=15,000, then exact replay
must be a no-op. The PG17 reference measured 9.56 ms decode, 836.72 ms Apply,
and 0.171 ms replay under the unchanged 2 s / 10 s / 2 s ceilings. The closest
M8 count-only PG17 run measured 8.21 ms, 853.28 ms, and 0.180 ms; the small
mixed differences are recorded as single-run evidence, not an improvement
claim or permission to hide regression. A test-only ordered operator-2 failure
also proves the operator-1 attempt and all transaction-local effects roll back.

`test-m9-count-sum.sh` proves the fixed two-operator INSERT/UPDATE/DELETE/NULL
example, the nullable relation's real two-position `D + K`, missing-row and
overflow rollback, crash after the first result, retry-once, exact-replay
short-circuit, and pre-EffectBatch DDL invalidation. `test-m9-operator-concurrency.sh`
holds source 1 at a test-only advisory lock: a second transaction for that
source waits on the binding mutex while source 2 commits independently; after
release, CountRows and SumInt8 finish in source order and all replays are no-op.
The existing M6 gates remain the streaming regression proof for the admitted
key-only CountRows shape; M9 does not add nullable-payload streaming admission.

`test-m10-committed-ingress.sh` is the first production transport gate. It
links the selected libpq for the requested PostgreSQL major, enters real COPY
BOTH without `pg_recvlogical`, receives protocol-v1 XLogData, assembles one
transaction, invokes the existing decoder and Runtime on a separate Apply
connection, and proves public CountRows/SumInt8 plus continuation.
Pure ingress tests independently split every byte boundary, coalesce frames and
transactions, enforce the 16 MiB buffer bound, validate `w`/`k`, and freeze the
34-byte status payload.

The same gate now proves M10.2: requested keepalive reports only the old durable
LSN; receive-before-Apply drop changes neither computation nor slot; Runtime
commit-before-feedback restarts as `AlreadyApplied`; explicit feedback flushes
the exact COMMIT `end_lsn`; decoder and Operator failures poison the receiver,
roll back all state, and do not advance the slot; clean restart retries once.

`test-m10-streaming-ingress.sh` runs the production receiver in explicit
protocol-v2 streaming mode with 64 KiB logical-decoding memory. The gate is
defined to prove real
multi-segment 10,000-change `S/R/I/E...c` delivery crosses arbitrary transport
chunk boundaries yet enters the existing Runtime decoder only after terminal
commit. Partial input and `E` produce neither Apply nor feedback; a crash during
partial assembly relies on slot replay and later applies exactly once. A real
matching `A` bypasses Runtime, leaves every Shiba durable fact absent, and may
advance only to the outer XLogData `dataStart` carrying that abort. Corruption,
unknown/mismatched XID, wrong terminal, 16 MiB overflow, and 10,001 changes fail
closed without feedback. Acceptance requires the same gate to pass on
PostgreSQL 17 and 18; no CLI may be used by the production receiver and no
persisted spool may be created.

The same M10.3 gate must exercise the closed terminal-authorization set:
Runtime `Applied`, exact-replay `AlreadyApplied`, strict `EmptyCommitted`, and
legal top-level `Aborted`. An empty commit must have exactly
`S(first=true) E (S(first=false) E)* c`, at least one complete segment, one XID,
flags zero, valid commit/end LSNs, and no other frame/trailing byte; it advances
only through explicit empty ACK and creates no continuation. Legal `R/I`
traffic must instead reach the sole Runtime decoder, and every other shape must
fail closed. This is structural evidence about the selected
publication's empty output only. Publication identity, mutation/recreation
drift, and rejection of empty ACK after invalidation are proved separately by
M10.4; they are not consequences of the M10.3 grammar alone.

The first PG17 run exposed a real multi-segment publication-empty commit and
failed the former single-segment assumption. The corrected constant-state
recognizer then passed the same production COPY BOTH gate on PG17.10 and
PG18.4. Those runs prove a real segment count greater than one, exact terminal
LSNs, pre-ACK replay, no empty-feedback loop, unchanged operator/continuation
state, later source Apply, streamed abort, partial-stream restart, the 10,000
change admission boundary, and rejection of change 10,001 without feedback.

`test-m10-catalog-ingress.sh` is the M10.4 catalog-governance gate. On PG17 and
PG18 it configures one exact source/publication/existing-slot tuple and proves
atomic duplicate rejection, wrong/missing/active/plugin/database slot failure,
publication shape admission, PUBLIC denial, and that configuration never
creates or drops a physical slot. It exercises publication ALTER rollback and
commit, remove-then-add persistence, drop plus same-name recreation, source
invalidation, pristine slot rotation, stale generation CAS, active/wrong/
non-pristine replacement rejection, and absence of dynamic progress columns.

The first PG17 publication-membership test failed because
`pg_event_trigger_ddl_commands()` returned no ObjectAddress for `ALTER
PUBLICATION ... DROP TABLE`. The accepted implementation retains the single
event writer but compares every configured publication OID and frozen snapshot
to live catalogs at `ddl_command_end`; the corrected gate passes independently
on PG17.10 and PG18.4. This is failure evidence for persistent publication
history, not permission to match by name or globally invalidate unrelated
sources.

`test-m10-governed-ingress.sh` is the separate governed-session gate and is
green on PG17.10 and PG18.4. It proves wrong role/generation and active-slot
failure, advisory ownership exclusion, exactly one Apply plus one replication
connection, detach/reattach, least-privilege streamed receive/Apply/ACK of
10,000 changes, and revalidation that rejects an already pending
`EmptyCommitted` after publication remove/re-add while CountRows and the slot
LSN remain at their last durable values. Pure tests freeze the advertised
32-source/64-connection cap and validate required explicit database, positive
connection timeouts, and positive Apply statement timeout. Neither gate creates
or drops a slot during ordinary session attach/detach.

The gate uses two distinct non-superuser roles. The Apply role has
`NOREPLICATION`, schema usage, and only the internal table privileges required
by governance and Runtime. Its `SELECT` on `source.events` exists solely because
Runtime preflight takes `ACCESS SHARE`; Runtime does not read source rows. Its
`UPDATE` privilege on `source_continuation` is required because the latest-row
replay check uses `SELECT ... FOR UPDATE`. The receiver role has `REPLICATION`
plus source-schema `USAGE` and source-table `SELECT`, and no Shiba internal
write grants. Swapping the roles in either connection fails safely.

`test-m10-performance-ingress.sh` freezes limits before accepting results:
15 s for a real 10,000-change source-commit-to-durable-Apply path, 2 s replay,
20 tx/s for 100 ready transactions, service p50/p95/p99 limits of
250/500/1,000 ms, a 300 ms slow-Apply floor, and 250 ms outstanding-receive
rejection. PG17 measures 860.865 ms E2E, 29.350 ms replay, 622.987 ms backlog
service, 160.52 tx/s, 6.216/6.355/6.533 ms service percentiles, 1.393 ms
rejection, and 357.969 ms slow Apply. PG18 measures 867.479 ms, 31.085 ms,
739.298 ms, 135.26 tx/s, 7.375/7.585/7.776 ms, 1.836 ms, and 358.370 ms.

The E2E timer starts before committing the 10,000-row source INSERT and stops
after durable Apply. The 100-transaction timer starts only after all ten-row
transactions are committed; those percentiles are receiver service latency
against a precommitted backlog, not source-commit latency. The same test freezes
Rust bounds of 16,777,216 assembly bytes, 10,000 decoded changes, two
connections per source, one outstanding input, and no queue. It does not measure
allocator/RSS peaks or cross-host soak.

`test-m10-shutdown-ingress.sh` proves cooperative idle shutdown through the
asynchronous libpq receive loop. PG17 returns in 42.262 ms and PG18 in
76.950 ms, both below 1 s, with no terminal token, ACK, Shiba write, or slot-LSN
advance; detach/reattach then succeeds. Its failure evidence fixes the receive
order: drain already-buffered libpq `CopyData` before socket polling, then use
`PQsocketPoll`/`PQconsumeInput`. Shutdown during Runtime Apply and automatic
reconnect/backoff remain outside the gate.

Pure Runtime session tests cover connection-scoped relation metadata: the first
transaction requires an exact `R`, repeated `R` is revalidated, a later omission
is admitted only for the same source, and a changed source/mismatch fails. The
constant-size `PgoutputRelationState` retains no relation frame list or bytes and
does not replace the semantic decoder.

`test-m6-stream-abort.sh` starts a live protocol-v2 receiver before a 10,000-row
transaction, observes real segments while it is open, rolls it back, and
requires real matching `A`. After abort feedback it restarts the same slot and
applies/replays a later streamed commit, proving the aborted stream left no
row/count/result/continuation state on PostgreSQL 17 and 18.

All live Apply tests register their source relation through the private M7.1
function; there is no unbound production test path. `test-m7-ddl-invalidation.sh`
proves exact relation ObjectAddress storage, unrelated-DDL isolation, rename
rollback, committed-rename invalidation, pre-Apply failure, historical replay,
and ordinary-role denial on PostgreSQL 17 and 18.

`test-m7-drop-invalidation.sh` proves direct DROP rollback, committed DROP,
exact old relation ObjectAddress retention, same-name/new-OID non-revival, and
schema CASCADE invalidation on PostgreSQL 17 and 18. Pending work fails before
row/count/result/continuation writes; historical exact replay remains a no-op.

`test-m7-column-invalidation.sh` proves the registered binding set contains the
relation and exact positive column attribute numbers. It covers type-change
rollback/apply, committed type-change fail-closed behavior, and an isolated
column rename whose durable cause is the exact column address on PG17 and PG18.

`test-m7-index-invalidation.sh` proves the identity-index binding, unrelated
index isolation, rename rollback/apply, committed exact-index invalidation,
pending pre-Apply rejection, state isolation, and historical replay on
PostgreSQL 17 and 18.

`test-m5-source-binding.sh` binds the decoder to a real relation OID, applies an
INSERT, renames both table and column, and applies another INSERT under the same
OID. It then drops/recreates the original qualified name, proves the new OID is
different and the wire is otherwise valid, and verifies the old binding rejects
before row/count/result/continuation writes on PostgreSQL 17 and 18.

`test-m6-stream-commit.sh` sets isolated-cluster logical decoding memory to
64 KiB and captures a 10,000-row protocol-v2 transaction with streaming on. It
requires at least two matching `S ... E` segments and terminal `c`, proves no
prefix/abort-shaped input is visible, then proves post-continuation crash rolls
all rows/count/result/continuation back before retry-once and replay no-op on
PostgreSQL 17 and 18.

During development run fmt, check, the Runtime unit tests, one current scenario,
and clippy. Run both complete PG matrices only at the milestone boundary.

## M11.1 contract gate

M11.1 first records the PostgreSQL semantic boundary; it is not a production
implementation gate. The paired PG17/18 experiment creates a new logical
slot through replication protocol with `EXPORT_SNAPSHOT`, records its exact
`consistent_point` and nonempty `snapshot_name`, and keep that exporter idle.
Multiple fresh `REPEATABLE READ READ ONLY` transactions must import the same
snapshot before their first query and observe the same baseline while a normal
transaction observes concurrently committed INSERT/UPDATE/DELETE.

The experiment must also prove that executing another exporter command or
closing it prevents a new import, without changing an already imported
transaction's view. It must leave no Shiba row/operator/result/continuation or
cursor mirror. The gate passes on PG17.10 and PG18.4; PG18 additionally proves
the opaque snapshot token may contain hexadecimal letters. Static checks
require the separate Bootstrap identities, one
checkpoint authority, building/unavailable public result, exact three-to-two
connection transition, pristine pre-scan reset, same-slot post-scan catch-up,
and explicit M12 deferral.

## M11.2 production vertical gate

Run `scripts/test-m11-bootstrap.sh` independently with the absolute PG17 and
PG18 `pg_config` paths. The gate uses the production Bootstrap session with an
absent slot, exact exported snapshot, batch limit two, CountRows, and SumInt8.
Baseline rows `(1,10),(2,NULL),(3,30)` must reach private `3/40` while public
results remain building/NULL after every batch.

During that snapshot, one source transaction inserts `(4,5)`, changes row 1 to
20, and deletes row 3. Catch-up must preserve building visibility, produce
private `3/25`, create exactly one real-WAL continuation, and activate public
`3/25` only after the exact attempt-bound fence is durably handled. Current
state must be exactly `(1,20),(2,NULL),(4,5)`, equal to the SQL differential.
Conversion to ordinary M10 then applies `(5,7)`, acknowledges its terminal, and
must yield exact `4/32`, four rows, and two WAL continuations without duplicate
snapshot contribution.

This production gate is green on PG17.10 and PG18.4. It is distinct from the
M11.3 crash matrix and M11.4 million-row performance gate documented below;
those later gates complete M11 without entering M12.

## M11.3 recovery gate

Run `scripts/test-m11-recovery.sh` independently for PG17 and PG18. The gate
reconstructs the committed crash-after-reservation state (`creating`, exact
slot absent); restart persists `cleanup_pending` without a fabricated
consistent point and performs exact pre-scan replacement. Reservation rejects
a preexisting slot before an attempt exists. Replacement rejects stale
generation and a foreign requested slot; partial rows and operator state are
cleared only with the old config/checkpoint; the distinct attempt and larger
generation remain building/NULL; and a failure rolls back the replacement.

The production recovery matrix additionally covers batch-before-commit,
batch-after-commit exact replay, post-`scan_complete` same-slot resume,
catch-up restart, active cutover before feedback, restart after feedback,
PostgreSQL restart, Shiba/session restart, duplicate worker advisory-lock
competition, and repeated start. Assertions compare source rows, CountRows,
SumInt8, public visibility, continuation, phase, slot generation and exact
`confirmed_flush_lsn`; no test infers success from names alone.

This gate is green on PG17.10 and PG18.4. It proves exact batch replay and
overflow rollback, duplicate-worker advisory conflict, `scan_complete` followed
by immediate PostgreSQL restart and resume, catch-up Apply committed before its
ACK connection is killed, active cutover committed before its ACK connection is
killed and then exact-fence replayed, feedback-covered active restart as a
no-op, and final source/current rows plus CountRows/SumInt8 SQL differential
`4/50`. It does not directly kill the process at the reservation instruction or
exercise an active foreign old-slot conflict.

Complexity checks remain structural. Runtime's roughly 2,260 production lines
are split among bootstrap model/Apply, source Apply, operator execution,
preflight and decoder responsibilities. The 1,200 total is a warning; 3,000 is
an audit stop, not a target. A production file warns above 300 and fails above
400. SQL files remain at most 150 lines. Warnings cannot fail CI, while hard
limits do; no threshold justifies compacting code or deleting recovery tests.

## M11.4 bounded performance gate

Run `scripts/test-m11-bootstrap-performance.sh` independently for PG17 and
PG18. The test freezes its limits before observation: 1,000,000 snapshot rows,
10,000 rows per batch, scan <=120 s and >=10,000 rows/s, one exact 10,000-change
concurrent WAL transaction plus activation <=15 s, Rust RSS growth <=256 MiB,
three bootstrap connections, synchronous delivery, and no queue.

PG17.10 passes with 100 batches in 3.098397625 s (322,747.47 rows/s), catch-up
in 1.320857542 s, and RSS 10,160→13,824 KiB (+3,664). PG18.4 passes in
3.136067542 s (318,870.68 rows/s), 1.329330584 s, and 10,160→13,824 KiB
(+3,664).
Both prove SQL differential after concurrent UPDATE/DELETE/INSERT and ordinary
M10 live handoff. M11 is complete at this declared boundary. This local bounded
gate is not evidence for indefinitely sustained writers, contention tail
latency, reconnect supervision, cross-host soak, or M12.

## Evidence handling

Fixtures in `tests/fixtures` must be data, not copied executable implementation.
The canonical Protocol vector has the adjacent
`tests/fixtures/protocol/canonical-v1.provenance.md`, including source, legacy
commit, old command, and clean-room command. It has been re-proved. The
`tests/fixtures/pg/deferred-evidence.json` file is only an A-class evidence
index: legacy scenarios remain provenance until reproduced case by case. M2's
independent rollback/crash tests do not claim equivalence to an old runtime.
Differential tests use the legacy
repository solely as an oracle and must never link it, load its SQL, or share a
catalog authority.

## M11.5 least-privilege bootstrap gate

Run `scripts/test-m11-bootstrap-roles.sh` independently with the absolute PG17
and PG18 `pg_config` paths. It uses a non-superuser `NOREPLICATION`
control/Apply/scanner, a distinct non-superuser `REPLICATION` transport, and a
public-result-only reader. The full snapshot, concurrent WAL catch-up,
activation and live handoff must match the CountRows/SumInt8 SQL oracle.

Negative cases swap roles and revoke bootstrap-function `EXECUTE`, source
`SELECT`, or checkpoint `UPDATE`; each must leave source/operator state,
continuation, public activation and feedback unchanged. PG17.10 and PG18.4
pass. TLS/password policy, cross-host credentials, column-level grants, and a
successful split-role abandoned-attempt replacement are not claimed.

## M12.1 rebuild contract gate

M12.1 is failure-first and must not claim the data path implemented. The
contract freezes the closed lifecycle, exact-old identity/generation CAS
model, `active -> building -> active` visibility, forward-only post-prepare
recovery, old-generation worker/token/Apply/ACK rejection and observable
physical-slot shape classification. Static checks forbid a candidate
binding/config, second bootstrap/continuation/decoder, slot-birth marker, alias
or fallback.

A real M11/M10 active-source gate plus PG17.10/PG18.4 experiment records
failure-first authority snapshots and all stable
`pg_replication_slots` identity/shape fields around a same-name logical-slot
drop/recreate. Equality is evidence that PostgreSQL exposes no immutable birth
identity, not permission to adopt the replacement. Negative cases for every
observable mismatch remain mandatory in M12.2 and later gates. Documentation
must describe the `REPLICATION` credential as a trusted capability and list an
identical privileged replacement as excluded residual risk.

M12.1 runs Markdown/static contract checks plus fmt, workspace check/tests and
clippy. It does not count as proof of destructive prepare, snapshot-to-live
rebuild, crash/DDL/concurrency/role matrix or performance; each belongs to its
subsequent independently green milestone.

## M12.2 rebuild admission gate

Run `scripts/test-m12-rebuild-admission.sh` independently with the absolute
PG17 and PG18 `pg_config` paths. PG17.10 and PG18.4 are green. The gate begins
with a real active, non-pristine M11/M10 source and snapshots every old
authority. Invalid nullable-int8 shape, missing caller `SELECT`, stale
BootstrapId/generation, active old slot, occupied target slot, foreign target
binding and mixed operator plan must fail with byte-for-byte-equivalent catalog
state. Two concurrent exact requests produce one winner.

The winning request must expose exactly one target relation, two column bindings
and one exact default-primary-key identity-index binding, target ingress
config/generation and `rebuild_prepared` lifecycle. Pre-M12 old state is accepted
only as the exact three-row shape; an M12-produced generation is accepted only
as the exact four-row shape with its persistent retired identity marker. It must
also prove `building/NULL`, empty
current-row and continuation state, zero private operator values, retired old
invalidations, an unchanged inactive old physical slot, and an absent target
physical slot. A foreign old-receiver token with a matching LSN is rejected by
the new receiver-local capability check.

Failure-first development also caught and corrected four concrete assumptions:
the initial test incorrectly expected a custom identity binding rather than the
existing relation-plus-two-columns set; SECURITY DEFINER permission validation
had to use `session_user`; PostgreSQL `bigint` values had to retain their exact
64-bit type at the Rust boundary; and PL/pgSQL block-label plus deferred-
constraint references required explicit qualification. The identity gate then
failed first on PG17 because unparenthesized PL/pgSQL `IF CASE` is invalid.
These are clean-room runtime failures, not legacy evidence. The independent
`scripts/test-m12-rebuild-identity-authority.sh` gate is green on PG17.10 and
PG18.4. It proves exact-four target persistence, recovery using only durable
Catalog coordinates, same-OID rename reconciliation, unrelated index isolation,
binding cardinality/kind/address rejection, a second rebuild whose old CAS comes
from the durable fourth row, and fail-closed same-shaped primary-key replacement.
M12.2 does not prove old-slot cleanup, new exported snapshot or snapshot-to-live
differential; those are M12.3 evidence. Crash recovery, DDL/least-privilege
breadth and performance remain later gates.

## M12.3 snapshot-to-live gate

Run `scripts/test-m12-rebuild-snapshot-live.sh` with the exact PG17 and PG18
`pg_config` paths. Both PG17.10 and PG18.4 are green. The test starts from a
real active, non-pristine generation 2 and proves generation 3 has the exact
four-row identity, drops only the exact inactive old slot, creates the target
slot with real `EXPORT_SNAPSHOT`, scans in bounded batches, and consumes a
concurrent INSERT/UPDATE/DELETE transaction before exact-fence activation.

Assertions cover `building/NULL` through activation, SQL-equal rows plus
CountRows/SumInt8 after catch-up, absence of copied old continuation, retained
retired triple, rejection of old token and attach, no second binding/config
switch, and a normal post-cutover M10 live Apply/ACK. This gate is the normal
forward path only. It must not be cited for M12.4 crash/restart windows.

The gate also probes Runtime eligibility before replay/Apply: retired generation
2 rejects after prepare and after activation; target generation 3 rejects in
`rebuild_prepared`, `creating`, `scanning`, and `scan_complete`; only
`catching_up` and `active` admit ordinary WAL under the locked sole binding.

## M12.4 rebuild recovery gate

`scripts/test-m12-rebuild-recovery.sh` is the focused PG17.10/PG18.4 gate for
instruction-level M12 recovery. It uses observable barriers, slot state and
deterministic transaction failures rather than sleeps. It covers prepare,
old-slot cleanup/new-slot creation, lost exporter/snapshot replacement, first/
middle/final scan retry, scan-complete PostgreSQL restart, catch-up, fence,
activation and pre-feedback restart, exact retry, stale/foreign slot rejection
and concurrent-worker exclusion.

Each injected boundary asserts lifecycle, public visibility, current rows,
private operator values, continuation/checkpoint, generation, slot flush
position and SQL oracle after retry. M12 lost snapshots must restart only with
fresh BootstrapId, distinct slot and exact successor generation; M11 marker-null
recovery is separately regressed unchanged. This gate is not evidence for M12.5
DDL/least-privilege or M12.6 million-row performance.

## M12.5 rebuild governance gate

Run `scripts/test-m12-rebuild-governance.sh` on PG17.10 and PG18.4. It starts
from a real active non-pristine source and covers exact ObjectAddress relation,
publication, identity-index, replica-identity, column and operator-plan drift;
same-OID index rename/unrelated-index isolation; publication remove/re-add or
OID replacement; and invalidation after durable prepare. It asserts that a
rejected building target never scans, Applies, ACKs or activates.

The gate also covers transport `IDENTIFY_SYSTEM` plus database and control-role
target `SELECT` preflight, exact-index `AccessShareLock`/`pg_relation_size`
validation, same-source one-winner ownership and different-source live progress.
It runs split-role success with `NOREPLICATION` control/Apply/scanner, a
separate trusted `REPLICATION` transport role with target `SELECT`, and a
public-result reader; exchanged roles and missing control/source/internal
privileges fail closed. It is not M12.6 performance, release-matrix, TLS,
reconnect or privileged-identical-slot-replacement evidence.

## M12.6 performance and release gate

Freeze these limits before running the active rebuild benchmark:

- snapshot scan <= 12 s;
- one real 10,000-change WAL catch-up <= 8 s;
- activation <= 2 s;
- complete rebuild <= 25 s;
- RSS growth <= 128 MiB;
- retained WAL <= 256 MiB.

The scenario starts from one million active rows with nonzero CountRows,
SumInt8 and a real WAL continuation. During the exported snapshot it commits
one 10,000-change transaction, then measures the existing bounded scan,
catch-up, activation and live handoff. It must assert the SQL differential,
building/NULL visibility, exact generation transition, ordinary post-cutover
live Apply and absence of an unbounded queue or per-row SQL path. The comparison
baseline from M11 is approximately 3.1 s scan, 1.3 s catch-up and 3.6 MiB RSS
growth. PG17.10/PG18.4 values are scan 4.357951916/4.429333333 s, catch-up
1.946769416/1.907849875 s, activation 9.755875/9.981958 ms, total
6.343139667/6.375927458 s, RSS +4,272/+4,320 KiB, and retained WAL
252,864,952/252,898,072 bytes. Relative to the recorded M11 baseline, rebuild
adds about 1.3 s scan, 0.6 s catch-up, and under 0.7 MiB RSS; this is the
deliberate prepare/retirement/generation validation cost, not a second queue or
per-row execution path.

The one-command current release matrix has this fixed order:

1. formatting, workspace check and focused tests;
2. workspace tests and clippy with warnings denied;
3. the complete PG17 integration matrix;
4. the complete PG18 integration matrix;
5. M12 differential, crash, concurrency, least-privilege and performance
   evidence;
6. exact script count, exact PostgreSQL versions and the observed performance
   report.

It may reuse existing scripts, but may not skip, delete or weaken a historical
gate. The wrapper's final status is insufficient unless every constituent
result and count is reported. `scripts/release-matrix.sh` is green on
PostgreSQL 17.10 and 18.4: 57 unique scripts and 114 successful invocations.
It emits exactly one performance record for each server version. The M16.7
static gate additionally verifies that concrete aggregate ABI dispatch is
absent from Runtime, Ingress, Catalog SQL, Bootstrap, Rebuild and Result Sink,
and remains confined to operator aggregate modules.
# M16 admission hardening

The M16 static and unit gates must exercise the shared HAVING node/depth/
boolean budgets, compiler and graph rejection of unsupported Aggregate
topologies, and the common aggregate work budget. Limit and limit-plus-one
cases are checked before maps/read sets/extrema state are built. Result-schema
tests cover duplicate names, 63-byte identifiers, ordered complete key
ordinals, canonical digest mismatch, and source-derived nullability. The
accepted aggregate subset remains strictly Int8: AVG, variance/stddev,
Numeric/Decimal, DISTINCT, CTEs, window functions, and general DAG execution
are rejected and remain outside M16.
