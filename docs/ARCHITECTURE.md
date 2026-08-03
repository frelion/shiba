# Architecture boundary

M14.6 cuts the production lifecycle over to the graph boundary frozen in
[OPERATOR_GRAPH_CONTRACT.md](OPERATOR_GRAPH_CONTRACT.md): one canonical typed
graph owns its exact source membership, nodes, edges, terminal outputs and hard
bounds. Runtime schedules canonical topology and persists generic deltas; only
the Operator crate may dispatch concrete node implementations. The cutover
replaces, rather than mirrors, the flat plan set and source-scoped continuation.
One `graph_definition`, ordered `graph_source_member` set, ingress configuration,
slot generation and `graph_continuation` own both single-input and admitted
two-input execution. ADR 0005 fixes one slot/generation and one transaction
authority for both sides of the admitted two-source INNER JOIN.

M14.2 implements the first stateless vertical slice. Compiler binds the full
ordered source layout and exact expression slots; Source Apply/Bootstrap create
one typed DeltaBatch; Filter, Compute, Project and Materialize execute purely;
Runtime loads and persists plans/state/results generically. The old ProjectRows
implementation is removed rather than adapted. M14.3 adds KeyBy, GroupedCount
and GroupedSumInt8; M14.6 moves their state into the sole private
`graph_node_state` authority. Runtime loads and persists canonical typed keyed
state/results set-wise in the same transaction as every member's Source Apply
and graph continuation.

M14.4 accepted the graph-wide two-source authority in
[JOIN_AUTHORITY_CONTRACT.md](JOIN_AUTHORITY_CONTRACT.md) and
[ADR 0006](adr/0006-m14-two-source-join-authority.md). Exactly
two explicit SourceIds share one database/publication/slot/generation, one
assembled pgoutput transaction, one graph continuation/ACK decision, one
exported-snapshot bootstrap and one graph-wide rebuild. Exact relation/column
ObjectAddresses and the exact right bigint PK/UK index ObjectAddress are part
of admission. A source may belong to at most one building or active graph.
There is no per-source continuation, second Runtime or persisted DeltaBatch.

M14.5 implements the database-free half: a nonzero `GraphId` owns ordered
`SourcePort` values, `SourcePort(SourceId)` identifies node inputs, and the
right port binds the exact effective replica-identity index. Compiler emits one
canonical multi-input plan; the kernel uses generic partition state and
computes mixed batches from pre-state to final-state with bounded normalized
output. M14.6 installs that graph as the sole Catalog/Runtime/lifecycle
authority: a single-input source is a one-member graph and the join is a
two-member graph, with no second Runtime or compatibility path. Directed
PG17.10/PG18.4 graph Runtime gates prove one- and two-member registration,
same-transaction two-side Apply, right fan-out/retraction, key changes, rollback,
exact replay and effective-identity invalidation. M14.7 closes the final
release/performance matrix.

The graph-wide durable authorities are `graph_definition`, ordered
`graph_source_member`, `graph_ingress_config`/`graph_ingress_source`,
`graph_ingress_invalidation`, `graph_continuation`, `graph_bootstrap` with
subordinate per-member `graph_bootstrap_checkpoint`, `graph_node_state`, and
the `shiba.graph_result` header plus private keyed rows exposed by
`shiba.graph_result_rows`. The source facts `source_binding`,
`source_invalidation`, and `source_row_state` remain SourceId-owned. Superseded
operator/source execution tables are absent; there is no alias, dual write,
migration adapter, persisted DeltaBatch, or per-source progress.

Every member durably binds its effective replica identity: the default primary
key when `relreplident='d'`, or the exact unique replica-identity index when
`relreplident='i'`. Registration rejects a relation without exactly one
admitted effective index. Runtime validates the same ObjectAddress before
Apply, and replacement invalidates the complete graph. Compiler also supplies
strict `ComputedProject` and filtered grouped pipelines while Runtime consumes
only graph topology and result contracts, never concrete node kinds.

Identity admission retains earlier row-shape boundaries without a second
Runtime. A zero-column one-member CountRows graph may omit identity only because
its layout has no addressable row key. Proven composite identities remain
admitted for their one-member shape. The M14 INNER JOIN right member is
narrower: exactly one non-null bigint default-PK or explicit unique
replica-identity key. These are explicit graph/source contracts, not fallback.

M14.7 re-proves this architecture through 51 scripts on PG17.10 and PG18.4
(102 invocations). It completes M14, not V2.

M13.1 froze the generic execution successor to the M9 aggregate-shaped API;
M13.2 supplied its pure kernel, and M13.3 made that kernel the sole Runtime and
Catalog execution path. See
[OPERATOR_KERNEL_CONTRACT.md](OPERATOR_KERNEL_CONTRACT.md): concrete plan
dispatch is confined to `shiba-operator`; Runtime owns the PostgreSQL
transaction and generic scalar/keyed sink only. Catalog durably orders plans by
operator ID and stores each strict specification, canonical compiled payload,
digest, state codec and output contract. Ingress/Bootstrap/Rebuild must consume
that set without kind, fixed-ID, fixed-count or column-position assumptions.
M13.4 closed those production lifecycle regression gates without an adapter or
parallel path. M13.5 closed the final PG17.10/PG18.4 release, performance and
static-specialization evidence gates.

## Direction

The intended dependency line is:

```text
Protocol -> Catalog -> Compiler -> Source Ingress -> Source Apply
         -> EffectStream -> Runtime -> minimal Operator -> Result Sink
```

Phase 1 proved `Protocol -> Catalog`. M2 implements only the narrow path from a
test-constructed committed source transaction through Apply, deterministic
count, Result Sink, and continuation. A right-hand component may depend on the
contracts to its left; a left-hand component must not import execution,
registration, or result code. PostgreSQL is the durable authority; Rust values
are not a second authority.

## Authority and schemas

`shiba_internal.catalog_identity` is the one Phase-1 writer-owned catalog fact.
Its constraints make the singleton and frozen versions explicit. The private
schema is not a public table contract. `shiba.versions()` is a read-only public
view of that fact. Neither user schemas nor legacy schemas are queried or
mirrored. The function pins `search_path` so an invoker cannot redirect catalog
lookup by creating a same-named object.

M2 adds four purpose-specific facts: `applied_insert`, `count_state`,
`source_continuation`, and public `shiba.count_result`. They have one logical
writer, the M2 processor, and are never independently repaired or mirrored.

M3.1 adds a pure Source Ingress decoder inside `shiba-runtime`. Its admitted
relation OID, source ID, and slot generation are explicit input configuration;
schema and relation names are decoded only for bounds checking and are not
identity. The decoder owns no connection and no durable state. It produces an
M2 `SourceTransaction` only after validating COMMIT, so the processor remains
the only transaction owner and writer.

M3.2 adds no production state. PostgreSQL's slot may lag the committed Shiba
continuation and replay an already visible transaction; the processor's durable
identity check makes that replay a no-op. Slot feedback never writes Shiba state
and cannot make a result visible.

M4.1 extends each existing Apply fact with `payload_present` and nullable
`payload_int8`. This distinguishes a relation with no payload column from a
present SQL NULL without creating another Apply table or authority. The decoder
admits only key-only or key-plus-nullable-int8 shapes; the same processor writes
the payload, count state, result, and continuation in one transaction.

M4.2 represents a zero-column INSERT with no `source_row_id`. Its durable cause
identity remains transaction plus input sequence, so multiple empty rows in one
transaction are distinct without a synthetic key. The existing Apply table and
writer remain unchanged; keyed paths continue to require a non-null key.

M4.3 stores a fixed second `int8` key component in the same Apply fact. A
partial `NULLS NOT DISTINCT` unique index covers only keyed rows, preserving
single-key uniqueness, composite-pair uniqueness, and multiple cause-identified
empty rows. The processor is still the sole writer and transaction owner.

M4.4 updates the nullable payload in that existing Apply row; it creates no
UPDATE log or second authority. The original INSERT cause remains attached to
the row, while `source_continuation` durably records each committed source
transaction. UPDATE does not advance count. Apply mutation, unchanged count
state/result, and continuation are owned by the same processor transaction.

M4.5 removes that same Apply row for a pgoutput `D + K` change with one stable
`int8` key. `applied_insert` has therefore evolved into the sole current
source-row-state authority: INSERT creates, UPDATE mutates, and DELETE removes.
Its name is recorded debt; renaming it is deferred because doing so would expand
this slice, and no alias, compatibility view, or second state table is allowed.
The processor remains the sole writer and PostgreSQL transaction owner. It
deletes exactly one row, decrements private count without underflow, publishes
the matching result, and records continuation in the same transaction.

M4.6 tightens the pure decoder's existing relation-shape check. It admits only
PostgreSQL default replica identity (`d`) and exact key flags for each frozen
shape: none for empty, key for single-key, key/non-key for nullable payload,
and two keys for composite identity. This adds no durable state or writer. A
live `FULL` relation is rejected before the processor owns a transaction.

M5.1 extends the same `applied_insert` row with `payload_text`; it does not add
a table or authority. A text INSERT writes the exact UTF-8 value. An
unchanged-TOAST UPDATE carries only a `u` token, so the processor validates that
the target is an existing text row and retains its durable value. Row state,
unchanged count/result, and continuation still share one processor transaction.

M5.2 uses the same text row and writer for a complete replacement value. The
wire owns the new UTF-8 bytes; the processor overwrites only `payload_text`,
keeps count/result stable, and records continuation in the same transaction.
No TOAST store, fetcher, or alternate value authority is introduced.

M5.3 carries the existing composite pair into DELETE without introducing a
general row-identity layer. The same current-state row stores both components;
the processor matches both (including NULL-safe single-key handling), deletes
exactly one row, decrements count/result, and commits continuation atomically.

M5.4 adds an explicit decoder configuration for the existing single-key shape
under PostgreSQL replica identity index. It changes only RELATION admission
from expected `d` to expected `i`; tuple decoding, the current-state authority,
sole writer, and transaction owner remain unchanged. This configuration is not
a durable catalog binding and cannot silently accept identity drift.

M5.5 adds no production state. It proves that the existing admitted-source
configuration is an OID binding rather than a name lookup: table/column rename
preserves admission, while same-name drop/recreate changes the OID and fails
before the processor owns a transaction. Registration, discovery, and DDL
observation are not introduced.

M6.1 adds a pure decoder for one complete streamed protocol-v2 transaction.
Segments have no durable authority and cannot call Apply; only stream commit
creates the existing stable transaction identity and hands one complete value
to the unchanged processor, sole writer, and PostgreSQL transaction owner.

M6.2 adds no production component. The PostgreSQL slot/receiver is test ingress:
real abort segments are discarded by the decoder, and restart feeds a later
complete commit into the same sole processor writer.

M7.1 adds two non-overlapping facts. `source_binding` is the immutable mapping
from source ID to exact admitted object addresses and is written only by the private
registration function. M7.3 makes that set one relation plus its live user
columns; M7.4 adds the current replica-identity index when one exists. Neither
adds another authority. `source_invalidation` is written only by one event
trigger function in the owning DDL transaction. For a non-replay transaction,
the processor resolves the bound OID, acquires relation `ACCESS SHARE`, checks
the exact invalidation, then performs Apply. The object lock is held through
commit, closing the DDL/check-to-Apply race without a second runtime writer.

M7.2 adds no authority or production path. PostgreSQL rolls back `sql_drop`
facts with direct DROP, while committed direct DROP and schema CASCADE retain
the old relation ObjectAddress. Recreating the same qualified name produces a
different OID and cannot satisfy or replace the immutable binding.

M7.3 proves PostgreSQL reports column rename by the exact column ObjectAddress
and column type change by a bound relation address. The existing event writer
handles both; the processor still locks only the unique relation row and rejects
an invalidation of any address in the source's frozen binding set.

M7.4 gives the otherwise shape-identical relation and index rows explicit
`binding_kind` values. Registration discovers only the current
`indisreplident` index. Runtime locks the relation-kind row; the unchanged event
writer matches index DDL by exact index ObjectAddress.

M7.5 adds no production component. A test-only trigger pauses Apply after
preflight; PostgreSQL lock inspection proves the processor already holds the
relation lock and conflicting DDL waits until the processor transaction commits.

M8.1 removes only the singleton-source restriction. A source's relation-binding
row is its serialization mutex; the processor probes replay, takes that row
lock, probes replay again, then validates DDL and source-local continuation
order. `count_state` and `count_result` remain one global aggregate over the
union of admitted sources and are still written in the same processor transaction.

M8.2 adds no production component. Database wait events prove same-source work
queues on the binding-row mutex and is resolved by the post-lock replay probe;
a different source has a different mutex and can progress to the global count
transaction independently.

M8.3 adds one decoder admission boundary, not a queue. Both committed and
streamed decoders borrow at most 16 MiB and own at most 10,000 decoded changes.
The shared check rejects excess input before parsing; each decoder rejects the
10,001st change before decoding or appending it. Rejection cannot reach the
processor, so it adds no authority, writer, transaction, or recovery path.

M8.4 centralizes the 10,000-change transaction limit. Constructors check it
before validation allocation, decoders check it before decoding change 10,001,
and the processor checks it before opening PostgreSQL state so a public struct
literal cannot bypass the bound. `process` is a synchronous call borrowing one
`Client` and owns no channel, queue, worker, or background buffer: database lock
and commit waits directly stop that caller. The global count row remains the
known cross-source serialization point.

M9.1 supersedes that fixed aggregate authority. `shiba-operator` defines pure
row effects and checked evaluation; `shiba-compiler` turns strict declarative
IR plus a supplied live descriptor into a name-independent compiled operator.
Runtime Source Apply reads UPDATE/DELETE before images under row lock, performs
each mutation once, and returns one in-memory EffectBatch. It then locks the
source's operator states in ascending operator-ID order, updates state and the
operator-keyed public sink, and writes continuation last.

`operator_definition` is written only by `compile_and_register`, which shares
the source binding lock/invalidation boundary and initializes definition,
state, and result atomically. Runtime is the sole state/result writer. The old
count tables are deleted rather than mirrored or exposed through aliases.
Multi-source CountRows results are source-scoped operator facts; a test may sum
them to compare with the historical union observation, but that sum is not a
new durable authority.

M9.2 registers CountRows and SumInt8 for one nullable-int8 source. Source Apply
builds one EffectBatch, then the runtime locks and updates operator IDs 1 and 2
in ascending order inside the same transaction. The fixed 10,000-row sample
produces count 10,000 and sum 15,000. A test-only ordered failure at operator 2
proves the earlier operator write, source mutation, audit, result, and
continuation are not independently visible. This adds no durable effect table,
execution SQL, scheduler, or second transaction owner.

M10 introduces Source Ingress as a transport owner, not another state writer.
One libpq logical-replication connection receives COPY BOTH payloads while a
separate existing PostgreSQL client invokes Runtime. A bounded incremental
assembler retains at most one transaction and the existing decoder remains the
only semantic pgoutput decoder. Waiting for WAL therefore owns no Apply
transaction or lock, and slow Apply stops further reads without a queue.

PostgreSQL's replication slot is the transport-cursor authority;
`source_continuation` remains the computation/replay authority. Ingress may
report a terminal end position only after Runtime returns `Applied` or
`AlreadyApplied`, or after another strictly classified terminal authorization.
It cannot write current rows, operator state/results, or continuation, and
Shiba does not mirror slot progress.

M10.4 makes ingress attachment catalog-governed without creating another
cursor. `source_ingress_config` binds database OID, exact publication OID plus a
frozen semantic snapshot, exact source binding, existing slot name, and slot
generation. One persistent invalidation writer prevents publication membership
history, drop/recreate, or same-name replacement from reviving the binding.
Ingress revalidates this authority and the live slot before receive, Apply, and
each ACK. PostgreSQL `pg_replication_slots` remains the physical slot authority;
no LSN, receiver-status, or WAL-spool mirror is added.

Each governed source has exactly two connections: a replication connection and
an Apply connection. A process cap admits 32 sources, a source-specific advisory
lock plus slot exclusivity admits one receiver, and both conninfo values require
an explicit matching database and positive connection timeout. Waiting for WAL
holds no Apply transaction or row lock. Slot rotation is an explicit
pristine-only generation CAS over existing inactive slots, never automatic
create/drop/discovery.

PG17/18 governed gates prove the two connections use distinct least-privilege
roles. The `NOREPLICATION` Apply role receives only Runtime/governance grants;
its source-table `SELECT` exists solely for the preflight `ACCESS SHARE` lock,
not a row lookup, and continuation `UPDATE` permits the latest-row `FOR UPDATE`
check. The receiver role has `REPLICATION` plus source schema `USAGE` and table
`SELECT`, but no Shiba-state write authority.

The final M10 receive loop is synchronous and bounded: one owned `CopyData`
vector is at most 16 MiB plus 25 envelope bytes, assembly is at most 16 MiB,
decoded changes are at most 10,000, and there is one outstanding transaction
with no queue. One constant-size connection-local relation state validates the
first and every repeated `R`, permitting later omission only for the same exact
source. It stores no frames and creates no decoder or durable authority.

Idle shutdown drains libpq-buffered `CopyData` before polling the socket, then
uses asynchronous `PQgetCopyData`, `PQsocketPoll`, and `PQconsumeInput` on a
bounded cycle. PG17/18 prove shutdown within 42.262/76.950 ms against a 1 s
limit with no ACK, durable-state change, or LSN advance, followed by successful
detach/reattach. The frozen local performance gates prove 10,000-change source-
commit-to-durable-Apply in 860.865/867.479 ms and direct slow-Apply
backpressure. M10 is complete at this production-ingress scope, not a claim
that Shiba V2 is complete.

M11.2 implements the initial-data vertical slice. Explicit bootstrap creates a
logical `pgoutput` slot with `EXPORT_SNAPSHOT`;
PostgreSQL's returned `consistent_point` and ephemeral `snapshot_name` are the
only allowed bridge between existing rows and M10 WAL. Fresh short read-only
repeatable-read scan transactions repeatedly import that snapshot while the
exporting replication connection stays idle. Each bounded batch has a distinct
bootstrap identity and atomically advances the one bootstrap checkpoint with
current-row and operator state. It is never a fake WAL transaction and never
advances `source_continuation`.

M11.2 replaces the WAL-cause-shaped `applied_insert` table with the sole
key-owned `source_row_state` and tags transaction-local effects as WAL or
bootstrap. Catch-up cutover requires an exact attempt-bound logical-message
`BootstrapFence`, never a keepalive `wal_end` or sampled LSN. The fence is a
transport terminal, not a source transaction or general pgoutput `M` path.

The initial slice is pristine and pre-active. Public results remain building/
unavailable through scan and catch-up; one transaction publishes complete
results and active lifecycle together. Before `scan_complete`, losing the
ephemeral snapshot resets the hidden attempt and starts a fresh never-used
attempt/slot/snapshot. After `scan_complete`, recovery retains that slot and
resumes M10 catch-up. Scan owns exactly three connections; catch-up/live return
to M10's two. No WAL spool, queue, second continuation, or durable EffectStream
is introduced. M12 subsequently proves active/non-pristine rebuild by reusing
this same path.

The production PG17/18 gate proves batches of two transform baseline private
state to `3/40`, apply one concurrent INSERT/UPDATE/DELETE WAL transaction to
`3/25`, publish only at the exact fence, and then use the ordinary M10 session
for `4/32`. Public values stay building/NULL before cutover, current rows equal
the SQL oracle, and only real WAL writes continuation.

M11.3 adds recovery without another data path. Before `scan_complete`, an
explicit coordinator drops only the exact inactive attempt-owned slot; one
catalog transaction then retires the exact old checkpoint/config, removes its
hidden partial rows, resets private operator state, and reserves a distinct
attempt with a larger generation through the existing live publication
validator. After `scan_complete`, recovery never resets the snapshot build: it
retains the same slot and resumes the existing M10 catch-up. An active cutover
whose feedback was interrupted is replayed only against the exact stored fence
marker and `activation_end_lsn`; Runtime is not reinvoked for the fence.
Per-source advisory ownership serializes competing coordinators, while
PostgreSQL remains the physical slot cursor authority.

The synchronous Runtime is currently about 2,260 production lines because
bootstrap identity/model, bounded batch Apply, operator execution, source
preflight, and WAL decoding remain separate named responsibilities. The
complexity gate warns at the historical 1,200-line soft budget and stops for a
fresh responsibility audit at 3,000; 3,000 is not a target or permission to
fill. Production files warn above 300 and fail above 400. No file split may add
an authority, compatibility path, or abstraction without two real callers.

## Phase gates

Every later module must name: its durable authority, sole writer, transaction
owner, input identity, retry boundary, recovery proof, and deletion/DDL policy.
No later code may use an old authority as a fallback. An implementation is
accepted only after its clean-room tests prove its new contract; legacy tests are
evidence inputs, not implementation dependencies.

M11.3's PG17.10/PG18.4 recovery gate proves pre-scan exact replacement,
partial-state reset, rollback/replay, advisory competition, same-slot resume
across PostgreSQL restart, catch-up and active-before-feedback replay, and final
SQL differential `4/50`. It reconstructs the durable creating/slot-absent crash
state rather than killing at that exact instruction; an active foreign old-slot
conflict is not directly exercised.

M11.4 closes the declared boundedness gate with one million snapshot rows in
100 synchronous 10,000-row batches and one concurrent 10,000-change WAL
transaction. PG17/PG18 scan+Apply are 3.098397625/3.136067542 s
(322,747.47/318,870.68 rows/s), catch-up+activation are
1.320857542/1.329330584 s, and observed Rust RSS growth is 3,664/3,664 KiB.
Both retain the three-connection scan budget, no queue, SQL differential, and
ordinary live handoff. The frozen limits were 120 s, 10,000 rows/s minimum,
15 s catch-up, and 256 MiB RSS growth.

M11.5 closes the role boundary on PG17.10/PG18.4: bootstrap
control/Apply/scanning runs as a non-superuser `NOREPLICATION` identity,
transport as a distinct non-superuser `REPLICATION` identity, and result
reading as a public-result-only identity. Permission loss and role reversal
fail before state or feedback advances. This changes no authority,
transaction owner, connection count, or production path.

M11 is complete at its declared pristine nullable-int8 CountRows/SumInt8
initialization boundary, not a complete V2.

**Unproved:** indefinite concurrent-writer catch-up and tail latency,
network/TLS behavior, shutdown during Apply, reconnect daemon/
backoff policy, allocator/RSS peaks, cross-host soak, admission
for `D + O` or replica identity `FULL`, composite UPDATE and broader old-tuple
shapes, NULL text, binary payloads, TOAST keys, composite replica indexes,
streaming interleaving,
production failover and persisted partial-stream recovery, SQL frontend,
additional operator families beyond non-aggregate ProjectRows, broader result
types, cross-host sustained soak, empirical heap peak, contention tail latency,
and recovery workers
remain.

## M12.1 one-authority rebuild architecture

M12 adopts an offline rebuild rather than parallel old/new computation. Before
destructive prepare, the old binding/config/generation and complete public
result remain sole active authority while all target checks are side-effect
free. Prepare takes the source ownership fence and exact-old CAS, then one
transaction makes the target binding/config/generation the sole catalog
authority, records the existing bootstrap lifecycle as building, publishes
`building/NULL`, and retires old current rows, private operator state and
continuation. That commit is the forward-only boundary.

Catalog identity and execution eligibility are deliberately distinct after
prepare: M11 scanner and Runtime resolve the sole target authority, but M10 live
receive/Apply/ACK remains disabled until activation. Activation promotes that
same authority and complete results atomically; it never installs a candidate
or performs another binding/config switch. Existing M11 snapshot scan/catch-up
and M10 receiver/decoder/Runtime/sink/feedback remain the only data path.

Physical slot create/drop remains PostgreSQL-owned and non-transactional with
the catalog, so durable lifecycle phases drive exact reconciliation. Every
observable slot mismatch fails closed. PostgreSQL 17/18 expose neither an
immutable slot birth identity nor per-slot ACL: the replication credential is
a trusted control-plane capability, and an identical privileged external slot
replacement is an explicit residual risk outside the M12 correctness threat
model. See [the rebuild contract](REBUILD_CONTRACT.md) and
[ADR 0003](adr/0003-m12-offline-rebuild.md). M12.1 freezes this architecture;
M12.2 proves destructive admission and durable identity; M12.3 proves the real
snapshot-to-live path; M12.4 proves forward recovery and M12.5 governs DDL,
concurrency and roles. M12.6 closes the million-row/release performance matrix.

## M12.2 destructive admission architecture

`PreparedRebuild::prepare` owns the admission orchestration. It takes the same
per-source advisory ownership as live/bootstrap work, opens a short Apply
transaction, locks and resolves the explicitly supplied target relation, and
invokes the sole catalog writer with exact old and target coordinates. All
target shape, publication, permission and slot-name checks precede mutation.
Any failure therefore leaves the old active binding/config/generation, public
result, rows, state, continuation and invalidations unchanged.

The writer's commit changes the existing authority in place: target
binding/config/generation and BootstrapId are the only catalog identity,
`source_bootstrap.phase` is `rebuild_prepared`, results are `building/NULL`,
current rows and continuation are gone, private operator states are zero, and
old-generation invalidations are retired. The old inactive physical slot is
deliberately retained and the target slot remains absent; their ordered,
recoverable transport cleanup/creation belongs to M12.3. The default single-
column bigint primary-key index OID is an explicit admission/CAS coordinate and,
for an M12-produced generation, a fourth exact `source_binding` ObjectAddress
beside the relation and two live columns. Pre-M12 active rows keep their proved
three-row shape. A non-null retired BootstrapId/slot/generation triple persists
through the M12 lifecycle and selects the four-row shape; recovery never guesses
from live catalogs or silently substitutes another index.

An index rename with the same OID permits only narrow reconciliation. A
replacement index has a new OID and fails closed until explicit rebuild. This
still reuses the one `source_binding` authority; no index registry is added.

Receiver terminal capabilities now carry an unforgeable, process-local
receiver authorization. A token from an old or foreign receiver fails even if
its LSN happens to match; durable generation/lifecycle checks remain the
catalog defense. This is no cursor authority and creates no durable state.
The earlier PG17.10/PG18.4 `scripts/test-m12-rebuild-admission.sh` run covered
invalid shape/permission/stale identity/active or preoccupied slot/foreign
binding/mixed-plan rollback, single-winner concurrency and the exact successful
building state. The follow-on identity-authority gate was failure-first and
initially exposed invalid unparenthesized PL/pgSQL `IF CASE` syntax on PG17.
After correction, `scripts/test-m12-rebuild-identity-authority.sh` is green on
PG17.10 and PG18.4.

## M12.3 snapshot-to-live architecture

PG17.10 and PG18.4 now pass `scripts/test-m12-rebuild-snapshot-live.sh`. A real
active, non-pristine generation 2 enters prepare as generation 3 with the exact
four-row target authority. The coordinator drops only the exact inactive old
slot, creates the named target slot with a real `EXPORT_SNAPSHOT`, and hands
that snapshot to the existing bounded M11 scanner. Concurrent INSERT, UPDATE
and DELETE are then consumed from the same slot through M10 catch-up, exact
fence, activation, ordinary live ingress and durable feedback.

Public results remain `building/NULL` until activation. The old continuation is
deleted rather than copied, snapshot batches never acquire WAL identity, and
the target binding/config/generation installed by prepare is not switched a
second time. The retired identity triple remains durable after activation.
Old-generation attach and terminal-token authorization fail closed. M12.3 did
not alone prove the instruction-level crash matrix; M12.4 subsequently closed
that recovery gate.

Runtime locks the sole binding before checking ingress config/bootstrap
generation and before any replay or Apply action. When a bootstrap lifecycle
exists, ordinary WAL processing is eligible only in `catching_up` or `active`.
Thus the retired generation rejects both immediately after prepare and after
activation, while target generation 3 cannot bypass bootstrap during
`rebuild_prepared`, `creating`, `scanning`, or `scan_complete`.

## M12.4 interrupted rebuild architecture

M12.4 retains one lifecycle and one data path. A durable `rebuild_prepared`
row resumes through the existing prepared-rebuild handoff and performs only its
next exact slot action. An M12-produced `creating` or `scanning` row whose
exported snapshot is lost uses the existing `restart_abandoned` mechanism: it
retires the abandoned attempt and reserves a fresh BootstrapId, distinct slot,
and exact successor generation. It never restores the retired active generation
or reuses an old snapshot or continuation.

This is not a second bootstrap implementation. It reuses the sole lifecycle
row, M11 scanner/checkpoint, and M10/M11 catch-up, fence, activation and ACK
paths. The target remains the same exact-four binding—including durable
identity-index OID—and stays `building/NULL` until ordinary activation. M11
marker-null recovery keeps its existing semantics; the fresh-slot/exact-successor
rule is restricted to M12-marked abandoned attempts.

## M12.5 rebuild governance architecture

M12.5 keeps the one-authority rebuild topology while proving its governance
boundary on PG17.10 and PG18.4 with `scripts/test-m12-rebuild-governance.sh`.
Before destructive prepare, the transport credential executes
`IDENTIFY_SYSTEM` and verifies the exact database; the control caller must
independently have `SELECT` on the approved target relation. These checks have
no side effect. The control/Apply/scanner role remains `NOREPLICATION`; the
separate transport role is the narrowly trusted `REPLICATION` control-plane
capability with target `SELECT`; the reader has only public-result `SELECT`.

The target relation, publication, key/payload columns, primary identity-index
and complete ordered compiled-plan set are compared by durable ObjectAddress
bindings and canonical plan digests. The exact
identity-index OID is held with `AccessShareLock` while `pg_relation_size`
checks shape. A same-OID rename is not rejected by name; replacement,
publication drift, replica-identity/column drift and post-prepare invalidation
stop the building generation before scan, Apply or activation. Preflight reads
operator definitions and ingress config without needless row-update locks.

The existing per-source ownership fence serializes live, DDL and rebuild work:
two rebuilds for one source have one winner, while another source can continue
ordinary Apply. Permission loss or a swapped role rolls back before prepare, or
leaves an already prepared target safely `building/NULL`; it never revives the
retired generation. M12.6 closed million-row rebuild performance and the
release matrix without changing the execution path.

## M12.6 bounded rebuild and release architecture

M12.6 adds evidence, not another execution path. The million-row active rebuild
uses the same destructive prepare, M11 bounded scanner, M10 catch-up,
activation, feedback and live handoff described above. Before measurements, the
acceptance limits are frozen at snapshot scan <= 12 s, 10,000-change catch-up
<= 8 s, activation <= 2 s, complete rebuild <= 25 s, RSS growth <= 128 MiB and
retained WAL <= 256 MiB. A failed limit requires implementation work; observed
results cannot be used to relax the limit.

The release matrix runs formatting/checks and focused tests, workspace tests
and clippy, the complete PG17 matrix, the complete PG18 matrix, then the M12
differential, crash, concurrency, least-privilege and performance gates. It
reports exact script and server-version counts rather than treating a wrapper
exit status as evidence. The green release run executes 48 unique PG scripts
per version grouping (41 foundation plus seven M12), 96 invocations total, on
PG17.10 and PG18.4. PG17/PG18 scan is 4.357951916/4.429333333 s, catch-up is
1.946769416/1.907849875 s, activation is 9.755875/9.981958 ms, total is
6.343139667/6.375927458 s, RSS growth is 4,272/4,320 KiB, and retained-WAL
peak is 252,864,952/252,898,072 bytes.

The comparison baseline is M11's pristine bootstrap: approximately 3.1 s scan,
1.3 s catch-up and 3.6 MiB RSS growth. Rebuild adds deliberate prepare,
retirement and activation work, but still may not add a queue, per-row SQL path,
second authority or parallel generation. M12.6 completes only the
declared active nullable-`int8` CountRows/SumInt8 rebuild boundary; TLS,
automatic reconnect supervision, cross-host/failover operation, broader source
shapes, SQL frontend and broader result/operator shapes remain outside it.
