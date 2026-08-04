# M14 Operator SDK and graph contract

## M16.2 wide terminal result contract

M16.1 freezes the aggregate extension: aggregate nodes refer to exact
`AggregateCallId` values and closed
function descriptors, state access is expressed as generic bounded exact/range
reads, and terminal output carries canonical `ResultSchemaV1` plus typed rows.
Only the Operator kernel dispatches CountStar, Count(nullable `int8`), SumInt8,
MinInt8 or MaxInt8;
Runtime continues to schedule the graph and persist generic deltas without
matching a function name. See
[AGGREGATE_FUNCTION_CONTRACT.md](AGGREGATE_FUNCTION_CONTRACT.md).

M16.2 implements canonical wide terminal output. Each `OutputContract` contains
one validated schema and, for scalar output, its canonical initial row.
Materialize projects an ordered field-slot list and emits complete typed rows;
scalar and keyed outputs share generic ResultDelta mutations. Runtime/Catalog
persist these bytes without node/function dispatch. The fixed key/value delta
and output-shape contracts are deleted. M16.3 subsequently replaces concrete
Count/Sum nodes with one versioned Aggregate node for CountStar, Count and Sum;
Multi-call SQL, MinInt8/MaxInt8 and restricted grouped HAVING are implemented
by M16.4/M16.5/M16.6; scalar HAVING
remains a later slice.

M14.2 implements the typed stateless graph path. M14.3 adds the sole generic
keyed-state authority plus KeyBy, GroupedCount and GroupedSumInt8. Runtime uses
canonical typed keys and set-based state/result persistence; typed NULL remains
distinct from absent. M14.4 accepts the exact two-source JOIN authority and
M14.5 implements its pure GraphId/SourcePort Compiler and Operator kernel.
M14.6 cuts Catalog, Runtime, Ingress, Bootstrap and Rebuild over to that same
graph authority. Directed PG17.10/PG18.4 graph Runtime evidence is green;
M14.7 closes the full lifecycle release/performance evidence. M14 is complete
at this contract boundary, not complete V2.

M14 extends the M13 database-free kernel; it does not add a SQL frontend or a
second Runtime. This contract freezes graph identity, typed deltas, state,
results, transaction ownership, recovery and bounds before production changes.

## One durable graph authority

One active graph owns one or two explicit `SourceId` members in the same
PostgreSQL database. A source may belong to only one building or active graph.
The compiler/registration writer atomically installs one canonical graph
payload and digest; node rows, edge rows, or caller arguments cannot become a
second plan authority. The canonical graph contains:

- nonzero `GraphId`, format version and exact source membership;
- each source relation and input column as an exact `ObjectAddress`;
- stable nonzero `NodeId` values, typed ports and canonical edges;
- deterministic topological order and explicit terminal outputs;
- state and result contracts plus the frozen graph limits.

Duplicate identities, cycles, dangling ports, ambiguous topology, an unused
node, type mismatch, noncanonical ordering, an unknown version, or a digest
mismatch fail closed. Concrete node dispatch remains private to
`shiba-operator`. Runtime, Catalog SQL, Ingress, Bootstrap and Rebuild never
branch on a node kind.

Admission is intentionally narrower than a general DAG executor. An Aggregate
may consume only Source → Filter/Project/Compute/KeyBy input and must have one
and only one direct Materialize child. Aggregate-to-Aggregate edges and
Aggregate fan-out fail during Compiler and OperatorGraph construction, before
Runtime state or result persistence. ResultSchema field names, key ordinals,
and source-derived nullability are validated at the same boundary.

The M14.6 schema cutover replaces the flat definition authority; it does not
mirror it. Likewise, the graph-scoped continuation replaces the source-scoped
continuation in the same cutover. The two forms never coexist as production
writers, and there is no compatibility view, adapter, fallback or dual write.

## Typed transaction-local SDK

`TypedValue` is one of `Absent`, `Null(ValueType)`, `Bool(bool)`, `Int8(i64)` or
`Text(String)`. `Absent` means that transport did not provide a value and is
never a SQL value, state value, result value or expression result. `Null` is
typed and distinct from absent. M14 expressions consume only boolean and
bigint; text remains representable so existing current-row/unchanged-TOAST
semantics are not weakened, but using text in an M14 expression fails closed.

`TypedRow` carries an exact layout digest and a bounded ordered vector of typed
values. Source column names are compile-time inputs only. Runtime values are
addressed by compiled ports/field positions whose layout maps to exact
ObjectAddresses.

`DeltaBatch` contains an exact transaction-local origin, input port and ordered
`before`/`after` `TypedRow` deltas. Source Apply constructs each source delta
once. A node receives all of its input-port batches together and returns:

- a bounded output `DeltaBatch` for downstream nodes;
- `StateDelta`, containing deterministic scalar or keyed state mutations;
- `ResultDelta`, containing scalar replacement or keyed result mutations only
  for an explicit `Materialize` terminal.

These values are Rust memory owned by the current processor transaction. They
are never written as an effect log, queue, spool, audit stream or replay cursor.
The sink persists declared state/result shapes but performs no computation.

## Expressions and stateless nodes

The closed expression IR is `Column`, typed bigint literal, typed NULL, bigint
comparison, `IS NULL`, `AND`, `OR`, `NOT`, and checked bigint `+`/`-`.
Compilation resolves every source column name once to its ObjectAddress and
typed port. Runtime never resolves a name.

Expression semantics follow PostgreSQL three-valued logic: comparisons and
arithmetic with NULL return typed NULL; `IS NULL` returns non-null boolean;
`AND`/`OR`/`NOT` use the SQL truth tables. Absent, wrong types and arithmetic
overflow fail the complete graph transaction.

`Filter` preserves a row only for boolean true; false and NULL both suppress
it. Therefore false-to-true emits an insertion and true-to-false emits a
withdrawal. `Project` replaces the row layout with its expression list.
`Compute` appends named compiled fields. ProjectRows is removed and represented
only as `Project(Column(key), Column(value)) -> Materialize`; no old plan kind,
empty-state special case or source-row-field shortcut remains.

## Keyed state and grouping

State is Runtime-opaque and versioned. In M14.3, generic keyed state is
addressed by `(operator_id, node_id, namespace, partition_key_payload,
item_key_payload)` canonical typed keys; `operator_id` remains the current
graph-plan identity until the later graph-wide authority cutover.
Runtime loads requested partitions in set-based queries and persists ordered
mutations in set-based statements; it never interprets a concrete node state.
`StateReadSet` is computed deterministically from the graph and input deltas
before state loading. Missing, extra, corrupt or wrong-codec state fails closed.

`KeyBy` produces a typed partition key. GroupedCount and GroupedSumInt8 maintain
one checked aggregate per key. NULL is one typed group key; absent is rejected.
Insert, update, delete and group-key change emit exact withdrawal/addition.
When a group becomes empty its state and materialized result are deleted.

Within one input transaction all deltas are netted against the pretransaction
state and final after-images. A state or result key has at most one normalized
final mutation. Observable behavior cannot depend on source-change order.

## Pure two-input INNER JOIN boundary

M14.5 compiles exactly two explicit, canonically ordered `SourcePort` members
under one nonzero `GraphId`; node inputs identify them as
`SourcePort(SourceId)`. The right source binds its exact effective replica-
identity PK/UK ObjectAddress. One publication, one slot and one generation will
provide their common PostgreSQL transaction order in M14.6. The
right join input is an exact non-null bigint PK or UK ObjectAddress; the join is
bigint equality INNER JOIN. Schemas may differ. Outer, three-table and non-
equality joins are excluded.

The M14 proof graph fixes its projection to left
`(id bigint PK, right_key bigint NULL)` joined to right
`(id bigint PK/UK, payload bigint NULL)`, materialized as
`left.id -> right.payload`. A NULL join key does not match; a matched NULL
payload is a typed NULL result.

The pure kernel uses generic partitioned state for left membership and right
payloads and computes mixed input batches from pre-state to final-state. Its
fixed-seed 300-step relational differential covers mixed changes. Exact fan-out
20,000 succeeds; 20,001 fails before returning a transition. Ordered affected-
row indexes replace the initial quadratic scan with `O(n log n)` behavior.

A graph transaction contains one WAL identity and changes tagged by SourceId.
Changes to both relations in one PostgreSQL transaction enter one Runtime
transaction and one join evaluation. Right-side update/delete fan-out uses a
bounded generic state partition read, never a source-table lookup or per-row
SQL. Both input batches are evaluated as one pre-state to final-state change so
intermediate ordering is not observable.

A source can belong to at most one building or active graph. The right PK/UK
index itself, not only its columns, is an exact durable ObjectAddress binding.
Admission, bootstrap, rebuild, DDL, crash, privilege and performance evidence
is frozen in the JOIN authority contract and
[ADR 0006](adr/0006-m14-two-source-join-authority.md). The pure compiler/kernel
is implemented; this does not claim the Runtime/Catalog or PostgreSQL path.

The continuation belongs to `(graph_id, slot_generation)` and records the exact
commit LSN and ingress transaction identity. There are no left/right or node
continuations. Graph bootstrap creates the slot with one `EXPORT_SNAPSHOT`,
scans both members through bounded batches under that snapshot, catches up that
slot, and atomically activates all terminal results. Rebuild replaces the whole
graph and generation through the existing forward-only M12 lifecycle.

## Transaction, locks and ACK

Runtime owns the only Apply transaction. The fixed order is:

1. graph/generation ownership mutex;
2. replay/continuation probe under the exact graph identity;
3. source bindings in ascending SourceId order;
4. current rows in `(SourceId, typed row key)` order;
5. node state in canonical `(NodeId, namespace, state key)` order;
6. pure graph computation;
7. generic state deltas;
8. generic result deltas;
9. graph continuation last;
10. commit, then and only then ACK.

No network wait, snapshot scan, slot operation or user interaction occurs while
these locks are held. A deadlock or serialization retry restarts the complete
transaction. Exact replay short-circuits before Source Apply and graph
execution. Any decode, binding, state, compute, bound, sink, constraint or
backend failure rolls back every current row, node state, terminal result and
continuation; it cannot authorize feedback.

Registration installs a complete graph only on pristine state or through
bootstrap/rebuild. It cannot add a node after a continuation exists. Bootstrap
checkpoints and activation are bound to the exact graph digest. Graph drift
between batches fails before checkpoint advance. A stale graph, generation,
worker or feedback token fails closed.

## Hard bounds and performance

M14 freezes these admission limits before implementation:

- at most 2 sources, 32 nodes, 16 typed fields per row and 10,000 source
  changes per transaction;
- at most 20,000 deltas emitted by one node;
- at most 200,000 total node-output deltas, 100,000 state mutations and
  100,000 result mutations per graph transaction;
- at most 64 MiB of tracked graph work memory, including typed rows, state
  snapshots, deltas and result mutations;
- existing 16 MiB wire/assembly limit remains unchanged.

All counters use checked arithmetic and are charged before allocation or fan-
out. Exceeding any limit aborts without continuation or ACK. Runtime decodes a
graph and state payload at most once per transaction, constructs each source
batch once, loads state by node/partition rather than change-by-node queries,
and persists keyed mutations in bounded sets rather than per-row round trips.

The frozen M13 CountRows/SumInt8 comparison baselines are
782.302750/787.157125 ms and M14 stop lines are 899.648163/905.230694 ms
(+15%). M14.7 five-run medians are 771.019625/821.920250 ms (-1.44%/+4.42%),
within those lines. M8--M13 absolute limits remain unchanged. The final M12
regression retains 252,905,752/252,938,872 bytes of WAL, below 268,435,456
bytes. No larger limit or smaller workload was used.

## Evidence and exclusions

Every implemented expression and node has an independent deterministic
reference model and fixed-seed randomized differential. Pure JOIN gates cover
mixed I/U/D, NULL/Absent, corrupt state, exact fan-out bounds and pre-to-final
semantics. PG17.10/PG18.4 directed Runtime gates cover complete joined rows,
left/right and same-transaction changes, bounded fan-out/retraction, rollback,
retry, replay and exact PK invalidation. Grouped materializations compare
complete rows with independent SQL oracles on both versions. Full graph
bootstrap, catch-up, rebuild, recovery, least privilege and performance are
re-proved by the M14.7 release matrix.

M14 excludes SQL parsing, outer or three-table joins, windows, DISTINCT,
Min/Max/Avg, plugins, schedulers and persisted intermediate deltas. M14 does not
claim complete V2.
