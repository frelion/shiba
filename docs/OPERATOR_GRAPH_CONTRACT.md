# M14 Operator SDK and graph contract

M14.2 implements the typed stateless graph path. M14.3 adds the sole generic
keyed-state authority plus KeyBy, GroupedCount and GroupedSumInt8. Runtime uses
canonical typed keys and set-based state/result persistence; typed NULL remains
distinct from absent. Two-input Join, graph-wide lifecycle and the full M14
release/performance matrix remain unproved, so M14.3 is not M14 completion.

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

## Two-input INNER JOIN

M14 admits exactly two source relations in one database. One publication, one
slot and one generation provide their common PostgreSQL transaction order. The
right join input is an exact non-null bigint PK or UK ObjectAddress; the join is
bigint equality INNER JOIN. Schemas may differ. Outer, three-table and non-
equality joins are excluded.

A graph transaction contains one WAL identity and changes tagged by SourceId.
Changes to both relations in one PostgreSQL transaction enter one Runtime
transaction and one join evaluation. Right-side update/delete fan-out uses a
bounded generic state partition read, never a source-table lookup or per-row
SQL. Both input batches are evaluated as one pre-state to final-state change so
intermediate ordering is not observable.

The continuation belongs to `(graph_id, slot_generation)` and records the exact
commit LSN and ingress transaction identity. There are no left/right or node
continuations. Graph bootstrap creates the slot with one `EXPORT_SNAPSHOT`,
scans both members through bounded batches under that snapshot, catches up that
slot, and atomically activates all terminal results. Rebuild replaces the whole
graph and generation through the existing forward-only M12 lifecycle.

## Transaction, locks and ACK

Runtime owns the only Apply transaction. The fixed order is:

1. graph/generation ownership mutex;
2. exact graph header and digest;
3. replay/continuation probe;
4. source bindings in ascending SourceId order;
5. current rows in `(SourceId, typed row key)` order;
6. node state in canonical `(NodeId, namespace, state key)` order;
7. pure graph computation;
8. generic state deltas;
9. generic result deltas;
10. graph continuation last;
11. commit, then and only then ACK.

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

The green M13 HEAD supplies the unchanged CountRows/SumInt8 comparison:
PG17/PG18 Apply medians are 782.302750/787.157125 ms. M14 stop lines are
899.648163/905.230694 ms (+15%). M8--M13 absolute limits remain unchanged;
especially, M12 retained WAL remains at most 256 MiB (latest evidence
252,876,464/252,917,880 bytes). A regression does not permit a larger limit or
smaller workload.

## Evidence and exclusions

Every expression and node has an independent deterministic reference model and
fixed-seed randomized differential. Gates cover I/U/D, NULL/Absent, key change,
overflow, corrupt codecs, bounds, filter truth transitions, group lifecycle,
join changes/fan-out/same-transaction two-side changes, rollback, kill, retry,
replay, DDL, bootstrap, catch-up, rebuild, recovery and least privilege.
Grouped and joined materializations compare complete rows with independent SQL
oracles on PG17.10 and PG18.4.

M14 excludes SQL parsing, outer or three-table joins, windows, DISTINCT,
Min/Max/Avg, plugins, schedulers and persisted intermediate deltas. M14 does not
claim complete V2.
