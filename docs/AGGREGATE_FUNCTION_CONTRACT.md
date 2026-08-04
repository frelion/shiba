# M16 generic aggregate function and wide result contract

## Status and scope

M16.1 freezes this contract and adds a database-free, test-only reference
model. Twelve pure tests cover CountStar/Count/Sum/Min/Max, exact multiplicity,
grouped lifecycle and key changes, HAVING visibility, fixed-seed randomized
I/U/D differential, codec corruption and exact bounds. No production aggregate
ABI, wide Result Sink, `MIN`/`MAX`, `HAVING`, or PostgreSQL evidence is claimed
at this boundary. M1--M15 authority, transaction, recovery, bootstrap, rebuild,
ACK and bounded SQL evidence remains unchanged.

M16.4 subsequently proves the first multi-call use of this ABI: one Aggregate
node carries ordered CountStar/Count(nullable-int8)/SumInt8 calls and one
complete wide result row through Binder, Compiler, bootstrap, live Apply/ACK,
replay and rebuild on PostgreSQL 17.10 and 18.4.

M16.5 now proves production MinInt8/MaxInt8. Their state is an ordered
per-group multiplicity map stored in the existing graph-node state authority;
NULL is excluded, duplicate extrema retract one copy at a time, and an empty
multiset finalizes to typed NULL. The same generic binder/compiler/runtime
path is exercised through PostgreSQL 17.10/18.4 bootstrap and live I/U/D SQL
oracles. M16.6 adds restricted grouped HAVING visibility transitions. M16.7
closes the frozen release, performance and extensibility gates; the remaining
function families below are deliberately outside this milestone.

M16 replaces aggregate-kind knowledge outside `shiba-operator` with one stable,
versioned function ABI and replaces the scalar-or-key/value result assumption
with a canonical typed row schema. The first implementation scope is:

- `COUNT(*)`, `COUNT(nullable int8 expression)`, `SUM(int8)`, `MIN(int8)` and
  `MAX(int8)`;
- zero or one existing `int8` group key;
- multiple aggregate calls in one aggregate node and one materialized result;
- grouped `HAVING` over the group key and aggregate outputs, using the existing
  checked `int8`/boolean/NULL expression semantics;
- one canonical wide result row of at most 16 fields.

It does not add `AVG`, `COUNT` over non-`int8` expressions, `DISTINCT`, multiple group keys,
grouping sets, ordered-set aggregates, user-defined aggregates, windows,
spilling, arbitrary numeric types, or a plugin registry.

## Admission and resource bounds

HAVING is admitted with shared limits `MAX_HAVING_NODES=256`,
`MAX_HAVING_DEPTH=32`, and `MAX_HAVING_BOOLEAN_TERMS=64`. QuerySpec
deserialization, SQL frontend validation, compiler lowering, and the pure
operator validator use the same limits; recursive lowering is never entered
for an over-limit tree. Aggregate work is bounded before state maps or
extrema multisets are built: touched groups, exact state keys, partition
entries, distinct extrema values, state mutations, and estimated transaction
bytes each have a fixed fail-closed limit shared with Runtime. An over-limit
transition cannot publish a result, continuation, or ACK.

Result schemas reject duplicate or NUL-containing names, names over 63 bytes,
and non-contiguous key ordinals. Typed layouts carry source-derived
nullability, and Materialize admission requires every result field to match
the layout before a graph can be built. The current graph admission accepts
only a linear stateless chain into one Aggregate and exactly one direct
Materialize; aggregate fan-out and Aggregate-to-Aggregate edges are rejected.

## Authority and ownership

The authority chain is singular:

```text
SQL aggregate name
  -> Binder AggregateFunctionV1 selection
  -> canonical QuerySpec AggregateCall
  -> Compiler-validated AggregateDescriptorV1
  -> canonical OperatorGraph + ResultSchemaV1
  -> generic Runtime state/result persistence
```

- Canonical `QuerySpecV1` remains the sole durable declaration authority.
- Canonical `OperatorGraph` remains the sole executable authority. Every
  aggregate descriptor and result schema is covered by its payload and digest.
- PostgreSQL Catalog descriptors remain the source/column/type/ObjectAddress
  fact authority.
- Runtime remains the only aggregate state and result writer. It owns the
  PostgreSQL transaction, locks, generic state reads/writes, continuation and
  commit. It does not match function names or function enum variants.
- `shiba-operator` is the only aggregate dispatch owner. It is database-free,
  performs no SQL or I/O and owns no transaction, clock or global state.
- Compiler consumes descriptors and validates types, state codecs and result
  schema. It does not implement transition semantics.
- Binder alone maps admitted SQL names to the closed ABI. SQL names, aliases
  and raw SQL are never execution or recovery authority.
- Ingress, Bootstrap and Rebuild load the same canonical graph/schema and call
  the same generic Runtime. They do not enumerate functions, infer result
  columns, or rebuild plans from SQL.
- Result Sink validates and persists `ResultDelta` against `ResultSchemaV1`; it
  never computes an aggregate or switches on its function.

No aggregate registry table, effect log, second state store, second
continuation, SQL workflow, compatibility decoder, fallback or dual write is
admitted.

## Stable Aggregate Function ABI

### Call identity

`AggregateCallId` is the stable nonzero, one-based `u16` ordinal of a unique
call in one aggregate node's canonical call list. Its state namespace is
derived exactly from that ordinal. It is assigned by canonical normalized
traversal, not by SQL alias, parser location, hash-map iteration or Runtime
registration order. Namespace `0` is reserved for kernel-owned group
membership, so a group exists independently of whether the plan contains
CountStar. Function-call namespaces start at `1`; Runtime treats every
namespace as opaque. One semantic call key is:

```text
(function ABI version, function tag, canonical bound input expression)
```

Exact duplicate calls in SELECT and HAVING are interned once and reference the
same `AggregateCallId`. Group expressions belong once to the Aggregate node,
not to each call. Aliases do not change call identity, while output aliases are
public ResultSchema field names and therefore do change QuerySpec/schema/graph
bytes. Redundant parentheses and nonsemantic formatting change neither.
Different input ObjectAddresses or function tags must change the compiled
graph digest.

### Closed versioned function set

The initial closed set is `AggregateFunctionV1`:

| Function tag | Input | Output | Output nullable | Empty/all-NULL result | Retraction |
| --- | --- | --- | --- | --- | --- |
| `count_star` | none | `int8` | no | `0` | checked decrement |
| `count_int8` | one nullable `int8` | `int8` | no | `0` | checked non-NULL decrement |
| `sum_int8` | one `int8` | `int8` | yes | typed `NULL` | checked subtract plus non-NULL multiplicity |
| `min_int8` | one `int8` | `int8` | yes | typed `NULL` | candidate multiplicity and ordered successor |
| `max_int8` | one `int8` | `int8` | yes | typed `NULL` | candidate multiplicity and ordered predecessor |

Unknown ABI versions, tags, descriptor fields, input arity/types, output types,
NULL contracts, state codec versions or retraction capabilities fail closed
before state reads. Adding a function changes the ABI version; existing V1
bytes are never reinterpreted.

Each function has exactly one immutable `AggregateDescriptorV1` selected by
its function tag. The descriptor fixes:

- ABI version and function tag;
- exact input arity and accepted `ValueType` values;
- output `ValueType` and nullability;
- empty and all-NULL semantics;
- state codec version, namespaces and key/value layouts;
- whether before-image retraction is required and supported;
- ordered-state read requirements;
- maximum state and output mutations per input effect.

Descriptors are code constants in `shiba-operator`, have unique canonical
payloads/digests, and cannot be supplied or overridden by SQL, Catalog rows or
Runtime configuration. Compiler uses the public descriptor API; it must not
duplicate a match over `AggregateFunctionV1`.

The kernel membership namespace `0` is part of the aggregate-node codec rather
than any function descriptor. It records bounded group existence needed for
empty-group deletion and HAVING transitions. A call descriptor cannot claim or
reinterpret it.

### Database-free lifecycle

The function ABI exposes deterministic, database-free operations equivalent to:

```text
describe(function) -> AggregateDescriptorV1
transition(call, current state view, after value) -> StateDelta
retract(call, current state view, before value) -> StateDelta
finalize(call, next state view) -> TypedValue
```

Names may differ in Rust, but the ownership may not. UPDATE is retract-before
then transition-after in one kernel invocation. DELETE only retracts. INSERT
only transitions. `Absent` is never SQL NULL and always fails closed where a
bound input is required. All counts, sums and multiplicities use checked
arithmetic. Codec corruption, negative multiplicity, unsupported retraction or
overflow aborts the complete PostgreSQL transaction.

`SUM`, `MIN` and `MAX` ignore typed NULL contributions. `COUNT(*)` counts rows,
including rows whose projected values are NULL. An existing group with only
NULL values emits NULL for Sum/Min/Max; a group with zero rows is deleted.

## MIN/MAX multiplicity and bounded ordered reads

Min/Max cannot retain only the current extreme: deleting the last copy of that
value must reveal the next exact candidate without rescanning the source table.
Their state uses the existing graph/node/namespace/partition/item state
authority:

- one candidate item key per distinct non-NULL `int8` value in a group;
- one checked positive multiplicity per candidate;
- zero multiplicity deletes that state item;
- no source-row copy or aggregate-specific table is created.

For a group with `k` distinct candidate values touched by one EffectBatch, the
pure kernel requests the first `k + 1` candidates for Min or last `k + 1` for
Max, plus every touched exact key. Runtime executes these as generic ordered
state read requests, set-wise and in canonical `(node, namespace, partition,
direction, item)` order. Merging that bounded view with the pending deltas is
sufficient: at most `k` old extremes can be removed by those touched keys, and
the extra candidate proves the next survivor. The function never executes SQL;
Runtime never learns whether an ordered read serves Min or Max.

An unbounded group scan, source-table rescan, per-group query or per-row SQL
round trip is forbidden. If the request, multiplicity or byte budget is
exceeded, the graph transaction rolls back and continuation/ACK do not advance.

## Canonical wide results

### ResultSchemaV1

Every terminal owns one canonical `ResultSchemaV1` covered by the OperatorGraph
digest. It contains:

- `format_version = 1`;
- graph/result identity and ordered nonempty fields;
- for each field: nonzero stable field ordinal, canonical public field name,
  `ValueType`, nullable flag and semantic source (`group_key`,
  `aggregate_call`, or admitted computed output);
- ordered key-field ordinals; scalar results use an empty key list;
- maximum row and delta bounds.

The Binder resolves an explicit SQL output alias, or the frozen default naming
rule when absent, into that canonical public field name. The name is durable
public schema semantics and participates in QuerySpec and graph digests; it is
not call identity. Parser spans, PostgreSQL display type names, generated DDL
and maps with unstable ordering are absent. Field ordinals follow normalized
SELECT order. Duplicate presentation of one interned aggregate call is allowed
only as distinct result fields referencing the same call; it does not
duplicate state.

Canonical schema encoding uses explicit tags, unsigned lengths and
domain-separated SHA-256 (`shiba.result.schema.v1\0`). Unknown versions/tags,
duplicate ordinals, invalid key ordinals, empty/oversized fields or a digest
mismatch fail closed.

### TypedResultRowV1 and ResultDelta

`TypedResultRowV1` contains the exact schema digest and exactly one
`TypedValue` per schema field. Its canonical payload uses explicit type and
NULL tags; `Null(Int8)` and `Absent` are distinct, and `Absent` is forbidden in
a materialized row. Encoding is unique and map-free.

Result Sink receives only generic deltas:

```text
ReplaceScalar { result_id, row: TypedResultRowV1 }
UpsertRow      { result_id, key, row: TypedResultRowV1 }
DeleteRow      { result_id, key }
```

The durable row authority is the canonical typed row payload plus its exact
schema digest. A later Catalog cutover may replace the M15 scalar/key/value
columns in place because this clean-room system has no users; it must not keep
an alias, mirror, dual-write or compatibility view. Public readers see only
active rows whose schema digest matches the active graph.

## HAVING and exact output deltas

HAVING is evaluated after all aggregate calls for every touched group finalize
their next values. It uses SQL three-valued logic; only TRUE is visible. The
kernel compares the old finalized row/predicate with the new finalized
row/predicate and emits exactly:

| Old HAVING | New HAVING | Delta |
| --- | --- | --- |
| not TRUE | not TRUE | none |
| not TRUE | TRUE | keyed upsert |
| TRUE | not TRUE | keyed delete |
| TRUE | TRUE, row changed | keyed upsert |
| TRUE | TRUE, row unchanged | none |

Group deletion always retracts a previously visible row. A group-key-changing
UPDATE independently retracts the old group and transitions the new group in
the same Runtime transaction. HAVING does not create state, cannot read public
results, and cannot suppress state maintenance. Its expression may reference
the one group key and interned aggregate calls only; aliases, volatile
functions, subqueries and non-aggregate source columns are rejected.

## Normalization and Compiler rules

Binder maps exact admitted SQL names to `AggregateFunctionV1`, resolves input
columns once, and emits calls rather than complete-query recipes. Compiler:

1. validates each call through its unique descriptor;
2. binds logical columns to exact ObjectAddresses;
3. interns exact duplicate calls;
4. assigns call IDs and node IDs by normalized traversal;
5. builds the result schema and HAVING expression over typed field/call slots;
6. includes calls, descriptor identities, schema payload and schema digest in
   the canonical OperatorGraph.

Whitespace, keyword case and redundant parentheses must not change QuerySpec,
call IDs, node IDs, schema or graph digest. An output alias is public
ResultSchema semantics, so changing it changes QuerySpec/schema/graph digest
without changing the interned call ID. SELECT field order is semantic and also
changes the schema. No commutative expression reordering, constant folding,
implicit cast or PostgreSQL optimizer plan is used as canonicalization
authority.

## Bounds

The following M16 admission limits are frozen before implementation:

- `MAX_AGGREGATE_CALLS = 16` unique calls per aggregate node;
- at most 16 result fields and one terminal result;
- at most one group key and one HAVING expression;
- existing expression limit 256 nodes, depth 32 and 64 boolean terms applies
  across SELECT plus HAVING;
- canonical ResultSchema payload at most 16 KiB;
- canonical TypedResultRow payload at most 4 KiB;
- at most 10,000 changed source rows, 10,000 touched groups and 20,000 emitted
  before/after result row images per graph transaction;
- ordered Min/Max reads are limited to `touched distinct candidates + 1` per
  touched group and share the existing graph-wide byte/memory budget;
- aggregate calls, bounds multiplications and payload lengths use checked
  arithmetic.

No queue, spill file, persistent intermediate delta, per-row SQL or unbounded
state/result vector is admitted. Limit failure is an ordinary fail-closed graph
error: no state/result/continuation commit and no ACK.

Every admission limit is inclusive. The exact limit must be accepted; `limit +
1` must fail before state reads or durable writes, including 17 calls, 17
result fields, a second group key/HAVING expression, expression node/depth/term
overflow, 16 KiB + 1 schema bytes, 4 KiB + 1 row bytes, 10,001 changed rows or
groups, and 20,001 emitted images. Checked arithmetic must reject an overflow
while deriving any composite bound.

## Transaction, locks and recovery

The existing processor-owned transaction and order remain:

```text
graph/generation mutex
-> replay probe
-> SourceId binding locks
-> source-row state locks
-> canonical aggregate state exact/range locks
-> pure transitions, retractions, finalize and HAVING
-> generic state deltas
-> generic wide result deltas
-> continuation last
-> commit
-> ACK
```

All functions in one node consume the same transaction-local DeltaBatch once.
Plan/schema/state decode, ordered read, transition, retraction, finalize,
HAVING, sink, constraint, serialization or backend failure rolls back source
rows, every aggregate call, wide results and continuation together. Retry
starts at the complete source transaction. Exact replay short-circuits before
kernel execution. ACK remains authorized only after durable `Applied`, exact
`AlreadyApplied`, or the existing strictly validated non-data terminal tokens.

Bootstrap uses the same calls/schema over its bounded batches while public
results remain building. Catch-up uses the same Runtime. Rebuild recompiles the
durable QuerySpec against explicit target descriptors and atomically activates
the same target graph/schema; it never reconstructs functions from SQL names.
Unknown ABI/schema/state versions or drift leave the graph building/invalid and
fail forward under the existing lifecycle. No M16 codec is silently upgraded.

## Performance stop lines

M16 implementation must freeze and measure on the existing release host before
observing new results:

- unchanged M15 Count/Sum and projection/Join scenarios may regress by at most
  15% in five-run median;
- one 10,000-change grouped Count+Sum+Min+Max wide-result transaction must use
  set-wise state/result I/O and may take at most 2.0x the same-data M15 grouped
  Count+Sum median;
- adding an aggregate call may add state work proportional to changed rows and
  touched candidates, never total source rows or total groups;
- no SQL statement count may scale with rows, groups or aggregate calls;
- peak RSS above the corresponding M15 workload is limited to 32 MiB and must
  return to a stable plateau under slow Apply;
- M8--M15 transaction, 16 MiB wire, 10,000-change, bootstrap/rebuild, ACK and
  M12 retained-WAL `<= 256 MiB` gates remain unchanged.

Any need to relax these limits, scan an entire source/group, add per-row SQL or
persist an intermediate batch is a stop condition requiring a smaller slice or
new architecture decision.

## Extension and residual boundaries

A future function is added only through a new ABI version, descriptor, pure
reference model, codec/retraction/finalize proof, SQL differential and
PG17/18 lifecycle/rebuild evidence. A future type or result encoding requires a
new `ValueType`/schema version; old bytes remain exact. External/UDAF plugins
require a separate trust, determinism, resource and upgrade model and are not
pre-authorized by this contract.

The M16.7 extensibility acceptance test is exact: adding a function over an
existing `ValueType` may change only its versioned function descriptor and
`shiba-operator` kernel implementation, Binder SQL-name mapping, and shared
tests/fixtures. It must not change Compiler logic (Compiler uses the descriptor
API only), Runtime, Ingress, Bootstrap, Rebuild, Catalog schema or SQL, Result
Sink, continuation, or ACK. A diff outside that allowlist fails the
extensibility gate unless a later ADR explicitly changes the ABI itself.

Future candidates include Avg, VarPop, VarSamp, StddevPop, StddevSamp,
CountDistinct, SumDistinct, BoolAnd, BoolOr, percentile and median. They are not
authorized by M16. Numeric accumulation/division and exact square-root
semantics are prerequisites for Avg/variance/stddev; no implementation may use
truncating `i64` division, binary `f64`, or an order-dependent approximation as
canonical state or result. DISTINCT, percentile and median additionally require
separately bounded exact multiplicity/ordering contracts.

M16.2 implements the canonical wide ResultSchema/TypedResultRow codec, the
single Catalog header/row authority, generic ResultDelta persistence and the
registration/bootstrap/rebuild schema handoff. It deliberately does not yet
implement the Aggregate Function ABI execution path.

At the M16.2 boundary, Aggregate ABI production dispatch, Min/Max ordered state
reads, multi-call SQL lowering, HAVING deltas, and final M16 performance
evidence were still unproved. M16.3--M16.5 subsequently close the first three
items except HAVING and final performance evidence. Also
outside M16 are Avg/VarPop/VarSamp/StddevPop/StddevSamp and Numeric exactness,
Count over non-`int8` expressions, CountDistinct/SumDistinct, BoolAnd/BoolOr,
percentile/median, multi-key grouping, grouping sets, ordered-set aggregates,
windows, outer/three-table joins,
plugins, spill-to-disk, cross-host failover and sustained production soak.

M16.7 must include a static extensibility audit proving aggregate function
names/tags are absent from Runtime, Catalog SQL, Ingress, Bootstrap, Rebuild and
Result Sink production paths; concrete dispatch is confined to
`shiba-operator`. It must also prove one Aggregate node owns group expressions,
namespace `0` is kernel membership, call namespaces are ordinal-derived, and
there is no function registry table, dual codec or compatibility path.

## M16.7 release and extensibility closure

The final M16 gate runs the exact-enrollment release matrix on PostgreSQL
17.10 and 18.4: 57 unique scripts and 114 successful PostgreSQL invocations.
It retains the M15 parser/registration/Apply limits, the 16 MiB and 10,000
change bounds, the one-million-row bootstrap/rebuild limits and the M12
retained-WAL limit of 256 MiB. No threshold is widened for M16.

The static extensibility audit is structural: concrete `AggregateFunctionV1`
dispatch is confined to `shiba-operator` aggregate modules. Runtime, Ingress,
Catalog SQL, Bootstrap, Rebuild and Result Sink do not name a function variant.
A future function using an existing `ValueType` therefore changes only its
versioned descriptor/kernel, Binder name mapping and shared reference fixtures;
it does not change Runtime transaction orchestration, Catalog authority,
lifecycle, Result Sink, continuation or ACK.

M16 is complete for the closed Int8 aggregate subset once that matrix and
audit are green. AVG, variance/stddev, Numeric/Decimal, DISTINCT, outer or
three-table joins, windows, plugins, cross-host failover and long-running
production soak remain outside this milestone.

At M16.3 the production result path is generic and wide and CountStar,
Count(nullable `int8`) and SumInt8 run through the versioned Aggregate ABI.
The specialized M15 node variants are deleted. M16.4 adds multi-call SQL,
M16.5 adds production MinInt8/MaxInt8, and M16.6 adds grouped HAVING without
changing Runtime or Catalog authority.
