# M15 QuerySpecV1 contract

## M16.3 generic Aggregate declaration

The current canonical declaration is QuerySpec format 2. One generic
`Aggregate` operation owns zero or one group expression and one or more ordered
`AggregateCall` values. Each call includes canonical ordinal, function semantic
version, closed function tag and optional bound expression. Compiler validates
arity and types through `shiba-operator`'s sole descriptor API and emits
OperatorGraph format 2; it does not duplicate function semantics. Compiler
version 4 is the only accepted Catalog writer format. Prior QuerySpec/graph
formats are historical evidence and fail closed without a compatibility path.

## M16.2 result declaration cutover

M16.1 freezes a strict QuerySpec extension in which each normalized
aggregate expression has one deterministic `AggregateCallId` and one closed,
versioned function descriptor. Repeated semantically identical calls are
interned once; result fields and HAVING refer to call identity, never to SQL
text, aliases or runtime recipes. Compiler resolves this declaration into the
same sole OperatorGraph authority and canonical `ResultSchemaV1`.

M16.2 implements its result declaration half: each terminal owns an ordered
list of named fields with value slots/nullability and canonical 1-based key
ordinals. Empty key ordinals denote the scalar singleton. SQL aliases are
durable public schema names and therefore change QuerySpec/schema/graph digest;
formatting and spans do not. Compiler emits one canonical `ResultSchemaV1` and
generic Materialize field-slot list. The old Scalar/Keyed result-shape enum is
deleted with no parallel decoder, adapter or dual format. AggregateCall
declarations and Count/CountStar/Sum compilation are implemented by M16.3;
multi-call SQL, MIN/MAX and restricted grouped HAVING are now implemented by
M16.4/M16.5/M16.6; scalar HAVING remains rejected.
the next slice.

That result-only cutover was stored by compiler version `3`. M16.3's breaking
Aggregate declaration uses compiler version `4`, QuerySpec format `2` and
OperatorGraph format `2`; earlier versions remain historical evidence, not
accepted current decoders.

## M15 declaration cutover and completion status

M15.2 replaced the complete-query `GraphSpecV1`/
`GraphOutputSpecV1` recipes with this generic QuerySpec. Compiler version 2
decodes strict QuerySpec nodes/results, resolves source-name selectors once,
builds the existing OperatorGraph, and stores canonical QuerySpec as
`graph_definition.spec_payload`. Registration and rebuild both use the same
declaration and compiler path; no compatibility decoder or dual format remains.

Acceptance evidence is complete: ten pure Compiler tests cover strict
codec/bounds/digest/topology and generic M14-shape compilation; the full
PG17.10/PG18.4 release runner passed 52 scripts and 104 invocations, including
Runtime registration, bootstrap/rebuild/recovery and Join lifecycle. M15.3 now
provides a bounded pure SQL-to-`UnboundQuery` parser. M15.4 adds the pure Binder
and the separate SQL registration control plane for the first single-source
projection/filter/compute vertical. SQL text and UnboundQuery remain ephemeral
and are not accepted by Runtime; only the resulting canonical QuerySpec enters
the existing registration writer.

## Authority

QuerySpecV1 is the sole durable declaration authority.
OperatorGraph is the sole Runtime execution authority.
SQL text is non-authoritative provenance.
`graph_definition.spec_payload` contains only exact canonical QuerySpecV1 bytes;
`graph_payload` and `graph_digest` contain the only executable bound graph.

In M15.2 `GraphOutputSpecV1` is deleted together with its complete-query recipe
variants and decoder. There is no adapter, fallback, compatibility alias, dual write or
second compiler path. Old recipe payloads fail closed after the cutover.

## Envelope and canonical encoding

`QuerySpecV1` has version 1, a nonzero GraphId, one or two ordered SourceIds,
topologically ordered generic nodes and a bounded nonempty result set. Strict decoding
denies unknown fields, versions and trailing bytes. It rejects zero, duplicate
or unordered identities and noncanonical payloads.

Canonical JSON uses fixed field order, tagged enums, decimal `i64`, no unstable
map order and no insignificant whitespace. Its SHA-256 digest is domain-
separated from OperatorGraph. Formatting, comments, keyword case, redundant
parentheses, relation aliases and column aliases cannot change canonical bytes.

## Generic nodes and edges

Each node has ordered inputs from a SourceId or earlier NodeId and one operation:
Filter, Project, Compute, KeyBy, Count, Sum, InnerJoin or Materialize. Count,
Sum and grouped forms are assembled from nodes and edges. There are no complete-
query `FilteredGroupedSum`, `ComputedProject`, `CountRowsSQL` or Join recipes.
Each declared result points to one Materialize terminal. M15 SQL lowering emits
exactly one; direct declarations retain bounded multiple terminals solely to
preserve M14's already-proved atomic Count/Sum/Project graph contract.

The Compiler assigns consecutive nonzero NodeIds by deterministic canonical
pre-order. SQL, aliases and callers never supply them. Cycles, forward or
disconnected references, duplicate terminals and wrong arity fail closed.

## Expressions and types

Expressions are column reference, bigint literal, typed NULL, checked bigint
addition/subtraction, comparisons, `IS NULL`, `IS NOT NULL`, and three-valued
`AND`/`OR`/`NOT`. Source-edge references retain normalized logical identifier
and quoted status for bind/rebind; node-edge expressions use typed slots.

No expression contains ObjectAddress, parser AST, SQL text, implicit cast,
function name or executable code. Absent is an internal delta condition, never
SQL NULL. Bare `SUM(bigint)` returns typed NULL for empty or all-NULL input;
Count returns non-null zero. Overflow rolls back the complete Runtime transaction.

## Binding and rebuild

Registration resolves logical sources and columns once against registered
SourceIds and locked PostgreSQL descriptors. The Compiler embeds exact relation,
column and effective identity-index ObjectAddresses only in OperatorGraph.
QuerySpec does not persist a generation-specific index OID.

Rebuild validates durable canonical QuerySpec and recompiles against explicit
target descriptors before destructive prepare. It never reparses SQL, uses
caller SQL, copies the old graph or resolves `search_path`. A valid new target
ObjectAddress yields a new graph/digest under the same logical QuerySpec;
missing, ambiguous or incompatible bindings fail before prepare.

Runtime, Ingress, Bootstrap, Operator and Result Sink do not interpret QuerySpec
operations, identifiers or parser types. They load only OperatorGraph and its
result contracts.

## Result contract

The terminal is either scalar bigint with explicit nullability or keyed bigint
to nullable-bigint rows. Column aliases are ephemeral presentation metadata,
not durable result identity, and cannot affect canonical QuerySpec. Contract,
type or nullability drift fails registration or rebuild atomically.

## Errors

Codec/version/canonical failures, topology/type failures and PostgreSQL binding
failures remain distinct. None falls back to GraphOutputSpecV1 or SQL. Frontend
diagnostics retain a stable byte span only outside durable QuerySpec.

## Bounds

- one or two sources, at most 32 total nodes/results and at least one result;
- at most 256 expression nodes, depth 32 and 64 boolean terms;
- at most two projected items and a 256 KiB canonical payload;
- all existing M14 graph/delta/fan-out/memory limits remain unchanged.

Validation is linear or `O(n log n)` for bounded sorting with checked counters.
No declaration node can create unbounded expansion.

## M15.2 evidence

Ten pure Compiler tests prove strict roundtrip, digest separation, semantic equivalence,
deterministic NodeIds, every M14 shape through generic nodes, corrupt input
rejection and graph behavior equivalence. PG17.10/PG18.4 M14 grouped, graph
Runtime and Join lifecycle gates passed within the complete 52-script,
104-invocation release matrix. Failure-first migration also preserved the exact
wrong-column catalog coordinate and corrected stale test-only node/result IDs;
no production fallback or alternate declaration path was introduced.

## M15.4 binding and lifecycle evidence

The pure Binder accepts an exact ordered `ResolvedSource` containing the live
`SourceDescriptor` and effective `IdentityIndexDescriptor`. It type-checks the
first admitted SQL shape, assigns deterministic generic nodes, and emits the
same strict QuerySpec contract; it performs no PostgreSQL access and owns no
transaction. `shiba-sql-registration` is the only control-plane bridge from
parser output to PostgreSQL. Parse completes before its short transaction; the
transaction resolves the exact schema/relation, requires one registered
SourceId, locks bindings in canonical order, takes/revalidates the relation and
ObjectAddresses, rejects invalidation, invokes the pure Binder, and calls
Runtime's transaction-local registration writer. Commit or rollback covers the
QuerySpec, OperatorGraph, membership and result contracts together.

On both PG17.10 and PG18.4, `scripts/test-m15-sql-vertical.sh` proves the quoted
cross-schema query `SELECT e."Id", e."Payload" + 1 ... WHERE e."Payload" > 0`
through registration rollback/success, exact bound graph persistence,
bootstrap/catch-up, live Apply/ACK, `AlreadyApplied` replay, complete keyed SQL
oracle, and an explicit target relation/index/publication with changed
ObjectAddresses rebuilt from the durable QuerySpec. A DDL-first column
replacement creates an observed lock wait and returns stable `ddl_drift`
without partial graph rows. The script is enrolled as release gate 53; a full
53-script run on both versions is not claimed as M15.4 evidence.

## M15.5 aggregate binding and lifecycle evidence

The pure Binder now lowers the four admitted single-source aggregate families
without adding a recipe or Runtime API: scalar `COUNT(*)`, scalar
`SUM(bigint)`, filtered grouped count and grouped sum. Column names are resolved
once to exact SourceDescriptor/ObjectAddress coordinates; canonical QuerySpec
stores slots and typed expressions, never names as execution identity. Aliases
and redundant parentheses normalize to the same canonical declaration.

Scalar Count has a non-nullable bigint result contract. Scalar Sum has a
nullable bigint result contract: empty input and all-NULL input produce typed
NULL, while NULL rows contribute nothing to arithmetic. The Operator kernel
keeps the checked sum and non-NULL input count as two generic StateKeys under
the existing `graph_node_state` authority. QuerySpec describes output
nullability; it does not expose those private state keys or create a second
state authority. Grouped Sum retains an existing group with typed NULL while
all values are NULL and retracts the result only when the group becomes empty.

Pure Binder and kernel tests cover canonical lowering, wrong/missing/duplicate
and non-bigint binding, NULL/state corruption and overflow. On PG17.10 and
PG18.4, `scripts/test-m15-sql-aggregates.sh` proves all four declarations
through the production registration/bootstrap/catch-up/live receiver/Apply/ACK
path against complete SQL oracles. It additionally proves overflow rollback,
retry and exact replay, plus changed-ObjectAddress grouped-SUM rebuild and
post-activation live ACK. Failure-first evidence corrected two independent
assumptions: sum-only state could not distinguish numeric zero from no non-NULL
input, and the generic scalar sink/catalog rejected typed NULL active results.
Both fixes remain inside existing plan/state/result authorities.

This is directed two-version M15.5 evidence, not by itself the complete release
matrix. M15.6 subsequently closes the admitted two-table Join SQL and directed
least-privilege lifecycle boundary below; M15.7 closes frozen frontend and
registration performance.

## M15.6 two-source Join binding and lifecycle evidence

For the exact admitted SQL shape, the Binder resolves two ordered
`ResolvedSource` values, the left projected identity, left equality key, right
equality identity and right payload. `QuerySpecV1.sources` remains sorted by
SourceId as required by its canonical envelope; the InnerJoin node's two
explicit Source inputs and field input ordinals preserve SQL left/right
semantics independently of numeric SourceId order. Pure tests use left
SourceId 20 and right SourceId 10 to prove that distinction.

The resulting declaration has one generic stateful `InnerJoin` node and one
keyed result referencing it. M14's established InnerJoin output layout is
already `[left.id, right.payload]`, so Compiler adds its ordinary terminal
Materialize directly. A no-op Project would only repeat those slots. This is a
canonical topology choice using existing generic operations, not a complete-
query recipe or alternate compiler/runtime path. The exact effective right
PK/UK ObjectAddress remains a generation-specific compiled binding, not a name
or QuerySpec identity.

Pure Binder tests prove reversed equality/alias canonical equivalence, reverse
SourceId order, exact fields and right identity, wrong/missing/type/identity
rejection and generic Compiler acceptance. On PG17.10 and PG18.4,
`scripts/test-m15-sql-join.sh` proves atomic least-privilege SQL registration,
one exported snapshot over both members, catch-up/activation, one both-source
transaction through live Apply/ACK, Apply-before-ACK `AlreadyApplied`, complete
keyed SQL oracle, exact right-PK replacement invalidation with no durable/ACK
advance, and changed-ObjectAddress whole-graph rebuild with post-cutover live
ACK. No production authority, transaction, continuation or ACK rule changes.

M15.7 re-enrolls this directed evidence in the final 56-script/112-invocation
PG17.10/PG18.4 release matrix. The same run closes the frozen frontend and
registration performance gates: frontend median/p95 6.833/11.958 us on PG17
and 8.125/13.792 us on PG18; registration median/p95
1.546625/1.623708 ms and 1.687958/1.782958 ms. QuerySpec remains the only
durable declaration and no SQL text, parser AST or complete-query recipe is
introduced. M15 is complete at this contract's bounded query shapes, not at a
general SQL or complete-V2 boundary.
