# M15 QuerySpecV1 contract

## M15.2 cutover and M15.4 binding status

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
