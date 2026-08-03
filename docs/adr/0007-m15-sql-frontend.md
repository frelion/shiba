# ADR 0007: bounded SQL frontend over QuerySpec

Status: accepted for M15.1; implemented through the M15.6 two-source SQL vertical.

## Context

M14 proves one canonical OperatorGraph through live ingress, snapshot,
recovery and rebuild, but callers must construct a Rust `GraphSpecV1`. Accepting
raw SQL directly as a durable plan would reintroduce an implicit SQL workflow,
name-based identity and parser-dependent recovery. Executing SQL inside an
operator would also split transaction and writer ownership.

## Decision

QuerySpecV1 is the sole durable declaration authority.
OperatorGraph is the sole Runtime execution authority.
SQL text is non-authoritative provenance.

Add a bounded, database-independent SQL parser/normalizer that emits strict
canonical `QuerySpecV1`. A short PostgreSQL adapter binds registered sources and
logical columns to exact ObjectAddresses, compiles the existing OperatorGraph,
and atomically registers declaration, graph, membership and result contracts.

Raw SQL and parser AST are ephemeral. QuerySpec is the declarative authority;
the bound OperatorGraph is the execution authority. Runtime, Operator, Ingress,
Bootstrap and Rebuild receive no SQL or parser type. Rebuild rebinds the durable
QuerySpec only against explicit target descriptors before destructive prepare.

Use `sqlparser` 0.62.0 with `PostgreSqlDialect` as the candidate, minimized
default features and its recursion limit plus Shiba's stricter byte/token/AST/
depth bounds. `Spanned`/`TokenWithSpan` locations are normalized to Shiba-owned
UTF-8 byte spans. The dialect is approximate; neither its AST nor PostgreSQL's
server parser/optimizer tree is authority. Unsupported syntax fails closed;
there is no fallback parser or database execution path.

The exact admitted language, identifier/NULL/type semantics, canonical digest,
deterministic NodeIds, registration lock order, stable errors, performance
limits and exclusions are normative in
[SQL_FRONTEND_CONTRACT.md](../SQL_FRONTEND_CONTRACT.md).

## Consequences

Users gain a familiar narrow declaration surface without changing Runtime
transactions, ACK, graph state/results, bootstrap or rebuild authority. Names
are used once for binding/rebinding, never execution identity. This intentionally
rejects much valid PostgreSQL SQL; extending the subset requires a separate
vertical slice with pure semantics, PG17/18 oracle evidence and unchanged
authority boundaries.

M15.4 validates the decision with a pure Binder and a separate short-lived
PostgreSQL registration adapter. The directed PG17.10/PG18.4 vertical preserves
the existing QuerySpec/OperatorGraph authority through registration, bootstrap,
live ACK/replay and changed-ObjectAddress rebuild. This is not evidence for the
remaining Join SQL shape, least privilege, frontend performance or the final
complete release matrix.

M15.5 validates that this boundary is not limited to projection/filter/compute.
The same pure Binder lowers scalar Count, nullable scalar Sum, filtered grouped
Count and grouped Sum into generic QuerySpec/OperatorGraph nodes. Scalar Sum's
checked value and non-NULL input count are two StateKeys under the sole
`graph_node_state` authority. Explicit scalar output nullability lets the same
Runtime Result Sink publish typed NULL for empty/all-NULL input. No aggregate
SQL workflow, state table, Runtime branch or writer is introduced.

Failure-first tests corrected two independent semantic gaps: a single numeric
sum state could not distinguish zero from no non-NULL input, and the generic
scalar sink/catalog required every active scalar to contain a non-NULL bigint.
Directed PG17.10/PG18.4 production lifecycle tests now prove bootstrap,
catch-up, live Apply/ACK, rollback/retry/replay and aggregate rebuild against
SQL oracles. At the M15.5 boundary they did not prove Join SQL or the final
least-privilege/performance/release gates; M15.6 closes the directed Join and
split-role portion below, while M15.7 retains performance and final release.

M15.6 validates the two-source edge using the existing M14 InnerJoin rather
than a complete-query recipe. QuerySpec membership remains sorted by SourceId,
while explicit node inputs preserve semantic SQL left/right order. The Binder
resolves four exact columns and requires the right equality column to match the
effective non-null bigint PK/UK identity. Since the M14 node already emits
`(left.id, right.payload)`, its result flows directly to the Compiler's generic
Materialize terminal; a redundant identity Project would add no semantics.

Directed PG17.10/PG18.4 evidence proves split-role atomic registration, one
publication/slot/generation/snapshot/continuation, both-source Apply and ACK,
Apply-before-ACK replay, exact right-PK replacement invalidation and graph-wide
changed-ObjectAddress rebuild. No Runtime, Ingress, Bootstrap, Rebuild,
continuation or ACK authority changes, and no legacy implementation is reused.
M15.7 still owns frozen frontend/registration performance and the complete
release matrix.

## Bounds

The frontend is limited to 64 KiB SQL, 4,096 tokens, 2,048 AST nodes, depth 32,
256 expression nodes, two sources, one join and one terminal. The normative
resource and performance limits are in `SQL_FRONTEND_CONTRACT.md` and cannot be
relaxed after measurement.

## Rejected alternatives

- Store raw SQL and reparse on recovery: parser/version/name drift changes the
  durable plan.
- Delegate parsing or execution to PostgreSQL: couples Compiler/Operator to SQL
  workflow and cannot yield the required canonical database-independent IR.
- Preserve both GraphSpec and QuerySpec: creates dual declaration authorities
  and recovery ambiguity.
- Hand-write a permissive SQL parser or accept parser AST serialization: adds
  grammar/codec authority that M15 cannot independently stabilize.
- Resolve unqualified tables through session `search_path`: makes registration
  depend on ambient mutable state.
