# ADR 0007: bounded SQL frontend over QuerySpec

Status: accepted for M15.1.

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
