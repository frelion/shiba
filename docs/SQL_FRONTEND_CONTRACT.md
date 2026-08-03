# M15 SQL frontend and QuerySpec contract

## Scope

M15 adds one bounded SQL declaration frontend to the M14 Operator Graph. It is
not a SQL execution engine. Parsing and normalization are database-independent;
binding is a short PostgreSQL catalog transaction; execution remains the sole
generic graph Runtime. Operator and Compiler crates do not execute SQL, access
PostgreSQL, own transactions, or learn parser AST types.

The flow is:

```text
SQL text -> bounded parser -> ephemeral UnboundQuery
         -> catalog binder -> canonical QuerySpecV1
         -> ObjectAddress-bound OperatorGraph -> atomic registration
         -> existing bootstrap/live/rebuild/Runtime/Result Sink
```

Raw SQL and parser ASTs are ephemeral input. They are never execution,
recovery, identity, or replay authority.

## Authority and writers

QuerySpecV1 is the sole durable declaration authority.
OperatorGraph is the sole Runtime execution authority.
SQL text is non-authoritative provenance. Every public diagnostic has a stable byte span.

- Canonical `QuerySpecV1` is the sole durable declarative query authority. It
  records normalized semantics, explicit SourceIds and logical column
  identifiers, never raw SQL, session `search_path`, relation OIDs guessed from
  names, or parser-specific nodes.
- The canonical `OperatorGraph` payload/digest is the sole bound execution
  authority. Its inputs are exact source/column/index ObjectAddresses.
- The registration adapter is the only QuerySpec/graph definition writer.
  Runtime remains the only node-state/result writer. Rebuild remains the only
  forward generation-transition writer.
- QuerySpec and OperatorGraph are different stages of one authority chain, not
  competing plans: declaration is used for explicit target rebind; only the
  exact bound graph is executable.
- SQL frontend state, diagnostics and spans are never durable Catalog state.
  No SQL workflow, prepared-query cache, fallback parser, compatibility path,
  effect log, second continuation, or second Runtime is introduced.

`graph_definition.spec_payload` stores canonical QuerySpec bytes after the M15
cutover. `graph_payload` and `graph_digest` retain the compiled execution
identity. The clean-room project has no external users, so the old GraphSpec
declaration is replaced rather than accepted through an adapter or dual format.

## QuerySpecV1

The public database-independent IR has a fixed `version = 1`, nonzero
`GraphId`, one or two ordered SourceIds, generic nodes/edges and a bounded
nonempty result set. Direct declarations preserve M14's proven atomic
multi-terminal graphs; this SQL frontend always emits exactly one terminal.
Deserialization denies unknown fields and trailing bytes.

QuerySpec retains identifier spelling plus quoted/unquoted status only where
rebinding needs a logical column. Sources are already SourceIds. It contains no
schema/table lookup name, PostgreSQL OID, parser enum, generated SQL, implicit
cast, session setting, or NodeId supplied by a caller.

Canonical JSON uses fixed struct field order, enum tags, decimal `i64` values,
no insignificant whitespace and no map whose order can vary. The QuerySpec
digest is SHA-256 over a distinct domain prefix, format version and exact
canonical bytes. The OperatorGraph digest remains independently domain-separated
and includes bound ObjectAddresses. Unknown versions, noncanonical payloads and
digest mismatch fail closed.

NodeIds are assigned by the Compiler, never parsed from SQL. Assignment is a
deterministic pre-order over normalized relational stages, followed by terminal
Materialize nodes, using consecutive nonzero IDs. Equivalent accepted SQL must
produce the same QuerySpec, NodeIds, graph payload and digests. A semantic
change must change QuerySpec bytes or fail registration.

## Accepted SQL

Exactly one read-only `SELECT` statement and no trailing statement is accepted.
Relations are ordinary registered tables in the current database and must be
written as `schema.table`; session `search_path` is never consulted. M15 admits:

- one-source `COUNT(*)`;
- one-source `SUM(bigint_column)` with PostgreSQL SQL semantics: NULL inputs are
  ignored and the scalar result is typed NULL for empty or all-NULL input;
- keyed projection of a non-null bigint key and nullable bigint value;
- keyed projection whose value is a checked bigint expression;
- optional `WHERE` using the expressions below;
- `GROUP BY` one bigint/nullable-bigint key with `COUNT(*)` or
  `SUM(bigint_column)`; an existing all-NULL group materializes typed NULL;
- exactly one two-table `INNER JOIN` on equality between a nullable bigint left
  foreign key and the right table's exact non-null bigint effective PK/UK,
  projecting left identity and nullable right payload.

Accepted scalar expressions are column reference, signed bigint literal, typed
`NULL`, checked `+`/`-`, bigint/boolean comparisons, `IS NULL`, `IS NOT NULL`,
`AND`, `OR`, `NOT`, and parentheses. SQL three-valued logic is preserved;
`WHERE` keeps only `TRUE`. No implicit text/numeric/boolean cast exists.
Arithmetic overflow is a Runtime error and rolls back the complete graph
transaction.

## Identifiers and aliases

Unquoted identifiers are limited to ASCII PostgreSQL-style
`[A-Za-z_][A-Za-z0-9_$]*`, folded to lowercase, and at most 63 UTF-8 bytes.
Quoted identifiers preserve exact UTF-8 spelling, allow PostgreSQL escaped
double quotes, reject NUL, and are also limited to 63 bytes. This avoids
inventing a partial Unicode case-folding rule.

A one-source query may use an optional relation alias and may omit qualification
only when a column is unambiguous. A JOIN requires two distinct explicit
relation aliases and every column reference must be alias-qualified. Duplicate
aliases after normalization, alias/name ambiguity, unknown or duplicate
columns, and references outside the declared sources fail closed. Column
aliases are accepted, identifier-validated and returned only as ephemeral
presentation metadata. They are discarded before QuerySpec canonicalization,
cannot change semantic digest/NodeId/result identity and are not durable result
labels. Duplicate presentation aliases fail closed. Ordinal references,
`USING`, `NATURAL`, lateral correlation and correlated subqueries are not
accepted in M15; result identity is the canonical terminal ordinal/NodeId, not
display text.

## Explicit rejections

M15 rejects mutation/DDL, multiple statements, CTEs, subqueries, set operations,
`SELECT *` except the token inside `COUNT(*)`, `DISTINCT`, `DISTINCT ON`,
`ORDER BY`, `LIMIT/OFFSET/FETCH`, windows, `HAVING`, grouping sets, more than
one group key, outer/cross/three-table joins, non-equality joins, functions other
than the exact aggregate forms above, parameters, casts, collations, arrays,
JSON, text operations, floating/decimal values, `IN`, `BETWEEN`, `LIKE`, CASE,
UDFs, volatile expressions and system columns.

Comments may be lexed but are discarded and cannot affect canonical semantics.
Hints and parser extensions are rejected. Unsupported syntax never falls back
to PostgreSQL execution or another parser.

## Parser selection

M15.3 implements the parser with exactly `sqlparser` 0.62.0
`PostgreSqlDialect`, default features disabled and only `std`. The resolved
dependency graph has no C-toolchain, PostgreSQL library or Runtime dependency.
Its parser recursion limit is enabled in addition to, not instead of, Shiba's
input/token/AST/expression-depth bounds. Shiba normalizes parser locations into
its own stable half-open UTF-8 byte spans; parser/library locations and messages
are not the public error contract.

The candidate must demonstrate that it can:

- parse PostgreSQL lexical rules for the accepted subset and expose reliable
  half-open UTF-8 byte spans;
- allow hard input/token/AST/depth limits before unbounded allocation or
  recursion;
- be deterministic, thread-safe, non-executing and independent of a live
  database;
- have an acceptable license, MSRV/build footprint, active maintenance and no
  required C toolchain or runtime PostgreSQL library;
- permit an explicit AST allowlist so unsupported syntax is rejected rather
  than normalized approximately.

`PostgreSqlDialect` is an approximation of a PostgreSQL grammar, not PostgreSQL
server-parser or optimizer authority. Shiba owns the strict AST allowlist and
rejects every unrecognized construct itself. The M15.3 pure evidence proves the
frozen span normalization and admission bounds without hand-writing PostgreSQL
wire/grammar support, shelling out to `psql`, or adding a second parser.

## Binding, registration and DDL races

Parsing/normalization holds no database lock. Binding and registration use one
short adapter-owned PostgreSQL transaction:

1. reject an existing GraphId; resolve every schema-qualified relation to
   exactly one already registered SourceId;
2. sort SourceIds and acquire the existing graph/source ownership and binding
   locks in canonical order;
3. reject source/graph/publication invalidation and require one database;
4. hold `AccessShareLock` on every exact relation and effective identity index;
5. read column names/types/nullability and exact ObjectAddresses from live
   catalogs, matching the durable source bindings;
6. bind QuerySpec names once, compile and canonicalize the OperatorGraph;
7. revalidate graph/source membership and invalidation;
8. atomically write canonical QuerySpec, graph, membership and result contracts,
   then commit or roll everything back.

Conflicting DDL waits for the binding transaction or commits first and is
observed as identity/invalidation drift. There is no unlocked name-to-OID gap.
Same-name drop/recreate, column replacement or identity-index replacement fails
closed. Registration never auto-registers a table, creates a publication/slot,
overwrites a GraphId, guesses SourceId, or executes the submitted query.

## Rebuild rebind

Rebuild loads and validates the canonical durable QuerySpec; it never reparses
raw SQL. Before destructive prepare, it binds that declaration only against the
explicit ordered target SourceIds/descriptors/identity indexes supplied by the
rebuild request. Logical column identifiers must resolve exactly once with the
required type/nullability; the new OperatorGraph contains only target
ObjectAddresses and a new digest.

QuerySpec deliberately does not persist a current or target identity-index OID.
That OID belongs to the generation-specific bound graph/source authority. The
Compiler must bind and validate the exact effective identity from each explicit
target descriptor during every registration or rebuild generation.

Any missing/duplicate/renamed/incompatible column, source-order drift, result
contract drift or canonical mismatch fails before the destructive boundary.
After prepare, recovery reads the target QuerySpec and bound graph from Catalog;
it cannot re-resolve a current same-name relation, revert to the old graph, or
accept caller SQL as replacement authority.

## Stable errors and spans

Frontend errors expose a closed stable code and one primary half-open UTF-8 byte
span into the original SQL. Line/column is derived for display and is not the
identity. Parser/library text may appear only as unstable debug context.

The initial codes are `input_too_large`, `token_limit`, `parse_error`,
`multiple_statements`, `unsupported_syntax`, `invalid_identifier`,
`duplicate_alias`, `ambiguous_column`, `unknown_relation`, `unknown_column`,
`source_not_registered`, `type_mismatch`, `identity_mismatch`, `query_too_complex`,
`ddl_drift`, `graph_conflict`, `canonicalization_failed`, and
`registration_failed`. Codes are never reused for a different class. Binding
errors point to the originating relation/column; generated nodes have no fake
SQL span.

## Resource and performance bounds

Admission limits are frozen before implementation:

- SQL input: 64 KiB; tokens: 4,096; AST nodes: 2,048;
- sources: 1 or 2; SELECT items: at most 2; JOINs: at most 1;
- expression depth: 32; expression nodes: 256; boolean terms: 64;
- canonical QuerySpec: 256 KiB; compiled graph and Runtime use all existing M14
  node/delta/memory bounds.

Limit accounting is checked with overflow-safe arithmetic. Rejection is linear
in consumed input and never creates Catalog state. No query cache, background
worker or unbounded diagnostic list is admitted.

M15.3 failure-first testing found that allowing a 4,096-token left-deep input to
reach parser AST construction could overflow the stack during recursive AST
destruction even if later lowering rejected it. A conservative 2,048-token
structural ceiling now rejects that shape before AST construction. Publicly
constructible UnboundQuery values are validated and canonically encoded with an
iterative bounded walk; invalid cardinality, source slots, identifiers, depth or
node counts return stable errors instead of saturating lengths or recursing.

On the frozen same machine, 10,000 representative accepted queries in a release
build must parse+normalize with median at most 1 ms and p95 at most 5 ms per
query; one maximum-size admitted input must finish within 20 ms and 4 MiB
frontend peak heap. Registration adds at most 25 ms p95 excluding PostgreSQL
lock wait. Live Apply contains no parser call, so all M14 Runtime/bootstrap/
rebuild/WAL/RSS thresholds must remain unchanged. Thresholds cannot be relaxed
after results are observed.

## Test matrix

The 22 M15.3 pure tests cover the frozen parser allowlist and explicit rejection
families, quoted/unquoted identifiers, aliases, precedence, NULL and arithmetic
syntax, canonical equivalence/difference and digest golden, stable codes,
direct exact UTF-8 spans, hard-limit boundaries and malformed/no-panic input.
They also prove iterative fail-closed canonical validation for public
UnboundQuery values. M15.4 separately proves type checking and deterministic
QuerySpec lowering in the pure Binder; these are not retroactively parser
evidence.

M15.4's independent control-plane and PG evidence covers the first
single-source projection/filter/compute shape. On PG17.10 and PG18.4,
`scripts/test-m15-sql-vertical.sh` proves the quoted cross-schema query
`SELECT e."Id", e."Payload" + 1 ... WHERE e."Payload" > 0` through exact
SourceId/ObjectAddress binding, registration rollback/success, deterministic
DDL-first drift rejection, bootstrap and concurrent WAL catch-up, live
Apply/ACK, exact replay, complete key/value SQL differential, and rebuild to a
changed target ObjectAddress followed by live ACK. Raw SQL/parser AST are not
persisted and Runtime/Ingress remain parser-free.

M15.5 directed tests prove scalar Count, PostgreSQL-compatible nullable scalar
Sum, filtered grouped Count and grouped Sum on both PG17.10 and PG18.4. The
Binder emits only generic QuerySpec nodes and output contracts. Scalar Sum's
checked value and non-NULL input count occupy two StateKeys in the same
`graph_node_state` authority; the explicit nullable scalar contract lets the
generic sink publish typed NULL for empty/all-NULL input. There is no aggregate
registration workflow or second state/result writer.

`scripts/test-m15-sql-aggregates.sh` takes all four graphs through production
registration, bootstrap/catch-up, live receiver Apply/ACK and full SQL oracles.
It also proves predicate/group transitions, deleting the last non-NULL scalar
input, overflow rollback with no continuation/feedback advance, retry/replay,
and changed-ObjectAddress grouped-SUM rebuild. Failure-first tests fixed the
sum-state zero/empty ambiguity and the generic scalar sink/catalog's previous
non-NULL-only assumption. M15.6 subsequently closes the admitted cross-schema
Join SQL and directed least-privilege registration boundary below. M15.7 closes
the remaining frozen negative/static and release evidence.

M15.6 lowers the exact two-source SQL shape to one generic `InnerJoin` QuerySpec
node and one keyed result. QuerySpec source membership is canonical SourceId
order; explicit node inputs preserve semantic SQL left/right order. The Binder
resolves the left identity/key and right identity/payload against their exact
descriptors and requires the right equality column to be the source's effective
non-null bigint PK/UK. Names disappear after compilation.

The existing M14 InnerJoin output is already `(left.id, right.payload)`, exactly
the admitted SQL projection. The result can therefore reference InnerJoin
directly and receive the Compiler's standard Materialize terminal. Inserting a
no-op Project would not make the declaration more generic. No Join recipe,
special registration function, Runtime branch or SQL workflow exists.

On PG17.10 and PG18.4, `scripts/test-m15-sql-join.sh` proves split-role atomic
registration, one publication/slot/generation and exported snapshot for both
members, catch-up/live Apply/ACK, one transaction changing both tables,
Apply-before-ACK replay, full keyed SQL oracle, right effective-PK replacement
invalidation and graph-wide changed-ObjectAddress rebuild. Missing registration
and bootstrap writer privileges leave no partial authority. This directed gate
is re-enrolled by the M15.7 complete release runner.

The final release runner retains exact script enrollment, every M1--M14 gate,
the frozen parser/registration thresholds and a static scan proving SQL/parser
types do not enter Operator, Runtime, Ingress or durable graph payloads. It
passed 56 scripts on both PG17.10 and PG18.4, for 112 PostgreSQL invocations.
Frontend median/p95 latency was 6.833/11.958 us and 8.125/13.792 us; the 64 KiB
case took 170.959/216.500 us. Registration median/p95 was
1.546625/1.623708 ms and 1.687958/1.782958 ms. Modeled frontend heap was
3,342,336 bytes; observed frontend/registration RSS deltas were 96/112 KiB on
PG17 and 80/64 KiB on PG18. All values remain within the predeclared limits.

## Non-goals and unproved boundary

M15 does not implement general PostgreSQL SQL, mutation/DDL, arbitrary
expressions/functions, implicit casts, durable output naming, SQL result execution,
outer/three-table joins, windows, DISTINCT, Min/Max/Avg, HAVING, subqueries,
parameters, prepared statements, views, permissions inferred from SQL,
scheduler, plugins, remote databases or cross-host failover. M15.1 freezes this
contract. M15.2 completed the generic QuerySpec cutover, M15.3 completed the
pure SQL-to-UnboundQuery parser, and M15.4 completed the Binder/registration and
first single-source production SQL vertical. M15.5 completed the admitted
single-source aggregate SQL verticals with nullable scalar output semantics.
M15.6 completed the admitted two-table Join SQL and its directed split-role
lifecycle. M15.7 completed the frozen performance, negative/static and full
release gates. M15 is complete at this explicitly bounded frontend scope, not
as general PostgreSQL SQL and not as the whole V2.
