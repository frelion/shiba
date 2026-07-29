# SQL contract

Shiba registers a continuously maintained result with PostgreSQL's
`CREATE TABLE ... AS SELECT` grammar:

```sql
CREATE TABLE shiba.order_stats AS
SELECT product_id,
       count(*) AS order_count,
       sum(amount) AS total_amount
FROM public.orders
GROUP BY product_id;
```

The declaration creates an ordinary result table in the reserved `shiba`
schema and stores one `DataflowPlan`. Initial source rows and later WAL changes
travel through the same operators. Maintenance is asynchronous: CTAS may
return before its bootstrap rows have reached the Sink.

## Database setup

```sql
CREATE EXTENSION shiba;
SELECT shiba.activate();
```

`shiba.activate()` creates the database's logical slot, publication, and one
Shiba Runtime. PostgreSQL does not allow slot creation inside
`CREATE EXTENSION`, so activation is a separate call.

The server must be configured with:

```conf
session_preload_libraries = 'shiba'
wal_level = logical
max_replication_slots = 4
shiba.replication_conninfo = 'host=/var/run/postgresql dbname=my_database user=postgres'
```

Call `shiba.activate()` again after a postmaster restart if no statement on a
registered source has yet requested the Runtime.

## Source tables

A source must be:

- a persistent ordinary PostgreSQL table;
- outside the `shiba` schema;
- free of row-level security;
- composed of `pg_catalog` types and `pg_catalog` collations.

Shiba changes a source to `REPLICA IDENTITY FULL` and adds it to the Shiba
publication. TOASTable built-in columns are supported; UPDATE decoding
reconstructs unchanged TOAST values from the full old tuple.
Ingress validates pgoutput field text against the source row type and retains
that text until typed source publication. Row identity uses PostgreSQL's named
composite text I/O before binary encoding, so textual type semantics such as
array lower bounds survive while non-textual NaN payload bits are normalized.

Temporary, unlogged, partitioned-root, foreign, view, and Shiba-managed result
relations are not source tables. User-defined types, collations, functions,
and operators are outside the trusted execution boundary.

`TRUNCATE` on a registered source is rejected because the row-by-row effect
stream cannot represent it. DML uses normal PostgreSQL permissions. Source
schema changes require dropping dependent Shiba results first.

## Query composition

Shiba lowers the analyzed PostgreSQL query into these generic operators:

```text
Scan
Filter
Project
Join
Distinct
Aggregate
Window
TopN
Sink
```

They may be composed into a multi-source, branching, or fan-in DAG. There is no
separate list of fixed query families.

### FROM and JOIN

Accepted inputs are source relations and non-LATERAL subqueries whose bodies
can themselves be lowered. Ordinary PostgreSQL joins lower to a generic Join:

- `INNER`
- `LEFT`
- `RIGHT`
- `FULL`
- cross join

The Join condition is a typed scalar expression; it is not restricted to a
hard-coded key column layout. Correlated `EXISTS`, `NOT EXISTS`, `IN`, and
`NOT IN` in top-level WHERE conjuncts lower to semi, anti, or null-aware anti
joins when their correlation can be represented by the two inputs.

LATERAL apply, correlation deeper than one query level, scalar subqueries in
arbitrary expression positions, and unsupported range-table kinds are rejected
during lowering.

### Scalar expressions

Supported expression nodes include:

- input columns and typed constants;
- immutable `pg_catalog` scalar functions;
- immutable `pg_catalog` operators;
- `AND`, `OR`, and `NOT`;
- `IS NULL`, `IS NOT NULL`, and Boolean tests;
- `IS DISTINCT FROM` and `NULLIF`;
- scalar-array operators such as analyzed `= ANY(...)`;
- `COALESCE`;
- searched and simple `CASE`;
- PostgreSQL casts, relabels, domains, and collations that resolve inside the
  trusted catalog boundary.

Set-returning functions, explicit variadic scalar calls, row-valued NULL tests,
and expression node types without a lowering rule are rejected. Registration
stores catalog OIDs and resolved `SlotType`s; execution revalidates them before
rendering SQL.

### DISTINCT

Top-level `SELECT DISTINCT` is maintained by the generic Distinct operator.
It keeps typed key multiplicity and emits only `0 ↔ positive` boundaries.

`DISTINCT ON` is not accepted because it requires ordered first-row state,
which is a different operator contract.

### Aggregate

GROUP BY expressions, multiple groups, multiple aggregates, HAVING, aggregate
FILTER, aggregate DISTINCT, and aggregate-local ORDER BY are represented in
one Aggregate stage.

Aggregate support is catalog-driven. Registration accepts a built-in aggregate
only when all of these are true:

- it is a normal, fixed-arity aggregate;
- its transition and final functions are immutable `pg_catalog` functions
  with exact signatures;
- its transition state is a concrete durable `pg_catalog` type;
- its final function, when present, is read-only and consumes only that state;
- strict NULL initialization can be resumed without PostgreSQL executor-only
  behavior.

Ordered-set, hypothetical-set, direct-argument, variadic, `internal`-state,
pseudo-state, `finalextra`, and mutating-final aggregates are rejected with a
capability error. The kernel does not branch on names such as `count`, `sum`,
or `max`.

`GROUPING SETS`, `ROLLUP`, `CUBE`, and `GROUP BY DISTINCT` need separate
operators and are rejected.

### Window

Each analyzed `WindowClause` becomes a Window stage with:

- any lowered partition expressions;
- typed order expressions and resolved sort operators;
- PostgreSQL frame option bits;
- typed start/end offsets;
- the window functions, arguments, FILTER, and output types.

Window function and frame capabilities are checked at provisioning. A query is
rejected if a function or frame cannot be executed with bounded durable state;
it is not redirected to a full source rescan.

### ORDER BY, LIMIT, and OFFSET

A global ordered result lowers to TopN when it has:

- at least one resolved ORDER BY expression;
- a nonnegative constant LIMIT;
- an optional nonnegative constant OFFSET;
- optional `WITH TIES`.

`LIMIT` or `OFFSET` without ORDER BY has no deterministic maintained row set
and is rejected. ORDER BY without a finite LIMIT has no relational effect in
an unordered table sink and is also rejected.

### Constructs not yet represented

The query must be a SELECT with at least one FROM relation. These constructs do
not currently have operators:

- `UNION`, `INTERSECT`, and `EXCEPT`;
- CTEs and recursive CTEs;
- target-list set-returning functions;
- row-lock clauses;
- LATERAL apply;
- `DISTINCT ON`;
- grouping sets.

Registration fails before a dataflow becomes active when it encounters an
unsupported construct or catalog capability.

## Visibility and progress

Only committed source WAL reaches Shiba, but a source transaction is not a
result-visibility boundary. A large source transaction is persisted,
published, processed, and applied in bounded prefixes.

Each successful Sink step commits its result-table DML and the corresponding
input cursor, continuation, and checkpoint in one PostgreSQL transaction. A
committed result effect is therefore not applied again after recovery. This
Sink step is the user-visible exactly-once boundary; upstream operator
transactions are internal recovery units, not a transaction spanning the
whole DAG or the original source transaction.

```sql
SELECT * FROM shiba.progress('shiba.order_stats');
```

The result contains:

- `applied_lsn`: the Sink input's consumed causal frontier;
- `pending_wal_bytes`: current WAL minus the logical slot's confirmed flush
  position;
- `updated_at`: the Sink consumer cursor update time.

`applied_lsn` does not claim that previously visible result prefixes were
source-transaction atomic.

For plan, stream, and checkpoint details:

```sql
SELECT shiba.explain_dataflow('shiba.order_stats');
```

## Result ownership and permissions

The `shiba` schema is reserved for managed results. Ordinary
`CREATE TABLE shiba.name (...)` is rejected. Native PostgreSQL materialized
views retain their normal behavior.

The result table is owned by the extension owner. The role that creates it
receives `SELECT`. Runtime mutation uses restricted `SECURITY DEFINER`
functions; a session GUC is not an authorization mechanism.

### Managed indexes

Index management is opt-in:

```sql
SELECT shiba.create_index(
  'shiba.order_stats',
  'order_stats_product_idx',
  ARRAY['product_id']
);

SELECT shiba.drop_index('shiba.order_stats_product_idx');
```

Grant `EXECUTE` on these functions explicitly to roles that may manage
indexes. Calls must be standalone autocommit statements.

Managed indexes are non-unique B-trees over at most eight supported
fixed-width built-in columns, with a conservative combined key-width limit and
at most eight managed indexes per result. Constraint DDL and unique indexes
are not exposed because they could reject a later Runtime update.

## Drop and deactivate

Dropping a Shiba result removes its dataflow, generated state, continuations,
operator streams, consumer cursors, and managed indexes. A shared source stream
remains while another result still uses it.

Before dropping the extension:

```sql
DROP TABLE shiba.order_stats;
SELECT shiba.deactivate();
DROP EXTENSION shiba;
```

`shiba.deactivate()` stops the Runtime and removes the logical slot,
publication, triggers, and the active ingress generation after all managed
results have been removed.
