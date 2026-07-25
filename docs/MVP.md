# Shiba MVP (v0.1)

## Product goal

Shiba adds a single-node, asynchronously maintained streaming-table capability to PostgreSQL. Users define a derived table with SQL; Shiba backfills it once and then maintains its result from committed changes in its source tables.

## User-facing contract

```sql
CREATE EXTENSION shiba;
SELECT shiba.activate();

CREATE TABLE shiba.order_stats AS
SELECT product_id, count(*) AS order_count, sum(amount) AS total_amount
FROM orders
GROUP BY product_id;

SELECT * FROM shiba.order_stats;
```

The `shiba` schema is reserved for Shiba-managed result tables. Source tables must be outside that schema, and ordinary `CREATE TABLE shiba.name (...)` declarations are rejected. PostgreSQL materialized views in every schema retain their native `REFRESH MATERIALIZED VIEW` semantics.

Shiba result tables are owned by the extension owner and ordinary roles receive
only `SELECT`. The worker changes them through a restricted `SECURITY DEFINER`
apply entrypoint; a session GUC is not an authorization mechanism. As in core
PostgreSQL, the extension owner and superusers remain trusted administrators.

Shiba uses PostgreSQL's `CREATE TABLE ... AS SELECT` grammar only for unquoted `shiba.<name>` declarations. `shiba.activate()` is a one-time database setup command that creates the required logical slot; PostgreSQL does not permit that operation during `CREATE EXTENSION`. A session-preloaded utility hook locks the source table during initial backfill, then adds it to the `pgoutput` publication before the declaration commits. This gives the initial snapshot a clean handoff to the WAL stream.

## Supported SQL subset

Shiba compiles PostgreSQL's analyzed Query tree into a durable logical DAG. The
MVP accepts these independently maintained shapes:

- grouped aggregation over one source or one two-source equality join:
  one group key, `COUNT(*)` or `COUNT(DISTINCT column)`, and
  `SUM(not-null-column)`;
- `HAVING` over the maintained `COUNT`, `COUNT(DISTINCT)`, and `SUM` state;
- typed `WHERE` expressions composed from `AND`/`OR`/`NOT`, comparisons,
  `IS NULL`, and `IS NOT NULL`;
- `INNER`, `LEFT`, `RIGHT`, and `FULL` equality joins with
  multiplicity-preserving arrangements and post-join predicates;
- correlated `EXISTS`/`NOT EXISTS` and `IN`/`NOT IN`, decorrelated to semi,
  anti, and null-aware anti joins;
- top-level `SELECT DISTINCT` over ordinary projected columns;
- global `ORDER BY` on one column with constant `LIMIT` and optional `OFFSET`;
- windows with one partition key and one order key. Ordinary projected columns,
  `row_number`, `rank`, `dense_rank`, `count`, `sum`, `avg`, `min`, and `max`
  are supported, including PostgreSQL's default frame and constant
  `ROWS`/`RANGE`/`GROUPS` frames.

For example:

```sql
CREATE TABLE shiba.order_stats AS
SELECT product_id, count(*) AS order_count, sum(amount) AS total_amount
FROM orders
GROUP BY product_id;
```

Filter, Project, Aggregate, and Having compose in one graph:

```sql
CREATE TABLE shiba.large_order_stats AS
SELECT product_id AS product,
       count(*) AS order_count,
       sum(amount) AS total_amount
FROM orders
WHERE amount >= 20 AND NOT (product_id = 8)
GROUP BY product_id
HAVING count(*) >= 2;
```

Each committed source row change is decoded from PostgreSQL WAL as `+1` or `-1`. The asynchronous worker later updates only its affected group: `COUNT(*)` receives `+1` or `-1`, and `SUM(amount)` receives `+amount` or `-amount`.
Predicates are evaluated through PostgreSQL's own column type and comparison
operator, not as JSON text. An update that crosses a predicate boundary is
therefore naturally maintained as an old-row retraction followed by a new-row
insertion.

The two-source aggregate accepts inner and outer equality joins. `SUM` must
currently read a NOT NULL value from the left source. Join key columns must
have the same PostgreSQL type and deterministic collations, because the
arrangements use a stable encoded key:

```sql
CREATE TABLE shiba.category_sales AS
SELECT i.category_id, count(*) AS sale_count, sum(s.amount) AS total_amount
FROM sales s
JOIN items i ON s.item_id = i.id
GROUP BY i.category_id;
```

Shiba keeps a multiplicity-preserving arrangement for each join input. A WAL
delta probes the opposite arrangement and emits joined `+/-` deltas. Outer
joins additionally retract and restore NULL-extended rows at the zero/one
match boundary. Semi and anti joins emit only when right-side multiplicity
crosses that boundary.

Window and TopN operators retain their complete input multiset in Shiba-owned
state. A window delta rebuilds only its affected old/new partition; TopN
recomputes only its bounded public sink from operator state. Neither path reads
the source table.

The MVP deliberately rejects set operations, non-equality joins, multiple
window partition/order keys, arbitrary scalar expressions, grouped
`AVG`/`MIN`/`MAX`, nullable grouped `SUM`, nonconstant limits/frame offsets,
aggregate/window `FILTER`, `FETCH ... WITH TIES`, self-joins, filtered
`IN`/`NOT IN` subqueries, TOASTable source columns, external sources, and
cascading Shiba results. Unsupported declarations fail during registration;
there is no full-refresh or source-scan fallback.

## Consistency model

Only committed WAL transactions appear on the logical slot, so rolled-back writes are never decoded. A dynamic Shiba worker consumes bounded batches in short transactions; result changes are therefore eventually consistent. Each committed transaction is durably deduplicated by its commit LSN, and each result records an activation LSN immediately after its locked CTAS backfill. This prevents both crash replay double-counting and replaying pre-backfill WAL into a newly-created result. `shiba_internal.view_progress.applied_lsn` exposes the per-result commit-LSN watermark.

Applications can inspect a result's public progress surface without reading internal tables:

```sql
SELECT * FROM shiba.progress('shiba.order_stats');
```

`TRUNCATE` is deliberately rejected for Shiba source tables in this MVP. Before
dropping the extension, drop all Shiba result tables and run
`SELECT shiba.deactivate()` to remove the logical slot and publication.

## Non-goals

- distributed execution or object storage;
- Kafka, CDC, or external sinks;
- arbitrary PostgreSQL SQL;
- compatibility changes to native PostgreSQL materialized views;
- a PostgreSQL parser fork.
