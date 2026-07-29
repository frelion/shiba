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
`SELECT` on the result data. The Runtime changes them through a restricted
`SECURITY DEFINER` apply entrypoint; a session GUC is not an authorization
mechanism. Index management is separately authorized by explicitly granting
`EXECUTE` on `shiba.create_index(regclass,text,text[])` and
`shiba.drop_index(regclass)`. Authorized consumers must also have schema
`USAGE` and table `SELECT`; they may drop only indexes they created through the
managed API. Calls must be standalone autocommit statements so privileged DDL
locks cannot survive in a caller-controlled transaction.

Managed indexes are non-unique B-trees over at most eight fixed-width built-in
columns, with a conservative maximum combined key width of 1024 bytes and at
most eight managed indexes per result. Variable-width and user-defined types
are rejected: an ordinary B-tree over a currently short `text` value could
otherwise reject a later wider Runtime value. Constraint DDL and unique indexes
are not exposed because user-defined enforcement rules could likewise reject a
later Shiba update. While index DDL owns a result's DAG lock, the singleton
Runtime skips that DAG and continues scheduling other ready DAGs. As in core
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

Each committed source row change is decoded from PostgreSQL WAL as `+1` or
`-1`. The asynchronous Runtime later schedules the result's logical
`DagRuntime` and updates only its affected group: `COUNT(*)` receives `+1` or
`-1`, and `SUM(amount)` receives `+amount` or `-amount`.
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

Shiba keeps a logged, multiplicity-preserving arrangement for each join input.
For each stable ingress batch, a set-oriented physical program reads the
current arrangements, derives the next arrangements for affected keys, and
emits the exact signed bag difference `new Join output - old Join output`.
The batch updates arrangements, aggregate state, result rows, and its cursor in
one transaction. Outer, semi, anti, and null-aware anti visibility follows
old/new match cardinality rather than a per-event callback.

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

Source row identity currently supports PostgreSQL built-ins with normalized
representations: boolean and integer/float/numeric types; date, time,
timestamp, and interval types; UUID, `pg_lsn`, and OID; character, bytea,
bit-string, network, and JSON types when the column is non-TOASTable. Domains,
arrays, enums, `money`, and user-defined base types are rejected. This closed
set prevents locale- or extension-GUC-dependent type output from changing a
state key between the registration session and the Runtime session.

## Consistency model

Only committed WAL transactions appear on the logical slot, so rolled-back
writes are never decoded. Each active database has exactly one real PostgreSQL
background worker named `shiba runtime`. Routing, round-robin DAG scheduling,
application, and garbage collection are bounded phases in this one
SPI-connected backend. A logical `DagRuntime` is cached plan metadata, not a
worker or thread. Result changes are therefore eventually consistent, and the
number of result DAGs does not change PostgreSQL process count.

The Runtime does not run SPI concurrently on Rust threads. After a source
commit is fully ingested, it applies one stable ingress batch for one DAG per
transaction and rotates ready DAGs between batches. Each transaction updates
operator state, result rows, and the DAG batch cursor together, so earlier
batches are visible before the complete source commit has been consumed. A
PostgreSQL statement is still non-preemptible, so high-fanout operator work can
temporarily delay routing, other DAGs, and garbage collection.

Each committed transaction is durably deduplicated by its commit LSN, and each
result records an activation LSN immediately after its locked CTAS backfill.
Every decoded delta is stored once in the shared durable `change_log`.
`dag_inbox` stores at most one lightweight `(result_oid, commit_lsn)` reference
and a batch cursor per affected DAG, so payload is not duplicated by fanout.
Operator SQL reads one `ingress_apply_batches` input range and directly updates
authoritative state and result rows in the same transaction that advances the
cursor. On the last batch, that transaction also advances progress and
acknowledges the DAG reference. This prevents crash replay from double-counting
a committed batch and prevents pre-backfill WAL from entering a newly-created
result. A partial source commit can remain visible if a later batch fails.
`shiba_internal.view_progress.applied_lsn` exposes the per-result commit-LSN
watermark for source commits whose every batch has completed; it does not
describe an already-visible partial commit.

Registration persists a versioned `PhysicalDagPlan`. A physical Stage is a
relation-reuse decision inside the one Runtime, not another worker. Inline
relations stay fused, relations reused inside one statement use
`MATERIALIZED` CTEs, and the current Join program uses one pre-created typed
UNLOGGED `join_delta` Stage when its exact output must cross SQL statements.
Join input delta remains statement-materialized.

All recovery authority remains logged: `change_log`, stable apply ranges,
`dag_inbox`, progress, arrangements, operator state, and result rows. Fold and
Join Stage relations are UNLOGGED scratch used only inside the current apply
transaction. Normal apply creates no temporary table and performs no DDL.
After a crash, the replacement Runtime resumes at the persisted batch cursor;
a batch rolled back by a crash or transient concurrency failure is retried,
while earlier committed state and result changes remain visible.

The Runtime is configured for PostgreSQL to restart it after an abnormal exit
while the postmaster stays up. Because it is dynamically registered, a
postmaster restart does not itself re-register it: the next statement on a
registered source table, or an explicit `SELECT shiba.activate()`, restores the
single Runtime.

If loading or applying one DAG fails, the current apply transaction rolls
back. Previously committed batches, the inbox cursor, and shared input remain
consistent. Resource-limit failures pause that DAG; deterministic plan/operator
failures quarantine it while other DAGs continue. Shared payload is
garbage-collected only after no DAG references its source transaction.

Applications can inspect a result's public progress surface without reading internal tables:

```sql
SELECT * FROM shiba.progress('shiba.order_stats');
```

The physical execution plan and Stage metadata are also inspectable:

```sql
SELECT shiba.explain_physical('shiba.order_stats');
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
