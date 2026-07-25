# Architecture

## Boundary with PostgreSQL materialized views

Native PostgreSQL materialized views use full `REFRESH MATERIALIZED VIEW`. Shiba does not intercept or alter them. Instead, it reserves the `shiba` schema: each table declared with `CREATE TABLE shiba.<name> AS SELECT ...` is a Shiba-owned streaming result. Source tables belong outside `shiba`.

For each supported declaration, Shiba creates:

1. a result table at `shiba.<name>`, backfilled by PostgreSQL CTAS and protected by a `UNIQUE NULLS NOT DISTINCT` group key;
2. metadata in `shiba_internal.stream_views`;
3. a durable, inspectable operator graph in `shiba_internal.stream_graphs`, `stream_graph_nodes`, and `stream_graph_edges`, compiled from PostgreSQL's analyzed Query tree;
4. a `pgoutput` logical replication slot, a `shiba_publication` publication, and `shiba_internal.view_progress` watermark;
5. one row-level source wakeup trigger, which starts a missing worker but never carries row data;
6. one database-level dynamic WAL Router, plus one dynamic executor worker for each Shiba result DAG. Both have persisted leases and heartbeats.

The session-preloaded utility hook acquires a short source-table lock before CTAS, retains it through result registration, sets `REPLICA IDENTITY FULL`, adds the source to the publication, and then installs the wakeup trigger. This closes the backfill-to-stream handoff without a parser fork. The public relation is always the Shiba table created by CTAS.

The CTAS hook reads PostgreSQL's analyzed `Query` tree before execution. It
extracts relation OIDs and aliases, typed targets, aggregate and window
identities, grouping references, join kind/operator/column origins, predicates,
subqueries, ordering, frames, and limits. Registration uses these resolved
facts instead of deriving topology from SQL text. SQL text is retained only to
identify the target CTAS declaration and to ask PostgreSQL for a diagnostic
physical plan.

The graph compiler retains PostgreSQL's `EXPLAIN (VERBOSE, FORMAT JSON)` plan
for diagnostics, but also persists a versioned Shiba logical plan. The stable
plan has explicit `scan`, `filter`, inner/outer/semi/anti join, `distinct`,
`aggregate`, `having`, `window`, `top_n`, `project`, and `sink` nodes with
numbered input edges. PostgreSQL may represent these operations differently in
its physical plan, so that plan is diagnostic data rather than Shiba's durable
execution contract.

Each DAG worker loads the persisted plan, validates every WAL source against
its Scan nodes, and applies a `DeltaBatch` through transactional operator
functions. A Rust predicate compiler accepts a restricted expression AST and
emits controlled SQL that invokes PostgreSQL's typed comparison operators.
Stateful operators own durable tables: aggregate accumulators, distinct
multiplicities, join arrangements, window partition rows, and TopN rows.
Project maps source columns to sink aliases without losing their PostgreSQL
types.

## Runtime flow

```text
source-table DML
  -> transaction commit
  -> PostgreSQL WAL
  -> one per-database pgoutput logical slot
  -> WAL Router: decode and atomically route to durable per-DAG inboxes
  -> one executor worker per result DAG
  -> lock one result OID, consume one commit from its inbox
  -> evaluate typed pre/post Filter predicates
  -> update/probe JOIN, DISTINCT, aggregate, window, or TopN state
  -> emit only affected differential rows or rebuild one state-owned partition
  -> Project into the protected public result table
  -> acknowledge inbox commit + update result watermark atomically
  -> SELECT from shiba.<table>
```

No source-table scan or `REFRESH MATERIALIZED VIEW` occurs during maintenance. The data path is PostgreSQL logical decoding; the source wakeup trigger contains no row capture or outbox fallback. Updates are decoded as old-row `-1` plus new-row `+1`, and the commit LSN becomes the result watermark.

One Router owns the logical slot for the database. Every result table has one
executor worker and a durable inbox. Executors serialize on a result-OID
transaction advisory lock. DDL teardown acquires that same result lock before
source locks and disables the DAG, so DROP and apply use one lock order instead
of forming a source/result inversion.

The Router uses a two-phase durability protocol. It first peeks a bounded set of
logical changes and commits the unique commit-LSN checkpoints plus all derived
inbox rows. Only after that transaction is durable does a second transaction
advance the slot through the corresponding pgoutput end LSN. A crash between
the phases replays the same WAL prefix; commit-LSN checkpoints make rerouting a
no-op before the slot advances. Thus a routing failure retains WAL instead of
losing an increment.

Each DAG executor consumes and deletes one complete inbox commit in the same
transaction as its result updates; a crash therefore rolls both actions back.
A result additionally ignores commits at or before its activation LSN, because
its CTAS snapshot already contains them. Initial rows and pgoutput rows pass
through one typed canonical encoding before becoming operator-state keys.
`shiba.progress(regclass)` is the public observation surface for the result
commit LSN and pending WAL bytes; metadata remains in `shiba_internal`.

## Extension lifecycle

`session_preload_libraries = 'shiba'` loads the CTAS declaration hook into each
client backend. `CREATE EXTENSION shiba` installs the catalog; a subsequent
one-time `SELECT shiba.activate()` creates the logical slot and starts the
Router, because PostgreSQL does not permit slot creation within the
extension-install transaction. The Router starts or restarts stale DAG
executors. Source ALTER, TRUNCATE, and DROP are rejected while dependent
results exist. Before removing the extension, drop all results and call
`shiba.deactivate()`; it removes the slot and publication. `DROP EXTENSION
shiba` is rejected while that slot still exists, preventing abandoned slots
from retaining WAL. The workers do not need `shared_preload_libraries`, but
`max_worker_processes` must have capacity for one Router plus each active Shiba
result.

## Development sequence

1. Extension packaging, table-style DDL registration, and real installation test. ✓
2. `pgoutput` logical decoding, asynchronous dynamic worker, commit-LSN watermark, and one-source `COUNT`/`SUM` operators. ✓
3. Inner/outer/semi/anti joins, Filter, Project, Aggregate, Having, and
   multiplicity state. ✓
4. Top-level DISTINCT, TopN, windows and explicit frames. ✓
5. Broader grouped aggregates, set operations, recovery tooling, and cascading
   result graphs.
