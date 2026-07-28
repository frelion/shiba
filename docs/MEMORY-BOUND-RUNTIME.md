# Single Runtime resource contract

Shiba uses one PostgreSQL background worker per active database. DAG runtimes
are backend-local plan metadata; source payload, operator state, intermediate
folds, and results live in PostgreSQL relations.

One DAG consumes one source commit in one outer PostgreSQL transaction. A
chunked operator may execute many SQL statements inside that transaction, but
state, sink rows, progress, Stage cleanup, and inbox acknowledgement become
visible atomically.

## Runtime limits

| Setting | Default | Purpose |
| --- | ---: | --- |
| `shiba.runtime_work_mem` | `16MB` | `work_mem` used by the Runtime session |
| `shiba.runtime_temp_file_limit` | `1GB` | temporary-file limit for the Runtime |
| `shiba.max_cached_dags` | `128` | backend-local DAG plan-cache capacity |
| `shiba.stage_chunk_rows` | `2048` | input/key/group rows processed per statement |
| `shiba.max_stage_rows` | `1000000` | commit Stage/output work quota |
| `shiba.max_commit_rows` | `1000000` | source-commit admission row quota |
| `shiba.max_commit_bytes` | `1073741824` | source-commit payload admission quota |

The Runtime also fixes `hash_mem_multiplier=1` and uses generic prepared plans.
All settings have `SIGHUP` context. After changing them, reload PostgreSQL
configuration; the singleton Runtime refreshes its session settings.

`work_mem` is a PostgreSQL per-plan-node budget, not a hard backend RSS limit.
The commit, Stage, and output quotas are therefore part of the resource
contract rather than optional tuning hints.

## Chunked operators

Aggregate and DISTINCT fold ordered change-log events into UNLOGGED,
commit-scoped Stages. Each key stores an associative summary:

```text
total(a || b) = total(a) + total(b)
minimum(a || b) = min(minimum(a), total(a) + minimum(b))
```

This preserves negative-prefix validation across chunk boundaries. Folded
DISTINCT keys and Aggregate groups are then applied in bounded batches.

TopN computes weighted cumulative ranks and expands only rows intersecting the
requested offset/limit interval. It no longer expands the complete retained
multiset.

Join output and affected Window partitions have explicit Stage/output quotas.
Join candidate generation and Window execution still use PostgreSQL
sort/hash/tuplestore plans; their working memory is governed by the Runtime
session settings and temporary-file limit.

UNLOGGED Stages are caches, never authority. Normal completion deletes all
rows. Empty Stage relations are truncated only after crossing a coarse 64 MiB
file threshold. After a PostgreSQL crash they can be rebuilt from the logged
inbox and change log.

## Resource-blocked DAGs

A configured resource limit reports SQLSTATE `53400`. Shiba rolls back all
operator changes, retains the inbox commit, disables only the affected DAG,
and records the error. The singleton Runtime remains alive and continues
serving other DAGs.

After changing the limit or splitting/removing the offending input, the DAG
creator (or an administrator with `UPDATE` privilege) can resume replay:

```sql
SELECT shiba.resume('shiba.my_result'::regclass);
```

Deterministic operator failures are not resumable through this API.

## Remaining bounds

The contract bounds active Runtime concurrency, cached DAG plans, admitted
commit size, per-statement chunk cardinality, and configured Stage/output
cardinality. It does not impose a hard database disk quota on retained
`change_log` backlog. A paused DAG can retain logged input indefinitely; disk
admission/backpressure requires a separate policy for rejecting source writes
or rebuilding a DAG from a fresh source snapshot.
