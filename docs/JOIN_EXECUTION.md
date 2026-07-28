# Incremental joins

The join executor accepts one two-input equality join. Each input owns a
logged, multiplicity-preserving arrangement keyed by its join column. One DAG
and one source commit are evaluated as one PostgreSQL transaction.

```text
logged change_log
  -> statement-materialized input delta
  -> transaction-entry logged arrangements
  -> versioned multiplicity and match-presence differences
  -> exact net Join delta
  -> typed UNLOGGED join_delta Stage
  -> downstream logged state and result
```

Updates are represented as an old-row `-1` followed by a new-row `+1` within a
single WAL commit batch. Ordered prefixes are validated before equal rows are
coalesced. The physical program directly evaluates delta-left × old-right,
old-left × delta-right, their cross term, and old/new presence boundaries from
the transaction-entry arrangement snapshot. It writes only the signed net bag
difference to `join_delta`, then consumes that relation downstream.

`LEFT`, `RIGHT`, and `FULL` joins derive NULL-extended visibility from old and
new match cardinality. SQL NULL keys never equality-match.

Decorrelated `EXISTS`/`IN` use a semi join. Anti joins invert its visibility.
`NOT IN` additionally compares old and new right-side NULL presence and expands
the affected left keys only when that global presence changes.

Filters that reference one safely pushable input are evaluated before its
arrangement. Cross-input and null-supplying-side outer-join predicates run
after join row construction. Equality keys currently require identical
PostgreSQL types and deterministic collations; non-equality joins are rejected.

The input delta is a PostgreSQL `MATERIALIZED` CTE. `join_delta` is a
pre-created typed UNLOGGED relation with commit LSN, sequence, signed weight,
and nullable source-composite rows. It exists only to reuse one exact delta
across SQL statements. Logged arrangements, downstream state, result,
progress, inbox, and `change_log` remain authoritative.

Normal apply creates no temporary table and performs no DDL. A failed apply
rolls back as one transaction and leaves its logged inbox/change-log input for
replay. After a PostgreSQL crash, the disposable UNLOGGED Stage is cleared and
rebuilt from that retained input.
