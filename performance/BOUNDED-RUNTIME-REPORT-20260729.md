# Bounded single-Runtime execution report

> **Status: FUNCTIONAL PASS / PERFORMANCE CONDITIONAL**

This change keeps one PostgreSQL background Runtime per active database and
makes each DAG an in-process scheduling/program abstraction. Operator
authority remains in PostgreSQL relations. Shared UNLOGGED fold Stages bound
chunked Aggregate and DISTINCT working sets, while apply admission and
operator quotas pause only the affected DAG and retain its inbox for replay.

## Evidence

- candidate worktree: `/Users/zzhang/Documents/Shiba-bounded-runtime`;
- branch: `codex/bounded-runtime`;
- PostgreSQL: 17.10 (Homebrew);
- correctness suite: `scripts/test-all.sh`;
- retained performance baseline:
  `/Users/zzhang/Documents/Shiba/performance/matrix-results/20260728-single-runtime-final`;
- candidate performance result:
  `performance/matrix-results/20260729-bounded-runtime-final-v5`;
- formal workload: 20,000 initial rows, 100 groups, 20 mutations,
  40 visibility probes, 5-second query phases, 4 clients, one 5,000-row
  transaction, and 3 randomized repetitions;
- scenario catalog SHA-256:
  `0418a7076bb19af18bb4e74cc18767bad1979450cf830f3967f04107ad9eddd2`
  for both baseline and candidate;
- both baseline and candidate use 64 MiB `work_mem`; the candidate explicitly
  sets `shiba.runtime_work_mem = '64MB'`.

The formal candidate manifest reports:

- 81 scenario runs;
- 3,801 correctness checks and zero failures;
- zero pgbench failures;
- zero PostgreSQL log errors;
- complete 16/16 operator coverage;
- exactly one `shiba runtime` and zero legacy workers in every topology sample.

## Resource contract

The Runtime applies these SIGHUP-reloadable settings:

- `shiba.runtime_work_mem` (default 16 MiB);
- `shiba.runtime_temp_file_limit` (default 1 GiB);
- `shiba.max_cached_dags` (default 128);
- `shiba.stage_chunk_rows` (default 2,048);
- `shiba.max_stage_rows` (default 1,000,000);
- `shiba.max_commit_rows` (default 1,000,000);
- `shiba.max_commit_bytes` (default 1 GiB).

Aggregate and DISTINCT fold commits through shared UNLOGGED Stages in stable
chunks. Join performs a key-cardinality preflight before candidate
materialization and checks the produced Stage before consumption. TopN avoids
expanding multiplicity outside the requested rank range and has state/output
admission. Window checks affected-partition cardinality before rebuilding it.

SQLSTATE `53400` rolls back the operator subtransaction, marks only that DAG
resource-blocked, preserves its inbox, and leaves the singleton Runtime PID
alive. `shiba.resume(regclass)` permits the recorded creator or an
UPDATE-capable administrator to retry after configuration or workload repair.

Shared Stage compaction is best-effort maintenance in a separate Runtime
transaction. It uses a non-blocking advisory lock and skips a cycle when
another lifecycle operation owns the lock.

## Correctness and bounded-resource gates

`scripts/test-all.sh` passed:

- Rust formatting, clippy with warnings denied, and 92 unit/pgrx tests;
- asynchronous E2E flow;
- 120-round single-source differential test;
- cross-chunk Aggregate `COUNT(DISTINCT)` and negative-prefix rollback;
- Join differential test: 68 commits and 424 comparisons;
- concurrency, transaction, persistent-slot, and failpoint recovery;
- singleton Runtime topology, fairness, poison-DAG isolation, DROP/GC;
- low-memory resource gate.

The resource gate uses 64 KiB Runtime `work_mem`, applies a 6,000-row
high-cardinality Aggregate commit, and exercises 80×80 genuinely distinct Join
pairs. It verifies pre-materialization quota pause, stable Runtime PID, retained
inbox, resume/replay, commit row/byte admission, empty Stages, and native
PostgreSQL equivalence. The final targeted resource run observed a Runtime RSS
peak of 49,248 KiB.

## Performance comparison

Median deltas are candidate versus the retained single-Runtime baseline:

| Metric | Result |
| --- | ---: |
| visibility p50 | 27/27 improved; median **-52.1%** |
| visibility mean | 27/27 improved; median **-45.1%** |
| visibility p95 | 22/27 improved; median **-36.6%** |
| visibility p99 | 26/27 improved; median **-29.2%** |
| backfill wall time | 0/27 improved; median **+10.8%** |
| warm result TPS | 7/27 improved; median **-2.7%** |
| single-client ingress rows/s | **+404.1%** |
| four-client ingress rows/s | **+718.7%** |
| multi-DAG fanout deliveries/s | **+1.2%** |
| 5,000-row transaction rows/s | **+21.2%** |

Visibility p95 regresses for RIGHT JOIN `+16.1%`, FULL JOIN `+14.9%`,
fanout inner Join `+13.0%`, LEFT JOIN `+9.0%`, and the composed Join scenario
`+5.1%`. The largest backfill regressions are SEMI IN `+22.7%`, RIGHT JOIN
`+21.6%`, NULL-aware anti `+20.9%`, one-to-one inner Join `+19.2%`, and SEMI
EXISTS `+17.6%`. Warm-query TPS has a median `-2.7%` regression; the worst
points are fanout inner Join `-8.6%`, one-to-one inner Join `-8.3%`, bigint
filter Aggregate `-7.8%`, and high-cardinality DISTINCT `-7.5%`.

For the 5,000-row transaction:

| Resource | Baseline | Candidate | Delta |
| --- | ---: | ---: | ---: |
| PostgreSQL process-tree RSS peak | 139,360 KiB | 145,408 KiB | **+4.3%** |
| Runtime RSS peak | 33,376 KiB | 41,376 KiB | **+24.0%** |

The candidate completes that transaction in a median 390.6 ms and at 21,335
rows/s. The higher RSS is PostgreSQL executor working memory from additional
quota/preflight and chunked relational work, not retained Rust payload. Formal
Stage samples end with zero live/dead tuples; the largest physical Stage is
2,580,480 bytes.

## Limits of the guarantee

This is a bounded execution contract, not a strict whole-process RSS theorem.
PostgreSQL `work_mem` applies per executor node, and Join/Window still use
set-oriented statements with multiple executor nodes. `temp_file_limit` does
not cover table, index, WAL, shared-buffer, or UNLOGGED-relation storage.

Apply admission occurs after logical decoding and routing into logged
`change_log`; it does not bound source WAL, router canonicalization, or an
unconsumed disk backlog. A separate backlog/backpressure policy is required
for a whole-system storage bound.

The performance matrix shows substantial latency and throughput gains, but it
does not support claiming that every individual metric or RSS peak is
non-regressing. The RSS and listed per-scenario regressions are explicit
release trade-offs.
