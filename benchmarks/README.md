# Shiba performance benchmarks

`../scripts/performance-benchmark.sh` starts a fresh PostgreSQL 17 cluster,
installs the current extension once, and measures four live-WAL workloads:

1. a large committed source transaction flowing through `Scan -> Sink`;
2. a single-row Join input with a large opposite-side fanout;
3. a keyed/generic selective Join A/B against the same large opposite-side
   state; and
4. `Join -> Join -> Aggregate -> Window -> TopN -> Sink`.

Every scenario waits for Sink visibility and compares the result relation with
the equivalent PostgreSQL query before recording a result. It does not reuse a
developer's PostgreSQL server or mix benchmark results with correctness gates.

```bash
# Small, repeatable smoke profile (the CI candidate)
./scripts/performance-benchmark.sh --profile smoke \
  --json-out benchmarks/results/smoke.json

# Dedicated quiet machine / nightly runner
./scripts/performance-benchmark.sh --profile full \
  --json-out benchmarks/results/full.json
```

The adjacent CSV path is derived from the JSON path unless `--csv-out` is
provided. The JSON has a stable top-level `run_id`, Git `commit`, `profile`,
overall `correctness`, and one scenario object per workload. Each scenario
contains rows, post-commit convergence time and throughput plus observable
resource metrics: generated output and source-stream chunks, persistent state
bytes, database bytes, and sampled peak
database / buffered-stream bytes / rows. It also includes a PostgreSQL,
extension, and selected-GUC environment fingerprint; the Git commit is a
separate run field, so a baseline can be compared with a candidate.

The resource values are PostgreSQL observables, not process RSS. They make
storage and stream-pressure regressions comparable without platform-specific
`time` or container accounting. Compare results only on the same PostgreSQL,
hardware, filesystem and profile; a noisy shared machine is not a performance
baseline.

The selective A/B uses a direct equality predicate for the keyed plan and a
semantically equivalent `OR ... IS NULL` predicate for the generic fallback.
The smoke profile uses 100,000 right rows; the full profile uses 1,000,000.
For a diagnostic 1,000,000-row run, both paths must pass correctness, the
keyed arrangement must expose an index-scan `EXPLAIN` plan, and the JSON
records convergence time, state bytes, and checkpoint advances so the index
maintenance trade-off is visible. One current same-host observation is 0.571 s
keyed versus 0.754 s generic; treat it as diagnostic until repeated by the
performance matrix on the target baseline host.
