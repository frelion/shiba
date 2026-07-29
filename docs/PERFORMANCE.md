# Performance testing

Correctness is the pull-request gate. Performance is measured separately: a
shared hosted runner is not stable enough to fail a change merely because its
neighbours were busy. The performance matrix produces artifacts on a known
machine, compares them with a baseline captured on that same machine, and has
an explicitly chosen allowance for each metric.

The matrix measures a streaming system, not only a source-table query. A
useful workload records the input size and measures at least one of:

- ingress rows or bytes per second, plus time until its first stream chunk;
- operator actions per second and peak queued chunks/bytes under fanout;
- end-to-end source-commit-to-Sink latency and result rows per second;
- maximum step work (rows, bytes, or continuation pages) for large groups,
  partitions, and `WITH TIES`.

The workload scripts own PostgreSQL setup, data generation, waits, and metric
definitions. The matrix owns repetitions, artifact format, summary statistics,
and baseline comparison. It does not alter `scripts/test-all.sh`.

## Profiles

`smoke` is a fast, repeatable sanity sample: one warm-up and three measured
runs. It is suitable for a pull request to publish evidence, but it does not
fail the correctness workflow.

`full` is for a pinned developer or scheduled benchmark machine: two warm-ups
and nine measured runs. Use its median for a comparison; the reported p95 is
there to expose jitter, not as a percentile claim from a large experiment.

Workloads receive `--profile smoke|full`; they choose profile-specific input
sizes. This makes a smoke run small without changing what a full run means.

## Workload contract

Every workload command accepts these arguments:

```text
--profile smoke|full --json-out PATH
```

It writes one JSON object to `PATH`. Numeric measurements belong in `metrics`;
arbitrary identifying details belong in `metadata`:

```json
{
  "metrics": {
    "sink_rows_per_second": 125000.0,
    "end_to_end_seconds": 2.4,
    "peak_queued_bytes": 524288
  },
  "metadata": {
    "source_rows": 300000,
    "stage_chunk_rows": 1024,
    "environment_fingerprint": "postgres=17.5;shiba=<build>;gucs=<hash>"
  }
}
```

`wall_seconds` is added automatically around the whole command if the workload
does not provide it. Metric names and units are workload API: changing their
meaning requires a new name.

The repository's all-in-one benchmark harness instead writes one run JSON with
`scenarios: [{scenario, metrics}]`. The matrix recognizes that shape directly:
each scenario becomes an independent matrix case, sharing the command's wall
time. This lets one isolated PostgreSQL cluster measure the ingress, fanout,
and complex-DAG workloads in a single invocation.

## Run a matrix

A JSON manifest is the normal interface. The repository's
[`benchmarks/matrix.json`](../benchmarks/matrix.json) invokes the maintained
PostgreSQL 17 harness. Commands may use `{profile}` and `{json_out}`
placeholders. If they do not, the matrix appends the two contract arguments
itself.

```json
{
  "cases": [
    {
      "name": "postgresql17",
      "command": "./scripts/performance-benchmark.sh"
    }
  ]
}
```

```bash
./scripts/performance-matrix.py run \
  --manifest benchmarks/matrix.json --profile smoke \
  --output artifacts/perf-smoke.json --csv artifacts/perf-smoke.csv
```

For an exploratory one-case run, no manifest is needed:

```bash
./scripts/performance-matrix.py run \
  --case postgresql17='./scripts/performance-benchmark.sh' \
  --profile smoke --output artifacts/ingress.json
```

The JSON artifact contains every measured sample, a median/mean/min/p95/max
summary, Git revision, and a host fingerprint. A standalone workload JSON can
also be normalized into this artifact shape:

```bash
./scripts/performance-matrix.py report \
  --input raw-workload.json --output artifacts/imported.json
```

## Compare with a baseline

Collect the baseline and candidate on the same host, with the same profile,
PostgreSQL configuration, data sizes, and no competing benchmark process.
Choose the direction and tolerance per metric; there is no universal meaning
for a count or a latency.

```bash
./scripts/performance-matrix.py compare \
  --baseline artifacts/main-full.json \
  --candidate artifacts/change-full.json \
  --metric post_commit_convergence_seconds:lower:0.25 \
  --metric throughput_rows_per_second:higher:0.20 \
  --metric peak_buffered_bytes:lower:0.30 \
  --output artifacts/comparison.json
```

The comparison uses medians. A 25% latency allowance means a candidate must be
more than 25% slower before it is called a regression. Different host
fingerprints are rejected by default, as are different workload profiles. Each
workload's stable `environment_fingerprint` (in metadata or at the JSON root)
is also compared, so a PostgreSQL version,
Shiba build, or relevant GUC change cannot silently become a performance
comparison. `--allow-cross-host` and `--allow-environment-mismatch` are
diagnostic overrides only; neither must be used for a regression decision.

A zero baseline latency or zero candidate throughput is represented as an
infinite regression when it gets worse; the comparison never divides by zero.

Add `--fail-on-regression` only to a dedicated benchmark job on that pinned
machine. GitHub Actions' normal `correctness` job intentionally does not run
this flag and does not compare against a baseline. This keeps noisy shared
hardware from making a correct pull request red.

## Matrix coverage

The first maintained harness invocation covers the three cases marked
**present**. Its scenario JSON expands them to separate matrix cases. The two
operator-specific stress cases are deliberately a roadmap: they need a clear
workload definition before becoming a number people compare.

| Case | Status | Primary measurements | Why it exists |
| --- | --- | --- |
| Large ingress transaction | **present** | rows/s, source chunks, post-commit convergence, stream-pressure peak | validates bounded ingress publication and drain |
| High-fanout Join | **present** | rows/s, output chunks, queue high-water | validates bounded output and backpressure |
| Complex DAG | **present** | post-commit convergence, Sink rows/s, stream/state/database growth | measures `Join → Join → Aggregate → Window → TopN → Sink` as a stream |
| Aggregate/Distinct hot key | roadmap | rows/s, Apply/Drain pages, peak state | catches per-row rebuilds |
| Window/TopN large partition | roadmap | rows/s, continuation pages, max step time | catches unbounded fold or diff work |

Record the exact command, profile, repetitions, hardware, and whether the
result is a same-host matrix comparison in a pull request. A diagnostic number
from a different machine is useful context, not a regression verdict.
