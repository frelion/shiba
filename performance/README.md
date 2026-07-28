# Shiba performance baseline

Two benchmark drivers are retained:

- `scripts/performance-matrix.py` is the formal full-operator and end-to-end
  baseline. It covers every `OperatorKind`, runs three randomized repetitions,
  and is the required tool for optimization comparisons.
- `scripts/performance-benchmark.sh` is the earlier aggregate-only pilot. It is
  useful for a short edit loop but is not evidence of full operator coverage.

Run the formal matrix from the repository root:

```bash
./scripts/performance-matrix.py
```

Its default formal parameters are 20,000 initial rows, 20-row semantic DML
batches, 40 visibility probes per scenario, 5-second source/result query
phases, one 5,000-row large transaction, and three randomized repetitions.
The large transaction size can be changed with
`SHIBA_MATRIX_LARGE_TX_ROWS`, but changing it makes the run incomparable with
the default formal baseline. Results are written to
`performance/matrix-results/<UTC run id>/`.

The formal result includes `manifest.json`, `operator-coverage.json`,
`metrics-raw.csv`, three-run `metrics-summary.csv`, pooled
`latency-summary.csv`, action and resource samples, PostgreSQL-wide WAL/I/O
snapshots, per-scenario plans and pgbench output, exact workload copies,
checksums, the working-tree patch, and an archive of untracked files.
`runtime-topology.json` records the singleton `runtime_state` owner PID and a
zero legacy-worker count. `resources.csv` records both the complete PostgreSQL
process-tree CPU/RSS and the individual Runtime PID CPU/RSS at 100 ms
intervals. The `large_transaction` phase therefore exposes
`rss_peak` and `runtime_rss_peak` independently.

For a deliberately reduced smoke run:

```bash
SHIBA_MATRIX_RUN_ID=smoke \
SHIBA_MATRIX_ROWS=200 \
SHIBA_MATRIX_GROUPS=20 \
SHIBA_MATRIX_MUTATIONS=5 \
SHIBA_MATRIX_REPETITIONS=1 \
SHIBA_MATRIX_QUERY_SECONDS=1 \
SHIBA_MATRIX_QUERY_CLIENTS=2 \
SHIBA_MATRIX_LATENCY_PROBES=1 \
SHIBA_MATRIX_LARGE_TX_ROWS=500 \
./scripts/performance-matrix.py
```

Filtered runs are supported with `SHIBA_MATRIX_SCENARIOS` as a comma-separated
list, but neither reduced nor filtered runs qualify as a formal baseline.

For ordinary correctness and performance runs, any PostgreSQL `WARNING`,
`ERROR`, `FATAL`, or `PANIC` invalidates the run. Deterministic failpoint runs
use a narrower allowlist: only the exact crash record expected from the armed
failpoint is allowed, and every other warning-or-higher entry still fails.

## Aggregate-only pilot

The benchmark in this directory is designed for repeatable before/after
comparisons of Shiba's performance. It creates and destroys an isolated
PostgreSQL 17 cluster; it does not connect to an existing cluster.

Run the default workload from the repository root:

```bash
./scripts/performance-benchmark.sh
```

Results are written to `performance/results/<UTC run id>/`. Every successful
run includes:

- `environment.txt`: commit, dirty-file count, host, toolchain, and workload
  parameters;
- `postgresql.conf`: the exact server configuration;
- `metrics.csv`: normalized summary metrics;
- `pgbench-*.txt`: unmodified pgbench output;
- `visibility-latency.csv`: every asynchronous visibility sample;
- `resources-*.csv`: 200 ms PostgreSQL process CPU/RSS samples;
- `explain-*.json`: machine-readable execution plans;
- `correctness-difference-count.txt`: bag-difference count against a fresh
  PostgreSQL recomputation (must be `0`);
- `final-state.json` and `postgresql.log`;
- `workload/` and `checksums.sha256`: the exact benchmark driver and SQL used.

The main scale parameters can be overridden without editing the script:

```bash
SHIBA_PERF_RUN_ID=after-change \
SHIBA_PERF_INITIAL_ROWS=300000 \
SHIBA_PERF_GROUPS=10000 \
SHIBA_PERF_WRITE_CLIENTS=4 \
SHIBA_PERF_WRITE_TX_PER_CLIENT=100 \
SHIBA_PERF_WRITE_BATCH_SIZE=10 \
SHIBA_PERF_QUERY_CLIENTS=8 \
SHIBA_PERF_QUERY_SECONDS=10 \
SHIBA_PERF_LATENCY_SAMPLES=40 \
SHIBA_PERF_LARGE_TX_ROWS=20000 \
./scripts/performance-benchmark.sh
```

For an optimization comparison, keep the machine, power mode, parameters,
PostgreSQL version, configuration, and workload checksums fixed. Run each
revision at least three times in alternating order and compare medians. Treat
correctness differences, failed transactions, PostgreSQL errors, or an
undrained inbox as a failed run rather than a performance result.

Runtime-architecture changes require the complete, unfiltered formal matrix;
a smoke, filtered scenario list, aggregate-only pilot, or statement that the
matrix “ran successfully” is not performance acceptance. Compare the candidate
against the retained pre-refactor baseline using the same host, PostgreSQL
build and configuration, workload checksums, formal parameters, and repetition
count. The harness must verify and record exactly one `shiba runtime` PID and no
legacy `shiba worker`, `shiba dag worker`, `shiba router`, or
`shiba executor` processes. Fanout measurements pause all participating
logical DAGs before source DML, then require exactly one `change_log` row per
source delta and exactly one `dag_inbox` reference per participating
DAG/source transaction before apply resumes. Preserve both raw result
directories and report per-scenario and overall median deltas for source-write
throughput, apply/drain throughput, visibility latency percentiles, query
throughput, PostgreSQL CPU/RSS, WAL/I/O, correctness, inbox drain completion,
shared-payload fanout, and large-transaction peak RSS.

Any statistically material regression must be explained and explicitly
accepted; otherwise it blocks the refactor. A faster aggregate-only pilot
cannot offset a regression or missing coverage elsewhere in the full operator
matrix.

The historical reports measure earlier per-result and Router+Executor
topologies. They are not evidence for the Single Runtime refactor. Until a new
complete matrix and matched per-scenario baseline comparison are present, the
Single Runtime performance verdict remains pending.
