# Contributing to Shiba

Shiba targets PostgreSQL 17 and 18 and uses `cargo-pgrx 0.19.1`.

```bash
cargo install cargo-pgrx --version 0.19.1
cargo pgrx init --pg17 /path/to/pg_config
```

For PostgreSQL 18, use `cargo pgrx init --pg18` and select the matching
`pg18` Cargo feature for builds and tests.

## Design rules

- PostgreSQL relations own durable state. Rust caches may be discarded at any
  point.
- `DataflowPlan` is the only plan contract from lowering through execution.
- Operators exchange typed weighted rows through `EffectStream`.
- One operator step atomically commits whichever durable changes it makes:
  state, cursor, continuation, output, checkpoint, or Sink result DML.
- Work is bounded by input/output rows and bytes. Large valid work resumes from
  a typed durable continuation.
- A source transaction is not a result-visibility boundary.
- Missing kernels and unsupported catalog capabilities fail during
  registration.
- Contract changes are clean cuts. Delete removed types, JSON fields,
  functions, tables, tests, and documentation; do not add aliases, dual writes,
  decoder branches, or adapters for the old contract.
- Every operator has an implementation page under `docs/operators/`. New
  operators must copy `docs/operators/_TEMPLATE.md` and keep the page in the
  same change as code and tests.

## Before a pull request

Run focused tests while editing and the complete gate before handing off a
change to planning, persistence, operators, recovery, or lifecycle code:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
./scripts/test-all.sh
```

New operator behavior needs:

- plan-shape and catalog-capability coverage;
- real PostgreSQL result coverage;
- row and byte bound assertions;
- crash tests on both sides of commit;
- backpressure and recovery coverage;
- a chained DAG case when the operator can fan out or fan in.
- an implementation page describing state relations, indexes, continuation
  phases, primitive complexity, access paths, performance evidence, and known
  limits; use `docs/OPERATOR_IMPLEMENTATION_STANDARD.md` as the checklist.

Do not run concurrent `cargo pgrx install` jobs against the same PostgreSQL
installation.

## Pull-request description

State:

- the user-visible behavior;
- the plan or state contract that changed;
- the recovery invariant;
- the exact test commands and results;
- any remaining unbounded statement or registration work.

Performance claims need a reproducible workload, environment, raw measurements,
and a matched baseline. Correctness gates must pass independently of benchmark
results.
