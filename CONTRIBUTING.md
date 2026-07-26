# Contributing to Shiba

Thanks for helping improve Shiba. Please open an issue before large changes so
the supported SQL contract and recovery invariants can be discussed first.

## Development setup

Shiba targets PostgreSQL 17 and uses `cargo-pgrx 0.19.1`:

```bash
cargo install cargo-pgrx --version 0.19.1
cargo pgrx init --pg17 /path/to/pg_config
```

## Before opening a pull request

Run the focused checks for your change, and run the complete gate when changing
execution, registration, WAL routing, or recovery behavior:

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
./scripts/test-all.sh
```

Every new operator or execution path should include plan-shape coverage,
reference/differential coverage, end-to-end SQL coverage, and recovery coverage
where applicable. Preserve commit ordering, atomic acknowledgement, and
idempotent replay semantics.

## Pull requests

Describe the user-visible behavior, the supported SQL shape, correctness
invariants, test commands, and any performance evidence. Do not present a
filtered benchmark as the formal performance matrix.
