# Testing strategy

## Milestone gates

Run from `/Users/zzhang/Documents/Shiba-v2-cleanroom`:

```bash
PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config ./scripts/test-l0.sh
PG_CONFIG=/opt/homebrew/opt/postgresql@18/bin/pg_config ./scripts/test-l0.sh
./scripts/test-empty-install.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-empty-install.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m2.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m2.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m3.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m3.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-empty.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-empty.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-composite.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-composite.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-update.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-update.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-delete.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-delete.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m4-replica-identity.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m4-replica-identity.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m5-toast.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m5-toast.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m5-incompressible-toast.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m5-incompressible-toast.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m5-composite-delete.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m5-composite-delete.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
```

`test-l0.sh` selects the matching `pg17` or `pg18` feature, then runs formatting,
Protocol/Catalog/Runtime checks and tests, clippy with warnings denied, `git diff
--check`, forbidden-surface scans, the canonical fixture byte/digest check, and
the deferred-evidence manifest completeness check.

Each `test-empty-install.sh` invocation creates an isolated empty cluster. It
proves complete rollback after a forced failure in the `CREATE EXTENSION`
transaction; successful install and the single `1|1` version result; public API
access and private table denial for an ordinary role; and clean extension drop.
This re-proves only the Phase-1 installation rollback boundary, not later
component recovery.

`test-m2.sh` creates another isolated cluster and packages the extension. Its
test-only source schema commits two ordinary rows before constructing the Rust
input. It proves result `2`, cross-schema placement, exact replay, identity
conflict, operator-error rollback, backend-termination rollback after the
continuation write, reconnect/replay, and ordinary-role result-only access.
Failure triggers are test objects and never ship in extension SQL.

`test-m3.sh` enables logical decoding in an isolated cluster, creates one
test-only publication and `pgoutput` slot, and captures two real committed
transactions. The integration test removes only that client's per-XLogData
delimiter, decodes pure pgoutput, applies through M2, and proves result `2`,
exact replay `2`, and corrupt/truncated input state unchanged. For the second
transaction it disables periodic feedback, stops the receiver after complete
COMMIT, applies result `3` while `confirmed_flush_lsn` is unchanged, kills the
receiver, then proves slot replay is the identical transaction and is a no-op.
PG17 and PG18 run the same gate.

`test-m4.sh` captures a real two-column relation containing SQL NULL and an
`int8` value. It proves operator-error rollback after payload Apply, exact Apply
facts, count result, replay no-op, and a precisely corrupted key tuple tag
failing before any durable state appears.

`test-m4-empty.sh` captures two zero-column INSERTs committed together. It
proves two NULL-key/absent-payload Apply facts, count `2`, replay no-op, and a
corrupt nonzero tuple column count failing with zero durable state.

`test-m4-composite.sh` captures two rows sharing key-1 but differing in key-2.
It proves exact two-part Apply facts, count `2`, replay no-op, and a precisely
corrupted second-key tag failing with zero durable state.

`test-m4-update.sh` applies a real INSERT and then captures an unchanged-key
UPDATE through both non-NULL canonical text and SQL NULL paths. It proves count
stays `1`, the Apply payload changes exactly once, a corrupt key tag fails before
writes, and a valid UPDATE for a missing row rolls mutation and continuation
back. Backend termination after continuation INSERT rolls payload, count,
result, and continuation back together before successful retry and replay no-op.

`test-m4-delete.sh` first applies one real INSERT transaction containing two
single-key rows, then captures one row's real DELETE as pgoutput protocol-v1
`D + K`. It proves relation OID, selector,
column count, and canonical text key decoding; only the target current-state row
is removed; another row is unchanged; private count and public result decrement
exactly once; and continuation commits with them. It separately proves invalid
tuple tag rejection before writes, missing-row and count-underflow rollback,
backend-crash rollback of row/count/result/continuation, successful retry, exact
replay no-op, and the same behavior on PostgreSQL 17 and 18.

`test-m4-replica-identity.sh` first captures and applies a default-identity
single-key INSERT, proving RELATION `d`, key flag `1`, result visibility, and
exact replay. It then changes the live source to replica identity FULL and
captures a real `RELATION f` plus `D + O` DELETE. The decoder rejects before
Apply, leaving the existing row, private count, public result, and continuation
unchanged on PostgreSQL 17 and 18.

`test-m5-toast.sh` stores a deterministic 64 KiB UTF-8 value in a source text
column forced to `STORAGE EXTERNAL`, verifies its TOAST relation has storage,
and applies the real INSERT into `payload_text`. A no-key-change UPDATE is
captured as `U + N` with canonical key `t` and payload `u`. The gate proves bad
payload-tag rejection before writes, continuation-after-insert crash rollback,
exact text retention, retry once, replay no-op, and unchanged private/public
count on PostgreSQL 17 and 18.

`test-m5-incompressible-toast.sh` uses two seeded, dependency-free 64 KiB
high-entropy ASCII values under default `EXTENDED` storage. It proves both
source values are out-of-line and uncompressed, then verifies the replacement
UPDATE carries exact `t` bytes and atomically replaces `payload_text` while
count/result stay `1`. A binary-tag corruption, post-continuation crash, retry,
and exact replay prove the failure and recovery boundaries on PG17/18.

`test-m5-composite-delete.sh` applies two composite-key rows sharing key1,
captures one real `D + K` with two canonical int8 fields, and proves only the
exact pair is removed while count/result decrement once. It also proves bad
second-key rejection, a valid missing-pair rollback, continuation-after-insert
crash rollback, retry, and replay on PostgreSQL 17 and 18.

`test-m5-replica-index.sh` captures a real single-column relation under
`REPLICA IDENTITY USING INDEX`, proves RELATION `i`, exact key flag and `D + K`,
then applies INSERT and DELETE through the existing current-state/count/result/
continuation transaction. It proves crash rollback, retry once, replay no-op,
and that a live switch back to default identity is rejected before writes on
PostgreSQL 17 and 18.

`test-m5-source-binding.sh` binds the decoder to a real relation OID, applies an
INSERT, renames both table and column, and applies another INSERT under the same
OID. It then drops/recreates the original qualified name, proves the new OID is
different and the wire is otherwise valid, and verifies the old binding rejects
before row/count/result/continuation writes on PostgreSQL 17 and 18.

`test-m6-stream-commit.sh` sets isolated-cluster logical decoding memory to
64 KiB and captures a 10,000-row protocol-v2 transaction with streaming on. It
requires at least two matching `S ... E` segments and terminal `c`, proves no
prefix/abort-shaped input is visible, then proves post-continuation crash rolls
all rows/count/result/continuation back before retry-once and replay no-op on
PostgreSQL 17 and 18.

During development run fmt, check, the Runtime unit tests, one current scenario,
and clippy. Run both complete PG matrices only at the milestone boundary.

## Evidence handling

Fixtures in `tests/fixtures` must be data, not copied executable implementation.
The canonical Protocol vector has the adjacent
`tests/fixtures/protocol/canonical-v1.provenance.md`, including source, legacy
commit, old command, and clean-room command. It has been re-proved. The
`tests/fixtures/pg/deferred-evidence.json` file is only an A-class evidence
index: legacy scenarios remain provenance until reproduced case by case. M2's
independent rollback/crash tests do not claim equivalence to an old runtime.
Differential tests use the legacy
repository solely as an oracle and must never link it, load its SQL, or share a
catalog authority.
