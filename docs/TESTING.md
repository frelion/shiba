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
./scripts/test-m10-committed-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-committed-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-streaming-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-streaming-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-catalog-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-catalog-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-governed-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-governed-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-performance-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-performance-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
./scripts/test-m10-shutdown-ingress.sh /opt/homebrew/opt/postgresql@17/bin/pg_config
./scripts/test-m10-shutdown-ingress.sh /opt/homebrew/opt/postgresql@18/bin/pg_config
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

`test-m7-concurrent-ddl.sh` uses bounded wait-event polling, not timing guesses,
to prove Apply's granted relation lock, DDL's waiting exclusive lock, zero
blocked-state writes, Apply-before-DDL commit order, and subsequent fail-closed
processing on PostgreSQL 17 and 18.

`test-m8-multi-source.sh` captures two real sources through independent slots
and publications. It proves source-local continuation/replay, the shared union
count/result, source-2 crash isolation and retry, and generation mismatch
rollback on PostgreSQL 17 and 18.

`test-m8-concurrent-sources.sh` uses real captures, wait-event polling, and
bounded channel receives to prove one-Apply duplicate CAS and independent
source progress with exact global/per-source state on PostgreSQL 17 and 18.

`test-m8-bounded-decode.sh` captures a real 10,000-change committed transaction,
decodes/applies/replays it, then captures 10,001 changes and requires explicit
limit rejection with unchanged durable state on PostgreSQL 17 and 18. A pure
test also proves both committed and streamed decoders reject input above 16 MiB.
This is decoder admission evidence, not a production receiver or throughput
claim.

`test-m8-performance.sh` freezes a real 10,000-change PG17/18 regression budget:
decode at most 2 seconds, first Apply at most 10 seconds, and exact replay at
most 2 seconds. The originating clean-room runs measured approximately 8 ms,
0.8 seconds, and 0.2 ms respectively. It also proves constructors reject 10,001
changes and a forged public value fails before database state or replay can be
reached. The threshold is a correctness regression gate, not a sustained-load,
tail-latency, or cross-hardware benchmark.

`test-m9-registration.sh` installs the operator authority, proves CountRows has
no input address, resolves a live int8 column name to its exact ObjectAddress,
and proves missing source/column, wrong type, and duplicate ID leave no partial
definition/state/result. The migrated M2–M8 gates explicitly register one
CountRows per source and query operator-keyed state/result; multi-source tests
sum those independent public facts only to retain the historical union
observation.

`test-m9-operator-performance.sh` captures one real 10,000-row nullable-int8
transaction: every fourth payload is NULL and every other payload is 2. One
EffectBatch must publish CountRows=10,000 and SumInt8=15,000, then exact replay
must be a no-op. The PG17 reference measured 9.56 ms decode, 836.72 ms Apply,
and 0.171 ms replay under the unchanged 2 s / 10 s / 2 s ceilings. The closest
M8 count-only PG17 run measured 8.21 ms, 853.28 ms, and 0.180 ms; the small
mixed differences are recorded as single-run evidence, not an improvement
claim or permission to hide regression. A test-only ordered operator-2 failure
also proves the operator-1 attempt and all transaction-local effects roll back.

`test-m9-count-sum.sh` proves the fixed two-operator INSERT/UPDATE/DELETE/NULL
example, the nullable relation's real two-position `D + K`, missing-row and
overflow rollback, crash after the first result, retry-once, exact-replay
short-circuit, and pre-EffectBatch DDL invalidation. `test-m9-operator-concurrency.sh`
holds source 1 at a test-only advisory lock: a second transaction for that
source waits on the binding mutex while source 2 commits independently; after
release, CountRows and SumInt8 finish in source order and all replays are no-op.
The existing M6 gates remain the streaming regression proof for the admitted
key-only CountRows shape; M9 does not add nullable-payload streaming admission.

`test-m10-committed-ingress.sh` is the first production transport gate. It
links the selected libpq for the requested PostgreSQL major, enters real COPY
BOTH without `pg_recvlogical`, receives protocol-v1 XLogData, assembles one
transaction, invokes the existing decoder and Runtime on a separate Apply
connection, and proves public CountRows/SumInt8 plus continuation.
Pure ingress tests independently split every byte boundary, coalesce frames and
transactions, enforce the 16 MiB buffer bound, validate `w`/`k`, and freeze the
34-byte status payload.

The same gate now proves M10.2: requested keepalive reports only the old durable
LSN; receive-before-Apply drop changes neither computation nor slot; Runtime
commit-before-feedback restarts as `AlreadyApplied`; explicit feedback flushes
the exact COMMIT `end_lsn`; decoder and Operator failures poison the receiver,
roll back all state, and do not advance the slot; clean restart retries once.

`test-m10-streaming-ingress.sh` runs the production receiver in explicit
protocol-v2 streaming mode with 64 KiB logical-decoding memory. The gate is
defined to prove real
multi-segment 10,000-change `S/R/I/E...c` delivery crosses arbitrary transport
chunk boundaries yet enters the existing Runtime decoder only after terminal
commit. Partial input and `E` produce neither Apply nor feedback; a crash during
partial assembly relies on slot replay and later applies exactly once. A real
matching `A` bypasses Runtime, leaves every Shiba durable fact absent, and may
advance only to the outer XLogData `dataStart` carrying that abort. Corruption,
unknown/mismatched XID, wrong terminal, 16 MiB overflow, and 10,001 changes fail
closed without feedback. Acceptance requires the same gate to pass on
PostgreSQL 17 and 18; no CLI may be used by the production receiver and no
persisted spool may be created.

The same M10.3 gate must exercise the closed terminal-authorization set:
Runtime `Applied`, exact-replay `AlreadyApplied`, strict `EmptyCommitted`, and
legal top-level `Aborted`. An empty commit must have exactly
`S(first=true) E (S(first=false) E)* c`, at least one complete segment, one XID,
flags zero, valid commit/end LSNs, and no other frame/trailing byte; it advances
only through explicit empty ACK and creates no continuation. Legal `R/I`
traffic must instead reach the sole Runtime decoder, and every other shape must
fail closed. This is structural evidence about the selected
publication's empty output only. Publication identity, mutation/recreation
drift, and rejection of empty ACK after invalidation are proved separately by
M10.4; they are not consequences of the M10.3 grammar alone.

The first PG17 run exposed a real multi-segment publication-empty commit and
failed the former single-segment assumption. The corrected constant-state
recognizer then passed the same production COPY BOTH gate on PG17.10 and
PG18.4. Those runs prove a real segment count greater than one, exact terminal
LSNs, pre-ACK replay, no empty-feedback loop, unchanged operator/continuation
state, later source Apply, streamed abort, partial-stream restart, the 10,000
change admission boundary, and rejection of change 10,001 without feedback.

`test-m10-catalog-ingress.sh` is the M10.4 catalog-governance gate. On PG17 and
PG18 it configures one exact source/publication/existing-slot tuple and proves
atomic duplicate rejection, wrong/missing/active/plugin/database slot failure,
publication shape admission, PUBLIC denial, and that configuration never
creates or drops a physical slot. It exercises publication ALTER rollback and
commit, remove-then-add persistence, drop plus same-name recreation, source
invalidation, pristine slot rotation, stale generation CAS, active/wrong/
non-pristine replacement rejection, and absence of dynamic progress columns.

The first PG17 publication-membership test failed because
`pg_event_trigger_ddl_commands()` returned no ObjectAddress for `ALTER
PUBLICATION ... DROP TABLE`. The accepted implementation retains the single
event writer but compares every configured publication OID and frozen snapshot
to live catalogs at `ddl_command_end`; the corrected gate passes independently
on PG17.10 and PG18.4. This is failure evidence for persistent publication
history, not permission to match by name or globally invalidate unrelated
sources.

`test-m10-governed-ingress.sh` is the separate governed-session gate and is
green on PG17.10 and PG18.4. It proves wrong role/generation and active-slot
failure, advisory ownership exclusion, exactly one Apply plus one replication
connection, detach/reattach, least-privilege streamed receive/Apply/ACK of
10,000 changes, and revalidation that rejects an already pending
`EmptyCommitted` after publication remove/re-add while CountRows and the slot
LSN remain at their last durable values. Pure tests freeze the advertised
32-source/64-connection cap and validate required explicit database, positive
connection timeouts, and positive Apply statement timeout. Neither gate creates
or drops a slot during ordinary session attach/detach.

The gate uses two distinct non-superuser roles. The Apply role has
`NOREPLICATION`, schema usage, and only the internal table privileges required
by governance and Runtime. Its `SELECT` on `source.events` exists solely because
Runtime preflight takes `ACCESS SHARE`; Runtime does not read source rows. Its
`UPDATE` privilege on `source_continuation` is required because the latest-row
replay check uses `SELECT ... FOR UPDATE`. The receiver role has `REPLICATION`
plus source-schema `USAGE` and source-table `SELECT`, and no Shiba internal
write grants. Swapping the roles in either connection fails safely.

`test-m10-performance-ingress.sh` freezes limits before accepting results:
15 s for a real 10,000-change source-commit-to-durable-Apply path, 2 s replay,
20 tx/s for 100 ready transactions, service p50/p95/p99 limits of
250/500/1,000 ms, a 300 ms slow-Apply floor, and 250 ms outstanding-receive
rejection. PG17 measures 860.865 ms E2E, 29.350 ms replay, 622.987 ms backlog
service, 160.52 tx/s, 6.216/6.355/6.533 ms service percentiles, 1.393 ms
rejection, and 357.969 ms slow Apply. PG18 measures 867.479 ms, 31.085 ms,
739.298 ms, 135.26 tx/s, 7.375/7.585/7.776 ms, 1.836 ms, and 358.370 ms.

The E2E timer starts before committing the 10,000-row source INSERT and stops
after durable Apply. The 100-transaction timer starts only after all ten-row
transactions are committed; those percentiles are receiver service latency
against a precommitted backlog, not source-commit latency. The same test freezes
Rust bounds of 16,777,216 assembly bytes, 10,000 decoded changes, two
connections per source, one outstanding input, and no queue. It does not measure
allocator/RSS peaks or cross-host soak.

`test-m10-shutdown-ingress.sh` proves cooperative idle shutdown through the
asynchronous libpq receive loop. PG17 returns in 42.262 ms and PG18 in
76.950 ms, both below 1 s, with no terminal token, ACK, Shiba write, or slot-LSN
advance; detach/reattach then succeeds. Its failure evidence fixes the receive
order: drain already-buffered libpq `CopyData` before socket polling, then use
`PQsocketPoll`/`PQconsumeInput`. Shutdown during Runtime Apply and automatic
reconnect/backoff remain outside the gate.

Pure Runtime session tests cover connection-scoped relation metadata: the first
transaction requires an exact `R`, repeated `R` is revalidated, a later omission
is admitted only for the same source, and a changed source/mismatch fails. The
constant-size `PgoutputRelationState` retains no relation frame list or bytes and
does not replace the semantic decoder.

`test-m6-stream-abort.sh` starts a live protocol-v2 receiver before a 10,000-row
transaction, observes real segments while it is open, rolls it back, and
requires real matching `A`. After abort feedback it restarts the same slot and
applies/replays a later streamed commit, proving the aborted stream left no
row/count/result/continuation state on PostgreSQL 17 and 18.

All live Apply tests register their source relation through the private M7.1
function; there is no unbound production test path. `test-m7-ddl-invalidation.sh`
proves exact relation ObjectAddress storage, unrelated-DDL isolation, rename
rollback, committed-rename invalidation, pre-Apply failure, historical replay,
and ordinary-role denial on PostgreSQL 17 and 18.

`test-m7-drop-invalidation.sh` proves direct DROP rollback, committed DROP,
exact old relation ObjectAddress retention, same-name/new-OID non-revival, and
schema CASCADE invalidation on PostgreSQL 17 and 18. Pending work fails before
row/count/result/continuation writes; historical exact replay remains a no-op.

`test-m7-column-invalidation.sh` proves the registered binding set contains the
relation and exact positive column attribute numbers. It covers type-change
rollback/apply, committed type-change fail-closed behavior, and an isolated
column rename whose durable cause is the exact column address on PG17 and PG18.

`test-m7-index-invalidation.sh` proves the identity-index binding, unrelated
index isolation, rename rollback/apply, committed exact-index invalidation,
pending pre-Apply rejection, state isolation, and historical replay on
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
