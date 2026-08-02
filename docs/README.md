# Shiba V2 clean-room

This worktree starts a new V2 on branch `codex/v2-cleanroom`. It is not a refactor
of the previous V1 or V2 code. Phase 1 established Protocol and Catalog; M2 adds
one transactional INSERT/count vertical path; M3.1 replaces its synthetic input
with a strict decoder fed by live PostgreSQL `pgoutput` version 1. M3.2 proves
safe slot replay across the post-result/pre-ack crash window. M4.1 adds a fixed
nullable `int8` payload while retaining the stable non-null `int8` row key.
M4.2 admits zero-column INSERT tuples without inventing a row identity.
M4.3 adds a fixed two-`int8` composite row identity. M4.4 admits a
single-key UPDATE that changes only the nullable payload. M4.5 admits DELETE
only for a stable, single-column `int8` key emitted as pgoutput `D + K`.

## Scope decision

**Facts.** The legacy repository at `/Users/zzhang/Documents/Shiba` remains an
oracle and evidence archive. PostgreSQL 17 and 18 are supported test targets.

**Decisions.** Phase 1 establishes a single database-local catalog authority,
protocol value contracts, static L0 gates, and empty-cluster installation
checks. `shiba_internal` owns private state; `shiba` exposes only a read-only
SQL surface. A database may contain many schemas; the catalog
authority is per installed database, not per application schema or client.

M2 accepts one test-driven, ingress-independent committed `SourceTransaction`.
One runtime-owned PostgreSQL transaction atomically applies INSERT facts,
advances a deterministic count, publishes `shiba.count_result`, and records its
continuation last. Exact replay is a no-op.

M3.1 decodes a complete live `BEGIN → RELATION → INSERT+ → COMMIT` transaction
for one admitted `int8` relation before invoking the unchanged M2 processor.
Decode failures therefore cannot expose Apply, result, or continuation state.

M4.4 treats the existing Apply row as current source state. An unchanged-key
UPDATE mutates its payload in the same processor transaction, leaves count at
the number of inserted rows, and advances continuation only with that mutation.

M4.5 makes that current-state meaning explicit: `applied_insert` is the sole
source-row-state table, despite its now-incomplete name. INSERT creates a row,
UPDATE mutates it in place, and DELETE removes it. DELETE, private count, public
result, and continuation commit in the same processor-owned PostgreSQL
transaction. Missing-row, decode, and backend-crash failures expose none of
those writes; retry applies once and exact transaction replay is a no-op.

M4.6 makes replica identity part of relation admission instead of silently
accepting any PostgreSQL setting. The four frozen shapes require default
replica identity and their exact key-column flags. A live change to `FULL`
therefore fails during decode before Apply can write or advance continuation.

M5.1 adds one real text-payload path to that same current-state authority. A
text INSERT stores the exact value; an UPDATE whose pgoutput payload is `u`
retains that committed value while count/result remain unchanged and
continuation advances in the same processor transaction.

M5.2 admits a present text-format replacement on that same UPDATE path. A real
default-EXTENDED, out-of-line and uncompressed payload is replaced exactly;
row state, unchanged count/result, and continuation still commit together.

M5.3 extends the already admitted two-int8 primary-key shape through DELETE.
Both key components identify one current-state row; deleting one pair leaves a
row sharing key1 untouched and atomically decrements count/result.

M5.4 explicitly admits the existing single-int8-key shape for a live replica
identity index. The default and index constructors require `d` and `i`
respectively, so a live identity change fails before Apply. This is decoder
configuration, not a second durable source-binding authority.

M5.5 proves the pgoutput decoder binding is independent of names and catalog
scan order. M7.1 supersedes rename admission at the processor boundary: the OID
is stable, but committed source DDL records invalidation before later Apply.

M6.1 admits one complete, non-interleaved streamed key-only INSERT transaction.
No segment is visible before stream commit; the assembled transaction uses the
existing Apply/count/result/continuation commit and replay boundary.

M6.2 proves a real streamed abort is never visible and that restarting the same
slot can subsequently deliver an independent streamed commit exactly once.

M7.1 requires every source to be registered by exact PostgreSQL ObjectAddress.
For each new transaction the processor locks the bound relation, checks the
DDL-owned invalidation fact, and only then applies. Rename rollback leaves the
binding valid; committed rename invalidates before any later result is visible.

M7.2 proves the same authority for object removal without adding production
code. Direct DROP rollback removes its invalidation atomically; committed DROP,
schema CASCADE, and same-name recreation preserve the old exact ObjectAddress
invalidation and cannot revive the source binding.

**Not proved.** M7 has no production replication transport/slot lifecycle,
admission for `D + O` or replica identity `FULL`, key-changing/composite UPDATE,
UPDATE old tuples, NULL text, binary payloads, TOAST keys, durable source/schema
binding lifecycle/registration or replica-index drift observation without a
RELATION message, streamed interleaving/subtransactions or bounded buffering,
slot-generation change, multiple
sources, column/type/index DDL coverage, external effect, compatibility path, alias, fallback,
or dual write.

Read [architecture](ARCHITECTURE.md), [protocol contract](PROTOCOL_CONTRACT.md),
[catalog contract](CATALOG_CONTRACT.md), and the [reuse manifest](contracts/REUSE_MANIFEST.md)
before extending the workspace. Ingress work must also follow the
[pgoutput contract](PGOUTPUT_CONTRACT.md).
Nullable tuple work is bounded by the [tuple contract](TUPLE_CONTRACT.md).
