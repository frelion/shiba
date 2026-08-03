# Catalog contract

## M13 generic operator authority (frozen, pending M13.3 cutover)

M13 replaces the aggregate-specific layout rather than migrating or mirroring
it. The sole definition authority will contain the strict declaration,
canonical compiled-plan payload, format version, digest, state contract and
output contract. Ordered exact ObjectAddress input bindings are children of
that same definition and are written only by `compile_and_register`; they are
not a second plan authority. Concrete operator names are not SQL constraints.

Runtime remains the only state/result writer. State is an opaque versioned
payload. Results have a generic visibility/header plus either one typed scalar
payload or operator-owned keyed rows. Public scalar/keyed surfaces expose only
active results. Registration and rebuild initialization call the same generic
Runtime writer; Catalog SQL must not infer a concrete zero, patch an input by
kind, or reset state independently.

At M12 destructive prepare, the registration/compiler writer installs the
complete target plan set and digest in the same transaction that makes the
target the sole building authority. Recovery validates that durable set;
activation only publishes the same set. There is no candidate table,
compatibility view, dual write or recovery-time kind reconstruction.

## Installation authority

The catalog extension installs one constrained row containing catalog and
protocol version `1`. This is the only Phase-1 durable authority. It belongs to
the PostgreSQL database in which `CREATE EXTENSION` executes, even if that
database hosts multiple application schemas. Names are not identity, and no
legacy catalog table, publication, or change log participates.

## Installation transaction

`CREATE EXTENSION` runs the extension SQL as one PostgreSQL transaction. It first
creates the schemas, constrained identity, and restricted version function, then
installs source-row/continuation and operator authority tables without seeded
operators. If installation
fails, PostgreSQL rolls back the whole installation; the empty-install test must
prove there is no partial `shiba` or `shiba_internal` state. Extension upgrade,
external side effects, and migrations are out of scope.

The extension does not perform remote calls, create replication resources, or
hold a cross-system lock. Retrying an installation error means first ending the
aborted transaction, then starting a complete new transaction.

M5.5 proves the runtime's explicit relation-OID binding behavior without adding
a source catalog table: rename remains valid and same-name drop/recreate fails
closed. Durable binding registration, DDL invalidation, source catalog rows,
and any multi-writer lifecycle remain unproved. See
[transaction and recovery](TRANSACTION_RECOVERY.md).

## M7.1 source ObjectAddress authority

`source_binding` stores one immutable relation ObjectAddress, each live
user-column ObjectAddress, and the current replica-identity index ObjectAddress
when configured. The private registration function inserts that complete set
atomically and is its only logical writer.
`source_invalidation` stores the exact matching address reported by PostgreSQL
event-trigger helpers; one event-trigger function is its only writer for both
`ddl_command_end` and `sql_drop`. Names and rendered object identities are not
stored or compared. Both tables remain inaccessible to ordinary roles.

M7.2 adds test evidence, not catalog state. The `sql_drop` invocation of the
same writer records a registered relation when it is dropped directly or by
schema CASCADE. PostgreSQL transaction rollback removes that fact; a committed
fact continues to name the old OID after a same-name table is recreated.

M7.3 keeps the same table and writer. Relation rows use `objsubid = 0`; column
rows use their positive PostgreSQL attribute number. The invalidation foreign
key therefore proves every durable cause was in the registered exact-address
set. Runtime relation locking explicitly selects the single zero-subid row.

M7.4 adds `binding_kind` because relation and index are both `pg_class` objects
with zero subid. Its closed values are `relation`, `column`, and
`identity_index`; this is not a dynamic kind registry.

M7.5 adds no catalog row. Its concurrent gate proves the relation lock orders
the existing Apply transaction before a conflicting DDL transaction; the DDL
transaction remains the sole writer of the resulting invalidation fact.

M8.1 reuses one independent binding set per source. The relation-kind row is
also the source-local processor mutex; registration and DDL invalidation writers
do not change. The count/result singleton is a global union aggregate, not a
second per-source authority.

M8.2 changes no catalog contract. The deterministic concurrency gate proves
each relation-kind row serializes only its owning source while the singleton
count/result remains the one aggregate commit point.

## M9.1 operator authority

The clean-room install contains `operator_definition`, `operator_state`, and
`shiba.operator_result`; it does not create or later drop old count tables.
Definitions freeze compiler version, source ID, operator kind, and nullable or
complete input ObjectAddress according to the kind. The registration adapter is
the sole definition writer and verifies source binding existence/invalidation.
Runtime alone updates state/result. State may be negative because SumInt8 is a
signed aggregate; CountRows enforces non-negativity in its pure evaluator.

Each public result has the same operator ID/kind as its definition through a
foreign key. Internal definition/state have no PUBLIC privileges; the public
sink grants SELECT only. Installation seeds no operator: definition, zero state,
and zero result are created atomically by explicit registration.

Later tuple slices alter only the existing current-state table. M5.1 adds
`payload_text` with int8/text mutual exclusion; it creates no fifth table,
source registry, change log, alias, or second writer.

## M10.4 source-ingress authority

`source_ingress_config` is the sole database-local ingress definition for a
source. Its private configuration writer locks an existing source binding,
rejects source invalidation, resolves the current database and exact
`pg_publication` OID, normalizes the publication's live column list to ordered
attribute numbers, and validates an existing inactive persistent logical
`pgoutput` slot in that database. It inserts the complete definition atomically
and never overwrites a duplicate source.

The publication ObjectAddress is durable identity. Its name, insert/update/
delete/truncate/via-root flags, and normalized attribute numbers are a frozen
semantic snapshot and transport locator, not a name-based identity fallback.
The admitted publication has exactly the bound relation, no row filter, all
required change operations, and no publish-via-root policy. Drift in any frozen
field is fail closed.

`source_ingress_invalidation` is the persistent publication-history fact. The
existing single DDL event writer owns it as well as source invalidation. Exact
drop addresses cover publication removal; on every DDL command end it compares
each configured OID and snapshot against the live catalogs. This scan is
necessary because a real PostgreSQL 17 `ALTER PUBLICATION ... DROP TABLE`
produced no command ObjectAddress. A committed membership/flag/column/filter/
name change, remove-then-add, drop, or same-name recreation therefore cannot
revive the old configuration. Rollback rolls back its invalidation too.

The private slot-rotation writer is a pristine-only generation CAS. It requires
the expected generation, locks the row, rejects invalidated or non-pristine
sources and active/current slots, validates a different existing inactive
`pgoutput` slot in the same database, then changes slot and increments generation
once. It never creates or drops a physical slot. Binding rebuild remains a
separate unimplemented lifecycle.

PostgreSQL `pg_replication_slots` remains physical slot/progress authority.
The catalog deliberately contains no `confirmed_flush_lsn`, receiver PID,
active flag, connection secret, transport status, WAL spool, or cursor mirror.
Ingress config/invalidation are inaccessible to PUBLIC, and ordinary startup is
not a definition writer.

Source Ingress is a transport owner, not another state writer. Governance
revalidates catalog/live publication and slot state before receive, Apply, and
every ACK; an invalidation cannot be bypassed by an empty commit. Ingress cannot
write current rows, operator state/results, or continuation.

## M11.2 bootstrap authority

M11.2 adds exactly one private `source_bootstrap` checkpoint/lifecycle
authority for a pristine source. Its identity is a never-reused `BootstrapId`;
it binds the exact source, slot generation and name, immutable
`consistent_point`, lifecycle, and committed scan progress. It stores neither
the ephemeral `snapshot_name` nor dynamic slot progress and is not a second WAL
cursor, continuation, row log, or EffectStream.

The bootstrap coordinator is its sole lifecycle writer. The least-privilege
Apply role has `SELECT` and `UPDATE` on this private table because live operator
execution takes `FOR UPDATE` on the phase to serialize with active cutover;
PUBLIC still has no privilege. Runtime remains the only writer of current
source rows and operator state/result. A batch's row/operator writes
and checkpoint advance share one transaction. Public results must represent
building/unavailable as `result_status = building, value_bigint = NULL`;
complete values require `result_status = active` and become visible with active
lifecycle in one cutover transaction. M11.2 replaces the WAL-cause-shaped
`applied_insert` with the sole key-owned `source_row_state`; no alias or second
current-row table remains.

Before `scan_complete`, a failed hidden attempt may be fully removed only by
the explicit pristine bootstrap owner and restarted with a fresh never-reused
attempt and slot/snapshot. After `scan_complete`, the checkpoint directs
recovery to the existing-slot catch-up phase. Ordinary M10 startup still cannot
create or drop slots. Active/non-pristine reset or generation rebuild is M12,
not an M11 cleanup path.

The implemented schema contains the closed lifecycle, exact ingress foreign
key, immutable consistent point, latest batch ordinal/key/digest, unique writer
fence token, and catch-up fence LSN. It stores no snapshot name, WAL payload, or
moving slot cursor. PG17/18 prove initial reservation, building visibility, two
bounded batch checkpoints, fence activation, WAL-only continuation, and
ordinary live handoff. M11.3 recovery is described below. M11.4 adds no catalog
authority and proves that the same checkpoint/result schema remains bounded at
one million rows; M11 is complete at its declared boundary, while M12 is not.

## M11.3 pristine attempt replacement writer

`shiba_internal.replace_pristine_source_bootstrap` is the sole catalog writer
for replacing a hidden pre-scan attempt. Its compare-and-swap input contains
the exact old BootstrapId/source/slot/generation plus a distinct new
BootstrapId, requested publication OID, new slot and strictly larger
generation. It locks the exact binding, bootstrap and ingress rows and admits
only `creating`, `scanning`, `cleanup_pending`, or `failed`. Both physical slot
names must already be absent; slot drop/create remain replication-transport
operations outside this SQL transaction.

The writer also requires no source continuation and only building/NULL public
results. It deletes partial `source_row_state`, resets the source's private
operator states, retires the exact old bootstrap/config, and calls the existing
reservation writer. The latter rechecks live binding, invalidation,
publication OID/membership/flags, empty physical slot, and pristine state before
inserting the new exact config/checkpoint and leaving results building/NULL.
To reuse that strict initial-reservation predicate, results are normalized to
active/zero only inside the uncommitted replacement transaction; no reader can
observe it, and any later validation failure rolls the whole replacement back.

The writer never handles `scan_complete`, `catching_up`, or `active`, never
touches `source_continuation`, and cannot create/drop slots. Public has no table
or function privileges. PG17.10 and PG18.4 prove exact replacement and partial-
state reset, stale-generation/foreign-slot rejection, exact replay, overflow
rollback, advisory competition, same-slot restart/catch-up, and active-before-
feedback exact-fence replay. The creating/slot-absent crash state is
reconstructed durably rather than reached by an instruction-level process kill;
an active foreign old-slot conflict is not directly tested.

M11.4 performs 100 atomic 10,000-row checkpoint advances, one real 10,000-
change WAL continuation, exact fence activation, and live handoff without a
batch table, moving LSN mirror, or additional writer. Operators must inspect
`result_status`, not a partial numeric value, and must never repair lifecycle by
manual catalog updates. Indefinite writer catch-up and M12 non-pristine rebuild
remain outside this authority.

M11.5 proves this privilege boundary on PG17.10 and PG18.4. The non-superuser
bootstrap control role has `NOREPLICATION` and only explicit private-table,
source, sequence, and revoked bootstrap-function grants. The transport role has
`REPLICATION` but cannot perform control Apply; the result reader has only
public result `SELECT`. Missing `EXECUTE`, source `SELECT`, or checkpoint
`UPDATE`, and swapped control/transport identities, leave the catalog fail
closed. PUBLIC gains no privilege and no writer or authority changes.

## M12.1 rebuild authority transition

M12 first extends the existing `source_bootstrap` lifecycle rather than adding
a candidate table. Before destructive prepare, the exact old binding, ingress
config, generation and bootstrap remain sole active authority. Target binding,
publication membership/flags, replica identity, operator plan, permissions and
slot-name availability are validated without catalog mutation.

Prepare is one exact-old identity/generation CAS transaction. Its commit makes
the target binding/config/new generation the sole catalog authority and the
bootstrap lifecycle building; all public operator results become
`building/NULL`. The same transaction retires old current rows, resets private
operator state and deletes the old continuation. From that point no old worker,
transaction or token is eligible to Apply or ACK, and recovery cannot restore
the old identity. Activation later changes only lifecycle/result visibility for
that same target authority; it does not install another binding/config.

Slot operations are outside this transaction. Durable lifecycle plus exact
old/new names, generation and observable `pg_replication_slots` shape govern
their recovery. Names never authorize adoption. PostgreSQL provides no
immutable slot incarnation identifier or per-slot ownership privilege; a
holder of the trusted `REPLICATION` credential can make an undetectable
same-name/same-shape replacement. This is a documented deployment assumption
and residual risk, not a database-enforced invariant. M12 adds no slot-birth
marker, parallel candidate authority, alias, fallback or dual write.

M12.1 is the frozen contract. M12.2 establishes the destructive writer and
durable index identity. M12.3 proves the target authority survives real
snapshot-to-live activation without another binding/config switch. Crash and
later lifecycle claims are proved by M12.4 recovery and M12.5 governance;
M12.6 closes bounded performance and the full release matrix.

## M12.2 admitted rebuild state

The existing `source_bootstrap` row is still the sole lifecycle authority.
M12.2 adds `rebuild_prepared` plus the exact retired BootstrapId/slot/generation
coordinates needed for the next forward recovery action; it adds no candidate
table or second checkpoint. The sole SECURITY DEFINER prepare writer first
validates and locks the exact active bootstrap, ingress config, relation,
publication, operators, states and results. It also verifies the target's
ordinary two-column nullable-bigint shape, single-column bigint primary-key
replica identity, exact single-table publication semantics, caller `SELECT`
permission and absent target slot. Permission is checked for `session_user`,
not the definer identity.

After exact-old CAS succeeds, one transaction replaces the old binding with
the target relation, two columns and exact primary-index ObjectAddress, installs the
target publication/slot/generation, rebinds SumInt8 to target column sub-ID 2,
deletes current rows and the WAL continuation, resets both operator states to
zero, clears old source/ingress invalidations, publishes `building/NULL`, and
marks the same lifecycle `rebuild_prepared`. Pre-M12 active state has the proved
three-row relation-plus-columns binding. Every M12-produced generation has four
exact rows, including `binding_kind='identity_index'`. Its retired
BootstrapId/slot/generation triple persists through later phases and is the
durable discriminator, not a row-count guess or live-catalog inference.
Deferred exact-ingress constraints permit this one atomic replacement
without exposing an intermediate authority.

Same-OID index rename permits only narrow reconciliation after validating that
it remains the current default primary key. Replacement OID fails closed;
recovery cannot discover and substitute a different index. The existing
`source_binding` table remains the sole identity authority.

The old inactive physical slot still exists and the target physical slot does
not. Catalog SQL neither drops nor creates either slot. The earlier
PG17.10/PG18.4 gate proved that failed preflight/CAS leaves its authority
snapshot unchanged,
two concurrent preparations have one winner, and success exposes only the
state above. The corrected four-row identity gate first exposed invalid
PL/pgSQL `IF CASE` syntax on PG17, then passed on PG17.10 and PG18.4 after the
fix.

## M12.3 activated rebuild authority

PG17.10 and PG18.4 `scripts/test-m12-rebuild-snapshot-live.sh` prove that the
same generation-3 exact-four binding/config prepared above remains the sole
authority through real exported-snapshot scan, WAL catch-up, activation and
ordinary live Apply/ACK. Activation changes lifecycle and result visibility;
it does not install binding/config again. The retired generation-2 triple is
preserved in the active row, and no old continuation is copied into generation
3. M12.4 still owns crash recovery between these durable transitions.

## M12.4 abandoned-attempt replacement

`source_bootstrap` remains the sole lifecycle and checkpoint authority. For a
marker-null M11 attempt, `replace_pristine_source_bootstrap` keeps its existing
narrow recovery contract. For an M12-marked `creating`, `scanning`,
`cleanup_pending`, or `failed` attempt, the same writer additionally requires
the durable abandoned BootstrapId/slot/generation, a different slot, and exact
`old_generation + 1`. It atomically clears partial rows, private state and
checkpoint, installs the fresh attempt, and records the abandoned triple as the
next retired identity.

Recovery reads and validates the durable four-row target binding before
mutation; it cannot select a current primary key. Missing, stale, malformed or
replacement-OID identity fails closed while results remain `building/NULL`.
This is forward replacement of an ephemeral snapshot, not a fallback or new
catalog authority.

## M12.5 target governance and role boundary

`scripts/test-m12-rebuild-governance.sh` is green on PG17.10 and PG18.4. It
proves that the sole prepare writer rejects relation, publication, primary-key
identity-index, replica-identity, payload-column and operator-plan drift by
durable ObjectAddress, not name. The exact identity-index OID is held with
`AccessShareLock` while `pg_relation_size` verifies it, so a concurrent
replacement cannot pass an unlocked shape check. An unrelated index change and
a same-OID rename do not pollute this source; a replacement OID or
post-prepare invalidation leaves the sole target authority `building/NULL` and
cannot activate it.

The writer remains the sole lifecycle/config/binding writer. Preflight does not
take row-update locks on `operator_definition` or ingress config when it only
reads them. The common source fence gives competing rebuilds one CAS winner
without turning different sources into a global lock. A non-superuser
control/Apply/scanner role succeeds with `NOREPLICATION`; separate trusted
transport has `REPLICATION` plus target `SELECT`; reader remains public-result
read-only. Missing control function/table/source privileges and role exchanges
fail closed. A compromised trusted replication credential remains the stated
residual physical-slot risk.

## M12.6 catalog acceptance boundary

The performance and release gate adds no catalog object, cursor or writer. A
million-row rebuild must continue to use the sole `source_bootstrap` lifecycle,
the sole exact-four target binding, the one ingress config, one WAL continuation
and the existing operator state/result rows. The 10,000-change catch-up and
activation therefore exercise the same transaction and authority boundaries as
M12.3--M12.5; measurement state is test output, never durable Catalog state.

The release matrix must re-prove cardinality, generation, visibility and old-
generation rejection on PG17 and PG18. Frozen limits are scan <= 12 s,
catch-up <= 8 s, activation <= 2 s, total <= 25 s, RSS growth <= 128 MiB and
retained WAL <= 256 MiB. PG17.10/PG18.4 complete in 6.343139667/6.375927458 s,
retain at most 252,864,952/252,898,072 bytes, and pass all 96 PG invocations.
No measurement row or additional Catalog state is created.
