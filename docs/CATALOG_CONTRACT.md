# Catalog contract

## M16.3 aggregate plan authority

M16.3 changes no Catalog table or writer. `graph_definition` now admits only
compiler version 4 and OperatorGraph format 2 because the persisted QuerySpec
and node wire shape changed. The cutover is deletion-only: version 1 aggregate
node bytes and compiler version 3 definitions are rejected, with no decoder,
adapter or dual format. Function identity and descriptors are opaque graph
payload facts to Catalog; no function registry or function-specific state table
exists. `graph_node_state` and the M16.2 wide-result tables remain the sole
state/result authorities.

## M16.2 canonical wide-result authority

M16.2 replaces the fixed scalar/key/value result shape in place. The only
terminal contract header is `shiba.graph_result(graph_id, result_id,
result_status, schema_payload, schema_digest)`. The only materialized row store
is `shiba_internal.graph_result_row(graph_id, result_id, schema_digest,
row_identity, row_payload)`. A composite foreign key requires every row to use
the exact schema digest installed in its header. The active-only
`shiba.graph_result_rows` API exposes schema metadata and complete canonical row
payloads; it contains none of the superseded fixed key/value columns.

`ResultSchemaV1` owns ordered field names, types, nullability and canonical key
ordinals. `TypedResultRowV1` owns a complete schema-bound row. Scalar output is
the same row authority with the one canonical singleton identity; keyed output
derives its canonical identity from the schema key fields. Registration writes
the schema and its operator-supplied initial scalar row atomically. Runtime is
the sole row writer and applies generic ReplaceScalar/Upsert/Delete mutations
without interpreting a node or aggregate function. Rebuild installs the target
schema while building, clears old rows, keeps building rows private and
publishes the same authority only at activation.

The old shape, scalar bigint/payload, result key/value columns and keyed-only
sink are deleted. There is no compatibility view, adapter, dual write, function
registry or second result authority. Generic Count/CountStar/Sum execution now
reuses these tables; MinInt8/MaxInt8 now use the same authority, and HAVING
remains the subsequent M16 slice.

## M14.6 graph execution authority

M14.6 replaces the M13 per-operator execution layout in place. The sole durable
plan is `graph_definition`: strict declaration bytes, one canonical
`OperatorGraph` payload/digest, graph format/compiler versions, source count and
state codec. `graph_source_member` supplies the complete ordered one- or
two-SourceId membership and enforces that a source belongs to at most one
building or active graph. Registration/compiler code is the sole writer of the
definition and membership; neither Runtime nor lifecycle code reconstructs a
plan from node names.

Each source member has one exact effective `identity_index` binding in the
existing `source_binding` authority. Registration resolves either the default
primary-key index or an explicit unique replica-identity index, persists its
`pg_class` ObjectAddress, and rejects missing, multiple, partial, expression,
invalid or non-ready identity. This rule applies to every single- and
two-member graph, not only the JOIN right side.

`graph_node_state` is Runtime's sole scalar/keyed private state authority.
Scalar state uses the graph contract's unit key; keyed state uses canonical
partition/item payloads. `shiba.graph_result` owns terminal output contracts,
building/active visibility; `shiba_internal.graph_result_row` owns every
private scalar/keyed typed row and
`shiba.graph_result_rows` exposes only active rows. Runtime is
the sole state/result writer and persists `GraphTransition` generically. It
does not match Filter, aggregate, Join or Materialize names.

`graph_ingress_config`, `graph_ingress_source` and
`graph_ingress_invalidation` bind one database-local publication, slot and
generation to the exact graph and all ordered members. `graph_continuation` is
the only compute/replay authority for that graph/slot generation.
`graph_bootstrap` is the sole bootstrap/rebuild lifecycle and exported-snapshot
authority; `graph_bootstrap_checkpoint` contains bounded child scan progress
per member, not a second lifecycle or cursor. Source binding, invalidation and
current rows remain the only SourceId-scoped facts.

The cutover removes `operator_definition`, `operator_state`,
`operator_node_state`, `operator_result`, `operator_result_row`,
`source_continuation`, `source_ingress_config`,
`source_ingress_invalidation`, and `source_bootstrap`. None survives as a view,
alias, adapter or dual-write target. Catalog and directed graph Runtime tests
are green on PG17.10/PG18.4; M14.7 closes the full lifecycle
release/performance evidence.

Failure-first integration exposed and fixed four contract defects. Keyed sink
upserts now persist the canonical typed `value_payload` together with SQL
projection columns. Rebuild prepare binds all 22 target coordinates/contracts,
including the final value-nullability array, so a missing last argument cannot
silently alter result identity. The member-cardinality trigger uses a valid
`COALESCE(NEW.graph_id, OLD.graph_id)` expression for INSERT/UPDATE/DELETE.
Finally, explicit rebuild may admit an already-invalid old authority as the
reason to move forward, but destructive prepare installs exact target
identities and any invalidation committed afterward blocks scan, Apply and
activation.

Identity cardinality is shape-specific and fail closed. A zero-column
single-member CountRows graph may have no identity binding because it has no
keyed row layout. Proven composite identities remain valid for their
single-member source shape. Every nullable-int8 graph member, and specifically
the JOIN right member, requires its exact effective identity; the JOIN right is
exactly one non-null bigint PK/UK used as replica identity.

The M14 lifecycle evidence closure adds no Catalog object or writer. Its real
PG17.10/PG18.4 path observes the same `graph_bootstrap`, per-member checkpoints,
`graph_continuation`, graph result/state and generation CAS from initial
snapshot through live ACK, replay and non-pristine whole-graph rebuild.

## M13 generic operator authority (historical, superseded by M14.6)

M13.3 replaced the aggregate-specific layout rather than migrating or mirroring
it. Each row in the sole `operator_definition` authority contains the strict
declaration bytes, canonical compiled-plan payload, format version, exact
32-byte digest, state codec and scalar/keyed output contract. Ordered exact
ObjectAddress inputs are inside the canonical plan payload; they are not a
second table or plan authority. `compile_and_register` is the sole writer and
operator ID supplies the durable lock/execution order. Concrete operator names
are absent from SQL constraints and workflow.

Runtime remains the only state/result writer. `operator_state` is an opaque
versioned payload tied structurally to the definition codec. The generic
`operator_result` header owns building/active visibility and scalar storage;
`operator_result_row` owns keyed `(operator_id, key, nullable value)` rows.
The public keyed view joins only active keyed headers, so building rows cannot
leak a partial projection. Registration and rebuild initialization call the
same generic Runtime writer; Catalog SQL does not infer a concrete zero, patch
an input by kind, or reset state independently.

At M12 destructive prepare, the registration/compiler writer installs the
complete target plan set in ascending operator-ID order in the same transaction
that makes the target the sole building authority. Recovery validates every
canonical payload/digest/input/output contract in that durable set; activation
only publishes the same authority. M13.4 re-proved this handoff for arbitrary
plan cardinality, including `ProjectRows`, across bootstrap, catch-up and active
rebuild on PG17.10/PG18.4. There is no candidate
table, compatibility view, dual write or recovery-time kind reconstruction.
Those table names describe the M13 evidence baseline only; the current graph
authority is the M14.6 layout above.

## Installation authority

The catalog extension installs one constrained row containing catalog and
protocol version `1`. This is the only Phase-1 durable authority. It belongs to
the PostgreSQL database in which `CREATE EXTENSION` executes, even if that
database hosts multiple application schemas. Names are not identity, and no
legacy catalog table, publication, or change log participates.

## Installation transaction

`CREATE EXTENSION` runs the extension SQL as one PostgreSQL transaction. It first
creates the schemas, constrained identity, and restricted version function, then
installs source facts and graph definition/membership, ingress, continuation,
lifecycle, node-state and result authorities without a seeded graph. If
installation fails, PostgreSQL rolls back the whole installation; the
empty-install test must prove there is no partial `shiba` or `shiba_internal`
state. Extension upgrade,
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

## M9.1 historical operator authority (superseded by M13.3)

M9.1 originally introduced `operator_definition`, `operator_state`, and
`shiba.operator_result`; it did not create or later drop old count tables. Its
definition rows froze compiler version, source ID, concrete kind and its input
ObjectAddress. M13.3 replaced that aggregate-shaped schema in place with the
generic authority described above; the old kind/input columns, constraints and
execution path no longer exist.

The enduring M9 invariant is writer ownership: registration is the sole
definition writer and Runtime alone updates state/result. Internal
definition/state have no PUBLIC privileges; public sink surfaces grant SELECT
only. Installation seeds no operator; explicit generic registration creates a
definition, initial opaque state and matching output header atomically.

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
once. It never creates or drops a physical slot. M12 provides the separate,
explicit active/non-pristine rebuild lifecycle; pristine rotation does not
silently enter it.

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
building/unavailable as `result_status = building`; its private typed rows are
not exposed by the active-only result API;
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
manual catalog updates. Indefinite writer catch-up remains outside this
authority; M12 subsequently proved non-pristine rebuild through the same sole
lifecycle and Catalog authority.

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
the target relation, its dynamically resolved key/payload columns and exact
primary-index ObjectAddress, installs the target publication/slot/generation,
installs and initializes the complete generic compiled-plan set through the
sole registration/Runtime writers, deletes current rows and the WAL
continuation, clears old source/ingress invalidations, publishes every generic
result header as `building/NULL`, and marks the same lifecycle
`rebuild_prepared`. Pre-M12 active state has the proved
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

The release matrix re-proved cardinality, generation, visibility and old-
generation rejection on PG17 and PG18. Frozen limits are scan <= 12 s,
catch-up <= 8 s, activation <= 2 s, total <= 25 s, RSS growth <= 128 MiB and
retained WAL <= 256 MiB. PG17.10/PG18.4 complete in 6.343139667/6.375927458 s,
retain at most 252,864,952/252,898,072 bytes, and passed all 96 M12 PG
invocations. M13's final matrix subsequently passed 49 unique scripts and 98
PG invocations without adding measurement rows or Catalog state.
