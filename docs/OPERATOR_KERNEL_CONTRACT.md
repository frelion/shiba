# M13 generic Operator Kernel contract

## Scope and ownership

M13 replaces the M9 aggregate-shaped execution API; it does not add a second
Runtime. `shiba-operator` owns deterministic, database-independent plan and
state codecs plus pure transitions. `shiba-compiler` is the only component that
turns strict declarative specifications and a supplied `SourceDescriptor` into
a compiled plan. Neither crate connects to PostgreSQL or executes SQL.

`compile_and_register` is the sole writer of operator definitions, compiled
plans and their input bindings. Runtime is the sole writer of operator state
and result values. Catalog tables are the sole durable authority. An
`EffectBatch` and an `OperatorTransition` exist only inside one processor-owned
PostgreSQL transaction; neither is logged or used as replay authority.

Ingress, bootstrap and rebuild deal only in source identity and an ordered
compiled-plan set. They may not branch on a concrete operator name, assume IDs,
assume a plan count, or synthesize a plan.

## Frozen contracts

Every `CompiledPlan` contains a format version, operator and source identities,
ordered exact `ObjectAddress` input bindings, a state contract, an output
contract, canonical payload bytes, and a domain-separated digest over all of
those fields. Unknown fields, versions, codecs, contracts or trailing bytes
fail closed. Names occur only in the declarative specification and live source
descriptor; durable input identity is an ObjectAddress. The declaration and
compiled representation are one definition authority, not competing plans.

`OperatorState` is an opaque `(codec_version, payload)` to Runtime. Decode is
strict and deterministic. Runtime never selects fields or initial values by
operator kind. A transition contains the complete next encoded state and one
of:

- a typed scalar replacement; or
- keyed mutations whose keys and typed nullable values use canonical codecs.

`Null` is a present SQL value and is distinct from `Absent`, which means an
input binding was not carried by that row shape. A keyed upsert of `Null` is
therefore not a delete. Deletes and upserts are explicit variants. Runtime
persists the transition according to the compiled output contract, never a
concrete operator name.

For the admitted maximum of 10,000 effects, a scalar plan emits at most one
replacement and `ProjectRows` emits at most two keyed mutations per effect.
The transaction-wide keyed-mutation limit is 20,000. Conflicting duplicate
mutations for one output key fail closed in M13; no hidden last-write policy is
introduced. Plans and state are decoded once per operator per batch, one
EffectBatch is constructed per source transaction, and keyed writes use a
bounded set operation rather than per-row SQL round trips.

## Implementations behind the kernel

Concrete dispatch is closed inside `shiba-operator`:

- `CountRows` applies checked `+1`, `-1`, or zero from row existence.
- `SumInt8` treats `Null` as contribution zero, rejects `Absent` or the wrong
  type, and uses checked subtraction/addition.
- `ProjectRows` owns a keyed result row for every current source row. INSERT is
  an upsert, DELETE is a withdrawal, and UPDATE withdraws the old key then
  upserts the new key/value. A projected SQL NULL remains an explicit null.

The first non-aggregate slice stays at one bigint identity and one nullable
bigint payload. To represent its required key-changing UPDATE, the sole
pgoutput decoder admits only the minimal already-shaped `U + K(old key) +
N(new key, payload)` extension. Source Apply locks/removes the old row and
writes the new row once, then emits one before/after effect. Composite key
change, `D + O`, replica identity FULL, general expressions and additional
types remain unsupported.

## Transaction, lock and failure order

Ordinary live Apply, snapshot batches, catch-up and rebuild call the same
generic execution entry. For a WAL transaction Runtime:

1. locks the source generation/binding mutex;
2. performs the replay probe;
3. locks and validates the exact relation/binding;
4. applies source changes once and creates one EffectBatch;
5. loads and locks all plans/states in ascending operator ID;
6. validates each plan/digest/state and computes every transition;
7. persists every next state and scalar/keyed output;
8. writes continuation last; and
9. commits before ingress can authorize ACK.

There is no external I/O while these database locks are held. Plan/state
decode, arithmetic, output-bound, sink, constraint, serialization or backend
failure aborts the entire transaction. Retry starts at the transaction
boundary. Exact replay returns before Source Apply and kernel execution.

Registration initializes state and output through the same generic Runtime
writer in its registration transaction. During rebuild, the registration/
compiler service recompiles the complete target plan set after exact target
preflight and installs it in the destructive-prepare transaction. That is the
same definition writer and the sole target authority: there is no candidate
table, kind-specific SQL rewrite or recovery-time reconstruction. After
prepare, recovery accepts only the durable target plans and exact plan-set
digest. Activation changes visibility of that same authority and does not
switch plans or bindings a second time.

## Result visibility

Catalog stores a generic result header/visibility contract, opaque typed scalar
payloads, and operator-owned keyed rows. Public scalar and keyed surfaces expose
only `active` results. Bootstrap/rebuild `building` results remain unavailable;
neither a partially written keyed set nor one successfully computed operator is
visible after another operator fails. The sink performs persistence only and
does not calculate operator semantics.

## Frozen evidence and budgets

M13.2 passed pure reference-model and randomized differential tests for all
three implementations, including corrupt codecs, NULL/Absent, overflow and
amplification. M13.3 proved atomic scalar/keyed PostgreSQL persistence and
rollback. M13.4 re-proved M10--M12 without kind, fixed-ID or fixed-count
knowledge. M13.5 ran the complete PG17.10/PG18.4 matrix and static forbidden-
specialization scan: 49 unique scripts and 98 PostgreSQL invocations, all
green. Five post-change Apply medians are 782.302750/787.157125 ms,
approximately 1.8%/2.2% above the frozen baselines and below the 15% stop
lines.

The pre-change five-run medians are: PG17 decode 9.607167 ms, Apply 768.727625
ms, replay 470.250 us; PG18 decode 9.537708 ms, Apply 770.174125 ms, replay
473.417 us. The corresponding 15% Apply ceilings are 884.036769 ms and
885.700244 ms. Existing absolute M8--M12 limits remain unchanged, including
the 256 MiB retained-WAL cap (M12 evidence is already about 94.2% of it).

M13 targets no more than 1,400 net new production lines: M13.2 at most 850,
M13.3 at most 450, and M13.4 at most 100. Work pauses before exceeding 1,500.
New files target 250 lines, require a stated responsibility reason above 300,
and fail above 400. Catalog `lib.rs` must not grow. These are readability and
responsibility budgets, never grounds to weaken tests or compress formatting.

## Explicit exclusions

M13 does not add SQL parsing, expressions, joins, windows, DAGs, plugins,
schedulers, workers, cross-host failover, another result writer, persisted
effects, a WAL spool, compatibility aliases, fallback or dual writes. It does
not claim complete V2.
