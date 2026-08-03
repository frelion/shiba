# ADR 0004: generic Operator Kernel and typed result sink

Status: accepted for M13.1.

## Context

M9 proved transaction-local effects and two aggregate implementations, but the
durable state and result shape is a bigint and Runtime/bootstrap/rebuild still
know the concrete operator kinds and fixed IDs. That boundary cannot express a
keyed non-aggregate result without leaking another special case into the
transaction owner.

## Decision

Use one canonical, versioned `CompiledPlan`, opaque versioned state, and a pure
`OperatorTransition`. The compiled output contract selects either a typed
scalar replacement or bounded keyed mutations. Only `shiba-operator` performs
closed concrete-plan dispatch. Runtime validates codecs and persists the
declared shape generically in the existing processor transaction.

Definitions, compiled payloads, ordered ObjectAddress bindings, state and
result remain one Catalog authority. Registration/compiler remains definition
writer; Runtime remains state/result writer. Rebuild compiles and installs the
target plan set at destructive prepare through that same writer, then recovery
uses only its durable digest. No candidate plan authority is introduced.

`ProjectRows` is the first keyed proof. Its deliberately narrow key-changing
UPDATE uses old and new bigint keys from the sole pgoutput decoder and Source
Apply path. This is sufficient to prove withdrawal/upsert semantics without a
general expression or row-identity framework.

## Consequences

All concrete names and cardinality assumptions leave Runtime, Ingress,
Bootstrap, Rebuild and SQL workflow. Scalar and keyed writes roll back with
source rows, all operator states/results and continuation. Strict codecs and
bounds can reject work that the M9 bigint API could not represent; rejection is
fail closed and never authorizes ACK.

The schema is clean-room and has no users, so M13 replaces the old aggregate-
specific authority atomically with no compatibility view, adapter or dual
write. SQL frontend, broader types and multi-relation operators remain future
work.
