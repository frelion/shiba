# ADR 0006: one graph-wide authority for a two-source JOIN

Status: accepted for the M14.4 contract slice; M14.5/M14.6 implementation
evidence pending.

## Context

M14.3 proves typed single-source graphs and generic keyed aggregate state. A
JOIN cannot safely inherit two source-scoped progress streams: independent
continuations or snapshots could observe different database boundaries, split
one PostgreSQL commit, publish a partial join result, or ACK one side early.

## Decision

Admit exactly two explicit SourceIds in the same database and bind them to one
canonical graph, publication, logical slot and slot generation. One pgoutput
transaction contains all admitted changes from both members of the committed
PostgreSQL transaction. Progress and feedback authorization are graph-scoped;
per-source continuations and ACK decisions are forbidden.

The admitted compiled shape is fixed: nullable `left.right_key` equals the
exact non-null bigint right PK/UK, then Project and Materialize emit
`left.id -> right.payload`. NULL join keys do not match and a matched NULL
payload stays typed NULL. That PK/UK is also the right source's exact effective
replica identity index, so logical UPDATE/DELETE old keys and Join lookup
identity cannot diverge. Broader projections remain outside M14.

A source belongs to at most one building or active graph. The graph durably
binds exact relation/column ObjectAddresses and the exact right bigint PK/UK
index ObjectAddress. Bootstrap uses one exported snapshot for both relations;
rebuild replaces the whole graph generation. No member can independently
activate, rebuild or resume.

Runtime retains one transaction and the fixed lock order: graph/generation,
replay, ascending SourceId bindings, source/key rows, node/state keys, pure
compute, state/results, continuation, commit, then ACK. Right-side fan-out is
bounded and set-based. Intermediate DeltaBatch values are never durable.

## Consequences

This design preserves one recovery direction and makes same-transaction
two-side changes atomic. It rejects source reuse, publication/generation drift,
ObjectAddress or right-index replacement, and partial lifecycle operations.
It intentionally excludes separate source continuations, a second Runtime,
persisted deltas, adapters, fallbacks and dual writes.

The decision does not claim implementation. PG17.10/PG18.4 admission,
differential, crash/replay, DDL, bootstrap/rebuild, least-privilege, fan-out and
performance gates in `JOIN_AUTHORITY_CONTRACT.md` must close before M14.4 can be
reported as proved.
