# ADR 0005: one typed graph and one transaction authority

Status: accepted for M14.1.

## Context

M13 proved a generic scalar/keyed sink and isolated concrete operator dispatch,
but its durable authority is still a flat per-source plan set. That shape cannot
express typed edges, keyed intermediate state or a two-source join, and its
source continuation cannot identify a graph-wide transaction.

## Decision

Adopt one canonical `OperatorGraph` as the sole durable plan authority. It owns
one or two exact source members, canonical nodes/ports/edges, explicit terminal
materializations, state/output contracts, limits and one digest. A source may
belong to only one building or active graph. Nodes and edges are not separately
mutable plan authorities.

Use typed transaction-local rows and delta batches. Runtime schedules canonical
topological order, loads generic keyed state, calls the database-free kernel and
persists generic state/results. Intermediate deltas are never durable.

Replace the source continuation with one graph/generation continuation during
the M14.6 schema cutover. A two-source transaction is assembled once from one
publication/slot/generation and committed once; per-source or per-node progress
is forbidden. Bootstrap uses one exported snapshot and rebuild switches the
whole graph through the existing forward-only lifecycle.

ProjectRows becomes Project plus Materialize. No adapter, compatibility view,
dual plan authority, second Runtime or second continuation survives the
cutover.

## Consequences

Graph registration is atomic and cannot mutate an active historical plan.
Source, state and result locks have a canonical order; complete transaction
retry follows deadlock or serialization failure. Internal node outputs are not
public results. Graph/state/result fan-out has hard row and byte bounds.

M14 remains limited to boolean/bigint expressions, grouping and a two-table
bigint equality INNER JOIN in one database. SQL parsing and broader relational
algebra remain outside the decision.
