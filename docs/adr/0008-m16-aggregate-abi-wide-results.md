# ADR 0008: versioned aggregate ABI and canonical wide results

Status: accepted; canonical wide results landed in M16.2, Count/CountStar/Sum
in M16.3, multi-call in M16.4, exact MinInt8/MaxInt8 in M16.5, and restricted
grouped HAVING in M16.6.

## Context

M14 made graph execution generic and M15 proved bounded SQL declarations, but
aggregate semantics remain represented by concrete Count/Sum node kinds and
the public Result Sink still assumes scalar or key/value shapes. Adding Min,
Max, several aggregate calls, or HAVING by extending matches in Binder,
Compiler, Runtime, Bootstrap, Rebuild and SQL schema would recreate the
specialization M13 removed.

Min/Max also expose a recovery-sensitive fact: exact retraction requires value
multiplicity and a bounded ordered successor/predecessor lookup. Retaining only
the current extreme is incorrect after DELETE or UPDATE.

## Decision

Adopt the contract in
[`AGGREGATE_FUNCTION_CONTRACT.md`](../AGGREGATE_FUNCTION_CONTRACT.md):

- a closed `AggregateFunctionV1` and unique immutable descriptors;
- canonical `AggregateCallId` identity and code-owned state/output semantics;
- one Aggregate-node group expression set, kernel membership namespace `0`,
  and one-based ordinal-derived Runtime-opaque call namespaces;
- pure transition, retract and finalize dispatch only in `shiba-operator`;
- generic exact and bounded ordered state requests serviced set-wise by Runtime;
- canonical `ResultSchemaV1` and `TypedResultRowV1` for scalar and keyed wide
  output, including durable public output field names/aliases;
- HAVING as a pure post-finalize visibility transition;
- unchanged processor transaction, continuation, recovery and ACK rules.

Binder maps SQL names. Compiler consumes descriptors and produces the only
bound graph/schema. Runtime, Catalog, Ingress, Bootstrap, Rebuild and Result
Sink remain function-independent. M16 may replace the clean-room result schema
in place, but it may not dual-write or preserve a compatibility authority.

## Rejected alternatives

- Add `MinInt8`, `MaxInt8` and every aggregate combination as QuerySpec graph
  recipes: this leaks function names and grows combinatorially.
- Let Runtime or SQL functions calculate aggregates: this creates a second
  compute owner and SQL workflow.
- Store only the current Min/Max: exact retraction cannot find the successor.
- Rescan the source table or an entire group: this violates transaction and
  boundedness contracts.
- Persist intermediate aggregate/HAVING deltas: this creates another durable
  authority and recovery protocol.
- Introduce a dynamic plugin registry now: determinism, trust, codec upgrade
  and resource authority are not proved.
- Keep scalar/key/value columns beside wide rows: that is dual-write and makes
  recovery ambiguous.

## Consequences

The initial ABI is intentionally closed and limited to CountStar,
Count(nullable `int8`), SumInt8, MinInt8 and MaxInt8. It requires descriptor
golden vectors, per-function
reference models, state codec corruption tests, bounded ordered state reads,
wide schema/row canonical tests and full PG17.10/PG18.4 lifecycle evidence.

M16.1 changed no production behavior. Its database-free reference cases are
supplemented by M16.5 production descriptor, multiplicity, corruption and
PG17.10/PG18.4 SQL lifecycle evidence. M16.7 must statically prove
concrete aggregate dispatch is confined to `shiba-operator` and that no
compatibility codec, registry table or function-aware lifecycle/sink path
survives.
