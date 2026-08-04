# ADR 0009: M16 admission bounds and schema authority

Status: accepted

M16 keeps the proven linear Aggregate runtime and rejects unsupported DAG
shapes at both QuerySpec/compiler and OperatorGraph admission. An Aggregate
may consume only Source through Filter, Project, Compute or KeyBy and must
have exactly one direct Materialize. Aggregate fan-out and Aggregate-to-
Aggregate edges never reach Runtime.

HAVING has one shared bounded contract: 256 total nodes, depth 32 and 64
boolean terms. SQL frontend validation, QuerySpec admission, compiler
lowering and database-free evaluation use these same values. Aggregate work
uses one operator-owned budget contract for touched groups, state keys,
partitions, extrema values, state mutations and estimated bytes; Runtime
imports those constants rather than maintaining a second limit.

`ResultSchemaV1` is the sole result-shape authority. It rejects duplicate or
overlong identifiers and requires key ordinals to be the ordered complete
prefix. `TypedLayout` carries source-derived nullability, and compiler plus
direct graph construction reject a field/nullability mismatch before any
transaction can write state or results. No compatibility view, alias,
fallback, second writer or new durable authority is introduced.

This decision does not implement AVG, variance/stddev, Numeric/Decimal,
DISTINCT, CTE, window functions or a general DAG executor.
