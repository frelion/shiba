# Incremental joins

The join executor accepts one two-input equality join. Each input owns a
multiplicity-preserving arrangement keyed by its join column. An incoming row
delta probes the opposite arrangement, emits one joined delta per matching row
times its multiplicity, and only then changes its own arrangement.

```text
left delta  -> left arrangement  -> probe right arrangement -> joined delta
right delta -> right arrangement -> probe left arrangement  -> joined delta
```

Updates are represented as an old-row `-1` followed by a new-row `+1` within a
single WAL commit batch. The downstream aggregate sees only these joined
deltas.

`LEFT`, `RIGHT`, and `FULL` joins track the opposite-side total for each key.
The first match retracts the preserved NULL-extended row; removing the last
match restores it. SQL NULL keys never equality-match.

Decorrelated `EXISTS`/`IN` use a semi join and emit left rows only when
right-side multiplicity crosses `0 -> 1`. Anti joins invert that transition.
`NOT IN` additionally tracks the right input's total and NULL count to
implement PostgreSQL's null-aware semantics.

Filters that reference one safely pushable input are evaluated before its
arrangement. Cross-input and null-supplying-side outer-join predicates run
after join row construction. Equality keys currently require identical
PostgreSQL types and deterministic collations; non-equality joins are rejected.
