# Operator and compiler contract

This file records the M9 aggregate proof. M13.1 freezes its replacement in
[the generic Operator Kernel contract](OPERATOR_KERNEL_CONTRACT.md). Until the
M13.2--M13.4 replacement commits land, the implementation remains the M9 API;
there will be no compatibility path after cutover.

## Pure boundary

`shiba-operator` owns database-independent operator IDs, ObjectAddress values,
row images, row effects, transaction-local effect batches, compiled kinds, and
checked evaluation. It cannot access PostgreSQL, execute SQL, read a clock, or
own durable state. `shiba-compiler` depends only on Protocol and Operator; it
cannot inspect a database or execute a plan.

An `EffectBatch` is created after Source Apply inside the processor-owned
PostgreSQL transaction. INSERT is `None → row`, UPDATE is `old → new`, and
DELETE is `old → None`. It is never stored in a table or used as replay
authority. Exact replay returns before Source Apply and therefore before batch
construction.

## Version-1 IR

The compact canonical JSON shape is one of:

```json
{"version":1,"operator_id":1,"source_id":1,"operation":{"kind":"count_rows"}}
```

```json
{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"sum_int8","input_column":"payload"}}
```

Unknown fields, aliases, unknown versions, zero IDs, unknown kinds, blank input
columns, and trailing data fail closed. The IR accepts no SQL text. Names exist
only at compile time: `SumInt8` resolves exactly one type-OID-20 column and the
compiled/durable identity retains only its exact ObjectAddress.

## Evaluation

`CountRows` adds one for `None → Some`, subtracts one for `Some → None`, and is
unchanged otherwise. Negative state, underflow, and overflow are errors.

`SumInt8` treats SQL NULL as contribution zero, subtracts the before value, and
adds the after value using checked arithmetic. `Absent` and `Text` fail closed:
an operator compiled for an int8 column cannot silently consume another row
shape. M9.2 proves CountRows and SumInt8 consume the same 10,000-change batch,
publish count 10,000 and sum 15,000 atomically, and roll back together when the
second ordered operator update fails. The fixed row-by-row gate also proves
NULL transitions, DELETE before images, SumInt8 overflow, crash between the two
result updates, retry, and exact replay.

## Durable ownership

`compile_and_register` is the only writer of operator definitions. In one
transaction it locks and validates the source binding, builds a live descriptor,
compiles, inserts the definition, and initializes private state plus public
result to zero. Runtime is the only later writer of state/result. It locks all
operators for one source in ascending operator-ID order, applies the batch once
to each pure operator, publishes each result, and writes continuation last.
