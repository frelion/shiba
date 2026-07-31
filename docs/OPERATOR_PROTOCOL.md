# Shiba Operator Protocol

This document is the compatibility contract for bounded operator execution.
It describes the control protocol only; typed SQL state and operator-specific
SQL plans remain owned by each operator.

## Stable lifecycle

Every durable primitive is classified as one of four semantic phases:

| Phase | Input | Durable work | External output | Continuation |
| --- | --- | --- | --- | --- |
| `Admit` | A prefix of one immutable input chunk | Apply input rows to typed state | Usually none | Keep the next input position, or finish the prefix |
| `Process` | A bounded action/candidate prefix | Apply one planned logical action | `Data` or none | Keep the next action, or finish the action set |
| `Drain` | Durable dirty state, not a new input prefix | Rebuild, compare, clean up, or emit state | `Data` or none | Keep the durable cursor, or finish the drain |
| `Frontier` | A consumed input frontier and completed drain | Advance frontier state | A `Frontier`, or none while bootstrapping a drain | Normally finished; a bootstrap may continue into `Drain` |

`KernelPhase`/`LifecyclePhase` is a validation label, not a forced implementation enum. A
concrete operator may have many internal phases inside `Process` or `Drain`.
The shared rule is that a frontier cannot be emitted from `Admit`, `Process`,
or `Drain`, and a frontier output cannot leave a continuation behind.

The current operator mapping is intentionally non-uniform:

| Operator | Semantic phases used |
| --- | --- |
| Scan / Filter / Project | `Admit` and `Process`-like bounded input work |
| Sink | `Process` and `Frontier` |
| Distinct | `Admit`, `Drain`, `Frontier` |
| Join | `Process`, `Frontier` |
| Aggregate | `Admit`, `Drain`, `Frontier` |
| Window | `Admit`, `Drain`, `Frontier` |
| TopN | `Admit`, `Drain`, `Frontier` |

This preserves each operator's SQL semantics while giving tests and reviewers
one vocabulary for input consumption, state mutation, output summaries,
continuation count, and completion.

## Transition and transaction results

`StepReceipt` is the only pre-commit result. It contains the lifecycle phase,
measured `WorkUsage`, a progress witness, and context-derived effects:

- `Continue`: the typed continuation remains present and the checkpoint will
  be schedulable again.
- `Finished`: the typed continuation is absent and the current input/action
  is complete.

`StepExecution` is a post-transaction result. Its `TransactionResult` is the
commit fact, independent of scheduler policy:

| Transaction result | Scheduler outcome | Meaning |
| --- | --- | --- |
| `Committed` | `Yield` | `Continue` committed with durable continuation |
| `Committed` | `Progress` | `Finished` committed with no continuation |
| `NotCommitted` | `Idle` | Admission found no input and no continuation |
| `NotCommitted` | `Blocked` | Admission found a durable dependency or backpressure |

The last two outcomes are produced before the operator function is invoked.
An operator cannot return `Idle` or `Blocked` through `StepReceipt`, and
an error after a write aborts the PostgreSQL transaction instead of producing
a successful step result. This is the invariant that prevents a persisted
write from being reported as idle or blocked.

`Progress` is the existing public scheduler meaning of a finished committed
step; `Finished` is the more precise pre-commit completion term. No existing
entry point or scheduler outcome was renamed.

## Primitive facts

`PrimitiveFacts` remains the stable SQL-to-Rust summary:

- `usage` reports input and output row/byte consumption;
- `state_rows` reports typed durable state changes;
- `output` reports no chunk, a data chunk, or a frontier chunk.

The step boundary produces the `StepReceipt`. Its progress witness set
records why a committed step is real progress (`InputAdvanced`, `StateChanged`,
`OutputAppended`, `FrontierAdvanced`, `ContinuationChanged`, or
`ActionCompleted`). Multiple durable witnesses may be present in one receipt;
a continuing step cannot use `ActionCompleted` as its only witness. `StepContext` is the authority for
continuation and output mutations; kernels do not construct the receipt or
choose output chunk sequences directly. SQL primitives that publish a data
chunk must report that publication through
`StepContext::record_output_append` before returning their facts. This records
the bounded data append in the step context; the shared commit path performs
the effect-stream append after the payload has been written and before the
checkpoint CAS. Frontier publication follows the analogous
`record_frontier_output` boundary.

Callers use `validate_protocol(budget, phase, completion)` for SQL primitive
facts. The common validator checks budget dimensions, output metadata, and
phase/output compatibility. Continuation presence is derived from the
`StepContext` mutation authority, not reported by primitive SQL facts. Operator
code retains only SQL-specific checks such as join multiplicity, aggregate
queue identity, or window cursor rules.

Zero-output progress is valid when durable state or a continuation changes;
the transition-count budget still bounds repeated metadata-only steps.

## Continuation and crash replay

Continuation handling has one authority path:

1. `StepContext` locks the operator checkpoint and records its continuation
   presence bit.
2. Each typed continuation loader validates relation ABI, decodes fields, and
   validates phase shape and input coordinates.
3. `replace_continuation_cas` deletes the expected old singleton and inserts
   the next singleton in the same transaction. `StepContext` verifies that the
   old presence bit was bound before replacement and records the new bit.
4. The checkpoint CAS commits only after state, input cursors, output
   publication, and continuation replacement have all succeeded.

Therefore a crash before commit replays the old continuation and the same
action; a crash after commit observes the new continuation and cannot apply the
old action again. The persisted continuation schema and existing SQL CAS
semantics are unchanged.

## OperatorProtocol trait decision

No broad `OperatorProtocol` trait is introduced at this stage. The existing
`Kernel`/`KernelFn` contract already centralizes dispatch, admission, commit,
and transition generation. A trait that also unified continuation decoding,
action planning, SQL fact application, and transition generation would need
operator-specific associated types and would move SQL-specific invariants into
generic bounds. That would increase coupling without changing the durable
protocol. Those operations remain explicit in each operator's state machine,
while shared `KernelContract`, `StepContext`, `PrimitiveFacts`, and
continuation helpers enforce the common boundary.

## Structural gates and regression coverage

The contract surface and clean-cut gates verify that operator implementations:

- do not call `StepContext::commit`, create a `StepReceipt` directly, or
  start/commit PostgreSQL transactions;
- use the shared transition path and expose continuation ABI validation;
- retain the common `KernelRunner` entry point and persistence schemas.

Protocol-level unit tests cover oversized indivisible rows, zero-output
progress, completion/continuation mismatch, frontier ordering, continuation
shape mismatch, and transaction commit-state mapping. Existing operator tests
cover crash-before-commit replay, post-commit replay, budget exhaustion,
backpressure, frontier ordering, and per-operator continuation/state
inconsistency for every operator family. PostgreSQL acceptance scripts remain
the final behavior gate.
