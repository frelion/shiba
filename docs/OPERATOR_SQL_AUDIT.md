# Operator SQL Audit

This is the audit for the single current maritime implementation. It does not
describe, preserve, or migrate an older dataflow implementation.

The audit distinguishes operator algorithmic complexity from protocol coupling.
Dynamic composite types, user schemas, typed row identity, ordering, and
operator-specific state remain local to the operator. The shared boundary owns
only the bounded output/continuation/checkpoint protocol.

| Operator / primitive | Persistent relations and SQL responsibility | Atomicity and causal LSN | Protocol coupling before/after | Decision |
| --- | --- | --- | --- | --- |
| Linear / `run_transform_primitive` | Reads one input payload prefix; writes typed output payload and returns measured rows/bytes. | One transaction is required so input cursor, payload, and facts describe the same prefix. Data LSN is the input chunk LSN. | Before: payload and effect append were in one SQL CTE. After: SQL returns payload facts; `StepContext` records and publishes the data chunk. Continuation and cursor remain Rust-owned. | Split the shared publication boundary; retain predicate/project SQL. |
| Linear / `run_bootstrap_primitive` | Reads the bootstrap relation and writes the initial output payload. | Atomic with bootstrap consumption and activation LSN. | Before: bootstrap SQL appended the effect chunk. After: payload facts go through the same deferred output boundary. | Split publication; retain bootstrap SQL. |
| Sink / `mutate_result_page` | Applies signed actions to the Sink result relation. | Atomic result DML; no effect-stream append and no output payload publication. | Continuation and input cursor are Rust-owned; facts are assembled by the action. | Retain. There is no protocol SQL to extract. |
| Distinct / `run_prefix` | Mutates typed state, bag, touched-key scratch, and durable queue for one input prefix. | Atomic: representative selection, counts, and queue identity must agree before commit. Input LSN is carried by the durable queue. | SQL does not own continuation or output. | Retain operator SQL; it is business algorithm complexity. |
| Distinct / `reconcile_representatives` | Resolves touched representatives and enqueues canonical output differences. | Atomic with representative state and queue; causal LSN is validated from the selected queue work. | No direct effect append; facts include state/queue mutation counts. | Retain; keep named primitive and explicit summary. |
| Distinct / `drain_queue` | Consumes one queue page, writes output payload, and removes exactly the emitted queue rows. | Atomic because payload, queue deletion, and state count must roll back together. SQL returns emitted bytes, one causal LSN, insertion/deletion counts, and remaining work. | Before: SQL also appended the effect stream. After: payload/queue mutation stays SQL-local and `StepContext` owns sequence/LSN publication. | Split publication; retain queue business SQL. |
| Aggregate / `step_apply` and rebuild pages | Mutate dynamic typed aggregate state, dirty groups, ordered bag, work rows, and group queue. | Atomic: dynamic row transition and rebuild cursor must commit together. Causal LSN stays in durable work/state. | Continuation and completion are derived by the Rust action; SQL returns bounded mutation/page facts. | Retain; dynamic row types and ordering prevent a useful generic SQL rewrite. |
| Aggregate / `aggregate_append_output` | Reconciles one pending aggregate row with typed output payload, group state, and dirty-work state. | Atomic with payload and group mutation; causal LSN is returned as text and parsed before context recording. | Before: SQL appended the effect stream and returned append outcome. After: SQL returns payload/state counts; shared context publishes the data chunk. | Split publication; retain typed reconciliation SQL. |
| Window / admission, enumeration, peers, frames, fold | Mutate dynamic ordered input, partition queues, peer/frame state, and aggregate fold state. | Atomic per bounded page; LSN remains in partition/fold state and is validated at output. | No direct continuation/output cursor ownership in SQL. | Retain; these are the window algorithm. Add responsibility comments to long primitives. |
| Window / `run_window_diff` | Compares one visible-output page, writes dynamic payload, and updates visible state. | Atomic because diff, payload, and visible state must agree; selected causal LSN is returned as text. | Before: SQL appended the effect stream. After: SQL returns insertion/mutation/LSN facts and the shared context records publication. | Split publication; retain diff SQL. |
| Window / cleanup | Removes one completed partition’s durable work after diff completion. | Atomic cleanup with its cursor and state. | No direct output append or continuation mutation in SQL. | Retain. |
| TopN / admission and selection | Mutate dynamic ranking state and advance deterministic sort/tie cursors. | Atomic per bounded page; ranking state and cursor must advance together. | No direct effect append; Rust owns continuation and phase. | Retain; sort/tie business complexity is operator-specific. |
| TopN / `run_topn_diff` | Reconciles one visible ranked page, writes payload, and updates visible state. | Atomic; selected causal LSN is returned as text and validated before publication. | Before: SQL appended the effect stream. After: payload/visible mutation returns facts and `StepContext` publishes the data chunk. | Split publication; retain rank/diff SQL. |
| Join / `append_inner_page`, `append_actions` | Writes typed join payload and join-side state for one bounded input/action page. | Atomic: multiplicity, payload, state, and input progress must agree. LSN is the input/event LSN. | Already uses `output_append_target` and `record_output_append`; no direct effect append or checkpoint write. | Retain. Join already matches the target boundary; a rewrite would only move algorithmic SQL. |

## Resulting boundary

Each data-producing SQL primitive now ends at payload/state facts. The shared
`StepContext::record_output_append` records the bounded data append and checks
the output sequence, row/byte occupancy, and causal LSN. The commit path then
publishes the effect-stream chunk before the checkpoint CAS. Frontier publication
uses the separate shared `record_frontier_output` path. No operator SQL calls
the effect-stream append function directly.

The SQL remains deliberately atomic where a payload write, typed state mutation,
queue deletion, or dynamic-row transition would otherwise be observable without
its corresponding facts. The refactor therefore reduces protocol coupling,
not merely SQL line count.

TopN's `next_generation_id` performs a read-only checkpoint revision lookup to
seed an operator-specific ranking generation. It does not mutate checkpoint
state or become a second continuation/completion authority; checkpoint writes
remain in the shared runner path.
