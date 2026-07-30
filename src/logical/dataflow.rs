//! Bounded scheduling primitives for resumable dataflow stages.
//!
//! A [`ReadyQueue`] only caches operator IDs. The authoritative input
//! frontiers and continuation stay in PostgreSQL and are represented here by
//! [`DurableOperatorState`]. After a restart the queue is rebuilt from those
//! durable rows, so losing the queue cannot lose work.
//!
//! An operator step must commit its state changes, input frontiers,
//! continuation, and emitted effects together before returning an outcome.
//! The step boundary is determined only by row and byte counts. A wall-clock
//! deadline may stop the outer Runtime between steps, but never changes where
//! a step checkpoints.

use std::collections::{BTreeSet, HashSet, VecDeque};

/// A stage in one maintained result-table dataflow.
///
/// `stage_id` is the stage's array position in the persisted `DataflowPlan`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct OperatorId {
    pub(crate) result_oid: u32,
    pub(crate) stage_id: u32,
}

impl OperatorId {
    pub(crate) const fn new(result_oid: u32, stage_id: u32) -> Self {
        Self {
            result_oid,
            stage_id,
        }
    }
}

/// A durable position in one ordered effect stream.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct StreamSequence(pub(crate) u64);

/// The consumed and currently available positions for one operator input.
///
/// Fan-in operators keep one value per input port. They are runnable when any
/// port has data, rather than waiting for all ports to reach the same batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InputFrontier {
    pub(crate) port: u16,
    pub(crate) consumed: StreamSequence,
    pub(crate) available: StreamSequence,
    /// The database has published a later complete source LSN even when no
    /// data chunk exists on this input.
    pub(crate) frontier_pending: bool,
}

impl InputFrontier {
    pub(crate) const fn has_pending(self) -> bool {
        self.available.0 > self.consumed.0 || self.frontier_pending
    }
}

/// The durable facts needed to decide whether one operator can run.
///
/// `outputs_have_capacity` is derived from persisted stream and consumer
/// frontiers. It is false while downstream backpressure is active. This core
/// does not choose high/low watermarks; the effect-stream persistence layer
/// must compute and store that hysteresis before Runtime integration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DurableOperatorState {
    pub(crate) operator: OperatorId,
    pub(crate) inputs: Vec<InputFrontier>,
    pub(crate) has_continuation: bool,
    pub(crate) outputs_have_capacity: bool,
    pub(crate) active: bool,
}

impl DurableOperatorState {
    pub(crate) fn is_runnable(&self) -> bool {
        self.active
            && self.outputs_have_capacity
            && (self.has_continuation || self.inputs.iter().any(|frontier| frontier.has_pending()))
    }
}

/// Deterministic target limits for one operator checkpoint.
///
/// A single indivisible transition may exceed a target and occupy one step by
/// itself. Otherwise valid work is split and resumed instead of rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WorkBudget {
    pub(crate) max_input_rows: usize,
    pub(crate) max_input_bytes: usize,
    pub(crate) max_output_rows: usize,
    pub(crate) max_output_bytes: usize,
}

impl WorkBudget {
    pub(crate) fn new(
        max_input_rows: usize,
        max_input_bytes: usize,
        max_output_rows: usize,
        max_output_bytes: usize,
    ) -> Self {
        assert!(max_input_rows > 0, "input-row budget must be positive");
        assert!(max_input_bytes > 0, "input-byte budget must be positive");
        assert!(max_output_rows > 0, "output-row budget must be positive");
        assert!(max_output_bytes > 0, "output-byte budget must be positive");
        Self {
            max_input_rows,
            max_input_bytes,
            max_output_rows,
            max_output_bytes,
        }
    }
}

/// Logical rows and bytes consumed and emitted by committed kernel work.
///
/// Runtime owns this type because it is the common currency for one
/// transaction quantum. Kernels report it; the scheduler decides whether
/// another transition still fits before committing the surrounding
/// PostgreSQL transaction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WorkUsage {
    pub(crate) input_rows: u64,
    pub(crate) input_bytes: u64,
    pub(crate) output_rows: u64,
    pub(crate) output_bytes: u64,
}

impl WorkUsage {
    pub(crate) const fn is_empty(self) -> bool {
        self.input_rows == 0
            && self.input_bytes == 0
            && self.output_rows == 0
            && self.output_bytes == 0
    }

    pub(crate) fn checked_add(self, other: Self) -> Result<Self, String> {
        Ok(Self {
            input_rows: self
                .input_rows
                .checked_add(other.input_rows)
                .ok_or_else(|| "quantum input-row usage overflow".to_string())?,
            input_bytes: self
                .input_bytes
                .checked_add(other.input_bytes)
                .ok_or_else(|| "quantum input-byte usage overflow".to_string())?,
            output_rows: self
                .output_rows
                .checked_add(other.output_rows)
                .ok_or_else(|| "quantum output-row usage overflow".to_string())?,
            output_bytes: self
                .output_bytes
                .checked_add(other.output_bytes)
                .ok_or_else(|| "quantum output-byte usage overflow".to_string())?,
        })
    }
}

/// Aggregate budget for all kernel transitions committed together.
///
/// Row and byte capacity is passed back to the next kernel transition, so a
/// transaction cannot multiply the configured stage budget merely because an
/// operator uses several resumable phases. `max_transitions` also bounds
/// metadata-only phases that report no row work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WorkQuantum {
    budget: WorkBudget,
    used: WorkUsage,
    transitions: usize,
    max_transitions: usize,
}

impl WorkQuantum {
    pub(crate) fn new(budget: WorkBudget, max_transitions: usize) -> Self {
        assert!(max_transitions > 0, "transition budget must be positive");
        Self {
            budget,
            used: WorkUsage::default(),
            transitions: 0,
            max_transitions,
        }
    }

    pub(crate) fn remaining(self) -> Option<WorkBudget> {
        if self.transitions >= self.max_transitions {
            return None;
        }
        Some(WorkBudget::new(
            remaining(self.budget.max_input_rows, self.used.input_rows)?,
            remaining(self.budget.max_input_bytes, self.used.input_bytes)?,
            remaining(self.budget.max_output_rows, self.used.output_rows)?,
            remaining(self.budget.max_output_bytes, self.used.output_bytes)?,
        ))
    }

    pub(crate) fn record(&mut self, usage: WorkUsage) -> Result<(), String> {
        if self.transitions >= self.max_transitions {
            return Err("work quantum exceeded its transition budget".into());
        }
        self.used = self.used.checked_add(usage)?;
        self.transitions += 1;
        Ok(())
    }

    pub(crate) const fn usage(self) -> WorkUsage {
        self.used
    }

    pub(crate) const fn transitions(self) -> usize {
        self.transitions
    }
}

fn remaining(limit: usize, used: u64) -> Option<usize> {
    let used = usize::try_from(used).unwrap_or(usize::MAX);
    limit.checked_sub(used).filter(|remaining| *remaining > 0)
}

/// One kernel transition completed inside the current PostgreSQL transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StepExecution {
    pub(crate) outcome: StepOutcome,
    pub(crate) usage: WorkUsage,
}

impl StepExecution {
    pub(crate) const fn new(outcome: StepOutcome, usage: WorkUsage) -> Self {
        Self { outcome, usage }
    }

    pub(crate) const fn empty(outcome: StepOutcome) -> Self {
        Self::new(
            outcome,
            WorkUsage {
                input_rows: 0,
                input_bytes: 0,
                output_rows: 0,
                output_bytes: 0,
            },
        )
    }
}

/// What the scheduler should do after a committed operator step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepOutcome {
    /// Work committed and the operator consumed all immediately available work.
    Progress,
    /// Work committed with a continuation; schedule this operator at the tail.
    Yield,
    /// A durable dependency or downstream capacity is unavailable.
    Blocked,
    /// No input or continuation exists.
    Idle,
}

/// A fair, rebuildable cache of runnable operator IDs.
///
/// The queue never owns a frontier or continuation. `rebuild` deliberately
/// accepts only durable snapshots and sorts IDs, making restart order stable
/// even when PostgreSQL returns rows in a different order.
#[derive(Debug, Default)]
pub(crate) struct ReadyQueue {
    ready: VecDeque<OperatorId>,
    queued: HashSet<OperatorId>,
}

impl ReadyQueue {
    pub(crate) fn rebuild<'a>(states: impl IntoIterator<Item = &'a DurableOperatorState>) -> Self {
        let runnable = states
            .into_iter()
            .filter(|state| state.is_runnable())
            .map(|state| state.operator)
            .collect::<BTreeSet<_>>();
        let ready = runnable.iter().copied().collect();
        let queued = runnable.into_iter().collect();
        Self { ready, queued }
    }

    /// Adds a node after its durable inputs or output capacity changed.
    pub(crate) fn activate(&mut self, operator: OperatorId) {
        if self.queued.insert(operator) {
            self.ready.push_back(operator);
        }
    }

    pub(crate) fn next(&mut self) -> Option<OperatorId> {
        let operator = self.ready.pop_front()?;
        self.queued.remove(&operator);
        Some(operator)
    }

    pub(crate) fn complete(&mut self, operator: OperatorId, outcome: StepOutcome) {
        if outcome == StepOutcome::Yield {
            self.activate(operator);
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operator(result_oid: u32, stage_id: u32) -> OperatorId {
        OperatorId::new(result_oid, stage_id)
    }

    fn ready_state(operator: OperatorId) -> DurableOperatorState {
        DurableOperatorState {
            operator,
            inputs: vec![InputFrontier {
                port: 0,
                consumed: StreamSequence(10),
                available: StreamSequence(11),
                frontier_pending: false,
            }],
            has_continuation: false,
            outputs_have_capacity: true,
            active: true,
        }
    }

    #[test]
    fn work_budget_keeps_input_and_output_row_byte_limits_distinct() {
        let budget = WorkBudget::new(2, 10, 3, 20);
        assert_eq!(budget.max_input_rows, 2);
        assert_eq!(budget.max_input_bytes, 10);
        assert_eq!(budget.max_output_rows, 3);
        assert_eq!(budget.max_output_bytes, 20);
    }

    #[test]
    fn transaction_quantum_passes_only_remaining_work_to_the_next_transition() {
        let mut quantum = WorkQuantum::new(WorkBudget::new(8, 80, 6, 60), 10);
        quantum
            .record(WorkUsage {
                input_rows: 3,
                input_bytes: 30,
                output_rows: 2,
                output_bytes: 20,
            })
            .unwrap();

        assert_eq!(quantum.remaining(), Some(WorkBudget::new(5, 50, 4, 40)));
        assert_eq!(quantum.transitions(), 1);
        assert_eq!(
            quantum.usage(),
            WorkUsage {
                input_rows: 3,
                input_bytes: 30,
                output_rows: 2,
                output_bytes: 20,
            }
        );
    }

    #[test]
    fn transaction_quantum_bounds_metadata_only_transitions() {
        let mut quantum = WorkQuantum::new(WorkBudget::new(8, 80, 6, 60), 2);
        quantum.record(WorkUsage::default()).unwrap();
        assert!(quantum.remaining().is_some());
        quantum.record(WorkUsage::default()).unwrap();
        assert_eq!(quantum.remaining(), None);
        assert!(quantum.record(WorkUsage::default()).is_err());
    }

    #[test]
    fn transaction_quantum_stops_when_any_work_dimension_is_full() {
        let mut quantum = WorkQuantum::new(WorkBudget::new(8, 80, 6, 60), 10);
        quantum
            .record(WorkUsage {
                input_rows: 1,
                input_bytes: 80,
                output_rows: 1,
                output_bytes: 10,
            })
            .unwrap();
        assert_eq!(quantum.remaining(), None);
    }

    #[test]
    fn blocked_operator_is_not_requeued_or_spun() {
        let state = ready_state(operator(41, 0));
        let mut queue = ReadyQueue::rebuild([&state]);
        let operator = queue.next().unwrap();
        queue.complete(operator, StepOutcome::Blocked);
        assert!(queue.is_empty());
    }

    #[test]
    fn published_source_frontier_wakes_a_scan_without_a_data_chunk() {
        let state = DurableOperatorState {
            operator: operator(41, 0),
            inputs: vec![InputFrontier {
                port: 0,
                consumed: StreamSequence(11),
                available: StreamSequence(11),
                frontier_pending: true,
            }],
            has_continuation: false,
            outputs_have_capacity: true,
            active: true,
        };
        let mut queue = ReadyQueue::rebuild([&state]);
        let operator = queue.next().unwrap();
        queue.complete(operator, StepOutcome::Progress);
        assert!(queue.is_empty());
    }

    #[test]
    fn yielding_operators_run_round_robin() {
        let states = [
            ready_state(operator(41, 2)),
            ready_state(operator(41, 0)),
            ready_state(operator(41, 1)),
        ];
        let mut queue = ReadyQueue::rebuild(&states);
        let mut order = Vec::new();

        for _ in 0..6 {
            let operator = queue.next().unwrap();
            order.push(operator.stage_id);
            queue.complete(operator, StepOutcome::Yield);
        }

        assert_eq!(order, [0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn restart_rebuilds_a_node_lost_with_the_memory_queue() {
        let pending_input = ready_state(operator(41, 0));
        let resume_only = DurableOperatorState {
            operator: operator(41, 1),
            inputs: Vec::new(),
            has_continuation: true,
            outputs_have_capacity: true,
            active: true,
        };
        let backpressured = DurableOperatorState {
            operator: operator(41, 2),
            inputs: vec![InputFrontier {
                port: 0,
                consumed: StreamSequence(0),
                available: StreamSequence(1),
                frontier_pending: false,
            }],
            has_continuation: false,
            outputs_have_capacity: false,
            active: true,
        };
        let states = [pending_input, resume_only, backpressured];

        let mut lost_queue = ReadyQueue::rebuild(&states);
        assert_eq!(lost_queue.next(), Some(operator(41, 0)));
        drop(lost_queue);

        let mut rebuilt = ReadyQueue::rebuild(&states);
        let mut recovered = Vec::new();
        while let Some(operator) = rebuilt.next() {
            recovered.push(operator);
            rebuilt.complete(operator, StepOutcome::Progress);
        }

        assert_eq!(recovered, [operator(41, 0), operator(41, 1)]);
        assert!(!recovered.contains(&operator(41, 2)));
    }
}
