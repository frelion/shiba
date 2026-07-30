//! Bounded work primitives for resumable dataflow stages.
//!
//! An operator step must commit its state changes, input frontiers,
//! continuation, and emitted effects together before returning an outcome.
//! The step boundary is determined only by row and byte counts. A wall-clock
//! deadline may stop the outer Runtime between steps, but never changes where
//! a step checkpoints.

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
