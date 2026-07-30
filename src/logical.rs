//! One typed relational-DAG model and its Runtime.

pub(crate) mod dataflow;
pub(crate) mod model;
mod runtime;
mod validate;

pub(crate) use dataflow::{StepExecution, StepOutcome, WorkBudget, WorkQuantum, WorkUsage};
pub(crate) use runtime::LoadedDataflow;
