//! One typed relational-DAG model and its Runtime.

pub(crate) mod dataflow;
pub(crate) mod lowering;
pub(crate) mod model;
mod runtime;
pub(crate) mod scalar_sql;
mod validate;

pub(crate) use dataflow::{StepExecution, StepOutcome, WorkBudget, WorkQuantum, WorkUsage};
pub(crate) use runtime::LoadedDataflow;
