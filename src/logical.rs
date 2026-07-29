//! One typed relational-DAG model and its Runtime.

pub(crate) mod dataflow;
pub(crate) mod model;
mod runtime;
mod validate;

pub(crate) use dataflow::{StepOutcome, WorkBudget};
pub(crate) use runtime::LoadedDataflow;
