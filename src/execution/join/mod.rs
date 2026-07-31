//! Pure control state for a bounded, resumable differential Join.
//!
//! The Join's planner, PostgreSQL execution path, and storage provisioner are
//! kept in separate modules while preserving the original Kernel contract.

mod planner;
mod provision;
mod runtime;

#[cfg(test)]
use crate::execution::{InputPosition, PhaseCode, PrimitiveFacts};
#[cfg(test)]
use crate::planner::WorkBudget;
use planner::*;

pub(crate) use provision::provision;
#[cfg(feature = "pg17")]
pub(crate) use runtime::execution::step;

pub(crate) const KERNEL: crate::execution::KernelFn = crate::execution::KernelFn::new(
    crate::execution::KernelContract::with_phases(
        &[
            crate::execution::InputContract::Operator,
            crate::execution::InputContract::Operator,
        ],
        crate::execution::OutputContract::EffectStream,
        &[
            crate::execution::LifecyclePhase::Process,
            crate::execution::LifecyclePhase::Frontier,
        ],
    ),
    step,
);

#[cfg(test)]
mod tests;
