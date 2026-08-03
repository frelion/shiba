//! Pure, database-independent operator contracts and evaluation.

#![forbid(unsafe_code)]

mod kernel;
mod model;
mod plan;

pub use kernel::{KernelError, apply_plan, decode_state, initial_state};
pub use model::{
    EffectBatch, EffectOrigin, EncodedOperatorState, KeyedMutation, ObjectAddress, OperatorId,
    OperatorTransition, OutputDelta, RowEffect, RowImage, ScalarValue, Value,
};
pub use plan::{
    CompiledPlan, InputBinding, InputRole, OutputContract, PlanError, PlanImplementation,
    StateContract, ValueType,
};
