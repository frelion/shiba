//! Pure, database-independent operator contracts and evaluation.

#![forbid(unsafe_code)]

mod apply;
mod model;

pub use apply::{OperatorError, apply_operator};
pub use model::{
    CompiledOperator, CompiledOperatorKind, EffectBatch, EffectOrigin, ObjectAddress, OperatorId,
    RowEffect, RowImage, Value,
};
