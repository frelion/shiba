use core::fmt;

use crate::{CompiledOperator, CompiledOperatorKind, RowEffect, Value};

/// A fail-closed pure operator evaluation error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperatorError {
    NegativeCountState,
    CountUnderflow,
    ArithmeticOverflow,
    AbsentSumInput,
    InvalidSumInputType,
}

impl fmt::Display for OperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeCountState => formatter.write_str("count state cannot be negative"),
            Self::CountUnderflow => formatter.write_str("count operation would underflow zero"),
            Self::ArithmeticOverflow => formatter.write_str("operator arithmetic overflow"),
            Self::AbsentSumInput => formatter.write_str("sum input is absent"),
            Self::InvalidSumInputType => formatter.write_str("sum input is not int8 or null"),
        }
    }
}

impl std::error::Error for OperatorError {}

/// Applies transaction-local row effects to one operator state.
///
/// # Errors
///
/// Fails closed for invalid state, invalid input values, or checked-arithmetic
/// overflow/underflow.
pub fn apply_operator(
    operator: &CompiledOperator,
    current_state: i64,
    effects: &[RowEffect],
) -> Result<i64, OperatorError> {
    match operator.kind {
        CompiledOperatorKind::CountRows => apply_count(current_state, effects),
        CompiledOperatorKind::SumInt8 { .. } => apply_sum(current_state, effects),
    }
}

fn apply_count(mut state: i64, effects: &[RowEffect]) -> Result<i64, OperatorError> {
    if state < 0 {
        return Err(OperatorError::NegativeCountState);
    }
    for effect in effects {
        match (&effect.before, &effect.after) {
            (None, Some(_)) => {
                state = state
                    .checked_add(1)
                    .ok_or(OperatorError::ArithmeticOverflow)?;
            }
            (Some(_), None) => {
                state = state.checked_sub(1).ok_or(OperatorError::CountUnderflow)?;
                if state < 0 {
                    return Err(OperatorError::CountUnderflow);
                }
            }
            _ => {}
        }
    }
    Ok(state)
}

fn apply_sum(mut state: i64, effects: &[RowEffect]) -> Result<i64, OperatorError> {
    for effect in effects {
        if let Some(before) = &effect.before {
            state = state
                .checked_sub(sum_contribution(&before.payload)?)
                .ok_or(OperatorError::ArithmeticOverflow)?;
        }
        if let Some(after) = &effect.after {
            state = state
                .checked_add(sum_contribution(&after.payload)?)
                .ok_or(OperatorError::ArithmeticOverflow)?;
        }
    }
    Ok(state)
}

const fn sum_contribution(value: &Value) -> Result<i64, OperatorError> {
    match value {
        Value::Null => Ok(0),
        Value::Int8(value) => Ok(*value),
        Value::Absent => Err(OperatorError::AbsentSumInput),
        Value::Text(_) => Err(OperatorError::InvalidSumInputType),
    }
}
