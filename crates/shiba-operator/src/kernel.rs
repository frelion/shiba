use core::fmt;
use std::collections::BTreeSet;

use crate::{
    CompiledPlan, EncodedOperatorState, KeyedMutation, OperatorTransition, OutputContract,
    OutputDelta, PlanImplementation, RowEffect, ScalarValue, Value,
    plan::{MAX_KEYED_MUTATIONS, PlanError, STATE_CODEC_VERSION},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KernelError {
    InvalidPlan,
    InvalidState,
    OutputContractMismatch,
    NegativeCount,
    Underflow,
    Overflow,
    AbsentInput,
    WrongType,
    MissingKey,
    ConflictingKey,
    OutputLimit,
}

impl fmt::Display for KernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "operator kernel rejected transition: {self:?}")
    }
}

impl std::error::Error for KernelError {}

impl From<PlanError> for KernelError {
    fn from(_: PlanError) -> Self {
        Self::InvalidPlan
    }
}

/// Creates the deterministic state required by a validated plan.
///
/// # Errors
///
/// Rejects invalid plans or unsupported codecs.
pub fn initial_state(plan: &CompiledPlan) -> Result<EncodedOperatorState, KernelError> {
    plan.validate()?;
    let payload = match plan.implementation {
        PlanImplementation::CountRows | PlanImplementation::SumInt8 { .. } => {
            0_i64.to_be_bytes().to_vec()
        }
        PlanImplementation::ProjectRows { .. } => Vec::new(),
    };
    Ok(EncodedOperatorState {
        codec_version: STATE_CODEC_VERSION,
        payload,
    })
}

/// Strictly validates and decodes state for reference and sink validation.
///
/// # Errors
///
/// Rejects corrupt plans, versions, lengths, or unexpected project state.
pub fn decode_state(plan: &CompiledPlan, state: &EncodedOperatorState) -> Result<i64, KernelError> {
    plan.validate()?;
    if state.codec_version != STATE_CODEC_VERSION {
        return Err(KernelError::InvalidState);
    }
    match plan.implementation {
        PlanImplementation::CountRows | PlanImplementation::SumInt8 { .. } => state
            .payload
            .as_slice()
            .try_into()
            .map(i64::from_be_bytes)
            .map_err(|_| KernelError::InvalidState),
        PlanImplementation::ProjectRows { .. } if state.payload.is_empty() => Ok(0),
        PlanImplementation::ProjectRows { .. } => Err(KernelError::InvalidState),
    }
}

/// Computes one deterministic transition without database access.
///
/// # Errors
///
/// Fails closed for invalid plan/state/input, arithmetic, contract or bounds.
pub fn apply_plan(
    plan: &CompiledPlan,
    state: &EncodedOperatorState,
    effects: &[RowEffect],
) -> Result<OperatorTransition, KernelError> {
    let current = decode_state(plan, state)?;
    match plan.implementation {
        PlanImplementation::CountRows => scalar_transition(plan, apply_count(current, effects)?),
        PlanImplementation::SumInt8 { .. } => scalar_transition(plan, apply_sum(current, effects)?),
        PlanImplementation::ProjectRows { .. } => project_transition(plan, effects),
    }
}

fn scalar_transition(plan: &CompiledPlan, value: i64) -> Result<OperatorTransition, KernelError> {
    if plan.output_contract
        != (OutputContract::Scalar {
            value_type: crate::ValueType::Int8,
        })
    {
        return Err(KernelError::OutputContractMismatch);
    }
    Ok(OperatorTransition {
        next_state: EncodedOperatorState {
            codec_version: STATE_CODEC_VERSION,
            payload: value.to_be_bytes().to_vec(),
        },
        output_delta: OutputDelta::ScalarReplacement {
            value: ScalarValue::Int8(value),
        },
    })
}

fn apply_count(mut value: i64, effects: &[RowEffect]) -> Result<i64, KernelError> {
    if value < 0 {
        return Err(KernelError::NegativeCount);
    }
    for effect in effects {
        match (&effect.before, &effect.after) {
            (None, Some(_)) => value = value.checked_add(1).ok_or(KernelError::Overflow)?,
            (Some(_), None) => {
                value = value.checked_sub(1).ok_or(KernelError::Underflow)?;
                if value < 0 {
                    return Err(KernelError::Underflow);
                }
            }
            _ => {}
        }
    }
    Ok(value)
}

fn apply_sum(mut value: i64, effects: &[RowEffect]) -> Result<i64, KernelError> {
    for effect in effects {
        if let Some(before) = &effect.before {
            value = value
                .checked_sub(contribution(&before.payload)?)
                .ok_or(KernelError::Overflow)?;
        }
        if let Some(after) = &effect.after {
            value = value
                .checked_add(contribution(&after.payload)?)
                .ok_or(KernelError::Overflow)?;
        }
    }
    Ok(value)
}

fn contribution(value: &Value) -> Result<i64, KernelError> {
    match value {
        Value::Null => Ok(0),
        Value::Int8(value) => Ok(*value),
        Value::Absent => Err(KernelError::AbsentInput),
        Value::Text(_) => Err(KernelError::WrongType),
    }
}

fn project_transition(
    plan: &CompiledPlan,
    effects: &[RowEffect],
) -> Result<OperatorTransition, KernelError> {
    if !matches!(
        plan.output_contract,
        OutputContract::KeyedRows {
            key_type: crate::ValueType::Int8,
            value_type: crate::ValueType::Int8,
            nullable: true
        }
    ) {
        return Err(KernelError::OutputContractMismatch);
    }
    let capacity = effects
        .len()
        .checked_mul(2)
        .ok_or(KernelError::OutputLimit)?;
    if capacity > MAX_KEYED_MUTATIONS {
        return Err(KernelError::OutputLimit);
    }
    let mut mutations = Vec::with_capacity(capacity);
    let mut effect_keys = BTreeSet::new();
    for effect in effects {
        let old = effect.before.as_ref().map(projected).transpose()?;
        let new = effect.after.as_ref().map(projected).transpose()?;
        let mut keys = BTreeSet::new();
        if let Some((key, _)) = old {
            keys.insert(key);
            mutations.push(KeyedMutation::Delete { key });
        }
        if let Some((key, value)) = new {
            keys.insert(key);
            mutations.push(KeyedMutation::Upsert { key, value });
        }
        if keys.iter().any(|key| !effect_keys.insert(*key)) {
            return Err(KernelError::ConflictingKey);
        }
    }
    Ok(OperatorTransition {
        next_state: EncodedOperatorState {
            codec_version: STATE_CODEC_VERSION,
            payload: Vec::new(),
        },
        output_delta: OutputDelta::KeyedMutations { mutations },
    })
}

fn projected(image: &crate::RowImage) -> Result<(i64, ScalarValue), KernelError> {
    let key = image.source_row_id.ok_or(KernelError::MissingKey)?;
    let value = match image.payload {
        Value::Null => ScalarValue::Null,
        Value::Int8(value) => ScalarValue::Int8(value),
        Value::Absent => return Err(KernelError::AbsentInput),
        Value::Text(_) => return Err(KernelError::WrongType),
    };
    Ok((key, value))
}
