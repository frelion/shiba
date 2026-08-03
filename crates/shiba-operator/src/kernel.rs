use crate::{
    CompiledPlan, DeltaBatch, EncodedOperatorState, KeyedMutation, OperatorTransition,
    OutputContract, OutputDelta, PlanImplementation, ResultDelta, ResultMutation, TypedRow,
    TypedValue, ValueType, apply_graph,
    plan::{PlanError, STATE_CODEC_VERSION},
};
use core::fmt;

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
    InvalidGraph,
    InvalidTransition,
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
        PlanImplementation::Graph { .. } => Vec::new(),
    };
    Ok(EncodedOperatorState {
        codec_version: STATE_CODEC_VERSION,
        payload,
    })
}

/// Produces the empty terminal output for a newly registered plan.
///
/// # Errors
///
/// Rejects a corrupt plan/state or a graph without one supported terminal.
pub fn initial_transition(
    plan: &CompiledPlan,
    state: &EncodedOperatorState,
) -> Result<OperatorTransition, KernelError> {
    let current = decode_state(plan, state)?;
    match &plan.implementation {
        PlanImplementation::CountRows | PlanImplementation::SumInt8 { .. } => {
            scalar_transition(plan, current)
        }
        PlanImplementation::Graph { graph } => {
            graph.validate().map_err(|_| KernelError::InvalidGraph)?;
            if !matches!(plan.output_contract, OutputContract::KeyedRows { .. }) {
                return Err(KernelError::OutputContractMismatch);
            }
            Ok(OperatorTransition {
                next_state: state.clone(),
                output_delta: OutputDelta::KeyedMutations {
                    mutations: Vec::new(),
                },
            })
        }
    }
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
        PlanImplementation::Graph { .. } if state.payload.is_empty() => Ok(0),
        PlanImplementation::Graph { .. } => Err(KernelError::InvalidState),
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
    batch: &DeltaBatch,
) -> Result<OperatorTransition, KernelError> {
    let current = decode_state(plan, state)?;
    match &plan.implementation {
        PlanImplementation::CountRows => scalar_transition(plan, apply_count(current, batch)?),
        PlanImplementation::SumInt8 { input_slot, .. } => {
            scalar_transition(plan, apply_sum(current, batch, *input_slot)?)
        }
        PlanImplementation::Graph { graph } => graph_transition(plan, graph, batch),
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
            value: TypedValue::Int8(value),
        },
    })
}

fn apply_count(mut value: i64, batch: &DeltaBatch) -> Result<i64, KernelError> {
    if value < 0 {
        return Err(KernelError::NegativeCount);
    }
    for effect in &batch.rows {
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

fn apply_sum(mut value: i64, batch: &DeltaBatch, input_slot: u16) -> Result<i64, KernelError> {
    for effect in &batch.rows {
        if let Some(before) = &effect.before {
            value = value
                .checked_sub(contribution(before, input_slot)?)
                .ok_or(KernelError::Overflow)?;
        }
        if let Some(after) = &effect.after {
            value = value
                .checked_add(contribution(after, input_slot)?)
                .ok_or(KernelError::Overflow)?;
        }
    }
    Ok(value)
}

fn contribution(row: &TypedRow, input_slot: u16) -> Result<i64, KernelError> {
    match row.values.get(usize::from(input_slot)) {
        Some(TypedValue::Null(ValueType::Int8)) => Ok(0),
        Some(TypedValue::Int8(value)) => Ok(*value),
        Some(TypedValue::Absent) | None => Err(KernelError::AbsentInput),
        _ => Err(KernelError::WrongType),
    }
}

fn graph_transition(
    plan: &CompiledPlan,
    graph: &crate::OperatorGraph,
    batch: &DeltaBatch,
) -> Result<OperatorTransition, KernelError> {
    let transition = apply_graph(graph, batch).map_err(|_| KernelError::InvalidGraph)?;
    if !transition.states.is_empty() || transition.results.len() != 1 {
        return Err(KernelError::InvalidTransition);
    }
    let result = transition
        .results
        .into_iter()
        .next()
        .expect("length checked");
    let ResultDelta::Keyed { mutations, .. } = result;
    let mutations = mutations
        .into_iter()
        .map(|mutation| match mutation {
            ResultMutation::Delete { key } => KeyedMutation::Delete { key },
            ResultMutation::Upsert { key, value } => KeyedMutation::Upsert { key, value },
        })
        .collect();
    if !matches!(plan.output_contract, OutputContract::KeyedRows { .. }) {
        return Err(KernelError::OutputContractMismatch);
    }
    Ok(OperatorTransition {
        next_state: EncodedOperatorState {
            codec_version: crate::plan::STATE_CODEC_VERSION,
            payload: Vec::new(),
        },
        output_delta: OutputDelta::KeyedMutations { mutations },
    })
}
