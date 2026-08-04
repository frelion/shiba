use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{AggregateCall, TypedValue, ValueType};

/// Shared structural limits for the closed HAVING predicate contract.
pub const MAX_HAVING_NODES: usize = 256;
pub const MAX_HAVING_DEPTH: usize = 32;
pub const MAX_HAVING_BOOLEAN_TERMS: usize = 64;

/// A bounded predicate evaluated against finalized aggregate-call values.
///
/// This is deliberately separate from source-row expressions: a HAVING
/// predicate may refer only to calls already present in the aggregate plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "having", rename_all = "snake_case", deny_unknown_fields)]
pub enum HavingExpression {
    Call { ordinal: u16 },
    Int8Literal { value: i64 },
    NullLiteral,
    Equal { left: Box<Self>, right: Box<Self> },
    NotEqual { left: Box<Self>, right: Box<Self> },
    Less { left: Box<Self>, right: Box<Self> },
    LessEqual { left: Box<Self>, right: Box<Self> },
    Greater { left: Box<Self>, right: Box<Self> },
    GreaterEqual { left: Box<Self>, right: Box<Self> },
    IsNull { input: Box<Self> },
    And { left: Box<Self>, right: Box<Self> },
    Or { left: Box<Self>, right: Box<Self> },
    Not { input: Box<Self> },
}

impl HavingExpression {
    /// Validates call references and the closed Int8/Bool expression typing.
    ///
    /// # Errors
    ///
    /// Rejects unknown call ordinals, wrong types and excessive depth.
    pub fn validate(&self, calls: &[AggregateCall]) -> Result<ValueType, HavingError> {
        let mut nodes = 0;
        let mut boolean_terms = 0;
        self.validate_inner(calls, 0, &mut nodes, &mut boolean_terms)
    }

    /// Evaluates one predicate using finalized aggregate values.
    ///
    /// # Errors
    ///
    /// Rejects unknown call ordinals, wrong types and excessive depth.
    pub fn evaluate(&self, calls: &[TypedValue]) -> Result<TypedValue, HavingError> {
        let mut nodes = 0;
        let mut boolean_terms = 0;
        self.evaluate_inner(calls, 0, &mut nodes, &mut boolean_terms)
    }

    fn validate_inner(
        &self,
        calls: &[AggregateCall],
        depth: usize,
        nodes: &mut usize,
        boolean_terms: &mut usize,
    ) -> Result<ValueType, HavingError> {
        *nodes = nodes.checked_add(1).ok_or(HavingError::NodeLimit)?;
        if depth > MAX_HAVING_DEPTH {
            return Err(HavingError::DepthLimit);
        }
        if *nodes > MAX_HAVING_NODES {
            return Err(HavingError::NodeLimit);
        }
        match self {
            Self::Call { ordinal } => {
                if *ordinal == 0 || usize::from(*ordinal) > calls.len() {
                    return Err(HavingError::InvalidCall);
                }
                Ok(
                    crate::aggregate_function_descriptor(calls[usize::from(*ordinal - 1)].function)
                        .output_type,
                )
            }
            Self::Int8Literal { .. } | Self::NullLiteral => Ok(ValueType::Int8),
            Self::Equal { left, right } | Self::NotEqual { left, right } => {
                charge_boolean(boolean_terms)?;
                require_comparison(
                    left.validate_inner(calls, depth + 1, nodes, boolean_terms)?,
                    right.validate_inner(calls, depth + 1, nodes, boolean_terms)?,
                )
            }
            Self::Less { left, right }
            | Self::LessEqual { left, right }
            | Self::Greater { left, right }
            | Self::GreaterEqual { left, right } => {
                charge_boolean(boolean_terms)?;
                require_comparison(
                    left.validate_inner(calls, depth + 1, nodes, boolean_terms)?,
                    right.validate_inner(calls, depth + 1, nodes, boolean_terms)?,
                )
            }
            Self::IsNull { input } => {
                charge_boolean(boolean_terms)?;
                input.validate_inner(calls, depth + 1, nodes, boolean_terms)?;
                Ok(ValueType::Bool)
            }
            Self::And { left, right } | Self::Or { left, right } => {
                charge_boolean(boolean_terms)?;
                require_bool(
                    left.validate_inner(calls, depth + 1, nodes, boolean_terms)?,
                    right.validate_inner(calls, depth + 1, nodes, boolean_terms)?,
                )
            }
            Self::Not { input } => {
                charge_boolean(boolean_terms)?;
                if input.validate_inner(calls, depth + 1, nodes, boolean_terms)? == ValueType::Bool
                {
                    Ok(ValueType::Bool)
                } else {
                    Err(HavingError::WrongType)
                }
            }
        }
    }

    fn evaluate_inner(
        &self,
        calls: &[TypedValue],
        depth: usize,
        nodes: &mut usize,
        boolean_terms: &mut usize,
    ) -> Result<TypedValue, HavingError> {
        *nodes = nodes.checked_add(1).ok_or(HavingError::NodeLimit)?;
        if depth > MAX_HAVING_DEPTH {
            return Err(HavingError::DepthLimit);
        }
        if *nodes > MAX_HAVING_NODES {
            return Err(HavingError::NodeLimit);
        }
        match self {
            Self::Call { ordinal } => calls
                .get(usize::from(ordinal.saturating_sub(1)))
                .cloned()
                .ok_or(HavingError::InvalidCall),
            Self::Int8Literal { value } => Ok(TypedValue::Int8(*value)),
            Self::NullLiteral => Ok(TypedValue::Null(ValueType::Int8)),
            Self::Equal { left, right } => {
                charge_boolean(boolean_terms)?;
                let left = left.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                let right = right.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                compare(&left, &right, |a, b| a == b)
            }
            Self::NotEqual { left, right } => {
                charge_boolean(boolean_terms)?;
                let left = left.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                let right = right.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                compare(&left, &right, |a, b| a != b)
            }
            Self::Less { left, right } => {
                charge_boolean(boolean_terms)?;
                let left = left.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                let right = right.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                compare(&left, &right, |a, b| a < b)
            }
            Self::LessEqual { left, right } => {
                charge_boolean(boolean_terms)?;
                let left = left.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                let right = right.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                compare(&left, &right, |a, b| a <= b)
            }
            Self::Greater { left, right } => {
                charge_boolean(boolean_terms)?;
                let left = left.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                let right = right.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                compare(&left, &right, |a, b| a > b)
            }
            Self::GreaterEqual { left, right } => {
                charge_boolean(boolean_terms)?;
                let left = left.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                let right = right.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                compare(&left, &right, |a, b| a >= b)
            }
            Self::IsNull { input } => {
                charge_boolean(boolean_terms)?;
                Ok(TypedValue::Bool(matches!(
                    input.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?,
                    TypedValue::Null(_)
                )))
            }
            Self::And { left, right } => {
                charge_boolean(boolean_terms)?;
                let left = left.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                let right = right.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                boolean(&left, &right, and_3vl)
            }
            Self::Or { left, right } => {
                charge_boolean(boolean_terms)?;
                let left = left.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                let right = right.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?;
                boolean(&left, &right, or_3vl)
            }
            Self::Not { input } => {
                charge_boolean(boolean_terms)?;
                match as_bool(&input.evaluate_inner(calls, depth + 1, nodes, boolean_terms)?)? {
                    Some(value) => Ok(TypedValue::Bool(!value)),
                    None => Ok(TypedValue::Null(ValueType::Bool)),
                }
            }
        }
    }
}

fn require_comparison(left: ValueType, right: ValueType) -> Result<ValueType, HavingError> {
    if left == ValueType::Int8 && right == ValueType::Int8 {
        Ok(ValueType::Bool)
    } else {
        Err(HavingError::WrongType)
    }
}

fn charge_boolean(terms: &mut usize) -> Result<(), HavingError> {
    *terms = terms.checked_add(1).ok_or(HavingError::BooleanLimit)?;
    if *terms > MAX_HAVING_BOOLEAN_TERMS {
        Err(HavingError::BooleanLimit)
    } else {
        Ok(())
    }
}

fn require_bool(left: ValueType, right: ValueType) -> Result<ValueType, HavingError> {
    if left == ValueType::Bool && right == ValueType::Bool {
        Ok(ValueType::Bool)
    } else {
        Err(HavingError::WrongType)
    }
}

fn compare(
    left: &TypedValue,
    right: &TypedValue,
    operation: impl FnOnce(i64, i64) -> bool,
) -> Result<TypedValue, HavingError> {
    match (left, right) {
        (TypedValue::Null(ValueType::Int8), _) | (_, TypedValue::Null(ValueType::Int8)) => {
            Ok(TypedValue::Null(ValueType::Bool))
        }
        (TypedValue::Int8(left), TypedValue::Int8(right)) => {
            Ok(TypedValue::Bool(operation(*left, *right)))
        }
        _ => Err(HavingError::WrongType),
    }
}

fn as_bool(value: &TypedValue) -> Result<Option<bool>, HavingError> {
    match value {
        TypedValue::Bool(value) => Ok(Some(*value)),
        TypedValue::Null(ValueType::Bool) => Ok(None),
        _ => Err(HavingError::WrongType),
    }
}

fn boolean(
    left: &TypedValue,
    right: &TypedValue,
    operation: impl FnOnce(Option<bool>, Option<bool>) -> Option<bool>,
) -> Result<TypedValue, HavingError> {
    Ok(match operation(as_bool(left)?, as_bool(right)?) {
        Some(value) => TypedValue::Bool(value),
        None => TypedValue::Null(ValueType::Bool),
    })
}

fn and_3vl(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    if left == Some(false) || right == Some(false) {
        Some(false)
    } else if left == Some(true) && right == Some(true) {
        Some(true)
    } else {
        None
    }
}

fn or_3vl(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    if left == Some(true) || right == Some(true) {
        Some(true)
    } else if left == Some(false) && right == Some(false) {
        Some(false)
    } else {
        None
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HavingError {
    InvalidCall,
    WrongType,
    DepthLimit,
    NodeLimit,
    BooleanLimit,
}

impl fmt::Display for HavingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "HAVING predicate rejected: {self:?}")
    }
}

impl std::error::Error for HavingError {}
