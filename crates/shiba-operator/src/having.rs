use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{AggregateCall, TypedValue, ValueType};

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
        self.validate_inner(calls, 0)
    }

    /// Evaluates one predicate using finalized aggregate values.
    ///
    /// # Errors
    ///
    /// Rejects unknown call ordinals, wrong types and excessive depth.
    pub fn evaluate(&self, calls: &[TypedValue]) -> Result<TypedValue, HavingError> {
        self.evaluate_inner(calls, 0)
    }

    fn validate_inner(
        &self,
        calls: &[AggregateCall],
        depth: usize,
    ) -> Result<ValueType, HavingError> {
        if depth >= 32 {
            return Err(HavingError::DepthLimit);
        }
        let child = |value: &Self| value.validate_inner(calls, depth + 1);
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
                require_comparison(child(left)?, child(right)?)
            }
            Self::Less { left, right }
            | Self::LessEqual { left, right }
            | Self::Greater { left, right }
            | Self::GreaterEqual { left, right } => require_comparison(child(left)?, child(right)?),
            Self::IsNull { input } => {
                child(input)?;
                Ok(ValueType::Bool)
            }
            Self::And { left, right } | Self::Or { left, right } => {
                require_bool(child(left)?, child(right)?)
            }
            Self::Not { input } => {
                if child(input)? == ValueType::Bool {
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
    ) -> Result<TypedValue, HavingError> {
        if depth >= 32 {
            return Err(HavingError::DepthLimit);
        }
        let child = |value: &Self| value.evaluate_inner(calls, depth + 1);
        match self {
            Self::Call { ordinal } => calls
                .get(usize::from(ordinal.saturating_sub(1)))
                .cloned()
                .ok_or(HavingError::InvalidCall),
            Self::Int8Literal { value } => Ok(TypedValue::Int8(*value)),
            Self::NullLiteral => Ok(TypedValue::Null(ValueType::Int8)),
            Self::Equal { left, right } => compare(&child(left)?, &child(right)?, |a, b| a == b),
            Self::NotEqual { left, right } => compare(&child(left)?, &child(right)?, |a, b| a != b),
            Self::Less { left, right } => compare(&child(left)?, &child(right)?, |a, b| a < b),
            Self::LessEqual { left, right } => {
                compare(&child(left)?, &child(right)?, |a, b| a <= b)
            }
            Self::Greater { left, right } => compare(&child(left)?, &child(right)?, |a, b| a > b),
            Self::GreaterEqual { left, right } => {
                compare(&child(left)?, &child(right)?, |a, b| a >= b)
            }
            Self::IsNull { input } => Ok(TypedValue::Bool(matches!(
                child(input)?,
                TypedValue::Null(_)
            ))),
            Self::And { left, right } => boolean(&child(left)?, &child(right)?, and_3vl),
            Self::Or { left, right } => boolean(&child(left)?, &child(right)?, or_3vl),
            Self::Not { input } => match as_bool(&child(input)?)? {
                Some(value) => Ok(TypedValue::Bool(!value)),
                None => Ok(TypedValue::Null(ValueType::Bool)),
            },
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
}

impl fmt::Display for HavingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "HAVING predicate rejected: {self:?}")
    }
}

impl std::error::Error for HavingError {}
