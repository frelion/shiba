use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{TypedLayout, TypedRow, TypedValue, ValueType};

const MAX_EXPRESSION_DEPTH: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "expression", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expression {
    Column { slot: u16 },
    Int8Literal { value: i64 },
    NullLiteral { value_type: ValueType },
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
    Add { left: Box<Self>, right: Box<Self> },
    Subtract { left: Box<Self>, right: Box<Self> },
}

impl Expression {
    /// Returns whether this expression may produce a typed NULL.
    ///
    /// # Errors
    ///
    /// Rejects invalid input slots and malformed expression layouts.
    pub fn nullable(&self, layout: &TypedLayout) -> Result<bool, ExpressionError> {
        let child = |expression: &Self| expression.nullable(layout);
        Ok(match self {
            Self::Column { slot } => *layout
                .nullable
                .get(usize::from(*slot))
                .ok_or(ExpressionError::InvalidSlot)?,
            Self::Int8Literal { .. } | Self::IsNull { .. } => false,
            Self::NullLiteral { .. } => true,
            Self::Equal { left, right }
            | Self::NotEqual { left, right }
            | Self::Less { left, right }
            | Self::LessEqual { left, right }
            | Self::Greater { left, right }
            | Self::GreaterEqual { left, right }
            | Self::And { left, right }
            | Self::Or { left, right }
            | Self::Add { left, right }
            | Self::Subtract { left, right } => child(left)? || child(right)?,
            Self::Not { input } => child(input)?,
        })
    }

    /// Validates all slots and operand/result types against one input layout.
    ///
    /// # Errors
    ///
    /// Rejects invalid slots, types, or expression depth.
    pub fn validate(&self, layout: &TypedLayout) -> Result<ValueType, ExpressionError> {
        self.validate_inner(layout, 0)
    }

    /// Evaluates one expression without external state or database access.
    ///
    /// # Errors
    ///
    /// Rejects layout/type drift, absent values, overflow, or excessive depth.
    pub fn evaluate(
        &self,
        layout: &TypedLayout,
        row: &TypedRow,
    ) -> Result<TypedValue, ExpressionError> {
        self.evaluate_inner(layout, row, 0)
    }

    fn validate_inner(
        &self,
        layout: &TypedLayout,
        depth: usize,
    ) -> Result<ValueType, ExpressionError> {
        if depth >= MAX_EXPRESSION_DEPTH {
            return Err(ExpressionError::DepthLimit);
        }
        let child = |expression: &Self| expression.validate_inner(layout, depth + 1);
        match self {
            Self::Column { slot } => layout
                .value_types
                .get(usize::from(*slot))
                .copied()
                .ok_or(ExpressionError::InvalidSlot),
            Self::Int8Literal { .. } => Ok(ValueType::Int8),
            Self::NullLiteral { value_type } => Ok(*value_type),
            Self::Equal { left, right } | Self::NotEqual { left, right } => {
                let left_type = child(left)?;
                if left_type == child(right)? && left_type == ValueType::Int8 {
                    Ok(ValueType::Bool)
                } else {
                    Err(ExpressionError::WrongType)
                }
            }
            Self::Less { left, right }
            | Self::LessEqual { left, right }
            | Self::Greater { left, right }
            | Self::GreaterEqual { left, right } => require_binary(
                child(left)?,
                child(right)?,
                ValueType::Int8,
                ValueType::Bool,
            ),
            Self::IsNull { input } => {
                child(input)?;
                Ok(ValueType::Bool)
            }
            Self::And { left, right } | Self::Or { left, right } => require_binary(
                child(left)?,
                child(right)?,
                ValueType::Bool,
                ValueType::Bool,
            ),
            Self::Not { input } => require_unary(child(input)?, ValueType::Bool),
            Self::Add { left, right } | Self::Subtract { left, right } => require_binary(
                child(left)?,
                child(right)?,
                ValueType::Int8,
                ValueType::Int8,
            ),
        }
    }

    fn evaluate_inner(
        &self,
        layout: &TypedLayout,
        row: &TypedRow,
        depth: usize,
    ) -> Result<TypedValue, ExpressionError> {
        if depth >= MAX_EXPRESSION_DEPTH {
            return Err(ExpressionError::DepthLimit);
        }
        let child = |expression: &Self| expression.evaluate_inner(layout, row, depth + 1);
        match self {
            Self::Column { slot } => match row.value(layout, *slot)? {
                TypedValue::Absent => Err(ExpressionError::Absent),
                value => Ok(value.clone()),
            },
            Self::Int8Literal { value } => Ok(TypedValue::Int8(*value)),
            Self::NullLiteral { value_type } => Ok(TypedValue::Null(*value_type)),
            Self::Equal { left, right } => compare(child(left)?, child(right)?, |a, b| a == b),
            Self::NotEqual { left, right } => compare(child(left)?, child(right)?, |a, b| a != b),
            Self::Less { left, right } => compare(child(left)?, child(right)?, |a, b| a < b),
            Self::LessEqual { left, right } => compare(child(left)?, child(right)?, |a, b| a <= b),
            Self::Greater { left, right } => compare(child(left)?, child(right)?, |a, b| a > b),
            Self::GreaterEqual { left, right } => {
                compare(child(left)?, child(right)?, |a, b| a >= b)
            }
            Self::IsNull { input } => Ok(TypedValue::Bool(matches!(
                child(input)?,
                TypedValue::Null(_)
            ))),
            Self::And { left, right } => boolean(&child(left)?, &child(right)?, and_3vl),
            Self::Or { left, right } => boolean(&child(left)?, &child(right)?, or_3vl),
            Self::Not { input } => match bool_3vl(&child(input)?)? {
                Some(value) => Ok(TypedValue::Bool(!value)),
                None => Ok(TypedValue::Null(ValueType::Bool)),
            },
            Self::Add { left, right } => arithmetic(child(left)?, child(right)?, i64::checked_add),
            Self::Subtract { left, right } => {
                arithmetic(child(left)?, child(right)?, i64::checked_sub)
            }
        }
    }
}

fn require_binary(
    left: ValueType,
    right: ValueType,
    expected: ValueType,
    output: ValueType,
) -> Result<ValueType, ExpressionError> {
    if left == expected && right == expected {
        Ok(output)
    } else {
        Err(ExpressionError::WrongType)
    }
}

fn require_unary(actual: ValueType, expected: ValueType) -> Result<ValueType, ExpressionError> {
    if actual == expected {
        Ok(expected)
    } else {
        Err(ExpressionError::WrongType)
    }
}

fn compare(
    left: TypedValue,
    right: TypedValue,
    operation: impl FnOnce(i64, i64) -> bool,
) -> Result<TypedValue, ExpressionError> {
    match (left, right) {
        (TypedValue::Null(ValueType::Int8), _) | (_, TypedValue::Null(ValueType::Int8)) => {
            Ok(TypedValue::Null(ValueType::Bool))
        }
        (TypedValue::Int8(left), TypedValue::Int8(right)) => {
            Ok(TypedValue::Bool(operation(left, right)))
        }
        _ => Err(ExpressionError::WrongType),
    }
}

fn arithmetic(
    left: TypedValue,
    right: TypedValue,
    operation: impl FnOnce(i64, i64) -> Option<i64>,
) -> Result<TypedValue, ExpressionError> {
    match (left, right) {
        (TypedValue::Null(ValueType::Int8), _) | (_, TypedValue::Null(ValueType::Int8)) => {
            Ok(TypedValue::Null(ValueType::Int8))
        }
        (TypedValue::Int8(left), TypedValue::Int8(right)) => operation(left, right)
            .map(TypedValue::Int8)
            .ok_or(ExpressionError::Overflow),
        _ => Err(ExpressionError::WrongType),
    }
}

fn bool_3vl(value: &TypedValue) -> Result<Option<bool>, ExpressionError> {
    match value {
        TypedValue::Bool(value) => Ok(Some(*value)),
        TypedValue::Null(ValueType::Bool) => Ok(None),
        TypedValue::Absent => Err(ExpressionError::Absent),
        _ => Err(ExpressionError::WrongType),
    }
}

fn boolean(
    left: &TypedValue,
    right: &TypedValue,
    operation: impl FnOnce(Option<bool>, Option<bool>) -> Option<bool>,
) -> Result<TypedValue, ExpressionError> {
    Ok(match operation(bool_3vl(left)?, bool_3vl(right)?) {
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
pub enum ExpressionError {
    InvalidSlot,
    WrongType,
    Absent,
    Overflow,
    DepthLimit,
    InvalidRow,
}

impl From<crate::TypedError> for ExpressionError {
    fn from(_: crate::TypedError) -> Self {
        Self::InvalidRow
    }
}
impl fmt::Display for ExpressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expression rejected: {self:?}")
    }
}
impl std::error::Error for ExpressionError {}
