use core::fmt;

use serde::{Deserialize, Serialize};

pub const MAX_ROW_VALUES: usize = 16;
pub const MAX_TEXT_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    Bool,
    Int8,
    Text,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TypedValue {
    Absent,
    Null(ValueType),
    Bool(bool),
    Int8(i64),
    Text(String),
}

impl TypedValue {
    #[must_use]
    pub const fn value_type(&self) -> Option<ValueType> {
        match self {
            Self::Absent => None,
            Self::Null(value_type) => Some(*value_type),
            Self::Bool(_) => Some(ValueType::Bool),
            Self::Int8(_) => Some(ValueType::Int8),
            Self::Text(_) => Some(ValueType::Text),
        }
    }

    /// Encodes one persistent typed value in its strict canonical JSON form.
    ///
    /// # Errors
    ///
    /// Rejects `Absent`, oversized text, or serialization failure.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, TypedError> {
        if matches!(self, Self::Absent) {
            return Err(TypedError::Absent);
        }
        if matches!(self, Self::Text(value) if value.len() > MAX_TEXT_BYTES) {
            return Err(TypedError::TextLimit);
        }
        serde_json::to_vec(self).map_err(|_| TypedError::Codec)
    }

    /// Decodes one exact persistent typed value from canonical JSON.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, absent, oversized, or trailing input.
    pub fn from_canonical_json(input: &[u8]) -> Result<Self, TypedError> {
        let value: Self = serde_json::from_slice(input).map_err(|_| TypedError::Codec)?;
        if matches!(value, Self::Absent)
            || matches!(&value, Self::Text(text) if text.len() > MAX_TEXT_BYTES)
            || value.to_canonical_json()? != input
        {
            return Err(TypedError::Codec);
        }
        Ok(value)
    }

    pub(crate) fn validate(&self, expected: ValueType) -> Result<(), TypedError> {
        if matches!(self, Self::Absent) {
            return Ok(());
        }
        if self.value_type() != Some(expected) {
            return Err(TypedError::WrongType);
        }
        if matches!(self, Self::Text(value) if value.len() > MAX_TEXT_BYTES) {
            return Err(TypedError::TextLimit);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedLayout {
    pub identity: [u8; 32],
    pub value_types: Vec<ValueType>,
    pub nullable: Vec<bool>,
}

impl TypedLayout {
    /// Constructs one bounded, nonzero-identity typed layout.
    ///
    /// # Errors
    ///
    /// Rejects zero identity or too many values.
    pub fn new(identity: [u8; 32], value_types: Vec<ValueType>) -> Result<Self, TypedError> {
        if identity == [0; 32] {
            return Err(TypedError::InvalidLayout);
        }
        if value_types.len() > MAX_ROW_VALUES {
            return Err(TypedError::ValueLimit);
        }
        let width = value_types.len();
        Ok(Self {
            identity,
            value_types,
            nullable: vec![false; width],
        })
    }

    /// Constructs a layout with one nullability bit per value slot.
    ///
    /// # Errors
    ///
    /// Rejects mismatched widths or other invalid layout bounds.
    pub fn with_nullability(
        identity: [u8; 32],
        value_types: Vec<ValueType>,
        nullable: Vec<bool>,
    ) -> Result<Self, TypedError> {
        if nullable.len() != value_types.len() {
            return Err(TypedError::LayoutMismatch);
        }
        let mut layout = Self::new(identity, value_types)?;
        layout.nullable = nullable;
        Ok(layout)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedRow {
    pub layout_identity: [u8; 32],
    pub values: Vec<TypedValue>,
}

impl TypedRow {
    /// Constructs one row exactly matching the supplied layout.
    ///
    /// # Errors
    ///
    /// Rejects width, type, or value-size mismatch.
    pub fn new(layout: &TypedLayout, values: Vec<TypedValue>) -> Result<Self, TypedError> {
        if values.len() != layout.value_types.len() || values.len() > MAX_ROW_VALUES {
            return Err(TypedError::LayoutMismatch);
        }
        for ((value, expected), nullable) in
            values.iter().zip(&layout.value_types).zip(&layout.nullable)
        {
            value.validate(*expected)?;
            if matches!(value, TypedValue::Null(_)) && !nullable {
                return Err(TypedError::WrongType);
            }
        }
        Ok(Self {
            layout_identity: layout.identity,
            values,
        })
    }

    /// Returns a strictly layout-checked value by slot.
    ///
    /// # Errors
    ///
    /// Rejects layout identity, slot, or value-type drift.
    pub fn value(&self, layout: &TypedLayout, slot: u16) -> Result<&TypedValue, TypedError> {
        if self.layout_identity != layout.identity {
            return Err(TypedError::LayoutMismatch);
        }
        let index = usize::from(slot);
        let value = self.values.get(index).ok_or(TypedError::InvalidSlot)?;
        value.validate(
            *layout
                .value_types
                .get(index)
                .ok_or(TypedError::InvalidSlot)?,
        )?;
        if matches!(value, TypedValue::Null(_))
            && !layout.nullable.get(index).copied().unwrap_or(false)
        {
            return Err(TypedError::WrongType);
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedError {
    Absent,
    Codec,
    InvalidLayout,
    LayoutMismatch,
    InvalidSlot,
    WrongType,
    ValueLimit,
    TextLimit,
}

impl fmt::Display for TypedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "typed row rejected: {self:?}")
    }
}

impl std::error::Error for TypedError {}
