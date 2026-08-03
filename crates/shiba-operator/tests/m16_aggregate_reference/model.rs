use std::collections::BTreeMap;

use super::transition::{apply_call, normalize, output_value, payload_matches, validate_payload};

pub const FORMAT_VERSION: u32 = 1;
pub const FUNCTION_VERSION: u32 = 1;
pub const STATE_CODEC_VERSION: u32 = 1;
pub const MAX_CALLS: usize = 16;
pub const MAX_CHANGES: usize = 10_000;
pub const MAX_ROW_WIDTH: usize = 16;
pub const MAX_TOUCHED_GROUPS: usize = 10_000;
pub const MAX_EMITTED_RESULT_IMAGES: usize = 20_000;
pub const MAX_GRAPH_OUTPUT_MUTATIONS: usize = 100_000;
pub const MAX_GRAPH_STATE_MUTATIONS: usize = 100_000;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Int8(i64),
    Bool(bool),
}

pub type Row = Vec<Value>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Function {
    CountStar,
    Count { slot: usize },
    Sum { slot: usize },
    Min { slot: usize },
    Max { slot: usize },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Call {
    pub ordinal: usize,
    pub function_version: u32,
    pub function: Function,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Having {
    pub ordinal: usize,
    pub greater_than: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Plan {
    pub version: u32,
    pub input_width: usize,
    pub calls: Vec<Call>,
    pub having: Option<Having>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Change {
    pub before: Option<Row>,
    pub after: Option<Row>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Payload {
    Count(i64),
    Sum { non_null: u64, value: i64 },
    Extrema(BTreeMap<i64, u64>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCall {
    pub function_tag: String,
    pub function_version: u32,
    pub payload: Payload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredState {
    pub codec_version: u32,
    pub calls: Vec<StoredCall>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredResult {
    pub version: u32,
    pub values: Row,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct State {
    calls: Vec<Payload>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelError {
    Bound,
    Corrupt,
    Overflow,
    RetractMissing,
    Schema,
    UnknownCodec,
    UnknownFunction,
    UnknownVersion,
}

impl Plan {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.version != FORMAT_VERSION {
            return Err(ModelError::UnknownVersion);
        }
        if self.input_width == 0
            || self.input_width > MAX_ROW_WIDTH
            || self.calls.is_empty()
            || self.calls.len() > MAX_CALLS
        {
            return Err(ModelError::Bound);
        }
        for (index, call) in self.calls.iter().enumerate() {
            if call.ordinal != index + 1 {
                return Err(ModelError::Corrupt);
            }
            if call.function_version != FUNCTION_VERSION {
                return Err(ModelError::UnknownVersion);
            }
            if let Some(slot) = call.function.slot()
                && slot >= self.input_width
            {
                return Err(ModelError::Schema);
            }
        }
        if self
            .having
            .as_ref()
            .is_some_and(|having| having.ordinal == 0 || having.ordinal > self.calls.len())
        {
            return Err(ModelError::Schema);
        }
        Ok(())
    }
}

impl Function {
    pub(super) fn slot(&self) -> Option<usize> {
        match self {
            Self::CountStar => None,
            Self::Count { slot } | Self::Sum { slot } | Self::Min { slot } | Self::Max { slot } => {
                Some(*slot)
            }
        }
    }

    fn tag(&self) -> &'static str {
        match self {
            Self::CountStar => "count_star",
            Self::Count { .. } => "count",
            Self::Sum { .. } => "sum",
            Self::Min { .. } => "min",
            Self::Max { .. } => "max",
        }
    }
}

impl State {
    pub fn empty(plan: &Plan) -> Result<Self, ModelError> {
        plan.validate()?;
        Ok(Self {
            calls: plan
                .calls
                .iter()
                .map(|call| match call.function {
                    Function::CountStar | Function::Count { .. } => Payload::Count(0),
                    Function::Sum { .. } => Payload::Sum {
                        non_null: 0,
                        value: 0,
                    },
                    Function::Min { .. } | Function::Max { .. } => {
                        Payload::Extrema(BTreeMap::new())
                    }
                })
                .collect(),
        })
    }

    pub fn decode(plan: &Plan, stored: StoredState) -> Result<Self, ModelError> {
        plan.validate()?;
        if stored.codec_version != STATE_CODEC_VERSION {
            return Err(ModelError::UnknownCodec);
        }
        if stored.calls.len() != plan.calls.len() {
            return Err(ModelError::Corrupt);
        }
        let mut calls = Vec::with_capacity(stored.calls.len());
        for (definition, stored_call) in plan.calls.iter().zip(stored.calls) {
            if stored_call.function_version != FUNCTION_VERSION {
                return Err(ModelError::UnknownVersion);
            }
            if stored_call.function_tag != definition.function.tag()
                || !payload_matches(&definition.function, &stored_call.payload)
            {
                return Err(ModelError::UnknownFunction);
            }
            validate_payload(&stored_call.payload)?;
            calls.push(stored_call.payload);
        }
        Ok(Self { calls })
    }

    pub fn encode(&self, plan: &Plan) -> StoredState {
        StoredState {
            codec_version: STATE_CODEC_VERSION,
            calls: plan
                .calls
                .iter()
                .zip(&self.calls)
                .map(|(call, payload)| StoredCall {
                    function_tag: call.function.tag().to_owned(),
                    function_version: call.function_version,
                    payload: payload.clone(),
                })
                .collect(),
        }
    }

    pub fn output(&self, plan: &Plan) -> Option<Row> {
        let row: Row = plan
            .calls
            .iter()
            .zip(&self.calls)
            .map(|(call, payload)| output_value(&call.function, payload))
            .collect();
        match &plan.having {
            None => Some(row),
            Some(having) => match row.get(having.ordinal - 1) {
                Some(Value::Int8(value)) if *value > having.greater_than => Some(row),
                Some(Value::Null | Value::Int8(_) | Value::Bool(_)) | None => None,
            },
        }
    }

    pub fn decode_result(plan: &Plan, stored: StoredResult) -> Result<Row, ModelError> {
        plan.validate()?;
        if stored.version != FORMAT_VERSION || stored.values.len() != plan.calls.len() {
            return Err(ModelError::Corrupt);
        }
        for (call, value) in plan.calls.iter().zip(&stored.values) {
            let nullable = !matches!(call.function, Function::CountStar | Function::Count { .. });
            if matches!(value, Value::Bool(_)) || (!nullable && matches!(value, Value::Null)) {
                return Err(ModelError::Schema);
            }
        }
        Ok(stored.values)
    }

    pub fn apply(&mut self, plan: &Plan, changes: &[Change]) -> Result<(), ModelError> {
        plan.validate()?;
        if changes.len() > MAX_CHANGES {
            return Err(ModelError::Bound);
        }
        let normalized = normalize(plan, changes)?;
        let mut staged = self.clone();
        for (row, multiplicity) in normalized.iter().filter(|(_, count)| **count < 0) {
            staged.apply_row(plan, row, *multiplicity)?;
        }
        for (row, multiplicity) in normalized.iter().filter(|(_, count)| **count > 0) {
            staged.apply_row(plan, row, *multiplicity)?;
        }
        *self = staged;
        Ok(())
    }

    pub fn stored_mut(&mut self, ordinal: usize) -> &mut Payload {
        &mut self.calls[ordinal]
    }

    fn apply_row(&mut self, plan: &Plan, row: &Row, multiplicity: i64) -> Result<(), ModelError> {
        for (call, payload) in plan.calls.iter().zip(&mut self.calls) {
            apply_call(&call.function, payload, row, multiplicity)?;
        }
        Ok(())
    }
}
