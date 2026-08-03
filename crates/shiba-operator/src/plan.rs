use serde::{Deserialize, Serialize};

use crate::ValueType;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "shape", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputContract {
    Scalar {
        value_type: ValueType,
        nullable: bool,
    },
    KeyedRows {
        key_type: ValueType,
        key_nullable: bool,
        value_type: ValueType,
        nullable: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateContract {
    pub codec_version: u32,
}
