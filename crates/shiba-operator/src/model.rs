use core::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use shiba_protocol::{BootstrapBatchId, SourceTransactionId};

use crate::TypedValue;

/// Stable identity of one registered operator.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperatorId(NonZeroU64);

impl OperatorId {
    #[must_use]
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Database-independent form of a `PostgreSQL` object address.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectAddress {
    pub class_id: u32,
    pub object_id: u32,
    pub sub_id: i32,
}

/// Closed identity namespace for WAL and bootstrap effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectOrigin {
    Wal(SourceTransactionId),
    Bootstrap(BootstrapBatchId),
}

/// Runtime-opaque, strictly versioned operator state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncodedOperatorState {
    pub codec_version: u32,
    pub payload: Vec<u8>,
}

/// One explicit keyed-result mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mutation", rename_all = "snake_case", deny_unknown_fields)]
pub enum KeyedMutation {
    Delete { key: TypedValue },
    Upsert { key: TypedValue, value: TypedValue },
}

/// Persistence-only output selected by the compiled output contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "shape", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputDelta {
    ScalarReplacement { value: TypedValue },
    KeyedMutations { mutations: Vec<KeyedMutation> },
}

/// Complete pure result of one plan/state/effect evaluation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorTransition {
    pub next_state: EncodedOperatorState,
    pub output_delta: OutputDelta,
}
