use serde::{Deserialize, Serialize};
use shiba_protocol::{BootstrapBatchId, SourceTransactionId};

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
