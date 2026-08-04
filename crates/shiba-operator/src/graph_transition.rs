use serde::{Deserialize, Serialize};
use shiba_protocol::{BootstrapBatchId, GraphTransactionId, SourceId};

use crate::{EffectOrigin, ResultDelta, StateDelta, TypedRow};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowDelta {
    pub before: Option<TypedRow>,
    pub after: Option<TypedRow>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaBatch {
    pub origin: EffectOrigin,
    pub layout_identity: [u8; 32],
    pub rows: Vec<RowDelta>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDeltaBatch {
    pub source_id: SourceId,
    pub delta: DeltaBatch,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiInputBatch {
    pub origin: GraphEffectOrigin,
    pub sources: Vec<SourceDeltaBatch>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphEffectOrigin {
    Wal(GraphTransactionId),
    Bootstrap(BootstrapBatchId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphTransition {
    pub state_deltas: Vec<StateDelta>,
    pub results: Vec<ResultDelta>,
}
