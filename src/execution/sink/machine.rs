use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SinkContinuation {
    pub(super) position: InputPosition,
    pub(super) remaining_weight: Option<i64>,
    pub(super) persisted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct EffectHead {
    pub(super) row_ordinal: i64,
    pub(super) weight: i64,
    pub(super) row_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SinkAction {
    pub(super) row_ordinal: i64,
    pub(super) applied_weight: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WeightPage {
    pub(super) applied_weight: i64,
    pub(super) remaining_weight: Option<i64>,
    pub(super) usage: WorkUsage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SinkMapping {
    pub(super) insert_columns: String,
    pub(super) select_columns: String,
    pub(super) delete_predicate: String,
    pub(super) effect_equality: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PayloadLayout {
    pub(super) relation: RelationRef,
    pub(super) attributes: Vec<AttributeRef>,
}
