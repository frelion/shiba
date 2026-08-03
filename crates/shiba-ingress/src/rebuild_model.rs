use shiba_operator::OperatorId;
use shiba_protocol::{BootstrapId, SlotGeneration, SourceId};

/// Exact catalog and transport identity on one side of a rebuild transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildIdentity {
    pub bootstrap_id: BootstrapId,
    pub relation_oid: u32,
    pub identity_index_oid: u32,
    pub publication_oid: u32,
    pub slot_name: String,
    pub slot_generation: SlotGeneration,
}

/// Exact old-authority CAS and the preflighted target authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildSpec {
    pub source_id: SourceId,
    pub expected: RebuildIdentity,
    pub target: RebuildIdentity,
    pub count_operator_id: OperatorId,
    pub sum_operator_id: OperatorId,
}

/// Durable identity retained after destructive prepare.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedAuthority {
    pub(crate) source_id: SourceId,
    pub(crate) target: RebuildIdentity,
    pub(crate) retired_bootstrap_id: BootstrapId,
    pub(crate) retired_slot_name: String,
    pub(crate) retired_slot_generation: SlotGeneration,
    pub(crate) count_operator_id: OperatorId,
    pub(crate) sum_operator_id: OperatorId,
}

impl PreparedAuthority {
    pub(crate) fn from_spec(spec: &RebuildSpec) -> Self {
        Self {
            source_id: spec.source_id,
            target: spec.target.clone(),
            retired_bootstrap_id: spec.expected.bootstrap_id,
            retired_slot_name: spec.expected.slot_name.clone(),
            retired_slot_generation: spec.expected.slot_generation,
            count_operator_id: spec.count_operator_id,
            sum_operator_id: spec.sum_operator_id,
        }
    }
}
