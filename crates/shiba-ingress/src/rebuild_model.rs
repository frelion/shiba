use shiba_protocol::{BootstrapId, SlotGeneration, SourceId};

use crate::operator_authority::PlanFingerprint;

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
}

/// Durable identity retained after destructive prepare.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedAuthority {
    pub(crate) source_id: SourceId,
    pub(crate) target: RebuildIdentity,
    pub(crate) retired_bootstrap_id: BootstrapId,
    pub(crate) retired_slot_name: String,
    pub(crate) retired_slot_generation: SlotGeneration,
    pub(crate) plans: Vec<PlanFingerprint>,
}

impl PreparedAuthority {
    pub(crate) fn matches_spec(&self, spec: &RebuildSpec) -> bool {
        self.source_id == spec.source_id
            && self.target == spec.target
            && self.retired_bootstrap_id == spec.expected.bootstrap_id
            && self.retired_slot_name == spec.expected.slot_name
            && self.retired_slot_generation == spec.expected.slot_generation
            && !self.plans.is_empty()
    }
}
