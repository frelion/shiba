use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};

use crate::operator_authority::GraphFingerprint;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildMemberIdentity {
    pub source_id: SourceId,
    pub relation_oid: u32,
    pub identity_index_oid: u32,
}

/// Exact catalog and transport identity on one side of a graph rebuild.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildIdentity {
    pub bootstrap_id: BootstrapId,
    pub graph_digest: [u8; 32],
    pub members: Vec<RebuildMemberIdentity>,
    pub publication_oid: u32,
    pub slot_name: String,
    pub slot_generation: SlotGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildSpec {
    pub graph_id: GraphId,
    pub expected: RebuildIdentity,
    pub target: RebuildIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedAuthority {
    pub(crate) graph_id: GraphId,
    pub(crate) target: RebuildIdentity,
    pub(crate) retired_bootstrap_id: BootstrapId,
    pub(crate) retired_slot_name: String,
    pub(crate) retired_slot_generation: SlotGeneration,
    pub(crate) graph: GraphFingerprint,
}

impl PreparedAuthority {
    pub(crate) fn matches_spec(&self, spec: &RebuildSpec) -> bool {
        self.graph_id == spec.graph_id
            && self.target == spec.target
            && self.retired_bootstrap_id == spec.expected.bootstrap_id
            && self.retired_slot_name == spec.expected.slot_name
            && self.retired_slot_generation == spec.expected.slot_generation
            && self.graph.graph_id == spec.graph_id
            && self.graph.digest == spec.target.graph_digest
    }
}
