use core::{fmt, num::NonZeroU32};
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shiba_protocol::{GraphId, SourceId};

use crate::{
    AggregateCall, Expression, HavingExpression, ObjectAddress, OutputContract, StateContract,
    TypedLayout, ValueType,
};

pub const GRAPH_FORMAT_VERSION: u32 = 2;
pub const MAX_GRAPH_NODES: usize = 32;
pub const MAX_INPUT_DELTA_ROWS: usize = 10_000;
pub const MAX_NODE_DELTA_ROWS: usize = 20_000;
pub const MAX_GRAPH_DELTA_ROWS: usize = 200_000;
pub const MAX_GRAPH_WORK_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const GRAPH_DOMAIN: &[u8] = b"shiba.operator.graph.v2\0";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(NonZeroU32);

impl NodeId {
    #[must_use]
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnBinding {
    pub address: ObjectAddress,
    pub value_type: ValueType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePort {
    pub source_id: SourceId,
    pub layout: Vec<ColumnBinding>,
    pub identity_index: Option<ObjectAddress>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeInput {
    SourcePort(SourceId),
    Node(NodeId),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorNodeKind {
    Aggregate {
        group_expressions: Vec<Expression>,
        calls: Vec<AggregateCall>,
        having: Option<HavingExpression>,
    },
    Filter {
        predicate: Expression,
    },
    Project {
        expressions: Vec<Expression>,
    },
    Compute {
        expressions: Vec<Expression>,
    },
    KeyBy {
        key: Expression,
    },
    InnerJoin {
        left_source_id: SourceId,
        right_source_id: SourceId,
        left_id_slot: u16,
        left_key_slot: u16,
        right_id_slot: u16,
        right_payload_slot: u16,
    },
    Materialize {
        field_slots: Vec<u16>,
        output: OutputContract,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorNode {
    pub node_id: NodeId,
    pub input: NodeInput,
    pub state_contract: Option<StateContract>,
    pub kind: OperatorNodeKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CanonicalGraph {
    pub(crate) format_version: u32,
    pub(crate) graph_id: GraphId,
    pub(crate) sources: Vec<SourcePort>,
    pub(crate) nodes: Vec<OperatorNode>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorGraph {
    pub format_version: u32,
    pub graph_id: GraphId,
    pub sources: Vec<SourcePort>,
    pub nodes: Vec<OperatorNode>,
    pub canonical_payload: Vec<u8>,
    pub digest: [u8; 32],
}

impl OperatorGraph {
    /// Builds and canonicalizes one bounded, topologically ordered graph.
    ///
    /// # Errors
    ///
    /// Rejects invalid topology, layouts, expressions, state, or output contracts.
    pub fn build(
        graph_id: GraphId,
        sources: Vec<SourcePort>,
        nodes: Vec<OperatorNode>,
    ) -> Result<Self, GraphError> {
        let canonical = CanonicalGraph {
            format_version: GRAPH_FORMAT_VERSION,
            graph_id,
            sources,
            nodes,
        };
        crate::graph_validation::validate_graph(&canonical)?;
        let canonical_payload = serde_json::to_vec(&canonical).map_err(|_| GraphError::Codec)?;
        let digest = hash(GRAPH_DOMAIN, &canonical_payload);
        Ok(Self {
            format_version: canonical.format_version,
            graph_id: canonical.graph_id,
            sources: canonical.sources,
            nodes: canonical.nodes,
            canonical_payload,
            digest,
        })
    }

    /// Decodes exact canonical bytes and verifies their supplied digest.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, structurally invalid, or mismatched input.
    pub fn from_canonical_payload(payload: &[u8], digest: [u8; 32]) -> Result<Self, GraphError> {
        let canonical: CanonicalGraph =
            serde_json::from_slice(payload).map_err(|_| GraphError::Codec)?;
        let rebuilt = Self::build(
            canonical.graph_id,
            canonical.sources.clone(),
            canonical.nodes.clone(),
        )?;
        if rebuilt.format_version != canonical.format_version
            || rebuilt.canonical_payload != payload
            || rebuilt.digest != digest
        {
            return Err(GraphError::DigestMismatch);
        }
        Ok(rebuilt)
    }

    /// Revalidates the complete canonical graph and digest.
    ///
    /// # Errors
    ///
    /// Rejects any in-memory or encoded contract drift.
    pub fn validate(&self) -> Result<(), GraphError> {
        let rebuilt = Self::from_canonical_payload(&self.canonical_payload, self.digest)?;
        if rebuilt != *self {
            return Err(GraphError::DigestMismatch);
        }
        Ok(())
    }

    /// Returns the generic result-sink contracts exposed by this graph.
    ///
    /// Concrete terminal-node dispatch remains inside the database-independent
    /// operator kernel; Runtime and Ingress only consume result identities and
    /// output shapes.
    pub fn result_contracts(&self) -> impl Iterator<Item = (NodeId, &OutputContract)> {
        self.nodes.iter().filter_map(|node| {
            let OperatorNodeKind::Materialize { output, .. } = &node.kind else {
                return None;
            };
            Some((node.node_id, output))
        })
    }

    pub(crate) fn layouts(
        &self,
    ) -> Result<(TypedLayout, BTreeMap<NodeId, TypedLayout>), GraphError> {
        crate::graph_validation::layout_graph(&CanonicalGraph {
            format_version: self.format_version,
            graph_id: self.graph_id,
            sources: self.sources.clone(),
            nodes: self.nodes.clone(),
        })
    }
}

fn hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    hash.finalize().into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphError {
    Codec,
    DigestMismatch,
    InvalidTopology,
    InvalidNode,
    InvalidStateContract,
    MissingResult,
    WrongType,
    Layout,
    Expression,
    OutputLimit,
    ConflictingKey,
}
impl From<crate::TypedError> for GraphError {
    fn from(_: crate::TypedError) -> Self {
        Self::Layout
    }
}
impl From<crate::ExpressionError> for GraphError {
    fn from(_: crate::ExpressionError) -> Self {
        Self::Expression
    }
}
impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "operator graph rejected: {self:?}")
    }
}
impl std::error::Error for GraphError {}
