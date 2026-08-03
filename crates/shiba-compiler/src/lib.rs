//! Strict, database-independent compilation of versioned operator graphs.

#![forbid(unsafe_code)]

mod binding;
mod graph;
mod join_compile;
mod pipeline;

use core::fmt;

use serde::{Deserialize, Deserializer, Serialize, de};
use shiba_operator::{NodeId, ObjectAddress};
use shiba_protocol::{GraphId, SourceId};

pub use graph::compile_graph;

pub const GRAPH_SPEC_VERSION: u32 = 1;
pub const POSTGRES_INT8_TYPE_OID: u32 = 20;
pub const POSTGRES_TEXT_TYPE_OID: u32 = 25;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GraphSpecV1 {
    pub version: u32,
    pub graph_id: GraphId,
    pub sources: Vec<SourceId>,
    pub outputs: Vec<GraphOutputSpecV1>,
}

impl GraphSpecV1 {
    /// Encodes the unique compact JSON representation.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if encoding fails.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Decodes one strict version-1 graph declaration.
    ///
    /// # Errors
    ///
    /// Rejects malformed, trailing, unknown, unsupported, unordered, or blank input.
    pub fn from_json(input: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(input)
    }
}

impl<'de> Deserialize<'de> for GraphSpecV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            version: u32,
            graph_id: GraphId,
            sources: Vec<SourceId>,
            outputs: Vec<GraphOutputSpecV1>,
        }
        let raw = Raw::deserialize(deserializer)?;
        if raw.version != GRAPH_SPEC_VERSION
            || !(1..=2).contains(&raw.sources.len())
            || raw.sources.windows(2).any(|pair| pair[0] >= pair[1])
            || raw.outputs.is_empty()
        {
            return Err(de::Error::custom("invalid graph spec envelope"));
        }
        for output in &raw.outputs {
            output.validate_names::<D::Error>()?;
        }
        Ok(Self {
            version: raw.version,
            graph_id: raw.graph_id,
            sources: raw.sources,
            outputs: raw.outputs,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphOutputSpecV1 {
    CountRows {
        source_id: SourceId,
        aggregate_node_id: NodeId,
        result_node_id: NodeId,
    },
    SumInt8 {
        source_id: SourceId,
        input_column: String,
        aggregate_node_id: NodeId,
        result_node_id: NodeId,
    },
    MaterializedProject {
        source_id: SourceId,
        key_column: String,
        value_column: String,
        project_node_id: NodeId,
        result_node_id: NodeId,
    },
    ComputedProject {
        source_id: SourceId,
        key_column: String,
        input_column: String,
        literal: i64,
        compute_node_id: NodeId,
        project_node_id: NodeId,
        result_node_id: NodeId,
    },
    GroupedCount {
        source_id: SourceId,
        key_column: String,
        key_node_id: NodeId,
        aggregate_node_id: NodeId,
        result_node_id: NodeId,
    },
    GroupedSumInt8 {
        source_id: SourceId,
        key_column: String,
        input_column: String,
        key_node_id: NodeId,
        aggregate_node_id: NodeId,
        result_node_id: NodeId,
    },
    FilteredGroupedCount {
        source_id: SourceId,
        filter_column: String,
        greater_than: i64,
        group_key_column: String,
        filter_node_id: NodeId,
        project_node_id: NodeId,
        key_node_id: NodeId,
        aggregate_node_id: NodeId,
        result_node_id: NodeId,
    },
    FilteredGroupedSumInt8 {
        source_id: SourceId,
        filter_column: String,
        greater_than: i64,
        group_key_column: String,
        input_column: String,
        filter_node_id: NodeId,
        project_node_id: NodeId,
        key_node_id: NodeId,
        aggregate_node_id: NodeId,
        result_node_id: NodeId,
    },
    InnerJoin {
        left_source_id: SourceId,
        right_source_id: SourceId,
        left_id_column: String,
        left_right_key_column: String,
        right_id_column: String,
        right_payload_column: String,
        right_identity_index: ObjectAddress,
        join_node_id: NodeId,
        result_node_id: NodeId,
    },
}

impl GraphOutputSpecV1 {
    fn validate_names<E: de::Error>(&self) -> Result<(), E> {
        let names: &[&str] = match self {
            Self::CountRows { .. } => &[],
            Self::SumInt8 { input_column, .. } => &[input_column],
            Self::MaterializedProject {
                key_column,
                value_column,
                ..
            } => &[key_column, value_column],
            Self::ComputedProject {
                key_column,
                input_column,
                ..
            }
            | Self::GroupedSumInt8 {
                key_column,
                input_column,
                ..
            } => &[key_column, input_column],
            Self::GroupedCount { key_column, .. } => &[key_column],
            Self::FilteredGroupedCount {
                filter_column,
                group_key_column,
                ..
            } => &[filter_column, group_key_column],
            Self::FilteredGroupedSumInt8 {
                filter_column,
                group_key_column,
                input_column,
                ..
            } => &[filter_column, group_key_column, input_column],
            Self::InnerJoin {
                left_id_column,
                left_right_key_column,
                right_id_column,
                right_payload_column,
                ..
            } => &[
                left_id_column,
                left_right_key_column,
                right_id_column,
                right_payload_column,
            ],
        };
        if names.iter().any(|name| name.trim().is_empty()) {
            return Err(E::custom("column name cannot be blank"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct IdentityIndexDescriptor {
    pub address: ObjectAddress,
    pub relation: ObjectAddress,
    pub key_column: ObjectAddress,
    pub unique: bool,
    pub valid: bool,
    pub ready: bool,
    pub has_expression: bool,
    pub has_predicate: bool,
    pub effective_replica_identity: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDescriptor {
    pub source_id: SourceId,
    pub relation: ObjectAddress,
    pub columns: Vec<SourceColumnDescriptor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceColumnDescriptor {
    pub name: String,
    pub address: ObjectAddress,
    pub type_oid: u32,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompilerError {
    InvalidSpec,
    SourceMismatch,
    MissingColumn(String),
    DuplicateColumn(String),
    WrongColumnType { column: String, type_oid: u32 },
    NullableKey(String),
    InvalidIdentityIndex,
    GraphEncoding,
}

impl fmt::Display for CompilerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "graph compilation rejected: {self:?}")
    }
}

impl std::error::Error for CompilerError {}
