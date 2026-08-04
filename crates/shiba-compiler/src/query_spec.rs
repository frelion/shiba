use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use shiba_operator::AggregateFunctionV1;
use shiba_protocol::{GraphId, SourceId};

pub const QUERY_SPEC_VERSION: u32 = 2;
const QUERY_SPEC_DOMAIN: &[u8] = b"shiba.query.spec.v2\0";
const MAX_CANONICAL_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QuerySpecV1 {
    pub version: u32,
    pub graph_id: GraphId,
    pub sources: Vec<SourceId>,
    pub nodes: Vec<QueryNodeV1>,
    pub results: Vec<QueryResultV1>,
}

impl QuerySpecV1 {
    /// Returns the unique compact declaration bytes.
    ///
    /// # Errors
    ///
    /// Rejects declarations that cannot round-trip through the strict versioned schema.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        crate::query_validate::validate_query::<serde::de::value::Error>(
            self.version,
            &self.sources,
            &self.nodes,
            &self.results,
        )
        .map_err(|_| invalid_json("invalid query declaration"))?;
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() > MAX_CANONICAL_BYTES {
            return Err(invalid_json("invalid query declaration"));
        }
        Ok(bytes)
    }

    /// Decodes one strict, bounded version-1 declaration.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, unknown, out-of-order, or out-of-bound declarations.
    pub fn from_json(input: &[u8]) -> Result<Self, serde_json::Error> {
        let value: Self = serde_json::from_slice(input)?;
        if serde_json::to_vec(&value)? != input || input.len() > MAX_CANONICAL_BYTES {
            return Err(invalid_json("noncanonical query declaration"));
        }
        Ok(value)
    }

    /// Hashes the canonical declaration in a domain separate from compiled plans.
    ///
    /// # Errors
    ///
    /// Rejects invalid declarations or serialization failure.
    pub fn canonical_digest(&self) -> Result<[u8; 32], serde_json::Error> {
        let mut hash = Sha256::new();
        hash.update(QUERY_SPEC_DOMAIN);
        hash.update(self.to_canonical_json()?);
        Ok(hash.finalize().into())
    }
}

fn invalid_json(message: &'static str) -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

impl<'de> Deserialize<'de> for QuerySpecV1 {
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
            nodes: Vec<QueryNodeV1>,
            results: Vec<QueryResultV1>,
        }
        let raw = Raw::deserialize(deserializer)?;
        crate::query_validate::validate_query::<D::Error>(
            raw.version,
            &raw.sources,
            &raw.nodes,
            &raw.results,
        )?;
        Ok(Self {
            version: raw.version,
            graph_id: raw.graph_id,
            sources: raw.sources,
            nodes: raw.nodes,
            results: raw.results,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryNodeV1 {
    pub inputs: Vec<QueryInputV1>,
    pub state_codec_version: Option<u32>,
    pub operation: QueryOperationV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "input", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryInputV1 {
    Source { source_id: SourceId },
    Node { node: u16 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryOperationV1 {
    Aggregate {
        group_expressions: Vec<QueryExpressionV1>,
        calls: Vec<QueryAggregateCallV1>,
        having: Option<QueryHavingExpressionV1>,
    },
    Filter {
        predicate: QueryExpressionV1,
    },
    Project {
        expressions: Vec<QueryExpressionV1>,
    },
    Compute {
        expressions: Vec<QueryExpressionV1>,
    },
    KeyBy {
        key: QueryExpressionV1,
    },
    InnerJoin {
        left_id: QueryFieldV1,
        left_key: QueryFieldV1,
        right_id: QueryFieldV1,
        right_payload: QueryFieldV1,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "having", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryHavingExpressionV1 {
    Call { ordinal: u16 },
    Int8Literal { value: i64 },
    NullLiteral,
    Equal { left: Box<Self>, right: Box<Self> },
    NotEqual { left: Box<Self>, right: Box<Self> },
    Less { left: Box<Self>, right: Box<Self> },
    LessEqual { left: Box<Self>, right: Box<Self> },
    Greater { left: Box<Self>, right: Box<Self> },
    GreaterEqual { left: Box<Self>, right: Box<Self> },
    IsNull { input: Box<Self> },
    And { left: Box<Self>, right: Box<Self> },
    Or { left: Box<Self>, right: Box<Self> },
    Not { input: Box<Self> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryAggregateCallV1 {
    pub ordinal: u16,
    pub function: AggregateFunctionV1,
    pub function_version: u32,
    pub expression: Option<QueryExpressionV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "expression", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryExpressionV1 {
    Column { field: QueryFieldV1 },
    Int8Literal { value: i64 },
    NullInt8,
    Equal { left: Box<Self>, right: Box<Self> },
    NotEqual { left: Box<Self>, right: Box<Self> },
    Less { left: Box<Self>, right: Box<Self> },
    LessEqual { left: Box<Self>, right: Box<Self> },
    Greater { left: Box<Self>, right: Box<Self> },
    GreaterEqual { left: Box<Self>, right: Box<Self> },
    IsNull { input: Box<Self> },
    And { left: Box<Self>, right: Box<Self> },
    Or { left: Box<Self>, right: Box<Self> },
    Not { input: Box<Self> },
    CheckedAdd { left: Box<Self>, right: Box<Self> },
    CheckedSubtract { left: Box<Self>, right: Box<Self> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryFieldV1 {
    pub input: u8,
    pub selector: QuerySelectorV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "selector", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuerySelectorV1 {
    Name { name: String, quoted: bool },
    Slot { slot: u16 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryResultV1 {
    pub input_node: u16,
    pub fields: Vec<QueryResultFieldV1>,
    pub key_ordinals: Vec<u16>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryResultFieldV1 {
    pub name: String,
    pub value_slot: u16,
    pub nullable: bool,
}
