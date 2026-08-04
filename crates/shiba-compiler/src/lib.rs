//! Strict, database-independent compilation of versioned query graphs.

#![forbid(unsafe_code)]

mod aggregate_compile;
mod binding;
mod expression;
mod graph;
mod join_compile;
mod node_compile;
mod query_spec;
mod query_validate;

use core::fmt;

use shiba_operator::ObjectAddress;
use shiba_protocol::SourceId;

pub use graph::{compile_query, compile_query_with_optional_identities};
pub use query_spec::{
    QUERY_SPEC_VERSION, QueryAggregateCallV1, QueryExpressionV1, QueryFieldV1,
    QueryHavingExpressionV1, QueryInputV1, QueryNodeV1, QueryOperationV1, QueryResultFieldV1,
    QueryResultV1, QuerySelectorV1, QuerySpecV1,
};

pub const POSTGRES_INT8_TYPE_OID: u32 = 20;
pub const POSTGRES_TEXT_TYPE_OID: u32 = 25;

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct IdentityIndexDescriptor {
    pub address: ObjectAddress,
    pub relation: ObjectAddress,
    pub key_column: ObjectAddress,
    pub key_arity: u16,
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
    InvalidTopology,
    WrongType,
    GraphEncoding,
}

impl fmt::Display for CompilerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "query compilation rejected: {self:?}")
    }
}

impl std::error::Error for CompilerError {}
