use core::fmt;

use postgres::{Client, Transaction};
use shiba_compiler::{CompilerError, QuerySpecV1};
use shiba_operator::OperatorGraph;
use shiba_protocol::{GraphId, SourceId};

use crate::M2Error;

#[derive(Debug)]
pub enum RegistrationError {
    Compiler(CompilerError),
    Runtime(M2Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RebuildSourceTarget {
    pub source_id: SourceId,
    pub relation_id: u32,
    pub identity_index_id: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphResultContract {
    pub result_id: i64,
    pub schema_payload: Vec<u8>,
    pub schema_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildGraphArtifact {
    pub spec_payload: Vec<u8>,
    pub graph_payload: Vec<u8>,
    pub graph_digest: [u8; 32],
    pub results: Vec<GraphResultContract>,
}

impl fmt::Display for RegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compiler(error) => write!(formatter, "graph compilation failed: {error}"),
            Self::Runtime(error) => write!(formatter, "graph registration failed: {error}"),
        }
    }
}

impl std::error::Error for RegistrationError {}
impl From<CompilerError> for RegistrationError {
    fn from(error: CompilerError) -> Self {
        Self::Compiler(error)
    }
}
impl From<M2Error> for RegistrationError {
    fn from(error: M2Error) -> Self {
        Self::Runtime(error)
    }
}
impl From<postgres::Error> for RegistrationError {
    fn from(error: postgres::Error) -> Self {
        Self::Runtime(M2Error::Postgres(error))
    }
}

/// Compiles and atomically installs the sole canonical graph authority.
///
/// # Errors
/// Fails closed if a source binding, compiler contract, or authority write fails.
pub fn compile_and_register(
    client: &mut Client,
    spec: &QuerySpecV1,
) -> Result<OperatorGraph, RegistrationError> {
    let mut transaction = client.transaction()?;
    let graph = compile_and_register_in_transaction(&mut transaction, spec)?;
    transaction.commit()?;
    Ok(graph)
}

/// Compiles and installs the canonical graph inside the caller-owned transaction.
///
/// This is the transaction-local registration boundary for control-plane
/// frontends. Callers may perform bounded catalog binding in the same
/// transaction, but this function remains the sole graph definition/member/
/// result writer and does not commit or roll back the transaction itself.
///
/// # Errors
/// Fails closed if the canonical declaration, exact source descriptors,
/// compiler contract, or any authority write is invalid.
pub fn compile_and_register_in_transaction(
    transaction: &mut Transaction<'_>,
    spec: &QuerySpecV1,
) -> Result<OperatorGraph, RegistrationError> {
    let graph = crate::registration_compile::compile_current(transaction, spec)?;
    crate::registration_write::insert_graph(transaction, spec, &graph, "active")?;
    Ok(graph)
}

/// Compiles a target rebuild artifact before the destructive Catalog boundary.
///
/// # Errors
/// Reads the durable declaration and exact target identities without changing
/// graph authority, state, result, or lifecycle rows.
pub fn compile_rebuild_graph(
    transaction: &mut Transaction<'_>,
    graph_id: GraphId,
    targets: &[RebuildSourceTarget],
) -> Result<RebuildGraphArtifact, RegistrationError> {
    crate::rebuild_compile::compile_rebuild_graph(transaction, graph_id, targets)
}
