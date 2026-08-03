use core::fmt;

use postgres::{Client, Transaction};
use shiba_compiler::{CompilerError, QuerySpecV1, compile_query};
use shiba_operator::{OperatorGraph, OutputContract, TypedValue, ValueType};
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
    pub output_shape: &'static str,
    pub key_nullable: bool,
    pub value_nullable: bool,
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
    let graph = compile_current(transaction, spec)?;
    insert_graph(transaction, spec, &graph, "active")?;
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
    let graph_key = bigint(graph_id.get())?;
    let payload: Vec<u8> = transaction
        .query_opt(
            "SELECT spec_payload FROM shiba_internal.graph_definition
             WHERE graph_id = $1 FOR UPDATE",
            &[&graph_key],
        )?
        .ok_or(M2Error::InvalidOperatorDefinition)?
        .get(0);
    let spec = QuerySpecV1::from_json(&payload).map_err(|_| CompilerError::InvalidSpec)?;
    if spec.graph_id != graph_id
        || spec
            .to_canonical_json()
            .map_err(|_| CompilerError::GraphEncoding)?
            != payload
    {
        return Err(M2Error::InvalidOperatorDefinition.into());
    }
    if targets
        .iter()
        .map(|target| target.source_id)
        .collect::<Vec<_>>()
        != spec.sources
    {
        return Err(M2Error::InvalidOperatorDefinition.into());
    }
    let mut descriptors = Vec::with_capacity(targets.len());
    let mut indexes = Vec::with_capacity(targets.len());
    for target in targets {
        let (descriptor, identity) =
            crate::registration_descriptor::target_descriptor(transaction, *target)?;
        descriptors.push(descriptor);
        indexes.push(identity);
    }
    let graph = compile_query(&spec, &descriptors, &indexes)?;
    Ok(RebuildGraphArtifact {
        spec_payload: payload,
        graph_payload: graph.canonical_payload.clone(),
        graph_digest: graph.digest,
        results: result_contracts(&graph),
    })
}

fn compile_current(
    transaction: &mut Transaction<'_>,
    spec: &QuerySpecV1,
) -> Result<OperatorGraph, RegistrationError> {
    let mut lock_order = spec.sources.iter().copied().enumerate().collect::<Vec<_>>();
    lock_order.sort_unstable_by_key(|(_, source_id)| source_id.get());
    if lock_order.windows(2).any(|pair| pair[0].1 == pair[1].1) {
        return Err(CompilerError::InvalidSpec.into());
    }

    let mut resolved = (0..spec.sources.len()).map(|_| None).collect::<Vec<_>>();
    for (ordinal, source_id) in lock_order {
        let (descriptor, identity) =
            crate::registration_descriptor::source_descriptor(transaction, source_id)?;
        resolved[ordinal] = Some((descriptor, identity));
    }
    let (descriptors, indexes): (Vec<_>, Vec<_>) = resolved
        .into_iter()
        .map(|entry| entry.ok_or(CompilerError::InvalidSpec))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip();
    Ok(shiba_compiler::compile_query_with_optional_identities(
        spec,
        &descriptors,
        &indexes,
    )?)
}

fn insert_graph(
    transaction: &mut Transaction<'_>,
    spec: &QuerySpecV1,
    graph: &OperatorGraph,
    status: &str,
) -> Result<(), RegistrationError> {
    let graph_id = bigint(graph.graph_id.get())?;
    let spec_payload = spec
        .to_canonical_json()
        .map_err(|_| CompilerError::GraphEncoding)?;
    transaction.execute(
        "INSERT INTO shiba_internal.graph_definition (
             graph_id, source_count, compiler_version, spec_payload,
             graph_format_version, graph_payload, graph_digest, state_codec_version)
         VALUES ($1, $2, 2, $3, $4, $5, $6, 1)",
        &[
            &graph_id,
            &i16::try_from(graph.sources.len()).map_err(|_| M2Error::InvalidOperatorDefinition)?,
            &spec_payload,
            &i32::try_from(graph.format_version).map_err(|_| M2Error::InvalidOperatorDefinition)?,
            &graph.canonical_payload,
            &&graph.digest[..],
        ],
    )?;
    for (ordinal, source) in graph.sources.iter().enumerate() {
        transaction.execute(
            "INSERT INTO shiba_internal.graph_source_member
                 (graph_id, source_id, input_ordinal, graph_digest)
             VALUES ($1, $2, $3, $4)",
            &[
                &graph_id,
                &bigint(source.source_id.get())?,
                &i16::try_from(ordinal).map_err(|_| M2Error::InvalidOperatorDefinition)?,
                &&graph.digest[..],
            ],
        )?;
    }
    insert_results(transaction, graph, status)?;
    Ok(())
}

fn insert_results(
    transaction: &mut Transaction<'_>,
    graph: &OperatorGraph,
    status: &str,
) -> Result<(), RegistrationError> {
    let graph_id = bigint(graph.graph_id.get())?;
    for (result_id, output) in graph.result_contracts() {
        let (shape, key_type, key_nullable, value_type, value_nullable) = metadata(output);
        let initial = if matches!(output, OutputContract::Scalar { .. }) {
            Some(TypedValue::Int8(0))
        } else {
            None
        };
        let payload = initial
            .as_ref()
            .map(TypedValue::to_canonical_json)
            .transpose()
            .map_err(|_| M2Error::InvalidOperatorDefinition)?;
        let value = initial
            .as_ref()
            .map(crate::result_sink::scalar_int8)
            .transpose()?;
        transaction.execute(
            "INSERT INTO shiba.graph_result (
                 graph_id, result_id, output_shape, output_key_type,
                 output_key_nullable, output_value_type, output_value_nullable,
                 result_status, value_payload, value_bigint)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            &[
                &graph_id,
                &i64::from(result_id.get()),
                &shape,
                &key_type,
                &key_nullable,
                &value_type,
                &value_nullable,
                &status,
                &if status == "active" { payload } else { None },
                &if status == "active" { value } else { None },
            ],
        )?;
    }
    Ok(())
}

fn result_contracts(graph: &OperatorGraph) -> Vec<GraphResultContract> {
    graph
        .result_contracts()
        .map(|(result_id, output)| {
            let (shape, _, key_nullable, _, value_nullable) = metadata(output);
            GraphResultContract {
                result_id: i64::from(result_id.get()),
                output_shape: shape,
                key_nullable,
                value_nullable,
            }
        })
        .collect()
}

fn metadata(
    contract: &OutputContract,
) -> (&'static str, Option<&'static str>, bool, &'static str, bool) {
    match contract {
        OutputContract::Scalar { value_type } => {
            ("scalar", None, false, type_name(*value_type), false)
        }
        OutputContract::KeyedRows {
            key_type,
            key_nullable,
            value_type,
            nullable,
        } => (
            "keyed",
            Some(type_name(*key_type)),
            *key_nullable,
            type_name(*value_type),
            *nullable,
        ),
    }
}
fn type_name(value: ValueType) -> &'static str {
    match value {
        ValueType::Bool => "bool",
        ValueType::Int8 => "int8",
        ValueType::Text => "text",
    }
}
fn bigint(value: u64) -> Result<i64, M2Error> {
    i64::try_from(value).map_err(|_| M2Error::InvalidOperatorDefinition)
}
