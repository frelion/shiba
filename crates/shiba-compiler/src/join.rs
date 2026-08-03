use core::num::NonZeroU32;

use shiba_operator::{
    ColumnBinding, NodeId, NodeInput, OperatorGraph, OperatorNode, OperatorNodeKind,
    OutputContract, SourcePort, StateContract, ValueType,
};

use crate::{
    CompilerError, IdentityIndexDescriptor, JoinSpecV1, OPERATOR_SPEC_VERSION,
    POSTGRES_INT8_TYPE_OID, SourceColumnDescriptor, SourceDescriptor,
};

/// Compiles one strict two-source equality join into a canonical graph.
///
/// # Errors
///
/// Rejects descriptor/identity drift, invalid source shape, ambiguous column
/// names, non-`int8` inputs, or a graph that cannot be canonically encoded.
pub fn compile_join(
    spec: &JoinSpecV1,
    left: &SourceDescriptor,
    right: &SourceDescriptor,
    index: &IdentityIndexDescriptor,
) -> Result<OperatorGraph, CompilerError> {
    validate_spec(spec, left, right)?;
    let (left_id_slot, left_id) = resolve(left, &spec.left_id_column)?;
    let (left_key_slot, left_key) = resolve(left, &spec.left_right_key_column)?;
    let (right_id_slot, right_id) = resolve(right, &spec.right_id_column)?;
    let (right_payload_slot, right_payload) = resolve(right, &spec.right_payload_column)?;
    if left_id.nullable
        || !left_key.nullable
        || right_id.nullable
        || !right_payload.nullable
        || left.columns.len() != 2
        || right.columns.len() != 2
        || left_id.address == left_key.address
        || right_id.address == right_payload.address
        || index.address != spec.right_identity_index
        || index.relation != right.relation
        || index.key_column != right_id.address
        || !index.unique
        || !index.valid
        || !index.ready
        || index.has_expression
        || index.has_predicate
        || !index.effective_replica_identity
    {
        return Err(CompilerError::InvalidIdentityIndex);
    }
    let mut sources = vec![
        source_port(left, None)?,
        source_port(right, Some(index.address))?,
    ];
    sources.sort_by_key(|source| source.source_id);
    let output = OutputContract::KeyedRows {
        key_type: ValueType::Int8,
        key_nullable: false,
        value_type: ValueType::Int8,
        nullable: true,
    };
    OperatorGraph::build_graph(
        spec.graph_id,
        sources.clone(),
        vec![
            OperatorNode {
                node_id: node(1),
                input: NodeInput::SourcePort(spec.left_source_id),
                state_contract: Some(StateContract { codec_version: 1 }),
                kind: OperatorNodeKind::InnerJoin {
                    left_source_id: spec.left_source_id,
                    right_source_id: spec.right_source_id,
                    left_id_slot: slot(left_id_slot)?,
                    left_key_slot: slot(left_key_slot)?,
                    right_id_slot: slot(right_id_slot)?,
                    right_payload_slot: slot(right_payload_slot)?,
                },
            },
            OperatorNode {
                node_id: node(2),
                input: NodeInput::Node(node(1)),
                state_contract: None,
                kind: OperatorNodeKind::Materialize {
                    key_slot: 0,
                    value_slot: 1,
                    output: output.clone(),
                },
            },
        ],
    )
    .map_err(|_| CompilerError::GraphEncoding)
}

fn validate_spec(
    spec: &JoinSpecV1,
    left: &SourceDescriptor,
    right: &SourceDescriptor,
) -> Result<(), CompilerError> {
    if spec.version != OPERATOR_SPEC_VERSION
        || spec.left_source_id == spec.right_source_id
        || left.source_id != spec.left_source_id
        || right.source_id != spec.right_source_id
        || [
            &spec.left_id_column,
            &spec.left_right_key_column,
            &spec.right_id_column,
            &spec.right_payload_column,
        ]
        .iter()
        .any(|name| name.trim().is_empty())
    {
        return Err(CompilerError::InvalidJoinSpec);
    }
    Ok(())
}

fn source_port(
    source: &SourceDescriptor,
    identity_index: Option<shiba_operator::ObjectAddress>,
) -> Result<SourcePort, CompilerError> {
    Ok(SourcePort {
        source_id: source.source_id,
        layout: source
            .columns
            .iter()
            .map(|column| {
                if column.type_oid != POSTGRES_INT8_TYPE_OID {
                    return Err(CompilerError::WrongColumnType {
                        column: column.name.clone(),
                        type_oid: column.type_oid,
                    });
                }
                Ok(ColumnBinding {
                    address: column.address,
                    value_type: ValueType::Int8,
                })
            })
            .collect::<Result<_, _>>()?,
        identity_index,
    })
}

fn resolve<'a>(
    source: &'a SourceDescriptor,
    name: &str,
) -> Result<(usize, &'a SourceColumnDescriptor), CompilerError> {
    let mut matches = source
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name == name);
    let found = matches
        .next()
        .ok_or_else(|| CompilerError::MissingColumn(name.into()))?;
    if matches.next().is_some() {
        return Err(CompilerError::DuplicateColumn(name.into()));
    }
    if found.1.type_oid != POSTGRES_INT8_TYPE_OID {
        return Err(CompilerError::WrongColumnType {
            column: name.into(),
            type_oid: found.1.type_oid,
        });
    }
    Ok(found)
}

fn node(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).expect("fixed nonzero node ID"))
}

fn slot(value: usize) -> Result<u16, CompilerError> {
    u16::try_from(value).map_err(|_| CompilerError::GraphEncoding)
}
