use core::num::NonZeroU32;

use shiba_operator::{
    ColumnBinding, Expression, NodeId, NodeInput, OperatorGraph, OperatorNode, OperatorNodeKind,
    OutputContract, StateContract, ValueType,
};

use crate::{CompilerError, OperatorOperationV1, OperatorSpecV1, SourceDescriptor};

/// Compiles the first non-aggregate declaration into ordinary graph nodes.
///
/// # Errors
///
/// Rejects non-graph declarations, identity/type/nullability drift, ambiguous
/// names, and any noncanonical graph.
pub fn compile_graph(
    spec: &OperatorSpecV1,
    source: &SourceDescriptor,
) -> Result<OperatorGraph, CompilerError> {
    if spec.source_id != source.source_id {
        return Err(CompilerError::SourceMismatch);
    }
    let source_layout = source
        .columns
        .iter()
        .map(|column| {
            let value_type = match column.type_oid {
                crate::POSTGRES_INT8_TYPE_OID => ValueType::Int8,
                crate::POSTGRES_TEXT_TYPE_OID => ValueType::Text,
                type_oid => {
                    return Err(CompilerError::WrongColumnType {
                        column: column.name.clone(),
                        type_oid,
                    });
                }
            };
            Ok(ColumnBinding {
                address: column.address,
                value_type,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let nodes = match &spec.operation {
        OperatorOperationV1::MaterializedProject {
            key_column,
            value_column,
        } => {
            let (key_slot, key) = resolve(source, key_column)?;
            if key.nullable {
                return Err(CompilerError::NullableKey(key_column.clone()));
            }
            let (value_slot, _) = resolve(source, value_column)?;
            project_nodes(key_slot, value_slot)?
        }
        OperatorOperationV1::GroupedCount { key_column } => {
            let (key_slot, key) = resolve(source, key_column)?;
            grouped_nodes(key_slot, key.nullable, None, source_layout.len())?
        }
        OperatorOperationV1::GroupedSumInt8 {
            key_column,
            input_column,
        } => {
            let (key_slot, key) = resolve(source, key_column)?;
            let (value_slot, _) = resolve(source, input_column)?;
            grouped_nodes(
                key_slot,
                key.nullable,
                Some(value_slot),
                source_layout.len(),
            )?
        }
        _ => return Err(CompilerError::PlanRequired),
    };
    OperatorGraph::build(spec.operator_id, spec.source_id, source_layout, nodes)
        .map_err(|_| CompilerError::GraphEncoding)
}

fn project_nodes(key_slot: usize, value_slot: usize) -> Result<Vec<OperatorNode>, CompilerError> {
    Ok(vec![
        OperatorNode {
            node_id: node_id(1),
            input: NodeInput::Source,
            state_contract: None,
            kind: OperatorNodeKind::Project {
                expressions: vec![
                    Expression::Column {
                        slot: slot(key_slot)?,
                    },
                    Expression::Column {
                        slot: slot(value_slot)?,
                    },
                ],
            },
        },
        OperatorNode {
            node_id: node_id(2),
            input: NodeInput::Node(node_id(1)),
            state_contract: None,
            kind: OperatorNodeKind::Materialize {
                key_slot: 0,
                value_slot: 1,
                output: OutputContract::KeyedRows {
                    key_type: ValueType::Int8,
                    key_nullable: false,
                    value_type: ValueType::Int8,
                    nullable: true,
                },
            },
        },
    ])
}

fn grouped_nodes(
    key_slot: usize,
    key_nullable: bool,
    value_slot: Option<usize>,
    source_width: usize,
) -> Result<Vec<OperatorNode>, CompilerError> {
    let key_slot = slot(key_slot)?;
    let grouped_key_slot = slot(source_width)?;
    let aggregate = match value_slot {
        None => OperatorNodeKind::GroupedCount {
            key_slot: grouped_key_slot,
        },
        Some(value_slot) => OperatorNodeKind::GroupedSumInt8 {
            key_slot: grouped_key_slot,
            value_slot: slot(value_slot)?,
        },
    };
    Ok(vec![
        OperatorNode {
            node_id: node_id(1),
            input: NodeInput::Source,
            state_contract: None,
            kind: OperatorNodeKind::KeyBy {
                key: Expression::Column { slot: key_slot },
            },
        },
        OperatorNode {
            node_id: node_id(2),
            input: NodeInput::Node(node_id(1)),
            state_contract: Some(StateContract { codec_version: 1 }),
            kind: aggregate,
        },
        OperatorNode {
            node_id: node_id(3),
            input: NodeInput::Node(node_id(2)),
            state_contract: None,
            kind: OperatorNodeKind::Materialize {
                key_slot: 0,
                value_slot: 1,
                output: OutputContract::KeyedRows {
                    key_type: ValueType::Int8,
                    key_nullable,
                    value_type: ValueType::Int8,
                    nullable: value_slot.is_some(),
                },
            },
        },
    ])
}

fn resolve<'a>(
    source: &'a SourceDescriptor,
    name: &str,
) -> Result<(usize, &'a crate::SourceColumnDescriptor), CompilerError> {
    let mut matches = source
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name == name);
    let (index, column) = matches
        .next()
        .ok_or_else(|| CompilerError::MissingColumn(name.into()))?;
    if matches.next().is_some() {
        return Err(CompilerError::DuplicateColumn(name.into()));
    }
    if column.type_oid != crate::POSTGRES_INT8_TYPE_OID {
        return Err(CompilerError::WrongColumnType {
            column: name.into(),
            type_oid: column.type_oid,
        });
    }
    Ok((index, column))
}

fn node_id(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).expect("fixed nonzero node identity"))
}

fn slot(value: usize) -> Result<u16, CompilerError> {
    u16::try_from(value).map_err(|_| CompilerError::GraphEncoding)
}
