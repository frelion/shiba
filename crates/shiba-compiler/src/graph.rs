use core::num::NonZeroU32;

use shiba_operator::{
    ColumnBinding, Expression, NodeId, NodeInput, OperatorGraph, OperatorNode, OperatorNodeKind,
    OutputContract, ValueType,
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
    let OperatorOperationV1::MaterializedProject {
        key_column,
        value_column,
    } = &spec.operation
    else {
        return Err(CompilerError::PlanRequired);
    };
    let (key_slot, key) = resolve(source, key_column)?;
    if key.nullable {
        return Err(CompilerError::NullableKey(key_column.clone()));
    }
    let (value_slot, _) = resolve(source, value_column)?;
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
    OperatorGraph::build(
        spec.operator_id,
        spec.source_id,
        source_layout,
        vec![
            OperatorNode {
                node_id: node_id(1),
                input: NodeInput::Source,
                state_contract: None,
                kind: OperatorNodeKind::Project {
                    expressions: vec![
                        Expression::Column {
                            slot: u16::try_from(key_slot)
                                .map_err(|_| CompilerError::GraphEncoding)?,
                        },
                        Expression::Column {
                            slot: u16::try_from(value_slot)
                                .map_err(|_| CompilerError::GraphEncoding)?,
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
                        value_type: ValueType::Int8,
                        nullable: true,
                    },
                },
            },
        ],
    )
    .map_err(|_| CompilerError::GraphEncoding)
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
