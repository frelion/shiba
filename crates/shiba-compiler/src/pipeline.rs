use shiba_operator::{
    Expression, NodeInput, OperatorNode, OperatorNodeKind, OutputContract, StateContract, ValueType,
};

use crate::binding::{int8, source};
use crate::{CompilerError, GraphOutputSpecV1, SourceDescriptor};

pub(crate) fn compile_pipeline(
    output: &GraphOutputSpecV1,
    sources: &[SourceDescriptor],
    nodes: &mut Vec<OperatorNode>,
) -> Option<Result<(), CompilerError>> {
    Some(match output {
        GraphOutputSpecV1::ComputedProject {
            source_id,
            key_column,
            input_column,
            literal,
            compute_node_id,
            project_node_id,
            result_node_id,
        } => source(sources, *source_id).and_then(|source| {
            computed_project(
                source,
                nodes,
                &ComputedArgs {
                    key_column,
                    input_column,
                    literal: *literal,
                    node_ids: (*compute_node_id, *project_node_id, *result_node_id),
                },
            )
        }),
        GraphOutputSpecV1::FilteredGroupedCount {
            source_id,
            filter_column,
            greater_than,
            group_key_column,
            filter_node_id,
            project_node_id,
            key_node_id,
            aggregate_node_id,
            result_node_id,
        } => source(sources, *source_id).and_then(|source| {
            filtered_group(
                source,
                nodes,
                &FilteredGroupArgs {
                    filter_column,
                    greater_than: *greater_than,
                    group_key_column,
                    input_column: None,
                    node_ids: (
                        *filter_node_id,
                        *project_node_id,
                        *key_node_id,
                        *aggregate_node_id,
                        *result_node_id,
                    ),
                },
            )
        }),
        GraphOutputSpecV1::FilteredGroupedSumInt8 {
            source_id,
            filter_column,
            greater_than,
            group_key_column,
            input_column,
            filter_node_id,
            project_node_id,
            key_node_id,
            aggregate_node_id,
            result_node_id,
        } => source(sources, *source_id).and_then(|source| {
            filtered_group(
                source,
                nodes,
                &FilteredGroupArgs {
                    filter_column,
                    greater_than: *greater_than,
                    group_key_column,
                    input_column: Some(input_column),
                    node_ids: (
                        *filter_node_id,
                        *project_node_id,
                        *key_node_id,
                        *aggregate_node_id,
                        *result_node_id,
                    ),
                },
            )
        }),
        _ => return None,
    })
}

pub(crate) struct ComputedArgs<'a> {
    pub(crate) key_column: &'a str,
    pub(crate) input_column: &'a str,
    pub(crate) literal: i64,
    pub(crate) node_ids: (
        shiba_operator::NodeId,
        shiba_operator::NodeId,
        shiba_operator::NodeId,
    ),
}

pub(crate) fn computed_project(
    source: &SourceDescriptor,
    nodes: &mut Vec<OperatorNode>,
    args: &ComputedArgs<'_>,
) -> Result<(), CompilerError> {
    let (key_slot, key) = int8(source, args.key_column)?;
    let (input_slot, _) = int8(source, args.input_column)?;
    if key.nullable {
        return Err(CompilerError::NullableKey(args.key_column.into()));
    }
    let computed_slot =
        u16::try_from(source.columns.len()).map_err(|_| CompilerError::GraphEncoding)?;
    nodes.push(OperatorNode {
        node_id: args.node_ids.0,
        input: NodeInput::SourcePort(source.source_id),
        state_contract: None,
        kind: OperatorNodeKind::Compute {
            expressions: vec![Expression::Add {
                left: Box::new(Expression::Column { slot: input_slot }),
                right: Box::new(Expression::Int8Literal {
                    value: args.literal,
                }),
            }],
        },
    });
    nodes.push(OperatorNode {
        node_id: args.node_ids.1,
        input: NodeInput::Node(args.node_ids.0),
        state_contract: None,
        kind: OperatorNodeKind::Project {
            expressions: vec![
                Expression::Column { slot: key_slot },
                Expression::Column {
                    slot: computed_slot,
                },
            ],
        },
    });
    nodes.push(crate::node_builders::materialize(
        args.node_ids.2,
        args.node_ids.1,
        OutputContract::KeyedRows {
            key_type: ValueType::Int8,
            key_nullable: false,
            value_type: ValueType::Int8,
            nullable: true,
        },
        true,
    ));
    Ok(())
}

pub(crate) struct FilteredGroupArgs<'a> {
    pub(crate) filter_column: &'a str,
    pub(crate) greater_than: i64,
    pub(crate) group_key_column: &'a str,
    pub(crate) input_column: Option<&'a String>,
    pub(crate) node_ids: (
        shiba_operator::NodeId,
        shiba_operator::NodeId,
        shiba_operator::NodeId,
        shiba_operator::NodeId,
        shiba_operator::NodeId,
    ),
}

pub(crate) fn filtered_group(
    source: &SourceDescriptor,
    nodes: &mut Vec<OperatorNode>,
    args: &FilteredGroupArgs<'_>,
) -> Result<(), CompilerError> {
    let (filter_slot, _) = int8(source, args.filter_column)?;
    let (key_slot, key) = int8(source, args.group_key_column)?;
    let value_slot = args
        .input_column
        .map(|name| int8(source, name).map(|found| found.0))
        .transpose()?;
    nodes.push(OperatorNode {
        node_id: args.node_ids.0,
        input: NodeInput::SourcePort(source.source_id),
        state_contract: None,
        kind: OperatorNodeKind::Filter {
            predicate: Expression::And {
                left: Box::new(Expression::Not {
                    input: Box::new(Expression::IsNull {
                        input: Box::new(Expression::Column { slot: filter_slot }),
                    }),
                }),
                right: Box::new(Expression::Greater {
                    left: Box::new(Expression::Column { slot: filter_slot }),
                    right: Box::new(Expression::Int8Literal {
                        value: args.greater_than,
                    }),
                }),
            },
        },
    });
    let mut expressions = vec![Expression::Column { slot: key_slot }];
    if let Some(slot) = value_slot {
        expressions.push(Expression::Column { slot });
    }
    nodes.push(OperatorNode {
        node_id: args.node_ids.1,
        input: NodeInput::Node(args.node_ids.0),
        state_contract: None,
        kind: OperatorNodeKind::Project { expressions },
    });
    nodes.push(OperatorNode {
        node_id: args.node_ids.2,
        input: NodeInput::Node(args.node_ids.1),
        state_contract: None,
        kind: OperatorNodeKind::KeyBy {
            key: Expression::Column { slot: 0 },
        },
    });
    let grouped_key_slot = if value_slot.is_some() { 2 } else { 1 };
    nodes.push(OperatorNode {
        node_id: args.node_ids.3,
        input: NodeInput::Node(args.node_ids.2),
        state_contract: Some(StateContract { codec_version: 1 }),
        kind: value_slot.map_or(
            OperatorNodeKind::GroupedCount {
                key_slot: grouped_key_slot,
            },
            |_| OperatorNodeKind::GroupedSumInt8 {
                key_slot: grouped_key_slot,
                value_slot: 1,
            },
        ),
    });
    nodes.push(crate::node_builders::materialize(
        args.node_ids.4,
        args.node_ids.3,
        OutputContract::KeyedRows {
            key_type: ValueType::Int8,
            key_nullable: key.nullable,
            value_type: ValueType::Int8,
            nullable: value_slot.is_some(),
        },
        true,
    ));
    Ok(())
}
