use shiba_operator::{
    Expression, NodeInput, OperatorNode, OperatorNodeKind, OutputContract, StateContract, ValueType,
};

use crate::binding::int8;
use crate::{CompilerError, SourceDescriptor};

pub(crate) fn scalar_nodes(
    source_id: shiba_protocol::SourceId,
    aggregate_id: shiba_operator::NodeId,
    result_id: shiba_operator::NodeId,
    sum_slot: Option<u16>,
    nodes: &mut Vec<OperatorNode>,
) {
    nodes.push(OperatorNode {
        node_id: aggregate_id,
        input: NodeInput::SourcePort(source_id),
        state_contract: Some(StateContract { codec_version: 1 }),
        kind: sum_slot.map_or(OperatorNodeKind::CountRows, |input_slot| {
            OperatorNodeKind::SumInt8 { input_slot }
        }),
    });
    nodes.push(materialize(
        result_id,
        aggregate_id,
        OutputContract::Scalar {
            value_type: ValueType::Int8,
        },
        false,
    ));
}

pub(crate) fn project_nodes(
    source_id: shiba_protocol::SourceId,
    project_id: shiba_operator::NodeId,
    result_id: shiba_operator::NodeId,
    key_slot: u16,
    value_slot: u16,
) -> Vec<OperatorNode> {
    vec![
        OperatorNode {
            node_id: project_id,
            input: NodeInput::SourcePort(source_id),
            state_contract: None,
            kind: OperatorNodeKind::Project {
                expressions: vec![
                    Expression::Column { slot: key_slot },
                    Expression::Column { slot: value_slot },
                ],
            },
        },
        materialize(
            result_id,
            project_id,
            OutputContract::KeyedRows {
                key_type: ValueType::Int8,
                key_nullable: false,
                value_type: ValueType::Int8,
                nullable: true,
            },
            true,
        ),
    ]
}

pub(crate) fn grouped_nodes(
    source: &SourceDescriptor,
    key_name: &str,
    value_name: Option<&String>,
    ids: (
        shiba_operator::NodeId,
        shiba_operator::NodeId,
        shiba_operator::NodeId,
    ),
    nodes: &mut Vec<OperatorNode>,
) -> Result<(), CompilerError> {
    let (key_slot, key) = int8(source, key_name)?;
    let value_slot = value_name
        .map(|name| int8(source, name).map(|found| found.0))
        .transpose()?;
    let grouped_key_slot =
        u16::try_from(source.columns.len()).map_err(|_| CompilerError::GraphEncoding)?;
    nodes.push(OperatorNode {
        node_id: ids.0,
        input: NodeInput::SourcePort(source.source_id),
        state_contract: None,
        kind: OperatorNodeKind::KeyBy {
            key: Expression::Column { slot: key_slot },
        },
    });
    nodes.push(OperatorNode {
        node_id: ids.1,
        input: NodeInput::Node(ids.0),
        state_contract: Some(StateContract { codec_version: 1 }),
        kind: value_slot.map_or(
            OperatorNodeKind::GroupedCount {
                key_slot: grouped_key_slot,
            },
            |value_slot| OperatorNodeKind::GroupedSumInt8 {
                key_slot: grouped_key_slot,
                value_slot,
            },
        ),
    });
    nodes.push(materialize(
        ids.2,
        ids.1,
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

pub(crate) fn materialize(
    node_id: shiba_operator::NodeId,
    input: shiba_operator::NodeId,
    output: OutputContract,
    keyed: bool,
) -> OperatorNode {
    OperatorNode {
        node_id,
        input: NodeInput::Node(input),
        state_contract: None,
        kind: OperatorNodeKind::Materialize {
            key_slot: 0,
            value_slot: u16::from(keyed),
            output,
        },
    }
}
