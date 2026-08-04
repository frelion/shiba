use core::num::NonZeroU32;

use shiba_operator::{
    AggregateCall, AggregateFunctionV1, ColumnBinding, DeltaBatch, EffectOrigin, Expression,
    GraphEffectOrigin, MultiInputBatch, NodeId, NodeInput, ObjectAddress, OperatorGraph,
    OperatorNode, OperatorNodeKind, OutputContract, ResultDelta, ResultField, ResultMutation,
    ResultSchemaV1, RowDelta, SourceDeltaBatch, SourcePort, StateContract, StateEntry,
    StateSnapshot, TypedResultRowV1, TypedRow, TypedValue, ValueType, apply_graph_plan,
    graph_state_read_set, source_typed_layout,
};
use shiba_protocol::{BootstrapBatchId, BootstrapId, GraphId, SourceId};

fn node(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).unwrap())
}

fn graph() -> OperatorGraph {
    let source_id = SourceId::new(1).unwrap();
    let count_scalar = scalar_output(false);
    let sum_scalar = scalar_output(true);
    let keyed = keyed_output(true);
    let keyed_count = keyed_output(false);
    OperatorGraph::build(
        GraphId::new(7).unwrap(),
        vec![SourcePort {
            source_id,
            layout: vec![binding(1), binding(2)],
            identity_index: Some(ObjectAddress {
                class_id: 1_259,
                object_id: 11_000,
                sub_id: 0,
            }),
        }],
        vec![
            stateful(
                1,
                source_id,
                aggregate(vec![], AggregateFunctionV1::CountStar, None),
            ),
            terminal(2, 1, count_scalar, 0, 0),
            stateful(
                3,
                source_id,
                aggregate(
                    vec![],
                    AggregateFunctionV1::SumInt8,
                    Some(Expression::Column { slot: 1 }),
                ),
            ),
            terminal(4, 3, sum_scalar, 0, 0),
            OperatorNode {
                node_id: node(5),
                input: NodeInput::SourcePort(source_id),
                state_contract: None,
                kind: OperatorNodeKind::Project {
                    expressions: vec![
                        Expression::Column { slot: 0 },
                        Expression::Column { slot: 1 },
                    ],
                },
            },
            terminal(6, 5, keyed.clone(), 0, 1),
            OperatorNode {
                node_id: node(7),
                input: NodeInput::SourcePort(source_id),
                state_contract: None,
                kind: OperatorNodeKind::KeyBy {
                    key: Expression::Column { slot: 0 },
                },
            },
            OperatorNode {
                node_id: node(8),
                input: NodeInput::Node(node(7)),
                state_contract: Some(StateContract { codec_version: 1 }),
                kind: aggregate(
                    vec![Expression::Column { slot: 2 }],
                    AggregateFunctionV1::CountStar,
                    None,
                ),
            },
            terminal(9, 8, keyed_count, 0, 1),
            OperatorNode {
                node_id: node(10),
                input: NodeInput::SourcePort(source_id),
                state_contract: None,
                kind: OperatorNodeKind::KeyBy {
                    key: Expression::Column { slot: 0 },
                },
            },
            OperatorNode {
                node_id: node(11),
                input: NodeInput::Node(node(10)),
                state_contract: Some(StateContract { codec_version: 1 }),
                kind: aggregate(
                    vec![Expression::Column { slot: 2 }],
                    AggregateFunctionV1::SumInt8,
                    Some(Expression::Column { slot: 1 }),
                ),
            },
            terminal(12, 11, keyed, 0, 1),
        ],
    )
    .unwrap()
}

fn aggregate(
    groups: Vec<Expression>,
    function: AggregateFunctionV1,
    expression: Option<Expression>,
) -> OperatorNodeKind {
    OperatorNodeKind::Aggregate {
        group_expressions: groups,
        calls: vec![AggregateCall {
            ordinal: 1,
            function_version: 1,
            function,
            expression,
        }],
        having: None,
    }
}

fn binding(sub_id: i32) -> ColumnBinding {
    ColumnBinding {
        address: ObjectAddress {
            class_id: 1_259,
            object_id: 10_000,
            sub_id,
        },
        value_type: ValueType::Int8,
    }
}

fn scalar_output(nullable: bool) -> OutputContract {
    let schema = ResultSchemaV1::new(
        vec![ResultField {
            ordinal: 1,
            name: "value".into(),
            value_type: ValueType::Int8,
            nullable,
        }],
        vec![],
    )
    .unwrap();
    let initial = if nullable {
        TypedValue::Null(ValueType::Int8)
    } else {
        TypedValue::Int8(0)
    };
    let row = TypedResultRowV1::new(&schema, vec![initial]).unwrap();
    OutputContract::new(schema, Some(row)).unwrap()
}

fn keyed_output(nullable: bool) -> OutputContract {
    let schema = ResultSchemaV1::new(
        vec![
            ResultField {
                ordinal: 1,
                name: "key".into(),
                value_type: ValueType::Int8,
                nullable: false,
            },
            ResultField {
                ordinal: 2,
                name: "value".into(),
                value_type: ValueType::Int8,
                nullable,
            },
        ],
        vec![1],
    )
    .unwrap();
    OutputContract::new(schema, None).unwrap()
}

fn stateful(id: u32, source_id: SourceId, kind: OperatorNodeKind) -> OperatorNode {
    OperatorNode {
        node_id: node(id),
        input: NodeInput::SourcePort(source_id),
        state_contract: Some(StateContract { codec_version: 1 }),
        kind,
    }
}

fn terminal(
    id: u32,
    input: u32,
    output: OutputContract,
    key_slot: u16,
    value_slot: u16,
) -> OperatorNode {
    OperatorNode {
        node_id: node(id),
        input: NodeInput::Node(node(input)),
        state_contract: None,
        kind: OperatorNodeKind::Materialize {
            field_slots: if output.schema.is_scalar() {
                vec![value_slot]
            } else {
                vec![key_slot, value_slot]
            },
            output,
        },
    }
}

fn input(graph: &OperatorGraph) -> MultiInputBatch {
    let source_id = graph.sources[0].source_id;
    let layout = source_typed_layout(source_id, &graph.sources[0].layout).unwrap();
    let batch_id = BootstrapBatchId::new(BootstrapId::new(2).unwrap(), 1).unwrap();
    let row = |id, value| TypedRow::new(&layout, vec![TypedValue::Int8(id), value]).unwrap();
    MultiInputBatch {
        origin: GraphEffectOrigin::Bootstrap(batch_id),
        sources: vec![SourceDeltaBatch {
            source_id,
            delta: DeltaBatch {
                origin: EffectOrigin::Bootstrap(batch_id),
                layout_identity: layout.identity,
                rows: vec![
                    RowDelta {
                        before: None,
                        after: Some(row(1, TypedValue::Int8(10))),
                    },
                    RowDelta {
                        before: None,
                        after: Some(row(2, TypedValue::Null(ValueType::Int8))),
                    },
                ],
            },
        }],
    }
}

#[test]
fn multiple_terminals_share_one_batch_and_generic_state() {
    let graph = graph();
    let input = input(&graph);
    let read_set = graph_state_read_set(&graph, &input).unwrap();
    assert_eq!(read_set.keys.len(), 12);
    let snapshot = StateSnapshot::new(
        &read_set,
        read_set
            .keys
            .iter()
            .map(|key| StateEntry {
                key: key.clone(),
                state: None,
            })
            .collect(),
    )
    .unwrap();
    let transition = apply_graph_plan(&graph, &snapshot, &input).unwrap();
    assert_eq!(transition.state_deltas.len(), 12);
    assert_eq!(transition.results.len(), 5);
    assert_eq!(
        transition.results[0],
        ResultDelta {
            node_id: node(2),
            mutations: vec![ResultMutation::ReplaceScalar {
                row: TypedResultRowV1::new(&scalar_output(false).schema, vec![TypedValue::Int8(2)])
                    .unwrap()
            }]
        }
    );
    assert_eq!(
        transition.results[1],
        ResultDelta {
            node_id: node(4),
            mutations: vec![ResultMutation::ReplaceScalar {
                row: TypedResultRowV1::new(&scalar_output(true).schema, vec![TypedValue::Int8(10)])
                    .unwrap()
            }]
        }
    );
    assert!(matches!(
        transition.results[2],
        ResultDelta { node_id, ref mutations } if node_id == node(6) && mutations.len() == 2
    ));
}

#[test]
fn typed_state_partition_values_require_exact_canonical_json() {
    let value = TypedValue::Null(ValueType::Int8);
    let encoded = value.to_canonical_json().unwrap();
    assert_eq!(TypedValue::from_canonical_json(&encoded).unwrap(), value);
    assert!(TypedValue::from_canonical_json(b"{ \"type\":\"int8\",\"value\":1}").is_err());
    assert!(TypedValue::from_canonical_json(br#"{"type":"absent"}"#).is_err());
}
