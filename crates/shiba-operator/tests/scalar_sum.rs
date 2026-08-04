use core::num::NonZeroU32;

use shiba_operator::{
    ColumnBinding, DeltaBatch, EffectOrigin, GraphEffectOrigin, MultiInputBatch, NodeId, NodeInput,
    ObjectAddress, OperatorGraph, OperatorNode, OperatorNodeKind, OutputContract, ResultField,
    ResultMutation, ResultSchemaV1, RowDelta, SourceDeltaBatch, SourcePort, StateContract,
    StateEntry, StateSnapshot, TypedResultRowV1, TypedRow, TypedValue, ValueType, apply_graph_plan,
    graph_state_read_set, source_typed_layout,
};
use shiba_protocol::{BootstrapBatchId, BootstrapId, GraphId, SourceId};

fn node(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).unwrap())
}

fn graph(nullable: bool) -> OperatorGraph {
    let source_id = SourceId::new(1).unwrap();
    OperatorGraph::build(
        GraphId::new(1).unwrap(),
        vec![SourcePort {
            source_id,
            layout: vec![
                ColumnBinding {
                    address: ObjectAddress {
                        class_id: 1_259,
                        object_id: 10_000,
                        sub_id: 1,
                    },
                    value_type: ValueType::Int8,
                },
                ColumnBinding {
                    address: ObjectAddress {
                        class_id: 1_259,
                        object_id: 10_000,
                        sub_id: 2,
                    },
                    value_type: ValueType::Int8,
                },
            ],
            identity_index: Some(ObjectAddress {
                class_id: 1_259,
                object_id: 11_000,
                sub_id: 0,
            }),
        }],
        vec![
            OperatorNode {
                node_id: node(1),
                input: NodeInput::SourcePort(source_id),
                state_contract: Some(StateContract { codec_version: 1 }),
                kind: OperatorNodeKind::SumInt8 { input_slot: 1 },
            },
            OperatorNode {
                node_id: node(2),
                input: NodeInput::Node(node(1)),
                state_contract: None,
                kind: OperatorNodeKind::Materialize {
                    field_slots: vec![0],
                    output: scalar_output(nullable),
                },
            },
        ],
    )
    .unwrap()
}

fn scalar_output(nullable: bool) -> OutputContract {
    let schema = ResultSchemaV1::new(
        vec![ResultField {
            ordinal: 1,
            name: "sum".into(),
            value_type: ValueType::Int8,
            nullable,
        }],
        vec![],
    )
    .unwrap();
    let value = if nullable {
        TypedValue::Null(ValueType::Int8)
    } else {
        TypedValue::Int8(0)
    };
    let initial_row = TypedResultRowV1::new(&schema, vec![value]).unwrap();
    OutputContract::new(schema, Some(initial_row)).unwrap()
}

fn batch(
    graph: &OperatorGraph,
    before: Option<TypedValue>,
    after: Option<TypedValue>,
) -> MultiInputBatch {
    let source_id = graph.sources[0].source_id;
    let layout = source_typed_layout(source_id, &graph.sources[0].layout).unwrap();
    let row = |value| TypedRow::new(&layout, vec![TypedValue::Int8(1), value]).unwrap();
    let batch_id = BootstrapBatchId::new(BootstrapId::new(1).unwrap(), 1).unwrap();
    MultiInputBatch {
        origin: GraphEffectOrigin::Bootstrap(batch_id),
        sources: vec![SourceDeltaBatch {
            source_id,
            delta: DeltaBatch {
                origin: EffectOrigin::Bootstrap(batch_id),
                layout_identity: layout.identity,
                rows: vec![RowDelta {
                    before: before.map(row),
                    after: after.map(row),
                }],
            },
        }],
    }
}

fn empty_snapshot(graph: &OperatorGraph, input: &MultiInputBatch) -> StateSnapshot {
    let read_set = graph_state_read_set(graph, input).unwrap();
    assert_eq!(read_set.keys.len(), 2);
    StateSnapshot::new(
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
    .unwrap()
}

#[test]
fn nullable_scalar_sum_distinguishes_all_null_from_zero() {
    let nullable = graph(true);
    let input = batch(&nullable, None, Some(TypedValue::Null(ValueType::Int8)));
    let transition =
        apply_graph_plan(&nullable, &empty_snapshot(&nullable, &input), &input).unwrap();
    assert_eq!(transition.state_deltas.len(), 2);
    assert!(matches!(
        transition.results.as_slice(),
        [shiba_operator::ResultDelta { mutations, .. }]
            if matches!(mutations.as_slice(), [ResultMutation::ReplaceScalar { row }]
                if row.values == [TypedValue::Null(ValueType::Int8)])
    ));

    let legacy = graph(false);
    let input = batch(&legacy, None, Some(TypedValue::Null(ValueType::Int8)));
    let transition = apply_graph_plan(&legacy, &empty_snapshot(&legacy, &input), &input).unwrap();
    assert!(matches!(
        transition.results.as_slice(),
        [shiba_operator::ResultDelta { mutations, .. }]
            if matches!(mutations.as_slice(), [ResultMutation::ReplaceScalar { row }]
                if row.values == [TypedValue::Int8(0)])
    ));
}
