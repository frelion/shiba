use core::num::{NonZeroU32, NonZeroU64};
use std::collections::BTreeMap;

use shiba_operator::{
    ColumnBinding, DeltaBatch, EffectOrigin, Expression, GraphError, NodeId, NodeInput,
    ObjectAddress, OperatorGraph, OperatorId, OperatorNode, OperatorNodeKind, OutputContract,
    ResultDelta, ResultMutation, RowDelta, TypedLayout, TypedRow, TypedValue, ValueType,
    apply_graph, source_typed_layout,
};
use shiba_protocol::{BootstrapBatchId, BootstrapId, SourceId};

fn node(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).unwrap())
}

fn binding(sub_id: i32) -> ColumnBinding {
    ColumnBinding {
        address: ObjectAddress {
            class_id: 1_259,
            object_id: 16_384,
            sub_id,
        },
        value_type: ValueType::Int8,
    }
}

fn origin() -> EffectOrigin {
    EffectOrigin::Bootstrap(BootstrapBatchId::new(BootstrapId::new(1).unwrap(), 1).unwrap())
}

fn row(layout: &TypedLayout, key: i64, value: TypedValue) -> TypedRow {
    TypedRow::new(layout, vec![TypedValue::Int8(key), value]).unwrap()
}

fn graph(predicate: Expression, compute: bool) -> OperatorGraph {
    let mut nodes = vec![OperatorNode {
        node_id: node(1),
        input: NodeInput::Source,
        state_contract: None,
        kind: OperatorNodeKind::Filter { predicate },
    }];
    let mapped = if compute {
        nodes.push(OperatorNode {
            node_id: node(2),
            input: NodeInput::Node(node(1)),
            state_contract: None,
            kind: OperatorNodeKind::Compute {
                expressions: vec![Expression::Add {
                    left: Box::new(Expression::Column { slot: 1 }),
                    right: Box::new(Expression::Int8Literal { value: 1 }),
                }],
            },
        });
        nodes.push(OperatorNode {
            node_id: node(3),
            input: NodeInput::Node(node(2)),
            state_contract: None,
            kind: OperatorNodeKind::Project {
                expressions: vec![
                    Expression::Column { slot: 0 },
                    Expression::Column { slot: 2 },
                ],
            },
        });
        node(3)
    } else {
        node(1)
    };
    nodes.push(OperatorNode {
        node_id: node(4),
        input: NodeInput::Node(mapped),
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
    });
    OperatorGraph::build(
        OperatorId::new(NonZeroU64::new(3).unwrap()),
        SourceId::new(1).unwrap(),
        vec![binding(1), binding(2)],
        nodes,
    )
    .unwrap()
}

#[test]
fn expressions_use_three_valued_logic_and_checked_arithmetic() {
    let layout = TypedLayout::new([7; 32], vec![ValueType::Int8, ValueType::Bool]).unwrap();
    let row = TypedRow::new(
        &layout,
        vec![TypedValue::Null(ValueType::Int8), TypedValue::Bool(false)],
    )
    .unwrap();
    let comparison = Expression::Greater {
        left: Box::new(Expression::Column { slot: 0 }),
        right: Box::new(Expression::Int8Literal { value: 0 }),
    };
    assert_eq!(
        comparison.evaluate(&layout, &row).unwrap(),
        TypedValue::Null(ValueType::Bool)
    );
    let conjunction = Expression::And {
        left: Box::new(comparison),
        right: Box::new(Expression::Column { slot: 1 }),
    };
    assert_eq!(
        conjunction.evaluate(&layout, &row).unwrap(),
        TypedValue::Bool(false)
    );
    let overflow = Expression::Add {
        left: Box::new(Expression::Int8Literal { value: i64::MAX }),
        right: Box::new(Expression::Int8Literal { value: 1 }),
    };
    assert!(overflow.evaluate(&layout, &row).is_err());
    let absent = TypedRow::new(&layout, vec![TypedValue::Absent, TypedValue::Bool(true)]).unwrap();
    assert!(
        Expression::Column { slot: 0 }
            .evaluate(&layout, &absent)
            .is_err()
    );
}

#[test]
fn filter_compute_project_and_materialize_emit_exact_retractions() {
    let graph = graph(
        Expression::Greater {
            left: Box::new(Expression::Column { slot: 1 }),
            right: Box::new(Expression::Int8Literal { value: 0 }),
        },
        true,
    );
    let layout = source_typed_layout(graph.source_id, &graph.source_layout).unwrap();
    let batch = DeltaBatch {
        origin: origin(),
        layout_identity: layout.identity,
        rows: vec![
            RowDelta {
                before: Some(row(&layout, 1, TypedValue::Int8(-1))),
                after: Some(row(&layout, 1, TypedValue::Int8(4))),
            },
            RowDelta {
                before: Some(row(&layout, 2, TypedValue::Int8(6))),
                after: Some(row(&layout, 3, TypedValue::Int8(8))),
            },
            RowDelta {
                before: Some(row(&layout, 4, TypedValue::Int8(2))),
                after: Some(row(&layout, 4, TypedValue::Null(ValueType::Int8))),
            },
        ],
    };
    assert_eq!(
        apply_graph(&graph, &batch).unwrap().results,
        vec![ResultDelta::Keyed {
            node_id: node(4),
            mutations: vec![
                ResultMutation::Upsert {
                    key: TypedValue::Int8(1),
                    value: TypedValue::Int8(5),
                },
                ResultMutation::Delete {
                    key: TypedValue::Int8(2),
                },
                ResultMutation::Upsert {
                    key: TypedValue::Int8(3),
                    value: TypedValue::Int8(9),
                },
                ResultMutation::Delete {
                    key: TypedValue::Int8(4),
                },
            ],
        }]
    );
}

#[test]
fn null_filter_is_false_and_plan_or_layout_drift_fails_closed() {
    let graph = graph(
        Expression::IsNull {
            input: Box::new(Expression::Column { slot: 1 }),
        },
        false,
    );
    let layout = source_typed_layout(graph.source_id, &graph.source_layout).unwrap();
    let batch = DeltaBatch {
        origin: origin(),
        layout_identity: layout.identity,
        rows: vec![RowDelta {
            before: None,
            after: Some(row(&layout, 7, TypedValue::Null(ValueType::Int8))),
        }],
    };
    assert!(apply_graph(&graph, &batch).is_ok());

    let mut corrupt = graph.clone();
    corrupt.digest[0] ^= 1;
    assert!(apply_graph(&corrupt, &batch).is_err());
    let mut wrong_layout = batch;
    wrong_layout.layout_identity = [9; 32];
    assert_eq!(apply_graph(&graph, &wrong_layout), Err(GraphError::Layout));
}

#[test]
fn fixed_seed_project_differential_matches_keyed_reference_model() {
    let graph = graph(
        Expression::Equal {
            left: Box::new(Expression::Int8Literal { value: 1 }),
            right: Box::new(Expression::Int8Literal { value: 1 }),
        },
        false,
    );
    let layout = source_typed_layout(graph.source_id, &graph.source_layout).unwrap();
    let mut source = BTreeMap::<i64, Option<i64>>::new();
    let mut result = BTreeMap::<i64, Option<i64>>::new();
    let mut seed = 0x5eed_u64;
    for step in 0..2_000_i64 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let old_key = source.keys().next().copied();
        let (before, after) = match (seed % 3, old_key) {
            (0, Some(key)) => {
                let value = source.remove(&key).unwrap();
                (Some(row_value(&layout, key, value)), None)
            }
            (1, Some(key)) => {
                let old = source.remove(&key).unwrap();
                let new_key = 10_000 + step;
                let new = random_value(seed);
                source.insert(new_key, new);
                (
                    Some(row_value(&layout, key, old)),
                    Some(row_value(&layout, new_key, new)),
                )
            }
            _ => {
                let key = 20_000 + step;
                let value = random_value(seed);
                source.insert(key, value);
                (None, Some(row_value(&layout, key, value)))
            }
        };
        let transition = apply_graph(
            &graph,
            &DeltaBatch {
                origin: origin(),
                layout_identity: layout.identity,
                rows: vec![RowDelta { before, after }],
            },
        )
        .unwrap();
        let ResultDelta::Keyed { mutations, .. } = &transition.results[0];
        for mutation in mutations {
            match mutation {
                ResultMutation::Delete {
                    key: TypedValue::Int8(key),
                } => {
                    result.remove(key);
                }
                ResultMutation::Upsert {
                    key: TypedValue::Int8(key),
                    value,
                } => {
                    let value = match value {
                        TypedValue::Int8(value) => Some(*value),
                        TypedValue::Null(ValueType::Int8) => None,
                        _ => panic!("unexpected projected value"),
                    };
                    result.insert(*key, value);
                }
                _ => panic!("unexpected projected key"),
            }
        }
        assert_eq!(result, source);
    }
}

#[test]
fn input_row_bound_fails_before_graph_evaluation() {
    let graph = graph(
        Expression::Equal {
            left: Box::new(Expression::Int8Literal { value: 1 }),
            right: Box::new(Expression::Int8Literal { value: 1 }),
        },
        false,
    );
    let layout = source_typed_layout(graph.source_id, &graph.source_layout).unwrap();
    let rows = (0..10_001)
        .map(|key| RowDelta {
            before: None,
            after: Some(row(&layout, key, TypedValue::Int8(key))),
        })
        .collect();
    let batch = DeltaBatch {
        origin: origin(),
        layout_identity: layout.identity,
        rows,
    };
    assert_eq!(apply_graph(&graph, &batch), Err(GraphError::OutputLimit));
}

fn random_value(seed: u64) -> Option<i64> {
    (seed & 3 != 0).then_some(i64::from((seed >> 32) as u32))
}

fn row_value(layout: &TypedLayout, key: i64, value: Option<i64>) -> TypedRow {
    row(
        layout,
        key,
        value.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8),
    )
}
