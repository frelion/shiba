use core::num::NonZeroU32;
use std::collections::BTreeMap;

use shiba_operator::{
    ColumnBinding, DeltaBatch, EffectOrigin, EncodedOperatorState, GraphEffectOrigin,
    MultiInputBatch, NodeId, NodeInput, ObjectAddress, OperatorGraph, OperatorNode,
    OperatorNodeKind, OutputContract, ResultDelta, ResultMutation, SourceDeltaBatch, SourcePort,
    StateContract, StateDelta, StateEntry, StateMutation, StateSnapshot, TypedLayout, TypedRow,
    TypedValue, ValueType, apply_graph_plan, graph_state_read_set, source_typed_layout,
};
use shiba_protocol::{
    GraphId, GraphTransactionId, IngressTransactionId, PostgresLsn, SlotGeneration, SourceId,
    SourceTransactionId,
};

fn node(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).unwrap())
}

fn binding(object: u32, sub: i32) -> ColumnBinding {
    ColumnBinding {
        address: ObjectAddress {
            class_id: 1_259,
            object_id: object,
            sub_id: sub,
        },
        value_type: ValueType::Int8,
    }
}

fn graph() -> OperatorGraph {
    let left = SourceId::new(2).unwrap();
    let right = SourceId::new(1).unwrap();
    let output = OutputContract::KeyedRows {
        key_type: ValueType::Int8,
        key_nullable: false,
        value_type: ValueType::Int8,
        nullable: true,
    };
    OperatorGraph::build(
        GraphId::new(9).unwrap(),
        vec![
            SourcePort {
                source_id: right,
                layout: vec![binding(30_000, 1), binding(30_000, 2)],
                identity_index: Some(ObjectAddress {
                    class_id: 1_259,
                    object_id: 31_000,
                    sub_id: 0,
                }),
            },
            SourcePort {
                source_id: left,
                layout: vec![binding(20_000, 1), binding(20_000, 2)],
                identity_index: Some(ObjectAddress {
                    class_id: 1_259,
                    object_id: 21_000,
                    sub_id: 0,
                }),
            },
        ],
        vec![
            OperatorNode {
                node_id: node(1),
                input: NodeInput::SourcePort(left),
                state_contract: Some(StateContract { codec_version: 1 }),
                kind: OperatorNodeKind::InnerJoin {
                    left_source_id: left,
                    right_source_id: right,
                    left_id_slot: 0,
                    left_key_slot: 1,
                    right_id_slot: 0,
                    right_payload_slot: 1,
                },
            },
            OperatorNode {
                node_id: node(2),
                input: NodeInput::Node(node(1)),
                state_contract: None,
                kind: OperatorNodeKind::Materialize {
                    key_slot: 0,
                    value_slot: 1,
                    output,
                },
            },
        ],
    )
    .unwrap()
}

fn layout(graph: &OperatorGraph, source: SourceId) -> TypedLayout {
    let port = graph
        .sources
        .iter()
        .find(|port| port.source_id == source)
        .unwrap();
    source_typed_layout(source, &port.layout).unwrap()
}

fn left_row(graph: &OperatorGraph, id: i64, key: Option<i64>) -> TypedRow {
    let layout = layout(graph, SourceId::new(2).unwrap());
    TypedRow::new(
        &layout,
        vec![
            TypedValue::Int8(id),
            key.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8),
        ],
    )
    .unwrap()
}

fn right_row(graph: &OperatorGraph, id: i64, payload: Option<i64>) -> TypedRow {
    let layout = layout(graph, SourceId::new(1).unwrap());
    TypedRow::new(
        &layout,
        vec![
            TypedValue::Int8(id),
            payload.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8),
        ],
    )
    .unwrap()
}

fn batch(
    graph: &OperatorGraph,
    ordinal: u64,
    left: Vec<shiba_operator::RowDelta>,
    right: Vec<shiba_operator::RowDelta>,
) -> MultiInputBatch {
    let generation = SlotGeneration::new(1).unwrap();
    let lsn = PostgresLsn::from_u64(ordinal + 1);
    let ingress = IngressTransactionId::new(ordinal + 1).unwrap();
    let graph_tx = GraphTransactionId::new(graph.graph_id, generation, lsn, ingress).unwrap();
    let make = |source_id, rows| DeltaBatch {
        origin: EffectOrigin::Wal(
            SourceTransactionId::new(source_id, generation, lsn, ingress).unwrap(),
        ),
        layout_identity: layout(graph, source_id).identity,
        rows,
    };
    MultiInputBatch {
        origin: GraphEffectOrigin::Wal(graph_tx),
        sources: vec![
            SourceDeltaBatch {
                source_id: SourceId::new(1).unwrap(),
                delta: make(SourceId::new(1).unwrap(), right),
            },
            SourceDeltaBatch {
                source_id: SourceId::new(2).unwrap(),
                delta: make(SourceId::new(2).unwrap(), left),
            },
        ],
    }
}

fn evaluate(
    graph: &OperatorGraph,
    state: &mut BTreeMap<shiba_operator::StateKey, EncodedOperatorState>,
    input: &MultiInputBatch,
) -> Result<Vec<ResultMutation>, shiba_operator::KernelError> {
    let read_set = graph_state_read_set(graph, input)?;
    let mut entries = BTreeMap::new();
    for key in &read_set.keys {
        entries.insert(key.clone(), state.get(key).cloned());
    }
    for (key, value) in state.iter() {
        if read_set.partitions.iter().any(|partition| {
            partition.node_id == key.node_id
                && partition.namespace == key.namespace
                && partition.partition_key == key.partition_key
        }) {
            entries.insert(key.clone(), Some(value.clone()));
        }
    }
    let snapshot = StateSnapshot::new(
        &read_set,
        entries
            .into_iter()
            .map(|(key, state)| StateEntry { key, state })
            .collect(),
    )
    .unwrap();
    let transition = apply_graph_plan(graph, &snapshot, input)?;
    for delta in transition.state_deltas {
        match delta {
            StateDelta {
                key,
                mutation: StateMutation::Delete,
            } => {
                state.remove(&key);
            }
            StateDelta {
                key,
                mutation: StateMutation::Upsert { state: value },
            } => {
                state.insert(key, value);
            }
        }
    }
    let ResultDelta::Keyed { mutations, .. } = transition.results.into_iter().next().unwrap()
    else {
        panic!()
    };
    Ok(mutations)
}

fn apply_output(output: &mut BTreeMap<i64, Option<i64>>, mutations: Vec<ResultMutation>) {
    for mutation in mutations {
        match mutation {
            ResultMutation::Delete {
                key: TypedValue::Int8(id),
            } => {
                output.remove(&id);
            }
            ResultMutation::Upsert {
                key: TypedValue::Int8(id),
                value,
            } => {
                let payload = match value {
                    TypedValue::Int8(value) => Some(value),
                    TypedValue::Null(ValueType::Int8) => None,
                    _ => panic!("join emitted a value outside its contract"),
                };
                output.insert(id, payload);
            }
            _ => panic!("join emitted a key outside its contract"),
        }
    }
}

#[test]
fn both_sides_share_one_pre_to_final_transition() {
    let graph = graph();
    let mut state = BTreeMap::new();
    let first = batch(
        &graph,
        1,
        vec![shiba_operator::RowDelta {
            before: None,
            after: Some(left_row(&graph, 10, Some(1))),
        }],
        vec![shiba_operator::RowDelta {
            before: None,
            after: Some(right_row(&graph, 1, None)),
        }],
    );
    assert_eq!(
        evaluate(&graph, &mut state, &first).unwrap(),
        vec![ResultMutation::Upsert {
            key: TypedValue::Int8(10),
            value: TypedValue::Null(ValueType::Int8)
        }]
    );
    let both = batch(
        &graph,
        2,
        vec![shiba_operator::RowDelta {
            before: Some(left_row(&graph, 10, Some(1))),
            after: Some(left_row(&graph, 10, Some(2))),
        }],
        vec![shiba_operator::RowDelta {
            before: None,
            after: Some(right_row(&graph, 2, Some(7))),
        }],
    );
    assert_eq!(
        evaluate(&graph, &mut state, &both).unwrap(),
        vec![ResultMutation::Upsert {
            key: TypedValue::Int8(10),
            value: TypedValue::Int8(7)
        }]
    );
    let right_update = batch(
        &graph,
        3,
        vec![],
        vec![shiba_operator::RowDelta {
            before: Some(right_row(&graph, 2, Some(7))),
            after: Some(right_row(&graph, 2, None)),
        }],
    );
    assert_eq!(
        evaluate(&graph, &mut state, &right_update).unwrap(),
        vec![ResultMutation::Upsert {
            key: TypedValue::Int8(10),
            value: TypedValue::Null(ValueType::Int8)
        }]
    );
    let right_delete = batch(
        &graph,
        4,
        vec![],
        vec![shiba_operator::RowDelta {
            before: Some(right_row(&graph, 2, None)),
            after: None,
        }],
    );
    assert_eq!(
        evaluate(&graph, &mut state, &right_delete).unwrap(),
        vec![ResultMutation::Delete {
            key: TypedValue::Int8(10)
        }]
    );
    let remove = batch(
        &graph,
        5,
        vec![shiba_operator::RowDelta {
            before: Some(left_row(&graph, 10, Some(2))),
            after: Some(left_row(&graph, 10, None)),
        }],
        vec![],
    );
    assert!(evaluate(&graph, &mut state, &remove).unwrap().is_empty());
}

#[test]
fn randomized_changes_match_relational_reference() {
    let graph = graph();
    let mut kernel_state = BTreeMap::new();
    let mut actual = BTreeMap::new();
    let mut left = BTreeMap::<i64, Option<i64>>::new();
    let mut right = BTreeMap::<i64, Option<i64>>::new();
    let mut seed = 0x5eed_f00d_cafe_babe_u64;
    for ordinal in 1..=300 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let left_id = i64::try_from((seed >> 8) % 24).unwrap();
        let right_id = i64::try_from((seed >> 16) % 8).unwrap();
        let change_left = seed & 1 == 0 || seed & 7 == 7;
        let change_right = seed & 2 != 0 || !change_left;
        let mut left_delta = Vec::new();
        let mut right_delta = Vec::new();
        if change_left {
            let before = left.get(&left_id).copied();
            let after = if before.is_some() && seed & 0x20 != 0 {
                None
            } else if seed & 0x40 != 0 {
                Some(None)
            } else {
                Some(Some(i64::try_from((seed >> 24) % 8).unwrap()))
            };
            left_delta.push(shiba_operator::RowDelta {
                before: before.map(|key| left_row(&graph, left_id, key)),
                after: after.map(|key| left_row(&graph, left_id, key)),
            });
            match after {
                Some(key) => {
                    left.insert(left_id, key);
                }
                None => {
                    left.remove(&left_id);
                }
            }
        }
        if change_right {
            let before = right.get(&right_id).copied();
            let after = if before.is_some() && seed & 0x80 != 0 {
                None
            } else {
                let payload =
                    (seed & 0x100 != 0).then(|| i64::try_from((seed >> 32) % 1_000).unwrap());
                Some(payload)
            };
            right_delta.push(shiba_operator::RowDelta {
                before: before.map(|payload| right_row(&graph, right_id, payload)),
                after: after.map(|payload| right_row(&graph, right_id, payload)),
            });
            match after {
                Some(payload) => {
                    right.insert(right_id, payload);
                }
                None => {
                    right.remove(&right_id);
                }
            }
        }
        let input = batch(&graph, ordinal, left_delta, right_delta);
        apply_output(
            &mut actual,
            evaluate(&graph, &mut kernel_state, &input).unwrap(),
        );
        let expected = left
            .iter()
            .filter_map(|(id, key)| key.and_then(|key| right.get(&key).map(|value| (*id, *value))))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected, "reference diverged at step {ordinal}");
    }
}

#[test]
fn right_fanout_accepts_twenty_thousand_and_rejects_next() {
    let graph = graph();
    for limit in [20_000, 20_001] {
        let mut state = BTreeMap::new();
        for (ordinal, start) in [(10, 0), (11, 10_000), (12, 20_000)] {
            if start >= limit {
                continue;
            }
            let end = (start + 10_000).min(limit);
            let rows = (start..end)
                .map(|id| shiba_operator::RowDelta {
                    before: None,
                    after: Some(left_row(&graph, i64::from(id), Some(1))),
                })
                .collect();
            assert!(
                evaluate(&graph, &mut state, &batch(&graph, ordinal, rows, vec![]))
                    .unwrap()
                    .is_empty()
            );
        }
        let right = batch(
            &graph,
            20,
            vec![],
            vec![shiba_operator::RowDelta {
                before: None,
                after: Some(right_row(&graph, 1, Some(9))),
            }],
        );
        assert_eq!(
            evaluate(&graph, &mut state, &right).is_ok(),
            limit == 20_000
        );
    }
}

#[test]
fn corrupt_missing_extra_state_and_graph_digest_fail_closed() {
    let graph = graph();
    let mut missing_identity = graph.sources.clone();
    missing_identity[0].identity_index = None;
    assert!(OperatorGraph::build(graph.graph_id, missing_identity, graph.nodes.clone()).is_err());
    let mut duplicate_identity = graph.sources.clone();
    duplicate_identity[1].identity_index = duplicate_identity[0].identity_index;
    assert!(OperatorGraph::build(graph.graph_id, duplicate_identity, graph.nodes.clone()).is_err());
    let input = batch(
        &graph,
        1,
        vec![shiba_operator::RowDelta {
            before: None,
            after: Some(left_row(&graph, 1, Some(1))),
        }],
        vec![],
    );
    let read_set = graph_state_read_set(&graph, &input).unwrap();
    let empty = StateSnapshot {
        entries: Vec::new(),
    };
    assert!(apply_graph_plan(&graph, &empty, &input).is_err());
    let mut extra_entries = read_set
        .keys
        .iter()
        .map(|key| StateEntry {
            key: key.clone(),
            state: None,
        })
        .collect::<Vec<_>>();
    let mut foreign = read_set.keys[0].clone();
    foreign.namespace = 999;
    extra_entries.push(StateEntry {
        key: foreign,
        state: None,
    });
    extra_entries.sort_by(|left, right| left.key.cmp(&right.key));
    assert!(StateSnapshot::new(&read_set, extra_entries).is_err());
    let mut corrupt = graph.clone();
    corrupt.digest[0] ^= 1;
    assert!(graph_state_read_set(&corrupt, &input).is_err());
    let entries = read_set
        .keys
        .iter()
        .map(|key| StateEntry {
            key: key.clone(),
            state: Some(EncodedOperatorState {
                codec_version: 9,
                payload: vec![],
            }),
        })
        .collect();
    let snapshot = StateSnapshot::new(&read_set, entries).unwrap();
    assert!(apply_graph_plan(&graph, &snapshot, &input).is_err());
}
