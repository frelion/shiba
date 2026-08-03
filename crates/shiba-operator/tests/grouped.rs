use core::num::{NonZeroU32, NonZeroU64};
use std::collections::BTreeMap;

use shiba_operator::{
    ColumnBinding, CompiledPlan, DeltaBatch, EffectOrigin, EncodedOperatorState, Expression,
    InputBinding, InputRole, KeyedMutation, NodeId, NodeInput, ObjectAddress, OperatorGraph,
    OperatorId, OperatorNode, OperatorNodeKind, OutputContract, OutputDelta, PlanImplementation,
    RowDelta, StateContract, StateDelta, StateEntry, StateMutation, StateSnapshot, TypedLayout,
    TypedRow, TypedValue, ValueType, apply_plan, initial_state, source_typed_layout,
    state_read_set,
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

fn grouped(sum: bool, filtered: bool) -> CompiledPlan {
    let operator_id = OperatorId::new(NonZeroU64::new(u64::from(sum) + 1).unwrap());
    let source_id = SourceId::new(1).unwrap();
    let mut nodes = Vec::new();
    if filtered {
        nodes.push(OperatorNode {
            node_id: node(1),
            input: NodeInput::Source,
            state_contract: None,
            kind: OperatorNodeKind::Filter {
                predicate: Expression::Greater {
                    left: Box::new(Expression::Column { slot: 2 }),
                    right: Box::new(Expression::Int8Literal { value: 0 }),
                },
            },
        });
    }
    let key_id = node(if filtered { 2 } else { 1 });
    nodes.push(OperatorNode {
        node_id: key_id,
        input: if filtered {
            NodeInput::Node(node(1))
        } else {
            NodeInput::Source
        },
        state_contract: None,
        kind: OperatorNodeKind::KeyBy {
            key: Expression::Column { slot: 1 },
        },
    });
    let aggregate_id = node(key_id.get() + 1);
    nodes.push(OperatorNode {
        node_id: aggregate_id,
        input: NodeInput::Node(key_id),
        state_contract: Some(StateContract { codec_version: 1 }),
        kind: if sum {
            OperatorNodeKind::GroupedSumInt8 {
                key_slot: 3,
                value_slot: 2,
            }
        } else {
            OperatorNodeKind::GroupedCount { key_slot: 3 }
        },
    });
    let output = OutputContract::KeyedRows {
        key_type: ValueType::Int8,
        key_nullable: true,
        value_type: ValueType::Int8,
        nullable: sum,
    };
    nodes.push(OperatorNode {
        node_id: node(aggregate_id.get() + 1),
        input: NodeInput::Node(aggregate_id),
        state_contract: None,
        kind: OperatorNodeKind::Materialize {
            key_slot: 0,
            value_slot: 1,
            output: output.clone(),
        },
    });
    let source_layout = vec![binding(1), binding(2), binding(3)];
    let graph = OperatorGraph::build(operator_id, source_id, source_layout.clone(), nodes).unwrap();
    CompiledPlan::build(
        operator_id,
        source_id,
        source_layout
            .iter()
            .enumerate()
            .map(|(index, column)| InputBinding {
                role: if index == 0 {
                    InputRole::Key
                } else {
                    InputRole::Payload
                },
                address: column.address,
            })
            .collect(),
        output,
        PlanImplementation::Graph { graph },
    )
    .unwrap()
}

fn grouped_with_filters(filter_count: u32) -> CompiledPlan {
    let operator_id = OperatorId::new(NonZeroU64::new(11).unwrap());
    let source_id = SourceId::new(1).unwrap();
    let mut nodes = Vec::new();
    for index in 0..filter_count {
        nodes.push(OperatorNode {
            node_id: node(index + 1),
            input: if index == 0 {
                NodeInput::Source
            } else {
                NodeInput::Node(node(index))
            },
            state_contract: None,
            kind: OperatorNodeKind::Filter {
                predicate: Expression::Equal {
                    left: Box::new(Expression::Int8Literal { value: 1 }),
                    right: Box::new(Expression::Int8Literal { value: 1 }),
                },
            },
        });
    }
    let key_id = node(filter_count + 1);
    nodes.push(OperatorNode {
        node_id: key_id,
        input: NodeInput::Node(node(filter_count)),
        state_contract: None,
        kind: OperatorNodeKind::KeyBy {
            key: Expression::Column { slot: 1 },
        },
    });
    nodes.push(OperatorNode {
        node_id: node(filter_count + 2),
        input: NodeInput::Node(key_id),
        state_contract: Some(StateContract { codec_version: 1 }),
        kind: OperatorNodeKind::GroupedCount { key_slot: 3 },
    });
    let output = OutputContract::KeyedRows {
        key_type: ValueType::Int8,
        key_nullable: false,
        value_type: ValueType::Int8,
        nullable: false,
    };
    nodes.push(OperatorNode {
        node_id: node(filter_count + 3),
        input: NodeInput::Node(node(filter_count + 2)),
        state_contract: None,
        kind: OperatorNodeKind::Materialize {
            key_slot: 0,
            value_slot: 1,
            output: output.clone(),
        },
    });
    let source_layout = vec![binding(1), binding(2), binding(3)];
    let graph = OperatorGraph::build(operator_id, source_id, source_layout.clone(), nodes).unwrap();
    CompiledPlan::build(
        operator_id,
        source_id,
        source_layout
            .iter()
            .enumerate()
            .map(|(index, binding)| InputBinding {
                role: if index == 0 {
                    InputRole::Key
                } else {
                    InputRole::Payload
                },
                address: binding.address,
            })
            .collect(),
        output,
        PlanImplementation::Graph { graph },
    )
    .unwrap()
}

fn layout(plan: &CompiledPlan) -> TypedLayout {
    let PlanImplementation::Graph { graph } = &plan.implementation else {
        unreachable!()
    };
    source_typed_layout(graph.sources[0].source_id, &graph.sources[0].layout).unwrap()
}

fn row(layout: &TypedLayout, id: i64, group: TypedValue, value: TypedValue) -> TypedRow {
    TypedRow::new(layout, vec![TypedValue::Int8(id), group, value]).unwrap()
}

fn batch(layout: &TypedLayout, rows: Vec<RowDelta>) -> DeltaBatch {
    DeltaBatch {
        origin: origin(),
        layout_identity: layout.identity,
        rows,
    }
}

fn evaluate(
    plan: &CompiledPlan,
    states: &mut BTreeMap<shiba_operator::StateKey, EncodedOperatorState>,
    input: &DeltaBatch,
) -> Vec<KeyedMutation> {
    let read_set = state_read_set(plan, input).unwrap();
    let snapshot = StateSnapshot::new(
        &read_set,
        read_set
            .keys
            .iter()
            .map(|key| StateEntry {
                key: key.clone(),
                state: states.get(key).cloned(),
            })
            .collect(),
    )
    .unwrap();
    let transition = apply_plan(plan, &initial_state(plan).unwrap(), &snapshot, input).unwrap();
    for delta in transition.state_deltas {
        match delta {
            StateDelta {
                key,
                mutation: StateMutation::Delete,
            } => {
                states.remove(&key);
            }
            StateDelta {
                key,
                mutation: StateMutation::Upsert { state },
            } => {
                states.insert(key, state);
            }
        }
    }
    let OutputDelta::KeyedMutations { mutations } = transition.output_delta else {
        panic!("expected keyed output")
    };
    mutations
}

#[test]
fn grouped_count_coalesces_updates_and_key_changes() {
    let plan = grouped(false, false);
    let layout = layout(&plan);
    let mut states = BTreeMap::new();
    let inserted = row(
        &layout,
        1,
        TypedValue::Int8(10),
        TypedValue::Null(ValueType::Int8),
    );
    assert_eq!(
        evaluate(
            &plan,
            &mut states,
            &batch(
                &layout,
                vec![RowDelta {
                    before: None,
                    after: Some(inserted.clone())
                }]
            )
        ),
        vec![KeyedMutation::Upsert {
            key: TypedValue::Int8(10),
            value: TypedValue::Int8(1)
        }]
    );
    assert!(
        evaluate(
            &plan,
            &mut states,
            &batch(
                &layout,
                vec![RowDelta {
                    before: Some(inserted.clone()),
                    after: Some(inserted.clone())
                }]
            )
        )
        .is_empty()
    );
    let moved = row(
        &layout,
        1,
        TypedValue::Null(ValueType::Int8),
        TypedValue::Int8(9),
    );
    assert_eq!(
        evaluate(
            &plan,
            &mut states,
            &batch(
                &layout,
                vec![RowDelta {
                    before: Some(inserted),
                    after: Some(moved)
                }]
            )
        )
        .len(),
        2
    );
}

#[test]
fn grouped_sum_distinguishes_all_null_and_filter_transitions() {
    let null_plan = grouped(true, false);
    let null_layout = layout(&null_plan);
    let mut null_states = BTreeMap::new();
    let all_null = evaluate(
        &null_plan,
        &mut null_states,
        &batch(
            &null_layout,
            vec![RowDelta {
                before: None,
                after: Some(row(
                    &null_layout,
                    1,
                    TypedValue::Int8(7),
                    TypedValue::Null(ValueType::Int8),
                )),
            }],
        ),
    );
    assert_eq!(
        all_null,
        vec![KeyedMutation::Upsert {
            key: TypedValue::Int8(7),
            value: TypedValue::Null(ValueType::Int8),
        }]
    );

    let plan = grouped(true, true);
    let layout = layout(&plan);
    let mut states = BTreeMap::new();
    let hidden = row(
        &layout,
        1,
        TypedValue::Int8(7),
        TypedValue::Null(ValueType::Int8),
    );
    assert!(
        evaluate(
            &plan,
            &mut states,
            &batch(
                &layout,
                vec![RowDelta {
                    before: None,
                    after: Some(hidden.clone())
                }]
            )
        )
        .is_empty()
    );
    let visible_null = row(&layout, 1, TypedValue::Int8(7), TypedValue::Int8(1));
    assert_eq!(
        evaluate(
            &plan,
            &mut states,
            &batch(
                &layout,
                vec![RowDelta {
                    before: Some(hidden),
                    after: Some(visible_null.clone())
                }]
            )
        ),
        vec![KeyedMutation::Upsert {
            key: TypedValue::Int8(7),
            value: TypedValue::Int8(1)
        }]
    );
    let same = evaluate(
        &plan,
        &mut states,
        &batch(
            &layout,
            vec![RowDelta {
                before: Some(visible_null.clone()),
                after: Some(visible_null),
            }],
        ),
    );
    assert!(same.is_empty());
}

#[test]
fn snapshot_corruption_absent_and_overflow_fail_closed() {
    let plan = grouped(true, false);
    let layout = layout(&plan);
    let input = batch(
        &layout,
        vec![RowDelta {
            before: None,
            after: Some(row(&layout, 1, TypedValue::Int8(2), TypedValue::Int8(1))),
        }],
    );
    let read_set = state_read_set(&plan, &input).unwrap();
    assert!(
        apply_plan(
            &plan,
            &initial_state(&plan).unwrap(),
            &StateSnapshot { entries: vec![] },
            &input
        )
        .is_err()
    );
    let bad = StateSnapshot::new(
        &read_set,
        vec![StateEntry {
            key: read_set.keys[0].clone(),
            state: Some(EncodedOperatorState {
                codec_version: 9,
                payload: vec![],
            }),
        }],
    )
    .unwrap();
    assert!(apply_plan(&plan, &initial_state(&plan).unwrap(), &bad, &input).is_err());
    let absent = batch(
        &layout,
        vec![RowDelta {
            before: None,
            after: Some(row(&layout, 1, TypedValue::Absent, TypedValue::Int8(1))),
        }],
    );
    assert!(state_read_set(&plan, &absent).is_err());
    let overflow = StateSnapshot::new(
        &read_set,
        vec![StateEntry {
            key: read_set.keys[0].clone(),
            state: Some(EncodedOperatorState {
                codec_version: 1,
                payload: [
                    1_i64.to_be_bytes(),
                    1_i64.to_be_bytes(),
                    i64::MAX.to_be_bytes(),
                ]
                .concat(),
            }),
        }],
    )
    .unwrap();
    assert!(apply_plan(&plan, &initial_state(&plan).unwrap(), &overflow, &input).is_err());

    let row = row(&layout, 2, TypedValue::Int8(2), TypedValue::Int8(1));
    let oversized = batch(
        &layout,
        (0..10_001)
            .map(|_| RowDelta {
                before: None,
                after: Some(row.clone()),
            })
            .collect(),
    );
    assert!(state_read_set(&plan, &oversized).is_err());
}

#[test]
fn fixed_seed_grouped_sum_matches_reference_model() {
    let plan = grouped(true, false);
    let layout = layout(&plan);
    let mut states = BTreeMap::new();
    let mut source = BTreeMap::<i64, (Option<i64>, Option<i64>)>::new();
    let mut results = BTreeMap::<TypedValue, TypedValue>::new();
    let mut seed = 0x5eed_u64;
    for step in 0..500_i64 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let id = i64::try_from(seed % 24).unwrap();
        let before = source.get(&id).copied();
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let after = match seed % 4 {
            0 => None,
            _ => Some((
                (!seed.is_multiple_of(5)).then_some(i64::try_from((seed >> 8) % 5).unwrap()),
                (!seed.is_multiple_of(3)).then_some(i64::try_from((seed >> 16) % 31).unwrap() - 15),
            )),
        };
        if before.is_none() && after.is_none() {
            continue;
        }
        let to_row = |(group, value): (Option<i64>, Option<i64>)| {
            row(
                &layout,
                id,
                group.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8),
                value.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8),
            )
        };
        for mutation in evaluate(
            &plan,
            &mut states,
            &batch(
                &layout,
                vec![RowDelta {
                    before: before.map(to_row),
                    after: after.map(to_row),
                }],
            ),
        ) {
            match mutation {
                KeyedMutation::Delete { key } => {
                    results.remove(&key);
                }
                KeyedMutation::Upsert { key, value } => {
                    results.insert(key, value);
                }
            }
        }
        match after {
            Some(value) => {
                source.insert(id, value);
            }
            None => {
                source.remove(&id);
            }
        }
        let mut grouped = BTreeMap::<TypedValue, Vec<Option<i64>>>::new();
        for (group, value) in source.values() {
            grouped
                .entry(group.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8))
                .or_default()
                .push(*value);
        }
        let oracle = grouped
            .into_iter()
            .map(|(key, values)| {
                let non_null = values.into_iter().flatten().collect::<Vec<_>>();
                let value = if non_null.is_empty() {
                    TypedValue::Null(ValueType::Int8)
                } else {
                    TypedValue::Int8(non_null.into_iter().sum())
                };
                (key, value)
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(results, oracle, "step {step}");
    }
}

#[test]
fn grouped_prefix_enforces_the_shared_work_byte_bound() {
    let operator_id = OperatorId::new(NonZeroU64::new(9).unwrap());
    let source_id = SourceId::new(1).unwrap();
    let source_layout = vec![
        binding(1),
        binding(2),
        ColumnBinding {
            address: binding(3).address,
            value_type: ValueType::Text,
        },
    ];
    let output = OutputContract::KeyedRows {
        key_type: ValueType::Int8,
        key_nullable: false,
        value_type: ValueType::Int8,
        nullable: false,
    };
    let graph = OperatorGraph::build(
        operator_id,
        source_id,
        source_layout.clone(),
        vec![
            OperatorNode {
                node_id: node(1),
                input: NodeInput::Source,
                state_contract: None,
                kind: OperatorNodeKind::KeyBy {
                    key: Expression::Column { slot: 0 },
                },
            },
            OperatorNode {
                node_id: node(2),
                input: NodeInput::Node(node(1)),
                state_contract: Some(StateContract { codec_version: 1 }),
                kind: OperatorNodeKind::GroupedCount { key_slot: 3 },
            },
            OperatorNode {
                node_id: node(3),
                input: NodeInput::Node(node(2)),
                state_contract: None,
                kind: OperatorNodeKind::Materialize {
                    key_slot: 0,
                    value_slot: 1,
                    output: output.clone(),
                },
            },
        ],
    )
    .unwrap();
    let plan = CompiledPlan::build(
        operator_id,
        source_id,
        source_layout
            .iter()
            .enumerate()
            .map(|(index, binding)| InputBinding {
                role: if index == 0 {
                    InputRole::Key
                } else {
                    InputRole::Payload
                },
                address: binding.address,
            })
            .collect(),
        output,
        PlanImplementation::Graph { graph },
    )
    .unwrap();
    let layout = layout(&plan);
    let text = "x".repeat(1 << 20);
    let rows = (0..65)
        .map(|id| RowDelta {
            before: None,
            after: Some(
                TypedRow::new(
                    &layout,
                    vec![
                        TypedValue::Int8(id),
                        TypedValue::Int8(1),
                        TypedValue::Text(text.clone()),
                    ],
                )
                .unwrap(),
            ),
        })
        .collect();
    assert!(state_read_set(&plan, &batch(&layout, rows)).is_err());
}

#[test]
fn ten_thousand_key_changes_emit_exact_twenty_thousand_mutations() {
    let plan = grouped(false, false);
    let layout = layout(&plan);
    let input = batch(
        &layout,
        (0..10_000)
            .map(|id| RowDelta {
                before: Some(row(&layout, id, TypedValue::Int8(id), TypedValue::Int8(1))),
                after: Some(row(
                    &layout,
                    id,
                    TypedValue::Int8(id + 10_000),
                    TypedValue::Int8(1),
                )),
            })
            .collect(),
    );
    let read_set = state_read_set(&plan, &input).unwrap();
    assert_eq!(read_set.keys.len(), 20_000);
    let snapshot = StateSnapshot::new(
        &read_set,
        read_set
            .keys
            .iter()
            .map(|key| StateEntry {
                key: key.clone(),
                state: matches!(key.partition_key, TypedValue::Int8(value) if value < 10_000).then(
                    || EncodedOperatorState {
                        codec_version: 1,
                        payload: 1_i64.to_be_bytes().to_vec(),
                    },
                ),
            })
            .collect(),
    )
    .unwrap();
    let transition = apply_plan(&plan, &initial_state(&plan).unwrap(), &snapshot, &input).unwrap();
    assert_eq!(transition.state_deltas.len(), 20_000);
    let OutputDelta::KeyedMutations { mutations } = transition.output_delta else {
        panic!("expected keyed output")
    };
    assert_eq!(mutations.len(), 20_000);
}

#[test]
fn aggregate_output_is_charged_to_the_shared_two_hundred_thousand_row_budget() {
    for (filters, succeeds) in [(18, true), (19, false)] {
        let plan = grouped_with_filters(filters);
        let layout = layout(&plan);
        let input = batch(
            &layout,
            (0..10_000)
                .map(|id| RowDelta {
                    before: None,
                    after: Some(row(&layout, id, TypedValue::Int8(id), TypedValue::Int8(1))),
                })
                .collect(),
        );
        let read_set = state_read_set(&plan, &input).unwrap();
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
        assert_eq!(
            apply_plan(&plan, &initial_state(&plan).unwrap(), &snapshot, &input).is_ok(),
            succeeds
        );
    }
}
