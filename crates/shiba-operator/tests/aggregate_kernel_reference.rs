use std::collections::BTreeMap;
use std::num::NonZeroU32;

use shiba_operator::{
    AggregateCall, AggregateFunctionV1, ColumnBinding, DeltaBatch, EffectOrigin,
    EncodedOperatorState, Expression, GraphEffectOrigin, KernelError, MultiInputBatch, NodeId,
    NodeInput, ObjectAddress, OperatorGraph, OperatorNode, OperatorNodeKind, OutputContract,
    ResultField, ResultMutation, ResultSchemaV1, RowDelta, SourceDeltaBatch, SourcePort,
    StateContract, StateEntry, StateKey, StateMutation, StateSnapshot, TypedResultRowV1, TypedRow,
    TypedValue, ValueType, apply_graph_plan, graph_state_read_set, source_typed_layout,
};
use shiba_protocol::{BootstrapBatchId, BootstrapId, GraphId, SourceId};

type Store = BTreeMap<StateKey, EncodedOperatorState>;
type ValueChange = (Option<Option<i64>>, Option<Option<i64>>);

fn node(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).unwrap())
}

fn graph(function_version: u32) -> Result<OperatorGraph, shiba_operator::GraphError> {
    let source_id = SourceId::new(1).unwrap();
    let schema = ResultSchemaV1::new(
        vec![
            field(1, "rows", false),
            field(2, "non_null", false),
            field(3, "total", true),
            field(4, "minimum", true),
            field(5, "maximum", true),
        ],
        vec![],
    )
    .unwrap();
    let initial = TypedResultRowV1::new(
        &schema,
        vec![
            TypedValue::Int8(0),
            TypedValue::Int8(0),
            TypedValue::Null(ValueType::Int8),
            TypedValue::Null(ValueType::Int8),
            TypedValue::Null(ValueType::Int8),
        ],
    )
    .unwrap();
    OperatorGraph::build(
        GraphId::new(1).unwrap(),
        vec![SourcePort {
            source_id,
            layout: vec![ColumnBinding {
                address: ObjectAddress {
                    class_id: 1_259,
                    object_id: 10_000,
                    sub_id: 1,
                },
                value_type: ValueType::Int8,
            }],
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
                kind: OperatorNodeKind::Aggregate {
                    group_expressions: vec![],
                    calls: vec![
                        call(1, function_version, AggregateFunctionV1::CountStar, None),
                        call(
                            2,
                            function_version,
                            AggregateFunctionV1::Count,
                            Some(Expression::Column { slot: 0 }),
                        ),
                        call(
                            3,
                            function_version,
                            AggregateFunctionV1::SumInt8,
                            Some(Expression::Column { slot: 0 }),
                        ),
                        call(
                            4,
                            function_version,
                            AggregateFunctionV1::MinInt8,
                            Some(Expression::Column { slot: 0 }),
                        ),
                        call(
                            5,
                            function_version,
                            AggregateFunctionV1::MaxInt8,
                            Some(Expression::Column { slot: 0 }),
                        ),
                    ],
                    having: None,
                },
            },
            OperatorNode {
                node_id: node(2),
                input: NodeInput::Node(node(1)),
                state_contract: None,
                kind: OperatorNodeKind::Materialize {
                    field_slots: vec![0, 1, 2, 3, 4],
                    output: OutputContract::new(schema, Some(initial)).unwrap(),
                },
            },
        ],
    )
}

fn field(ordinal: u16, name: &str, nullable: bool) -> ResultField {
    ResultField {
        ordinal,
        name: name.into(),
        value_type: ValueType::Int8,
        nullable,
    }
}

fn call(
    ordinal: u16,
    function_version: u32,
    function: AggregateFunctionV1,
    expression: Option<Expression>,
) -> AggregateCall {
    AggregateCall {
        ordinal,
        function_version,
        function,
        expression,
    }
}

fn changes(graph: &OperatorGraph, ordinal: u64, values: &[ValueChange]) -> MultiInputBatch {
    let source_id = graph.sources[0].source_id;
    let layout = source_typed_layout(source_id, &graph.sources[0].layout).unwrap();
    let batch_id = BootstrapBatchId::new(BootstrapId::new(1).unwrap(), ordinal).unwrap();
    let row = |value: Option<i64>| {
        TypedRow::new(
            &layout,
            vec![value.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8)],
        )
        .unwrap()
    };
    MultiInputBatch {
        origin: GraphEffectOrigin::Bootstrap(batch_id),
        sources: vec![SourceDeltaBatch {
            source_id,
            delta: DeltaBatch {
                origin: EffectOrigin::Bootstrap(batch_id),
                layout_identity: layout.identity,
                rows: values
                    .iter()
                    .map(|(before, after)| RowDelta {
                        before: before.map(row),
                        after: after.map(row),
                    })
                    .collect(),
            },
        }],
    }
}

fn apply(
    graph: &OperatorGraph,
    store: &mut Store,
    batch: &MultiInputBatch,
) -> Result<Vec<TypedValue>, KernelError> {
    let read_set = graph_state_read_set(graph, batch)?;
    let snapshot = StateSnapshot::new(
        &read_set,
        read_set
            .keys
            .iter()
            .map(|key| StateEntry {
                key: key.clone(),
                state: store.get(key).cloned(),
            })
            .chain(
                store
                    .iter()
                    .filter(|(key, _)| {
                        read_set.partitions.iter().any(|partition| {
                            partition.node_id == key.node_id
                                && partition.namespace == key.namespace
                                && partition.partition_key == key.partition_key
                        })
                    })
                    .map(|(key, state)| StateEntry {
                        key: key.clone(),
                        state: Some(state.clone()),
                    }),
            )
            .collect(),
    )
    .map_err(|_| KernelError::InvalidState)?;
    let transition = apply_graph_plan(graph, &snapshot, batch)?;
    for delta in transition.state_deltas {
        match delta.mutation {
            StateMutation::Delete => {
                store.remove(&delta.key);
            }
            StateMutation::Upsert { state } => {
                store.insert(delta.key, state);
            }
        }
    }
    match &transition.results[0].mutations[0] {
        ResultMutation::ReplaceScalar { row } => Ok(row.values.clone()),
        _ => panic!("scalar aggregate emitted keyed mutation"),
    }
}

fn expected(rows: &[Option<i64>]) -> Vec<TypedValue> {
    let values = rows.iter().flatten().copied().collect::<Vec<_>>();
    vec![
        TypedValue::Int8(i64::try_from(rows.len()).unwrap()),
        TypedValue::Int8(i64::try_from(values.len()).unwrap()),
        if values.is_empty() {
            TypedValue::Null(ValueType::Int8)
        } else {
            TypedValue::Int8(values.iter().sum())
        },
        values
            .iter()
            .min()
            .copied()
            .map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8),
        values
            .iter()
            .max()
            .copied()
            .map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8),
    ]
}

#[test]
fn shared_production_harness_proves_iud_and_fixed_seed_differential() {
    let graph = graph(1).unwrap();
    let mut store = Store::new();
    let mut rows = Vec::new();
    let mut seed = 0x6d31_365f_6b65_726e_u64;
    for ordinal in 1..=1_000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let value = (!seed.is_multiple_of(5)).then_some(i64::try_from(seed % 101).unwrap() - 50);
        let change = if rows.is_empty() || seed.is_multiple_of(3) {
            rows.push(value);
            (None, Some(value))
        } else if seed % 3 == 1 {
            let index = usize::try_from(seed).unwrap() % rows.len();
            let before = std::mem::replace(&mut rows[index], value);
            (Some(before), Some(value))
        } else {
            let index = usize::try_from(seed).unwrap() % rows.len();
            (Some(rows.swap_remove(index)), None)
        };
        let actual = apply(&graph, &mut store, &changes(&graph, ordinal, &[change]));
        assert_eq!(actual.unwrap(), expected(&rows));
    }
}

#[test]
fn versions_and_corrupt_state_fail_closed() {
    assert!(graph(2).is_err());
    let graph = graph(1).unwrap();
    let batch = changes(&graph, 1, &[(None, Some(Some(1)))]);
    let read_set = graph_state_read_set(&graph, &batch).unwrap();
    let mut store = Store::new();
    store.insert(
        read_set.keys[0].clone(),
        EncodedOperatorState {
            codec_version: 99,
            payload: 0_i64.to_be_bytes().to_vec(),
        },
    );
    assert_eq!(
        apply(&graph, &mut store, &batch),
        Err(KernelError::InvalidState)
    );
}

#[test]
fn extrema_multiplicity_corruption_and_missing_retract_fail_closed() {
    let graph = graph(1).unwrap();
    let insert = changes(&graph, 1, &[(None, Some(Some(7)))]);
    let delete = changes(&graph, 2, &[(Some(Some(7)), None)]);
    let mut store = Store::new();
    apply(&graph, &mut store, &insert).unwrap();
    let key = store
        .keys()
        .find(|key| key.namespace == 4)
        .cloned()
        .unwrap();
    store.get_mut(&key).unwrap().payload = 0_i64.to_be_bytes().to_vec();
    assert_eq!(
        apply(&graph, &mut store, &delete),
        Err(KernelError::InvalidState)
    );
    assert_eq!(
        apply(&graph, &mut Store::new(), &delete),
        Err(KernelError::Underflow)
    );
}

#[test]
fn normalized_net_zero_and_min_retraction_are_exact() {
    let graph = graph(1).unwrap();
    let mut store = Store::new();
    apply(
        &graph,
        &mut store,
        &changes(&graph, 1, &[(None, Some(Some(i64::MAX)))]),
    )
    .unwrap();
    let before = store.clone();
    assert_eq!(
        apply(
            &graph,
            &mut store,
            &changes(&graph, 2, &[(None, Some(Some(1))), (Some(Some(1)), None)]),
        )
        .unwrap(),
        vec![
            TypedValue::Int8(1),
            TypedValue::Int8(1),
            TypedValue::Int8(i64::MAX),
            TypedValue::Int8(i64::MAX),
            TypedValue::Int8(i64::MAX),
        ]
    );
    assert_eq!(store, before);

    let mut store = Store::new();
    apply(
        &graph,
        &mut store,
        &changes(&graph, 3, &[(None, Some(Some(i64::MIN)))]),
    )
    .unwrap();
    assert_eq!(
        apply(
            &graph,
            &mut store,
            &changes(&graph, 4, &[(Some(Some(i64::MIN)), None)])
        )
        .unwrap(),
        vec![
            TypedValue::Int8(0),
            TypedValue::Int8(0),
            TypedValue::Null(ValueType::Int8),
            TypedValue::Null(ValueType::Int8),
            TypedValue::Null(ValueType::Int8),
        ]
    );
}
