use core::num::NonZeroU32;

use shiba_compiler::{
    GRAPH_SPEC_VERSION, GraphOutputSpecV1, GraphSpecV1, IdentityIndexDescriptor,
    SourceColumnDescriptor, SourceDescriptor, compile_graph,
    compile_graph_with_optional_identities,
};
use shiba_operator::{
    DeltaBatch, EffectOrigin, GraphEffectOrigin, MultiInputBatch, NodeId, ObjectAddress,
    OperatorNodeKind, ResultDelta, ResultMutation, RowDelta, SourceDeltaBatch, StateEntry,
    StateSnapshot, TypedRow, TypedValue, ValueType, apply_graph, apply_graph_plan,
    graph_state_read_set, source_typed_layout,
};
use shiba_protocol::{BootstrapBatchId, BootstrapId, GraphId, SourceId};

fn node(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).unwrap())
}

fn address(object: u32, sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        class_id: 1_259,
        object_id: object,
        sub_id,
    }
}

fn source(id: u64, object: u32, names: &[(&str, bool)]) -> SourceDescriptor {
    SourceDescriptor {
        source_id: SourceId::new(id).unwrap(),
        relation: address(object, 0),
        columns: names
            .iter()
            .enumerate()
            .map(|(index, (name, nullable))| SourceColumnDescriptor {
                name: (*name).into(),
                address: address(object, i32::try_from(index + 1).unwrap()),
                type_oid: 20,
                nullable: *nullable,
            })
            .collect(),
    }
}

fn identity(source: &SourceDescriptor, object: u32) -> IdentityIndexDescriptor {
    IdentityIndexDescriptor {
        address: address(object, 0),
        relation: source.relation,
        key_column: source.columns[0].address,
        key_arity: 1,
        unique: true,
        valid: true,
        ready: true,
        has_expression: false,
        has_predicate: false,
        effective_replica_identity: true,
    }
}

#[test]
fn singleton_graph_compiles_multiple_terminal_results_canonically() {
    let source = source(1, 10_000, &[("id", false), ("payload", true)]);
    let index = identity(&source, 11_000);
    let spec = GraphSpecV1 {
        version: GRAPH_SPEC_VERSION,
        graph_id: GraphId::new(9).unwrap(),
        sources: vec![source.source_id],
        outputs: vec![
            GraphOutputSpecV1::CountRows {
                source_id: source.source_id,
                aggregate_node_id: node(1),
                result_node_id: node(2),
            },
            GraphOutputSpecV1::SumInt8 {
                source_id: source.source_id,
                input_column: "payload".into(),
                aggregate_node_id: node(3),
                result_node_id: node(4),
            },
            GraphOutputSpecV1::MaterializedProject {
                source_id: source.source_id,
                key_column: "id".into(),
                value_column: "payload".into(),
                project_node_id: node(5),
                result_node_id: node(6),
            },
            GraphOutputSpecV1::GroupedCount {
                source_id: source.source_id,
                key_column: "id".into(),
                key_node_id: node(7),
                aggregate_node_id: node(8),
                result_node_id: node(9),
            },
            GraphOutputSpecV1::GroupedSumInt8 {
                source_id: source.source_id,
                key_column: "id".into(),
                input_column: "payload".into(),
                key_node_id: node(10),
                aggregate_node_id: node(11),
                result_node_id: node(12),
            },
        ],
    };
    assert!(compile_graph(&spec, std::slice::from_ref(&source), &[]).is_err());
    let first = compile_graph(
        &spec,
        std::slice::from_ref(&source),
        std::slice::from_ref(&index),
    )
    .unwrap();
    let second = compile_graph(
        &spec,
        std::slice::from_ref(&source),
        std::slice::from_ref(&index),
    )
    .unwrap();
    assert_eq!(first.canonical_payload, second.canonical_payload);
    assert_eq!(first.digest, second.digest);
    assert_eq!(first.nodes.len(), 12);
    assert_eq!(first.graph_id, spec.graph_id);
    assert_eq!(first.sources[0].identity_index, Some(index.address));
    let json = spec.to_canonical_json().unwrap();
    assert_eq!(GraphSpecV1::from_json(&json).unwrap(), spec);
    let mut unknown = json;
    unknown.pop();
    unknown.extend_from_slice(br#","alias":true}"#);
    assert!(GraphSpecV1::from_json(&unknown).is_err());
}

#[test]
fn join_requires_exact_effective_right_identity() {
    let right = source(1, 20_000, &[("id", false), ("payload", true)]);
    let left = source(2, 30_000, &[("id", false), ("right_key", true)]);
    let index = identity(&right, 21_000);
    let left_index = identity(&left, 31_000);
    let spec = GraphSpecV1 {
        version: 1,
        graph_id: GraphId::new(10).unwrap(),
        sources: vec![right.source_id, left.source_id],
        outputs: vec![GraphOutputSpecV1::InnerJoin {
            left_source_id: left.source_id,
            right_source_id: right.source_id,
            left_id_column: "id".into(),
            left_right_key_column: "right_key".into(),
            right_id_column: "id".into(),
            right_payload_column: "payload".into(),
            right_identity_index: index.address,
            join_node_id: node(1),
            result_node_id: node(2),
        }],
    };
    let graph = compile_graph(
        &spec,
        &[right.clone(), left.clone()],
        &[index.clone(), left_index.clone()],
    )
    .unwrap();
    assert!(matches!(
        graph.nodes[0].kind,
        OperatorNodeKind::InnerJoin { .. }
    ));
    assert_eq!(graph.sources[0].identity_index, Some(index.address));
    assert_eq!(graph.sources[1].identity_index, Some(left_index.address));
    let mut wrong_declared = spec.clone();
    let GraphOutputSpecV1::InnerJoin {
        right_identity_index,
        ..
    } = &mut wrong_declared.outputs[0]
    else {
        unreachable!()
    };
    *right_identity_index = left_index.address;
    assert!(
        compile_graph(
            &wrong_declared,
            &[right.clone(), left.clone()],
            &[index.clone(), left_index.clone()]
        )
        .is_err()
    );
    let mut drift = index;
    drift.effective_replica_identity = false;
    assert!(compile_graph(&spec, &[right, left], &[drift, left_index]).is_err());
}

#[test]
fn every_source_requires_one_exact_effective_identity() {
    let source = source(1, 40_000, &[("id", false), ("payload", true)]);
    let spec = GraphSpecV1 {
        version: 1,
        graph_id: GraphId::new(11).unwrap(),
        sources: vec![source.source_id],
        outputs: vec![GraphOutputSpecV1::CountRows {
            source_id: source.source_id,
            aggregate_node_id: node(1),
            result_node_id: node(2),
        }],
    };
    let valid = identity(&source, 41_000);
    let graph = compile_graph(
        &spec,
        std::slice::from_ref(&source),
        std::slice::from_ref(&valid),
    )
    .unwrap();
    assert_eq!(graph.sources[0].identity_index, Some(valid.address));
    let mut invalid = Vec::new();
    let mut wrong_relation = valid.clone();
    wrong_relation.relation = address(99_000, 0);
    invalid.push(wrong_relation);
    let mut wrong_key = valid.clone();
    wrong_key.key_column = address(40_000, 2);
    invalid.push(wrong_key);
    for mutate in [
        |index: &mut IdentityIndexDescriptor| index.unique = false,
        |index: &mut IdentityIndexDescriptor| index.valid = false,
        |index: &mut IdentityIndexDescriptor| index.ready = false,
        |index: &mut IdentityIndexDescriptor| index.has_expression = true,
        |index: &mut IdentityIndexDescriptor| index.has_predicate = true,
        |index: &mut IdentityIndexDescriptor| index.effective_replica_identity = false,
    ] {
        let mut index = valid.clone();
        mutate(&mut index);
        invalid.push(index);
    }
    for index in invalid {
        assert!(
            compile_graph(
                &spec,
                std::slice::from_ref(&source),
                std::slice::from_ref(&index)
            )
            .is_err()
        );
    }
}

#[test]
fn only_zero_column_singleton_count_may_omit_identity() {
    let empty = source(1, 45_000, &[]);
    let count = GraphSpecV1 {
        version: 1,
        graph_id: GraphId::new(45).unwrap(),
        sources: vec![empty.source_id],
        outputs: vec![GraphOutputSpecV1::CountRows {
            source_id: empty.source_id,
            aggregate_node_id: node(1),
            result_node_id: node(2),
        }],
    };
    let graph =
        compile_graph_with_optional_identities(&count, std::slice::from_ref(&empty), &[None])
            .unwrap();
    assert_eq!(graph.sources[0].identity_index, None);

    let keyed = source(1, 46_000, &[("id", false)]);
    assert!(
        compile_graph_with_optional_identities(&count, &[keyed], &[None]).is_err(),
        "an identity-free source with any durable column must fail closed"
    );
}

#[test]
fn bounded_pipeline_declarations_are_canonical_and_typed() {
    let source = source(
        1,
        50_000,
        &[("id", false), ("group_id", true), ("payload", true)],
    );
    let index = identity(&source, 51_000);
    let spec = GraphSpecV1 {
        version: 1,
        graph_id: GraphId::new(12).unwrap(),
        sources: vec![source.source_id],
        outputs: vec![
            GraphOutputSpecV1::ComputedProject {
                source_id: source.source_id,
                key_column: "id".into(),
                input_column: "payload".into(),
                literal: 5,
                compute_node_id: node(1),
                project_node_id: node(2),
                result_node_id: node(3),
            },
            GraphOutputSpecV1::FilteredGroupedCount {
                source_id: source.source_id,
                filter_column: "payload".into(),
                greater_than: 10,
                group_key_column: "group_id".into(),
                filter_node_id: node(4),
                project_node_id: node(5),
                key_node_id: node(6),
                aggregate_node_id: node(7),
                result_node_id: node(8),
            },
            GraphOutputSpecV1::FilteredGroupedSumInt8 {
                source_id: source.source_id,
                filter_column: "payload".into(),
                greater_than: 10,
                group_key_column: "group_id".into(),
                input_column: "payload".into(),
                filter_node_id: node(9),
                project_node_id: node(10),
                key_node_id: node(11),
                aggregate_node_id: node(12),
                result_node_id: node(13),
            },
        ],
    };
    let graph = compile_graph(
        &spec,
        std::slice::from_ref(&source),
        std::slice::from_ref(&index),
    )
    .unwrap();
    assert!(matches!(
        graph.nodes[0].kind,
        OperatorNodeKind::Compute { .. }
    ));
    assert!(matches!(
        graph.nodes[3].kind,
        OperatorNodeKind::Filter { .. }
    ));
    assert!(matches!(
        graph.nodes[4].kind,
        OperatorNodeKind::Project { .. }
    ));
    assert!(matches!(
        graph.nodes[5].kind,
        OperatorNodeKind::KeyBy { .. }
    ));
    assert_eq!(
        GraphSpecV1::from_json(&spec.to_canonical_json().unwrap()).unwrap(),
        spec
    );
}

#[test]
fn computed_project_checks_arithmetic_and_preserves_null() {
    let source = source(1, 60_000, &[("id", false), ("payload", true)]);
    let index = identity(&source, 61_000);
    let spec = GraphSpecV1 {
        version: 1,
        graph_id: GraphId::new(13).unwrap(),
        sources: vec![source.source_id],
        outputs: vec![GraphOutputSpecV1::ComputedProject {
            source_id: source.source_id,
            key_column: "id".into(),
            input_column: "payload".into(),
            literal: 1,
            compute_node_id: node(1),
            project_node_id: node(2),
            result_node_id: node(3),
        }],
    };
    let graph = compile_graph(
        &spec,
        std::slice::from_ref(&source),
        std::slice::from_ref(&index),
    )
    .unwrap();
    let layout = source_typed_layout(source.source_id, &graph.sources[0].layout).unwrap();
    let row = |id, value| TypedRow::new(&layout, vec![TypedValue::Int8(id), value]).unwrap();
    let batch = |value| DeltaBatch {
        origin: EffectOrigin::Bootstrap(
            BootstrapBatchId::new(BootstrapId::new(1).unwrap(), 1).unwrap(),
        ),
        layout_identity: layout.identity,
        rows: vec![RowDelta {
            before: None,
            after: Some(row(1, value)),
        }],
    };
    assert!(matches!(
        apply_graph(&graph, &batch(TypedValue::Int8(4))).unwrap().results[0],
        ResultDelta::Keyed { ref mutations, .. }
            if matches!(mutations[0], shiba_operator::ResultMutation::Upsert { value: TypedValue::Int8(5), .. })
    ));
    assert!(matches!(
        apply_graph(&graph, &batch(TypedValue::Null(ValueType::Int8))).unwrap().results[0],
        ResultDelta::Keyed { ref mutations, .. }
            if matches!(mutations[0], shiba_operator::ResultMutation::Upsert { value: TypedValue::Null(ValueType::Int8), .. })
    ));
    assert!(apply_graph(&graph, &batch(TypedValue::Int8(i64::MAX))).is_err());
}

fn filtered_spec(source_id: SourceId) -> GraphSpecV1 {
    GraphSpecV1 {
        version: 1,
        graph_id: GraphId::new(14).unwrap(),
        sources: vec![source_id],
        outputs: vec![
            GraphOutputSpecV1::FilteredGroupedCount {
                source_id,
                filter_column: "payload".into(),
                greater_than: 10,
                group_key_column: "group_id".into(),
                filter_node_id: node(1),
                project_node_id: node(2),
                key_node_id: node(3),
                aggregate_node_id: node(4),
                result_node_id: node(5),
            },
            GraphOutputSpecV1::FilteredGroupedSumInt8 {
                source_id,
                filter_column: "payload".into(),
                greater_than: 10,
                group_key_column: "group_id".into(),
                input_column: "payload".into(),
                filter_node_id: node(6),
                project_node_id: node(7),
                key_node_id: node(8),
                aggregate_node_id: node(9),
                result_node_id: node(10),
            },
        ],
    }
}

#[test]
fn filtered_grouped_pipelines_match_reference_rows() {
    let source = source(
        1,
        70_000,
        &[("id", false), ("group_id", true), ("payload", true)],
    );
    let index = identity(&source, 71_000);
    let spec = filtered_spec(source.source_id);
    let graph = compile_graph(
        &spec,
        std::slice::from_ref(&source),
        std::slice::from_ref(&index),
    )
    .unwrap();
    let layout = source_typed_layout(source.source_id, &graph.sources[0].layout).unwrap();
    let batch_id = BootstrapBatchId::new(BootstrapId::new(1).unwrap(), 1).unwrap();
    let rows = [
        (1, 1, Some(5)),
        (2, 1, Some(20)),
        (3, 2, None),
        (4, 2, Some(30)),
    ]
    .into_iter()
    .map(|(id, group, payload)| RowDelta {
        before: None,
        after: Some(
            TypedRow::new(
                &layout,
                vec![
                    TypedValue::Int8(id),
                    TypedValue::Int8(group),
                    payload.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8),
                ],
            )
            .unwrap(),
        ),
    })
    .collect();
    let input = MultiInputBatch {
        origin: GraphEffectOrigin::Bootstrap(batch_id),
        sources: vec![SourceDeltaBatch {
            source_id: source.source_id,
            delta: DeltaBatch {
                origin: EffectOrigin::Bootstrap(batch_id),
                layout_identity: layout.identity,
                rows,
            },
        }],
    };
    let reads = graph_state_read_set(&graph, &input).unwrap();
    let snapshot = StateSnapshot::new(
        &reads,
        reads
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
    assert_eq!(transition.results.len(), 2);
    assert_eq!(
        transition.results[0],
        ResultDelta::Keyed {
            node_id: node(5),
            mutations: vec![
                ResultMutation::Upsert {
                    key: TypedValue::Int8(1),
                    value: TypedValue::Int8(1),
                },
                ResultMutation::Upsert {
                    key: TypedValue::Int8(2),
                    value: TypedValue::Int8(1),
                },
            ],
        }
    );
    assert_eq!(
        transition.results[1],
        ResultDelta::Keyed {
            node_id: node(10),
            mutations: vec![
                ResultMutation::Upsert {
                    key: TypedValue::Int8(1),
                    value: TypedValue::Int8(20),
                },
                ResultMutation::Upsert {
                    key: TypedValue::Int8(2),
                    value: TypedValue::Int8(30),
                },
            ],
        }
    );
}
