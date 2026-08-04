use shiba_compiler::{
    CompilerError, IdentityIndexDescriptor, QUERY_SPEC_VERSION, QueryAggregateCallV1,
    QueryExpressionV1, QueryFieldV1, QueryInputV1, QueryNodeV1, QueryOperationV1,
    QueryResultFieldV1, QueryResultV1, QuerySelectorV1, QuerySpecV1, SourceColumnDescriptor,
    SourceDescriptor, compile_query, compile_query_with_optional_identities,
};
use shiba_operator::{AggregateFunctionV1, ObjectAddress, OperatorNodeKind};
use shiba_protocol::{GraphId, SourceId};

fn address(object_id: u32, sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        class_id: 1_259,
        object_id,
        sub_id,
    }
}

fn source(id: u64, object_id: u32) -> SourceDescriptor {
    SourceDescriptor {
        source_id: SourceId::new(id).unwrap(),
        relation: address(object_id, 0),
        columns: [("id", false), ("payload", true)]
            .into_iter()
            .enumerate()
            .map(|(index, (name, nullable))| SourceColumnDescriptor {
                name: name.into(),
                address: address(object_id, i32::try_from(index + 1).unwrap()),
                type_oid: 20,
                nullable,
            })
            .collect(),
    }
}

fn identity(source: &SourceDescriptor, object_id: u32) -> IdentityIndexDescriptor {
    IdentityIndexDescriptor {
        address: address(object_id, 0),
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

fn name(value: &str) -> QueryExpressionV1 {
    QueryExpressionV1::Column {
        field: QueryFieldV1 {
            input: 0,
            selector: QuerySelectorV1::Name {
                name: value.into(),
                quoted: false,
            },
        },
    }
}

fn slot(value: u16) -> QueryExpressionV1 {
    QueryExpressionV1::Column {
        field: QueryFieldV1 {
            input: 0,
            selector: QuerySelectorV1::Slot { slot: value },
        },
    }
}

fn source_input(source_id: SourceId) -> Vec<QueryInputV1> {
    vec![QueryInputV1::Source { source_id }]
}

fn node_input(node: u16) -> Vec<QueryInputV1> {
    vec![QueryInputV1::Node { node }]
}

fn stateful(inputs: Vec<QueryInputV1>, operation: QueryOperationV1) -> QueryNodeV1 {
    QueryNodeV1 {
        inputs,
        state_codec_version: Some(1),
        operation,
    }
}

fn stateless(inputs: Vec<QueryInputV1>, operation: QueryOperationV1) -> QueryNodeV1 {
    QueryNodeV1 {
        inputs,
        state_codec_version: None,
        operation,
    }
}

fn aggregate(
    group_expressions: Vec<QueryExpressionV1>,
    function: AggregateFunctionV1,
    expression: Option<QueryExpressionV1>,
) -> QueryOperationV1 {
    QueryOperationV1::Aggregate {
        group_expressions,
        calls: vec![QueryAggregateCallV1 {
            ordinal: 1,
            function,
            function_version: 1,
            expression,
        }],
        having: None,
    }
}

fn count() -> QueryOperationV1 {
    aggregate(vec![], AggregateFunctionV1::CountStar, None)
}

fn sum(value: QueryExpressionV1) -> QueryOperationV1 {
    aggregate(vec![], AggregateFunctionV1::SumInt8, Some(value))
}

fn scalar(input_node: u16, nullable: bool) -> QueryResultV1 {
    QueryResultV1 {
        input_node,
        fields: vec![QueryResultFieldV1 {
            name: "value".into(),
            value_slot: 0,
            nullable,
        }],
        key_ordinals: vec![],
    }
}

fn keyed(input_node: u16) -> QueryResultV1 {
    QueryResultV1 {
        input_node,
        fields: vec![
            QueryResultFieldV1 {
                name: "key".into(),
                value_slot: 0,
                nullable: false,
            },
            QueryResultFieldV1 {
                name: "value".into(),
                value_slot: 1,
                nullable: true,
            },
        ],
        key_ordinals: vec![1],
    }
}

fn keyed_non_null(input_node: u16) -> QueryResultV1 {
    let mut result = keyed(input_node);
    result.fields[1].nullable = false;
    result
}

#[test]
fn generic_query_preserves_all_single_source_graph_shapes() {
    let source = source(1, 10_000);
    let index = identity(&source, 11_000);
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(9).unwrap(),
        sources: vec![source.source_id],
        nodes: vec![
            stateful(source_input(source.source_id), count()),
            stateful(source_input(source.source_id), sum(name("payload"))),
            stateless(
                source_input(source.source_id),
                QueryOperationV1::Project {
                    expressions: vec![name("id"), name("payload")],
                },
            ),
            stateless(
                source_input(source.source_id),
                QueryOperationV1::KeyBy { key: name("id") },
            ),
            stateful(
                node_input(4),
                aggregate(vec![slot(2)], AggregateFunctionV1::CountStar, None),
            ),
            stateless(
                source_input(source.source_id),
                QueryOperationV1::KeyBy { key: name("id") },
            ),
            stateful(
                node_input(6),
                aggregate(vec![slot(2)], AggregateFunctionV1::SumInt8, Some(slot(1))),
            ),
        ],
        results: vec![
            scalar(1, false),
            scalar(2, true),
            keyed(3),
            keyed_non_null(5),
            keyed(7),
        ],
    };
    let graph = compile_query(&spec, &[source], &[index]).unwrap();
    assert_eq!(graph.nodes.len(), 12);
    assert_eq!(graph.nodes[0].node_id.get(), 1);
    assert_eq!(graph.nodes[7].node_id.get(), 8);
    assert_eq!(graph.result_contracts().count(), 5);
    assert_eq!(
        graph,
        compile_query(
            &spec,
            &[graph_source(1, 10_000)],
            &[identity(&graph_source(1, 10_000), 11_000)]
        )
        .unwrap()
    );
}

#[test]
fn source_column_type_error_preserves_exact_catalog_coordinate() {
    let mut source = source(1, 10_000);
    source.columns.push(SourceColumnDescriptor {
        name: "label".into(),
        address: address(10_000, 3),
        type_oid: 25,
        nullable: true,
    });
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(10).unwrap(),
        sources: vec![source.source_id],
        nodes: vec![stateful(source_input(source.source_id), sum(name("label")))],
        results: vec![scalar(1, false)],
    };
    assert_eq!(
        compile_query(&spec, &[source.clone()], &[identity(&source, 11_000)]),
        Err(CompilerError::WrongColumnType {
            column: "label".into(),
            type_oid: 25,
        })
    );
}

fn graph_source(id: u64, object_id: u32) -> SourceDescriptor {
    source(id, object_id)
}

#[test]
fn filter_compute_and_project_are_generic_nodes() {
    let source = source(1, 10_000);
    let index = identity(&source, 11_000);
    let predicate = QueryExpressionV1::Greater {
        left: Box::new(name("payload")),
        right: Box::new(QueryExpressionV1::Int8Literal { value: 10 }),
    };
    let add = QueryExpressionV1::CheckedAdd {
        left: Box::new(slot(1)),
        right: Box::new(QueryExpressionV1::Int8Literal { value: 1 }),
    };
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(2).unwrap(),
        sources: vec![source.source_id],
        nodes: vec![
            stateless(
                source_input(source.source_id),
                QueryOperationV1::Filter { predicate },
            ),
            stateless(
                node_input(1),
                QueryOperationV1::Compute {
                    expressions: vec![add],
                },
            ),
            stateless(
                node_input(2),
                QueryOperationV1::Project {
                    expressions: vec![slot(0), slot(2)],
                },
            ),
        ],
        results: vec![keyed(3)],
    };
    let graph = compile_query(&spec, &[source], &[index]).unwrap();
    assert!(matches!(
        graph.nodes[0].kind,
        OperatorNodeKind::Filter { .. }
    ));
    assert!(matches!(
        graph.nodes[1].kind,
        OperatorNodeKind::Compute { .. }
    ));
    assert!(matches!(
        graph.nodes[2].kind,
        OperatorNodeKind::Project { .. }
    ));
}

#[test]
fn join_binds_exact_effective_right_identity_without_recipe_oid() {
    let left = source(1, 10_000);
    let right = source(2, 20_000);
    let left_index = identity(&left, 11_000);
    let right_index = identity(&right, 21_000);
    let field = |input, name: &str| QueryFieldV1 {
        input,
        selector: QuerySelectorV1::Name {
            name: name.into(),
            quoted: false,
        },
    };
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(3).unwrap(),
        sources: vec![left.source_id, right.source_id],
        nodes: vec![stateful(
            vec![
                QueryInputV1::Source {
                    source_id: left.source_id,
                },
                QueryInputV1::Source {
                    source_id: right.source_id,
                },
            ],
            QueryOperationV1::InnerJoin {
                left_id: field(0, "id"),
                left_key: field(0, "payload"),
                right_id: field(1, "id"),
                right_payload: field(1, "payload"),
            },
        )],
        results: vec![keyed(1)],
    };
    let graph = compile_query(&spec, &[left, right], &[left_index, right_index.clone()]).unwrap();
    assert_eq!(graph.sources[1].identity_index, Some(right_index.address));
    assert!(matches!(
        graph.nodes[0].kind,
        OperatorNodeKind::InnerJoin { .. }
    ));
}

#[test]
fn strict_json_digest_and_input_selector_boundaries_fail_closed() {
    let source = source(1, 10_000);
    let index = identity(&source, 11_000);
    let valid = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(4).unwrap(),
        sources: vec![source.source_id],
        nodes: vec![stateful(source_input(source.source_id), count())],
        results: vec![scalar(1, false)],
    };
    let bytes = valid.to_canonical_json().unwrap();
    assert_eq!(QuerySpecV1::from_json(&bytes).unwrap(), valid);
    assert_eq!(
        valid.canonical_digest().unwrap(),
        valid.canonical_digest().unwrap()
    );
    let mut unknown = bytes.clone();
    unknown.pop();
    unknown.extend_from_slice(b",\"alias\":1}");
    assert!(QuerySpecV1::from_json(&unknown).is_err());

    let mut wrong_function_version = valid.clone();
    let QueryOperationV1::Aggregate { calls, .. } = &mut wrong_function_version.nodes[0].operation
    else {
        panic!("fixture must remain a generic aggregate")
    };
    calls[0].function_version = 2;
    assert!(wrong_function_version.to_canonical_json().is_err());

    let mut too_many_calls = valid.clone();
    let QueryOperationV1::Aggregate { calls, .. } = &mut too_many_calls.nodes[0].operation else {
        unreachable!()
    };
    *calls = (1..=shiba_operator::MAX_AGGREGATE_CALLS + 1)
        .map(|ordinal| QueryAggregateCallV1 {
            ordinal: u16::try_from(ordinal).unwrap(),
            function: AggregateFunctionV1::CountStar,
            function_version: 1,
            expression: None,
        })
        .collect();
    assert!(too_many_calls.to_canonical_json().is_err());

    let mut too_many_groups = valid.clone();
    let QueryOperationV1::Aggregate {
        group_expressions, ..
    } = &mut too_many_groups.nodes[0].operation
    else {
        unreachable!()
    };
    *group_expressions = vec![slot(0); shiba_operator::MAX_GROUP_EXPRESSIONS + 1];
    assert!(too_many_groups.to_canonical_json().is_err());

    let invalid = QuerySpecV1 {
        nodes: vec![stateful(source_input(source.source_id), sum(slot(1)))],
        ..valid.clone()
    };
    assert_eq!(
        compile_query(
            &invalid,
            std::slice::from_ref(&source),
            std::slice::from_ref(&index),
        ),
        Err(CompilerError::InvalidSpec)
    );
    let forward = br#"{"version":1,"graph_id":4,"sources":[1],"nodes":[{"inputs":[{"input":"node","node":1}],"state_codec_version":1,"operation":{"operation":"count_rows"}}],"results":[{"input_node":1,"shape":{"shape":"scalar","value_slot":0}}]}"#;
    assert!(QuerySpecV1::from_json(forward).is_err());
    assert!(compile_query_with_optional_identities(&valid, &[source], &[None]).is_err());
}

#[test]
fn identity_free_zero_column_count_remains_the_only_exception() {
    let source = SourceDescriptor {
        source_id: SourceId::new(1).unwrap(),
        relation: address(10_000, 0),
        columns: vec![],
    };
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(5).unwrap(),
        sources: vec![source.source_id],
        nodes: vec![stateful(source_input(source.source_id), count())],
        results: vec![scalar(1, false)],
    };
    assert!(compile_query_with_optional_identities(&spec, &[source], &[None]).is_ok());
}

fn add_chain(mut expression: QueryExpressionV1, depth: usize) -> QueryExpressionV1 {
    for _ in 0..depth {
        expression = QueryExpressionV1::CheckedAdd {
            left: Box::new(expression),
            right: Box::new(QueryExpressionV1::Int8Literal { value: 1 }),
        };
    }
    expression
}

fn boolean_predicate() -> QueryExpressionV1 {
    let comparison = || QueryExpressionV1::Equal {
        left: Box::new(slot(0)),
        right: Box::new(QueryExpressionV1::Int8Literal { value: 1 }),
    };
    QueryExpressionV1::And {
        left: Box::new(comparison()),
        right: Box::new(comparison()),
    }
}

fn bounded_spec() -> QuerySpecV1 {
    let source_id = SourceId::new(1).unwrap();
    QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(6).unwrap(),
        sources: vec![source_id],
        nodes: vec![stateless(
            source_input(source_id),
            QueryOperationV1::Project {
                expressions: vec![name("id")],
            },
        )],
        results: vec![QueryResultV1 {
            input_node: 1,
            fields: vec![QueryResultFieldV1 {
                name: "id".into(),
                value_slot: 0,
                nullable: false,
            }],
            key_ordinals: vec![1],
        }],
    }
}

#[test]
fn expression_depth_fails_before_encoding() {
    let base = bounded_spec();
    let source_id = base.sources[0];
    let too_deep = QuerySpecV1 {
        nodes: vec![stateless(
            source_input(source_id),
            QueryOperationV1::Project {
                expressions: vec![add_chain(name("id"), 32)],
            },
        )],
        ..base.clone()
    };
    assert!(too_deep.to_canonical_json().is_err());
}

#[test]
fn query_wide_expression_count_fails_before_encoding() {
    let base = bounded_spec();
    let source_id = base.sources[0];
    let mut many_expressions = Vec::new();
    for index in 0..31u16 {
        let inputs = if index == 0 {
            source_input(source_id)
        } else {
            node_input(index)
        };
        many_expressions.push(stateless(
            inputs,
            QueryOperationV1::Compute {
                expressions: vec![add_chain(slot(0), 4), add_chain(slot(0), 4)],
            },
        ));
    }
    let expression_bound = QuerySpecV1 {
        nodes: many_expressions,
        results: vec![QueryResultV1 {
            input_node: 31,
            fields: vec![
                QueryResultFieldV1 {
                    name: "key".into(),
                    value_slot: 0,
                    nullable: false,
                },
                QueryResultFieldV1 {
                    name: "value".into(),
                    value_slot: 1,
                    nullable: false,
                },
            ],
            key_ordinals: vec![1],
        }],
        ..base.clone()
    };
    assert!(expression_bound.to_canonical_json().is_err());
}

#[test]
fn query_wide_boolean_count_fails_before_encoding() {
    let base = bounded_spec();
    let source_id = base.sources[0];
    let mut boolean_nodes = Vec::new();
    for index in 0..22u16 {
        boolean_nodes.push(stateless(
            if index == 0 {
                source_input(source_id)
            } else {
                node_input(index)
            },
            QueryOperationV1::Filter {
                predicate: boolean_predicate(),
            },
        ));
    }
    let boolean_bound = QuerySpecV1 {
        nodes: boolean_nodes,
        results: vec![keyed(22)],
        ..base.clone()
    };
    assert!(boolean_bound.to_canonical_json().is_err());
}

#[test]
fn logical_name_bounds_and_normalization_fail_before_encoding() {
    let base = bounded_spec();
    let source_id = base.sources[0];
    let invalid_name = QuerySpecV1 {
        nodes: vec![stateless(
            source_input(source_id),
            QueryOperationV1::Project {
                expressions: vec![name("")],
            },
        )],
        ..base
    };
    assert!(invalid_name.to_canonical_json().is_err());
    let unnormalized = QuerySpecV1 {
        nodes: vec![stateless(
            source_input(source_id),
            QueryOperationV1::Project {
                expressions: vec![QueryExpressionV1::Column {
                    field: QueryFieldV1 {
                        input: 0,
                        selector: QuerySelectorV1::Name {
                            name: "Payload".into(),
                            quoted: false,
                        },
                    },
                }],
            },
        )],
        ..invalid_name
    };
    assert!(unnormalized.to_canonical_json().is_err());
}
