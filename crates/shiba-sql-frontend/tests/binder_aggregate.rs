use shiba_compiler::{
    IdentityIndexDescriptor, POSTGRES_INT8_TYPE_OID, POSTGRES_TEXT_TYPE_OID, QueryOperationV1,
    QuerySelectorV1, SourceColumnDescriptor, SourceDescriptor, compile_query,
};
use shiba_operator::{AggregateFunctionV1, ObjectAddress};
use shiba_protocol::{GraphId, SourceId};
use shiba_sql_frontend::{ErrorClass, ErrorCode, ResolvedSource, bind_query, parse_sql};

const PG_CLASS: u32 = 1_259;

fn address(object_id: u32, sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        class_id: PG_CLASS,
        object_id,
        sub_id,
    }
}

fn source() -> ResolvedSource {
    let relation = address(20_000, 0);
    let key = address(20_000, 1);
    ResolvedSource {
        descriptor: SourceDescriptor {
            source_id: SourceId::new(8).unwrap(),
            relation,
            columns: vec![
                SourceColumnDescriptor {
                    name: "id".into(),
                    address: key,
                    type_oid: POSTGRES_INT8_TYPE_OID,
                    nullable: false,
                },
                SourceColumnDescriptor {
                    name: "payload".into(),
                    address: address(20_000, 2),
                    type_oid: POSTGRES_INT8_TYPE_OID,
                    nullable: true,
                },
            ],
        },
        identity: IdentityIndexDescriptor {
            address: address(20_001, 0),
            relation,
            key_column: key,
            key_arity: 1,
            unique: true,
            valid: true,
            ready: true,
            has_expression: false,
            has_predicate: false,
            effective_replica_identity: true,
        },
    }
}

fn bind(sql: &str, source: &ResolvedSource) -> shiba_compiler::QuerySpecV1 {
    bind_query(
        GraphId::new(10).unwrap(),
        &parse_sql(sql).unwrap(),
        core::slice::from_ref(source),
    )
    .unwrap()
}

fn assert_compiles(spec: &shiba_compiler::QuerySpecV1, source: &ResolvedSource) {
    compile_query(
        spec,
        core::slice::from_ref(&source.descriptor),
        core::slice::from_ref(&source.identity),
    )
    .unwrap();
}

#[test]
fn scalar_count_and_sum_use_generic_stateful_nodes() {
    let source = source();
    let count = bind("SELECT count(*) FROM app.events", &source);
    assert_eq!(count.nodes.len(), 1);
    assert!(
        matches!(&count.nodes[0].operation, QueryOperationV1::Aggregate { group_expressions, calls }
        if group_expressions.is_empty() && calls.len() == 1
            && calls[0].function == AggregateFunctionV1::CountStar)
    );
    assert_eq!(count.nodes[0].state_codec_version, Some(1));
    assert!(count.results[0].key_ordinals.is_empty());
    assert_eq!(count.results[0].fields.len(), 1);
    assert_eq!(count.results[0].fields[0].name, "count");
    assert_eq!(count.results[0].fields[0].value_slot, 0);
    assert!(!count.results[0].fields[0].nullable);
    assert_compiles(&count, &source);

    let sum = bind("SELECT sum(payload) FROM app.events", &source);
    let QueryOperationV1::Aggregate {
        group_expressions,
        calls,
    } = &sum.nodes[0].operation
    else {
        panic!("SUM must use the generic scalar sum node")
    };
    assert!(group_expressions.is_empty());
    assert_eq!(calls[0].function, AggregateFunctionV1::SumInt8);
    let value = calls[0].expression.as_ref().unwrap();
    assert!(matches!(
        value,
        shiba_compiler::QueryExpressionV1::Column { field }
            if matches!(&field.selector, QuerySelectorV1::Name { name, quoted: false } if name == "payload")
    ));
    assert!(sum.results[0].key_ordinals.is_empty());
    assert_eq!(sum.results[0].fields.len(), 1);
    assert_eq!(sum.results[0].fields[0].name, "sum");
    assert!(sum.results[0].fields[0].nullable);
    assert_compiles(&sum, &source);
}

#[test]
fn scalar_multi_call_aggregate_uses_one_node_and_count_expression() {
    let source = source();
    let spec = bind(
        "SELECT count(*) AS rows, count(payload) AS non_null, sum(payload) AS total \
         FROM app.events",
        &source,
    );
    let QueryOperationV1::Aggregate {
        group_expressions,
        calls,
    } = &spec.nodes[0].operation
    else {
        panic!("multi-call scalar query must use one Aggregate node")
    };
    assert!(group_expressions.is_empty());
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls.iter().map(|call| call.ordinal).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(calls[0].function, AggregateFunctionV1::CountStar);
    assert_eq!(calls[1].function, AggregateFunctionV1::Count);
    assert!(calls[1].expression.is_some());
    assert_eq!(calls[2].function, AggregateFunctionV1::SumInt8);
    assert_eq!(
        spec.results[0]
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.value_slot, field.nullable))
            .collect::<Vec<_>>(),
        vec![
            ("rows", 0, false),
            ("non_null", 1, false),
            ("total", 2, true),
        ]
    );
    assert_compiles(&spec, &source);
}

#[test]
fn scalar_min_max_use_nullable_int8_function_abi() {
    let source = source();
    let spec = bind(
        "SELECT min(payload) AS minimum, max(payload) AS maximum FROM app.events",
        &source,
    );
    let QueryOperationV1::Aggregate { calls, .. } = &spec.nodes[0].operation else {
        panic!("MIN/MAX must use the generic Aggregate node")
    };
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].function, AggregateFunctionV1::MinInt8);
    assert_eq!(calls[1].function, AggregateFunctionV1::MaxInt8);
    assert!(spec.results[0].fields.iter().all(|field| field.nullable));
    assert_compiles(&spec, &source);
}

#[test]
fn grouped_multi_call_aggregate_preserves_group_and_call_ordinals() {
    let source = source();
    let spec = bind(
        "SELECT id AS region, count(*) AS rows, count(payload) AS non_null, \
         sum(payload) AS total FROM app.events GROUP BY id",
        &source,
    );
    let QueryOperationV1::Aggregate { calls, .. } = &spec.nodes[2].operation else {
        panic!("grouped multi-call query must end in one Aggregate node")
    };
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls.iter().map(|call| call.ordinal).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(calls[0].function, AggregateFunctionV1::CountStar);
    assert_eq!(calls[1].function, AggregateFunctionV1::Count);
    assert_eq!(calls[2].function, AggregateFunctionV1::SumInt8);
    assert_eq!(spec.results[0].key_ordinals, vec![1]);
    assert_eq!(
        spec.results[0]
            .fields
            .iter()
            .map(|field| field.value_slot)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_compiles(&spec, &source);
}

#[test]
fn duplicate_default_output_identity_is_rejected() {
    let source = source();
    assert_error(
        "SELECT count(*), count(payload) FROM app.events",
        &source,
        ErrorCode::DuplicateAlias,
    );
}

#[test]
fn aggregate_call_bound_is_fail_closed() {
    let source = source();
    let projection = (0..17)
        .map(|index| format!("count(*) AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    assert_error(
        &format!("SELECT {projection} FROM app.events"),
        &source,
        ErrorCode::QueryTooComplex,
    );
}

#[test]
fn grouped_count_lowers_filter_keyby_aggregate_and_keyed_result() {
    let source = source();
    let spec = bind(
        "SELECT id, count(*) FROM app.events WHERE payload > 100 GROUP BY id",
        &source,
    );
    assert_eq!(spec.nodes.len(), 4);
    assert!(matches!(
        spec.nodes[0].operation,
        QueryOperationV1::Project { .. }
    ));
    assert!(matches!(
        spec.nodes[1].operation,
        QueryOperationV1::Filter { .. }
    ));
    assert!(matches!(
        spec.nodes[2].operation,
        QueryOperationV1::KeyBy { .. }
    ));
    assert!(matches!(
        spec.nodes[3].operation,
        QueryOperationV1::Aggregate { .. }
    ));
    assert_eq!(spec.nodes[3].state_codec_version, Some(1));
    assert_eq!(spec.results[0].key_ordinals, vec![1]);
    assert_eq!(spec.results[0].fields.len(), 2);
    assert!(!spec.results[0].fields[0].nullable);
    assert!(!spec.results[0].fields[1].nullable);
    assert_compiles(&spec, &source);
}

#[test]
fn grouped_sum_preserves_nullable_key_and_all_null_value_contract() {
    let source = source();
    let spec = bind(
        "SELECT payload, sum(payload) FROM app.events GROUP BY payload",
        &source,
    );
    assert!(matches!(
        spec.nodes.last().unwrap().operation,
        QueryOperationV1::Aggregate { .. }
    ));
    assert_eq!(spec.results[0].key_ordinals, vec![1]);
    assert!(spec.results[0].fields[0].nullable);
    assert!(spec.results[0].fields[1].nullable);
    assert_compiles(&spec, &source);
}

#[test]
fn grouped_sum_aliases_and_parentheses_have_one_canonical_spec() {
    let source = source();
    let plain = bind(
        "SELECT id, sum(payload) FROM app.events WHERE payload > 100 GROUP BY id",
        &source,
    );
    let formatted = bind(
        "SELECT (e.id) AS k, SUM((e.payload)) AS v FROM app.events AS e \
         WHERE ((e.payload) > (100)) GROUP BY (e.id)",
        &source,
    );
    assert_eq!(plain.sources, formatted.sources);
    assert_eq!(plain.nodes, formatted.nodes);
    assert_ne!(plain.results[0].fields, formatted.results[0].fields);
    assert_ne!(
        plain.to_canonical_json().unwrap(),
        formatted.to_canonical_json().unwrap()
    );
}

#[test]
fn aggregate_type_identity_and_current_topology_fail_closed() {
    let source = source();
    assert_error(
        "SELECT count(*) FROM app.events WHERE payload > 0",
        &source,
        ErrorCode::UnsupportedSyntax,
    );

    let mut text = source.clone();
    text.descriptor.columns[1].type_oid = POSTGRES_TEXT_TYPE_OID;
    assert_error(
        "SELECT sum(payload) FROM app.events",
        &text,
        ErrorCode::TypeMismatch,
    );

    let mut stale = source.clone();
    stale.identity.valid = false;
    assert_error(
        "SELECT count(*) FROM app.events",
        &stale,
        ErrorCode::IdentityMismatch,
    );
}

#[test]
fn grouped_shape_does_not_expand_beyond_two_bound_source_columns() {
    let mut wide = source();
    wide.descriptor.columns.push(SourceColumnDescriptor {
        name: "guard".into(),
        address: address(20_000, 3),
        type_oid: POSTGRES_INT8_TYPE_OID,
        nullable: false,
    });
    assert_error(
        "SELECT id, sum(payload) FROM app.events WHERE guard > 0 GROUP BY id",
        &wide,
        ErrorCode::UnsupportedSyntax,
    );
}

fn assert_error(sql: &str, source: &ResolvedSource, code: ErrorCode) {
    let query = parse_sql(sql).unwrap();
    let error = bind_query(
        GraphId::new(10).unwrap(),
        &query,
        core::slice::from_ref(source),
    )
    .unwrap_err();
    assert_eq!(error.class, ErrorClass::Binding);
    assert_eq!(error.code, code);
    assert!(error.span.end > error.span.start);
}
