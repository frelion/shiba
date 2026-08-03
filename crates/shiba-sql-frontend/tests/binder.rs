use shiba_compiler::{
    IdentityIndexDescriptor, POSTGRES_INT8_TYPE_OID, POSTGRES_TEXT_TYPE_OID, QueryExpressionV1,
    QueryInputV1, QueryOperationV1, QueryResultShapeV1, QuerySelectorV1, SourceColumnDescriptor,
    SourceDescriptor, compile_query,
};
use shiba_operator::ObjectAddress;
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

fn source(names: (&str, &str), payload_nullable: bool) -> ResolvedSource {
    let relation = address(10_000, 0);
    let key = address(10_000, 1);
    ResolvedSource {
        descriptor: SourceDescriptor {
            source_id: SourceId::new(7).unwrap(),
            relation,
            columns: vec![
                SourceColumnDescriptor {
                    name: names.0.into(),
                    address: key,
                    type_oid: POSTGRES_INT8_TYPE_OID,
                    nullable: false,
                },
                SourceColumnDescriptor {
                    name: names.1.into(),
                    address: address(10_000, 2),
                    type_oid: POSTGRES_INT8_TYPE_OID,
                    nullable: payload_nullable,
                },
            ],
        },
        identity: IdentityIndexDescriptor {
            address: address(10_001, 0),
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
        GraphId::new(9).unwrap(),
        &parse_sql(sql).unwrap(),
        core::slice::from_ref(source),
    )
    .unwrap()
}

#[test]
fn sample_lowers_names_then_filter_then_slot_project() {
    let source = source(("id", "payload"), true);
    let spec = bind(
        "SELECT id, payload + 1 FROM app.events WHERE payload > 0",
        &source,
    );
    assert_eq!(spec.sources, vec![source.descriptor.source_id]);
    assert_eq!(spec.nodes.len(), 3);
    let QueryOperationV1::Project { expressions } = &spec.nodes[0].operation else {
        panic!("source normalization must be a project")
    };
    assert!(expressions.iter().all(|expression| matches!(
        expression,
        QueryExpressionV1::Column { field }
            if matches!(field.selector, QuerySelectorV1::Name { .. })
    )));
    assert!(matches!(
        spec.nodes[0].inputs.as_slice(),
        [QueryInputV1::Source { .. }]
    ));
    assert!(matches!(
        spec.nodes[1].operation,
        QueryOperationV1::Filter { .. }
    ));
    let QueryOperationV1::Project { expressions } = &spec.nodes[2].operation else {
        panic!("terminal input must be projected")
    };
    assert!(matches!(
        &expressions[0],
        QueryExpressionV1::Column { field }
            if field.input == 0 && matches!(field.selector, QuerySelectorV1::Slot { slot: 0 })
    ));
    assert!(matches!(
        &expressions[1],
        QueryExpressionV1::CheckedAdd { .. }
    ));
    assert_eq!(
        spec.results[0].shape,
        QueryResultShapeV1::Keyed {
            key_slot: 0,
            key_nullable: false,
            value_slot: 1,
            value_nullable: true,
        }
    );
    compile_query(
        &spec,
        core::slice::from_ref(&source.descriptor),
        core::slice::from_ref(&source.identity),
    )
    .unwrap();
}

#[test]
fn aliases_and_quoted_names_do_not_become_execution_identity() {
    let source = source(("Id", "Payload"), true);
    let plain = bind(
        "SELECT e.\"Id\", e.\"Payload\" + 1 FROM app.\"Events\" AS e WHERE e.\"Payload\" > 0",
        &source,
    );
    let renamed = bind(
        "SELECT row.\"Id\" AS visible_key, row.\"Payload\" + 1 AS visible_value \
         FROM app.\"Events\" AS row WHERE row.\"Payload\" > 0",
        &source,
    );
    assert_eq!(plain, renamed);
    let QueryOperationV1::Project { expressions } = &plain.nodes[0].operation else {
        panic!()
    };
    assert!(matches!(
        &expressions[0],
        QueryExpressionV1::Column { field }
            if matches!(&field.selector, QuerySelectorV1::Name { name, quoted: true } if name == "Id")
    ));
}

#[test]
fn nullability_is_inferred_without_conflating_null_and_absent() {
    let nullable = source(("id", "payload"), true);
    let nullable_spec = bind("SELECT id, payload + NULL FROM app.events", &nullable);
    assert!(matches!(
        nullable_spec.results[0].shape,
        QueryResultShapeV1::Keyed {
            value_nullable: true,
            ..
        }
    ));
    let required = source(("id", "payload"), false);
    let required_spec = bind("SELECT id, payload - 1 FROM app.events", &required);
    assert!(matches!(
        required_spec.results[0].shape,
        QueryResultShapeV1::Keyed {
            value_nullable: false,
            ..
        }
    ));
}

#[test]
fn missing_wrong_type_and_identity_drift_fail_at_origin() {
    let source = source(("id", "payload"), true);
    assert_error(
        "SELECT id, missing + 1 FROM app.events",
        &source,
        ErrorCode::UnknownColumn,
    );

    let mut text = source.clone();
    text.descriptor.columns[1].type_oid = POSTGRES_TEXT_TYPE_OID;
    assert_error(
        "SELECT id, payload + 1 FROM app.events",
        &text,
        ErrorCode::TypeMismatch,
    );

    let mut stale = source.clone();
    stale.identity.key_column = stale.descriptor.columns[1].address;
    assert_error(
        "SELECT id, payload + 1 FROM app.events",
        &stale,
        ErrorCode::IdentityMismatch,
    );
}

#[test]
fn unsupported_shapes_and_more_than_two_source_columns_fail_closed() {
    let source = source(("id", "payload"), true);
    assert_error(
        "SELECT count(*) FROM app.events",
        &source,
        ErrorCode::UnsupportedSyntax,
    );
    let mut wide = source.clone();
    wide.descriptor.columns.push(SourceColumnDescriptor {
        name: "guard".into(),
        address: address(10_000, 3),
        type_oid: POSTGRES_INT8_TYPE_OID,
        nullable: false,
    });
    assert_error(
        "SELECT id, payload + 1 FROM app.events WHERE guard > 0",
        &wide,
        ErrorCode::UnsupportedSyntax,
    );
}

#[test]
fn boolean_equality_is_not_an_implicit_m15_integer_comparison() {
    let source = source(("id", "payload"), true);
    assert_error(
        "SELECT id, payload FROM app.events WHERE (payload > 0) = (id > 0)",
        &source,
        ErrorCode::TypeMismatch,
    );
}

fn assert_error(sql: &str, source: &ResolvedSource, code: ErrorCode) {
    let query = parse_sql(sql).unwrap();
    let error = bind_query(
        GraphId::new(9).unwrap(),
        &query,
        core::slice::from_ref(source),
    )
    .unwrap_err();
    assert_eq!(error.class, ErrorClass::Binding);
    assert_eq!(error.code, code);
    assert!(error.span.end > error.span.start);
}
