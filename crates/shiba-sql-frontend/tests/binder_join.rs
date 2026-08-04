use shiba_compiler::{
    IdentityIndexDescriptor, POSTGRES_INT8_TYPE_OID, POSTGRES_TEXT_TYPE_OID, QueryInputV1,
    QueryOperationV1, QuerySelectorV1, SourceColumnDescriptor, SourceDescriptor, compile_query,
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

fn source(id: u64, relation_id: u32, names: (&str, &str), nullable: bool) -> ResolvedSource {
    let relation = address(relation_id, 0);
    let key = address(relation_id, 1);
    ResolvedSource {
        descriptor: SourceDescriptor {
            source_id: SourceId::new(id).unwrap(),
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
                    address: address(relation_id, 2),
                    type_oid: POSTGRES_INT8_TYPE_OID,
                    nullable,
                },
            ],
        },
        identity: IdentityIndexDescriptor {
            address: address(relation_id + 1, 0),
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

fn sources() -> [ResolvedSource; 2] {
    [
        source(20, 30_000, ("id", "foreign_id"), true),
        source(10, 40_000, ("id", "payload"), true),
    ]
}

fn bind(sql: &str, sources: &[ResolvedSource; 2]) -> shiba_compiler::QuerySpecV1 {
    bind_query(GraphId::new(11).unwrap(), &parse_sql(sql).unwrap(), sources).unwrap()
}

fn assert_compiles(spec: &shiba_compiler::QuerySpecV1, sources: &[ResolvedSource; 2]) {
    let mut ordered = sources.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|source| source.descriptor.source_id);
    compile_query(
        spec,
        &ordered
            .iter()
            .map(|source| source.descriptor.clone())
            .collect::<Vec<_>>(),
        &ordered
            .iter()
            .map(|source| source.identity.clone())
            .collect::<Vec<_>>(),
    )
    .unwrap();
}

#[test]
fn cross_schema_join_binds_exact_fields_and_right_identity() {
    let sources = sources();
    let spec = bind(
        "SELECT l.id, r.payload FROM left_schema.events AS l \
         INNER JOIN right_schema.lookup AS r ON l.foreign_id = r.id",
        &sources,
    );
    assert_eq!(
        spec.sources,
        vec![
            sources[1].descriptor.source_id,
            sources[0].descriptor.source_id
        ]
    );
    assert_eq!(spec.nodes.len(), 1);
    assert!(matches!(
        spec.nodes[0].inputs.as_slice(),
        [QueryInputV1::Source { source_id: left }, QueryInputV1::Source { source_id: right }]
            if *left == sources[0].descriptor.source_id && *right == sources[1].descriptor.source_id
    ));
    let QueryOperationV1::InnerJoin {
        left_id,
        left_key,
        right_id,
        right_payload,
    } = &spec.nodes[0].operation
    else {
        panic!("JOIN SQL must lower to the generic InnerJoin node")
    };
    for (field, input, name) in [
        (left_id, 0, "id"),
        (left_key, 0, "foreign_id"),
        (right_id, 1, "id"),
        (right_payload, 1, "payload"),
    ] {
        assert_eq!(field.input, input);
        assert!(
            matches!(&field.selector, QuerySelectorV1::Name { name: actual, quoted: false } if actual == name)
        );
    }
    assert_eq!(spec.results[0].key_ordinals, vec![1]);
    assert_eq!(spec.results[0].fields.len(), 2);
    assert_eq!(spec.results[0].fields[0].name, "id");
    assert_eq!(spec.results[0].fields[0].value_slot, 0);
    assert!(!spec.results[0].fields[0].nullable);
    assert_eq!(spec.results[0].fields[1].name, "payload");
    assert_eq!(spec.results[0].fields[1].value_slot, 1);
    assert!(spec.results[0].fields[1].nullable);
    assert_compiles(&spec, &sources);
}

#[test]
fn aliases_quoted_identifiers_and_reversed_equality_are_canonical() {
    let quoted = [
        source(20, 30_000, ("Id", "ForeignId"), true),
        source(10, 40_000, ("Id", "Payload"), true),
    ];
    let first = bind(
        "SELECT l.\"Id\", r.\"Payload\" FROM left_schema.\"Events\" l \
         JOIN right_schema.\"Lookup\" r ON l.\"ForeignId\" = r.\"Id\"",
        &quoted,
    );
    let second = bind(
        "SELECT lhs.\"Id\" AS visible_id, rhs.\"Payload\" AS visible_payload \
         FROM left_schema.\"Events\" AS lhs JOIN right_schema.\"Lookup\" AS rhs \
         ON rhs.\"Id\" = lhs.\"ForeignId\"",
        &quoted,
    );
    assert_eq!(first.sources, second.sources);
    assert_eq!(first.nodes, second.nodes);
    assert_ne!(first.results[0].fields, second.results[0].fields);
    assert_eq!(second.results[0].fields[0].name, "visible_id");
    assert_eq!(second.results[0].fields[1].name, "visible_payload");
    let QueryOperationV1::InnerJoin { left_id, .. } = &first.nodes[0].operation else {
        panic!()
    };
    assert!(matches!(
        &left_id.selector,
        QuerySelectorV1::Name { name, quoted: true } if name == "Id"
    ));
    assert_compiles(&first, &quoted);
}

#[test]
fn missing_wrong_type_and_wrong_identity_fail_at_exact_binding() {
    let sources = sources();
    assert_binding_error(
        "SELECT l.id, r.missing FROM s.events l JOIN t.lookup r ON l.foreign_id=r.id",
        &sources,
        ErrorCode::UnknownColumn,
    );

    let mut wrong_type = sources.clone();
    wrong_type[0].descriptor.columns[1].type_oid = POSTGRES_TEXT_TYPE_OID;
    assert_binding_error(
        "SELECT l.id, r.payload FROM s.events l JOIN t.lookup r ON l.foreign_id=r.id",
        &wrong_type,
        ErrorCode::TypeMismatch,
    );

    let mut wrong_identity = sources.clone();
    wrong_identity[1].identity.key_column = wrong_identity[1].descriptor.columns[1].address;
    assert_binding_error(
        "SELECT l.id, r.payload FROM s.events l JOIN t.lookup r ON l.foreign_id=r.id",
        &wrong_identity,
        ErrorCode::IdentityMismatch,
    );

    assert_binding_error(
        "SELECT l.foreign_id, r.payload FROM s.events l JOIN t.lookup r ON l.foreign_id=r.id",
        &sources,
        ErrorCode::IdentityMismatch,
    );
}

#[test]
fn ambiguous_non_equality_and_unproven_join_shapes_fail_closed() {
    for sql in [
        "SELECT id, r.payload FROM s.events l JOIN t.lookup r ON l.foreign_id=r.id",
        "SELECT l.id, r.payload FROM s.events l JOIN t.lookup r ON l.foreign_id>r.id",
    ] {
        let error = parse_sql(sql).unwrap_err();
        assert!(matches!(
            error.code,
            ErrorCode::AmbiguousColumn | ErrorCode::UnsupportedSyntax
        ));
    }

    let sources = sources();
    assert_binding_error(
        "SELECT l.id, r.payload FROM s.events l JOIN t.lookup r ON l.foreign_id=r.id \
         WHERE l.id > 0",
        &sources,
        ErrorCode::UnsupportedSyntax,
    );
}

fn assert_binding_error(sql: &str, sources: &[ResolvedSource; 2], code: ErrorCode) {
    let query = parse_sql(sql).unwrap();
    let error = bind_query(GraphId::new(11).unwrap(), &query, sources).unwrap_err();
    assert_eq!(error.class, ErrorClass::Binding);
    assert_eq!(error.code, code);
    assert!(error.span.end > error.span.start);
}
