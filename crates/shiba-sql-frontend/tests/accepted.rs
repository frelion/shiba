use shiba_sql_frontend::{
    Aggregate, BinaryOperator, SelectExpression, UnboundExpression, parse_sql,
};

#[test]
fn accepts_each_frozen_single_source_shape() {
    let cases = [
        "SELECT count(*) FROM source.events",
        "SELECT sum(payload) FROM source.events WHERE payload IS NOT NULL",
        "SELECT id, payload AS value FROM source.events",
        "SELECT id, payload + 7 FROM source.events WHERE id >= -9 AND NOT payload IS NULL",
        "SELECT id, count(*) FROM source.events GROUP BY id",
        "SELECT id, sum(payload) FROM source.events GROUP BY id",
        "SELECT \"e\".\"id\", \"e\".\"payload\" FROM \"source\".\"events\" AS \"e\"",
    ];
    for sql in cases {
        parse_sql(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    let sum = parse_sql("SELECT SUM(payload) FROM SOURCE.EVENTS").unwrap();
    assert!(matches!(
        sum.projection[0].expression,
        SelectExpression::Aggregate(Aggregate::Sum { .. })
    ));
    let computed = parse_sql("SELECT id, payload + 7 FROM source.events").unwrap();
    assert!(matches!(
        computed.projection[1].expression,
        SelectExpression::Expression(UnboundExpression::Binary {
            operator: BinaryOperator::Add,
            ..
        })
    ));
}

#[test]
fn accepts_cross_schema_inner_join_with_exact_qualified_columns() {
    let query = parse_sql(
        "SELECT l.id AS left_id, r.payload AS value \
         FROM left_schema.events AS l \
         INNER JOIN right_schema.lookup AS r ON l.foreign_id = r.id \
         WHERE r.payload IS NOT NULL",
    )
    .unwrap();
    assert_eq!(query.sources.len(), 2);
    let join = query.join.unwrap();
    assert_eq!((join.left.source, join.right.source), (0, 1));
    assert_eq!(join.left.name.value, "foreign_id");
    assert_eq!(join.right.name.value, "id");
}

#[test]
fn whitespace_case_parentheses_and_aliases_have_one_canonical_identity() {
    let variants = [
        "SELECT e.id, e.payload FROM source.events e",
        " select X.ID as key, (((X.payload))) AS value from SOURCE.EVENTS AS X ",
        "SELECT q.id, q.payload FROM source.events AS q;",
        "SELECT /*ignored*/ q.id, q.payload FROM source.events AS q",
    ];
    let queries = variants.map(|sql| parse_sql(sql).unwrap());
    assert_eq!(
        queries[0].canonical_payload(),
        queries[1].canonical_payload()
    );
    assert_eq!(queries[0].canonical_digest(), queries[2].canonical_digest());
    assert_ne!(
        queries[0].canonical_digest(),
        parse_sql("SELECT e.id, e.payload + 1 FROM source.events e")
            .unwrap()
            .canonical_digest()
    );
}

#[test]
fn canonical_payload_has_a_fixed_golden_prefix_and_ignores_spans() {
    let query = parse_sql("SELECT id, payload FROM source.events").unwrap();
    let payload = query.canonical_payload().unwrap();
    assert_eq!(
        query.canonical_digest().unwrap(),
        [
            118, 24, 18, 239, 248, 94, 210, 132, 0, 198, 176, 176, 60, 9, 166, 213, 191, 141, 1,
            40, 56, 161, 208, 244, 60, 168, 89, 244, 236, 201, 127, 8,
        ]
    );
    assert_eq!(&payload[..4], &[b'U', b'Q', b'1', 1]);
    assert_eq!(
        query.canonical_digest(),
        parse_sql("\nSELECT id,payload\nFROM source.events\n")
            .unwrap()
            .canonical_digest()
    );
}
