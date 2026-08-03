use shiba_sql_frontend::{
    BinaryOperator, ColumnRef, ErrorCode, Identifier, SelectExpression, Span, UnboundExpression,
    parse_sql,
};

fn span() -> Span {
    Span { start: 0, end: 0 }
}

fn literal() -> UnboundExpression {
    UnboundExpression::Int8(1, span())
}

fn deep(mut value: UnboundExpression, count: usize) -> UnboundExpression {
    for _ in 0..count {
        value = UnboundExpression::Unary {
            operator: shiba_sql_frontend::UnaryOperator::Not,
            input: Box::new(value),
            span: span(),
        };
    }
    value
}

#[test]
fn manually_constructed_ast_cannot_bypass_identifier_or_cardinality_bounds() {
    let mut query = parse_sql("SELECT id, payload FROM s.t").unwrap();
    query.sources[0].schema.value = "x".repeat(300);
    assert_eq!(
        query.canonical_payload().unwrap_err().code,
        ErrorCode::InvalidIdentifier
    );

    let mut query = parse_sql("SELECT id, payload FROM s.t").unwrap();
    query.sources.push(query.sources[0].clone());
    query.sources.push(query.sources[0].clone());
    assert_eq!(
        query.canonical_payload().unwrap_err().code,
        ErrorCode::CanonicalizationFailed
    );

    let mut query = parse_sql("SELECT id, payload FROM s.t").unwrap();
    query.projection.extend(query.projection.clone());
    assert_eq!(
        query.canonical_digest().unwrap_err().code,
        ErrorCode::CanonicalizationFailed
    );
}

#[test]
fn iterative_validation_rejects_deep_and_oversized_public_expressions_before_encoding() {
    let mut query = parse_sql("SELECT id, payload FROM s.t").unwrap();
    query.projection[1].expression = SelectExpression::Expression(deep(literal(), 128));
    assert_eq!(
        query.canonical_payload().unwrap_err().code,
        ErrorCode::QueryTooComplex
    );

    let mut query = parse_sql("SELECT id, payload FROM s.t").unwrap();
    let mut expression = literal();
    for _ in 0..130 {
        expression = UnboundExpression::Binary {
            operator: BinaryOperator::Add,
            left: Box::new(expression),
            right: Box::new(literal()),
            span: span(),
        };
    }
    query.projection[1].expression = SelectExpression::Expression(expression);
    assert_eq!(
        query.canonical_digest().unwrap_err().code,
        ErrorCode::QueryTooComplex
    );
}

#[test]
fn invalid_public_source_slot_and_long_names_never_saturate_into_canonical_bytes() {
    let mut query = parse_sql("SELECT id, payload FROM s.t").unwrap();
    query.projection[0].expression =
        SelectExpression::Expression(UnboundExpression::Column(ColumnRef {
            source: u8::MAX,
            name: Identifier {
                value: "id".into(),
                quoted: false,
                span: span(),
            },
            span: span(),
        }));
    assert_eq!(
        query.canonical_payload().unwrap_err().code,
        ErrorCode::CanonicalizationFailed
    );

    for length in [255, 256] {
        let mut query = parse_sql("SELECT id, payload FROM s.t").unwrap();
        query.sources[0].relation.value = "q".repeat(length);
        assert!(query.canonical_payload().is_err());
    }
}

#[test]
fn quoted_and_unquoted_group_keys_are_not_assumed_equivalent_before_binding() {
    let error = parse_sql("SELECT \"id\", count(*) FROM s.t GROUP BY id").unwrap_err();
    assert_eq!(error.code, ErrorCode::UnsupportedSyntax);
}
