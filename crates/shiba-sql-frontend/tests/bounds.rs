use shiba_sql_frontend::{ErrorClass, ErrorCode, parse_sql};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

fn balanced_comparisons(count: usize) -> String {
    if count == 1 {
        return "id = 1".into();
    }
    let left = count / 2;
    format!(
        "({} AND {})",
        balanced_comparisons(left),
        balanced_comparisons(count - left)
    )
}

fn balanced_additions(count: usize) -> String {
    if count == 1 {
        return "id".into();
    }
    let left = count / 2;
    format!(
        "({} + {})",
        balanced_additions(left),
        balanced_additions(count - left)
    )
}

fn token_count(sql: &str) -> usize {
    Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()
        .unwrap()
        .into_iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_)))
        .count()
}

#[test]
fn input_byte_limit_accepts_exact_and_rejects_next() {
    let base = "SELECT count(*) FROM s.t";
    let exact = format!("{base}{}", " ".repeat(65_536 - base.len()));
    assert_eq!(exact.len(), 65_536);
    assert!(parse_sql(&exact).is_ok());
    let error = parse_sql(&(exact + " ")).unwrap_err();
    assert_eq!(
        (error.class, error.code),
        (ErrorClass::Limit, ErrorCode::InputTooLarge)
    );
}

#[test]
fn expression_depth_exact_and_next_are_distinct() {
    let expression = |adds: usize| format!("id{}", "+1".repeat(adds));
    assert!(parse_sql(&format!("SELECT id, {} FROM s.t", expression(30))).is_ok());
    let error = parse_sql(&format!("SELECT id, {} FROM s.t", expression(32))).unwrap_err();
    assert_eq!(
        (error.class, error.code),
        (ErrorClass::Limit, ErrorCode::QueryTooComplex)
    );
}

#[test]
fn boolean_term_limit_accepts_exact_and_rejects_next() {
    let exact = format!("NOT ({})", balanced_comparisons(32));
    assert!(parse_sql(&format!("SELECT id, payload FROM s.t WHERE {exact}")).is_ok());
    let next = format!("NOT NOT ({})", balanced_comparisons(32));
    let error = parse_sql(&format!("SELECT id, payload FROM s.t WHERE {next}")).unwrap_err();
    assert_eq!(
        (error.class, error.code),
        (ErrorClass::Limit, ErrorCode::QueryTooComplex)
    );
}

#[test]
fn token_limit_precedes_parser_or_expression_work() {
    let sql = format!(
        "SELECT id, payload FROM s.t WHERE id = 1 {}",
        "+ 1 ".repeat(4_100)
    );
    let error = parse_sql(&sql).unwrap_err();
    assert_eq!(
        (error.class, error.code),
        (ErrorClass::Limit, ErrorCode::TokenLimit)
    );
}

#[test]
fn token_limit_exact_is_admitted_then_later_bounds_reject_and_next_is_token_limit() {
    let mut sql = "SELECT id, payload FROM s.t WHERE id = 1".to_string();
    while token_count(&sql) + 2 <= 4_096 {
        sql.push_str(" + 1");
    }
    if token_count(&sql) < 4_096 {
        sql.push(';');
    }
    assert_eq!(token_count(&sql), 4_096);
    assert_ne!(parse_sql(&sql).unwrap_err().code, ErrorCode::TokenLimit);
    sql.push(';');
    assert_eq!(parse_sql(&sql).unwrap_err().code, ErrorCode::TokenLimit);
}

#[test]
fn expression_node_limit_accepts_exact_and_rejects_next() {
    let exact = format!("({}) IS NULL", balanced_additions(127));
    assert!(parse_sql(&format!("SELECT id, payload FROM s.t WHERE {exact}")).is_ok());
    let error = parse_sql(&format!("SELECT id, payload FROM s.t WHERE NOT ({exact})")).unwrap_err();
    assert_eq!(
        (error.class, error.code),
        (ErrorClass::Limit, ErrorCode::QueryTooComplex)
    );
}

#[test]
fn source_projection_and_identifier_exact_limits_are_enforced() {
    let name = "a".repeat(63);
    assert!(parse_sql(&format!("SELECT id, payload FROM s.{name}")).is_ok());
    assert_eq!(
        parse_sql(&format!("SELECT id, payload FROM s.{}", "a".repeat(64)))
            .unwrap_err()
            .code,
        ErrorCode::InvalidIdentifier
    );
    assert_eq!(
        parse_sql("SELECT id, payload, id FROM s.t")
            .unwrap_err()
            .code,
        ErrorCode::QueryTooComplex
    );
    assert_eq!(
        parse_sql(
            "SELECT a.id, b.payload FROM s.a a JOIN s.b b ON a.id=b.id JOIN s.c c ON b.id=c.id"
        )
        .unwrap_err()
        .code,
        ErrorCode::QueryTooComplex
    );
}

#[test]
fn utf8_locations_are_stable_half_open_byte_spans() {
    let sql = "SELECT \"值\" FROM \"模式\".\"表\"";
    let error = parse_sql(sql).unwrap_err();
    assert!(error.span.start <= error.span.end && error.span.end <= sql.len());
    assert!(sql.is_char_boundary(error.span.start));
    assert!(sql.is_char_boundary(error.span.end));
}

#[test]
fn identifier_and_alias_errors_have_exact_half_open_utf8_byte_offsets() {
    let long = "界".repeat(22);
    let sql = format!("SELECT id, payload FROM s.\"{long}\"");
    let error = parse_sql(&sql).unwrap_err();
    let start = sql.find('"').unwrap();
    assert_eq!(
        error.span,
        shiba_sql_frontend::Span {
            start,
            end: sql.len()
        }
    );

    let sql = "SELECT id AS first, payload AS first FROM s.t";
    let error = parse_sql(sql).unwrap_err();
    let start = sql.match_indices("first").nth(1).unwrap().0;
    assert_eq!(
        error.span,
        shiba_sql_frontend::Span {
            start,
            end: start + 5
        }
    );
}
