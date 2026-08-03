use shiba_sql_frontend::{ErrorClass, ErrorCode, parse_sql};

#[test]
fn rejects_every_explicitly_unsupported_query_family() {
    let cases = [
        "SELECT id FROM s.t; SELECT id FROM s.t",
        "INSERT INTO s.t VALUES (1)",
        "CREATE TABLE s.t(id bigint)",
        "WITH q AS (SELECT id FROM s.t) SELECT id FROM q",
        "SELECT id FROM (SELECT id FROM s.t) q",
        "SELECT id FROM s.t UNION SELECT id FROM s.t",
        "SELECT * FROM s.t",
        "SELECT DISTINCT id, payload FROM s.t",
        "SELECT id, payload FROM s.t ORDER BY id",
        "SELECT id, payload FROM s.t LIMIT 1",
        "SELECT id, payload FROM s.t OFFSET 1",
        "SELECT id, payload FROM s.t FETCH FIRST 1 ROW ONLY",
        "SELECT id, count(*) FROM s.t GROUP BY id HAVING count(*) > 1",
        "SELECT id, count(*) OVER () FROM s.t",
        "SELECT min(payload) FROM s.t",
        "SELECT max(payload) FROM s.t",
        "SELECT avg(payload) FROM s.t",
        "SELECT id::bigint, payload FROM s.t",
        "SELECT id, CASE WHEN id=1 THEN payload END FROM s.t",
        "SELECT id, payload FROM s.t WHERE id IN (1)",
        "SELECT id, payload FROM s.t WHERE id BETWEEN 1 AND 2",
        "SELECT id, payload FROM s.t WHERE payload LIKE 'x'",
        "SELECT id, payload FROM s.t WHERE id = $1",
        "SELECT l.id, r.payload FROM s.l l LEFT JOIN s.r r ON l.k=r.id",
        "SELECT l.id, r.payload FROM s.l l CROSS JOIN s.r r",
        "SELECT l.id, r.payload FROM s.l l JOIN s.r r USING (id)",
        "SELECT l.id, r.payload FROM s.l l JOIN s.r r ON l.k > r.id",
        "SELECT l.id, r.payload FROM s.l l JOIN s.r r ON l.k=r.id JOIN s.x x ON x.id=r.id",
        "SELECT id, payload FROM t",
        "SELECT id FROM s.t",
        "SELECT count(id) FROM s.t",
        "SELECT sum(payload + 1) FROM s.t",
        "SELECT id, payload, id + 1 FROM s.t",
        "SELECT id, count(*) FROM s.t GROUP BY id, payload",
    ];
    for sql in cases {
        let error = parse_sql(sql).unwrap_err();
        assert!(
            matches!(
                error.class,
                ErrorClass::Parser | ErrorClass::Unsupported | ErrorClass::Limit
            ),
            "{sql}: {error:?}"
        );
    }
}

#[test]
fn stable_error_classes_separate_parser_unsupported_and_limit() {
    let parser = parse_sql("SELECT (").unwrap_err();
    assert_eq!(
        (parser.class, parser.code),
        (ErrorClass::Parser, ErrorCode::ParseError)
    );
    let unsupported = parse_sql("DELETE FROM s.t").unwrap_err();
    assert_eq!(
        (unsupported.class, unsupported.code),
        (ErrorClass::Unsupported, ErrorCode::UnsupportedSyntax)
    );
    let oversized = parse_sql(&" ".repeat(65_537)).unwrap_err();
    assert_eq!(
        (oversized.class, oversized.code),
        (ErrorClass::Limit, ErrorCode::InputTooLarge)
    );
}

#[test]
fn public_error_codes_are_the_frozen_snake_case_set() {
    let values = [
        (ErrorCode::InputTooLarge, "input_too_large"),
        (ErrorCode::TokenLimit, "token_limit"),
        (ErrorCode::ParseError, "parse_error"),
        (ErrorCode::MultipleStatements, "multiple_statements"),
        (ErrorCode::UnsupportedSyntax, "unsupported_syntax"),
        (ErrorCode::InvalidIdentifier, "invalid_identifier"),
        (ErrorCode::DuplicateAlias, "duplicate_alias"),
        (ErrorCode::AmbiguousColumn, "ambiguous_column"),
        (ErrorCode::UnknownRelation, "unknown_relation"),
        (ErrorCode::UnknownColumn, "unknown_column"),
        (ErrorCode::SourceNotRegistered, "source_not_registered"),
        (ErrorCode::TypeMismatch, "type_mismatch"),
        (ErrorCode::IdentityMismatch, "identity_mismatch"),
        (ErrorCode::QueryTooComplex, "query_too_complex"),
        (ErrorCode::DdlDrift, "ddl_drift"),
        (ErrorCode::GraphConflict, "graph_conflict"),
        (ErrorCode::CanonicalizationFailed, "canonicalization_failed"),
        (ErrorCode::RegistrationFailed, "registration_failed"),
    ];
    for (code, text) in values {
        assert_eq!(code.as_str(), text);
        assert_eq!(code.to_string(), text);
    }
}

#[test]
fn aliases_identifiers_and_join_qualification_fail_closed() {
    for sql in [
        "SELECT x.id, x.payload FROM s.left x JOIN s.right x ON x.id=x.id",
        "SELECT id, r.payload FROM s.left l JOIN s.right r ON l.id=r.id",
        "SELECT l.id, r.payload FROM s.left JOIN s.right r ON left.id=r.id",
        "SELECT id AS x, payload AS x FROM s.t",
        "SELECT id AS x, payload AS \"x\" FROM s.t",
        "SELECT id, payload FROM s.t AS `bad`",
    ] {
        assert!(parse_sql(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn malformed_truncated_and_fixed_seed_text_never_panics() {
    let corpus = [
        "",
        "S",
        "SELECT",
        "SELECT /*",
        "SELECT \"",
        "SELECT 1e",
        "SELECT )",
        "\0",
        "💥 SELECT",
    ];
    for input in corpus {
        assert!(std::panic::catch_unwind(|| parse_sql(input)).is_ok());
    }
    let mut state = 0x5eed_u64;
    for _ in 0..1_000 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let text = format!(
            "SELECT {} FROM s.t",
            String::from_utf8_lossy(&state.to_le_bytes())
        );
        assert!(std::panic::catch_unwind(|| parse_sql(&text)).is_ok());
    }
}
