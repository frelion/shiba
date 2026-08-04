use shiba_operator::{
    MAX_RESULT_FIELDS, OutputContract, ResultError, ResultField, ResultRowKey, ResultSchemaV1,
    TypedResultRowV1, TypedValue, ValueType,
};

fn field(ordinal: u16, name: &str, nullable: bool) -> ResultField {
    ResultField {
        ordinal,
        name: name.into(),
        value_type: ValueType::Int8,
        nullable,
    }
}

fn keyed_schema() -> ResultSchemaV1 {
    ResultSchemaV1::new(
        vec![field(1, "id", false), field(2, "payload", true)],
        vec![1],
    )
    .unwrap()
}

#[test]
fn schema_payload_digest_and_field_bound_are_strict() {
    let schema = keyed_schema();
    assert_eq!(
        ResultSchemaV1::from_canonical_payload(&schema.canonical_payload, schema.digest).unwrap(),
        schema
    );
    let mut wrong = schema.digest;
    wrong[0] ^= 1;
    assert!(ResultSchemaV1::from_canonical_payload(&schema.canonical_payload, wrong).is_err());
    let fields = (1..=MAX_RESULT_FIELDS)
        .map(|n| field(u16::try_from(n).unwrap(), &format!("f{n}"), false))
        .collect();
    assert!(ResultSchemaV1::new(fields, vec![1]).is_ok());
    let too_many = (1..=MAX_RESULT_FIELDS + 1)
        .map(|n| field(u16::try_from(n).unwrap(), &format!("f{n}"), false))
        .collect();
    assert_eq!(
        ResultSchemaV1::new(too_many, vec![1]),
        Err(ResultError::FieldLimit)
    );
}

#[test]
fn row_key_codec_matches_schema_and_forbids_absent() {
    let schema = keyed_schema();
    let row = TypedResultRowV1::new(
        &schema,
        vec![TypedValue::Int8(7), TypedValue::Null(ValueType::Int8)],
    )
    .unwrap();
    let payload = row.to_canonical_payload().unwrap();
    assert_eq!(
        TypedResultRowV1::from_canonical_payload(&schema, &payload).unwrap(),
        row
    );
    assert_eq!(
        TypedResultRowV1::new(&schema, vec![TypedValue::Int8(7), TypedValue::Absent]),
        Err(ResultError::Absent)
    );
    let key = ResultRowKey::from_row(&schema, &row).unwrap();
    let payload = key.to_canonical_payload().unwrap();
    assert_eq!(
        ResultRowKey::from_canonical_payload(&schema, &payload).unwrap(),
        key
    );
}

#[test]
fn complete_row_rejects_missing_extra_wrong_nullability_and_foreign_schema() {
    let schema = keyed_schema();
    assert_eq!(
        TypedResultRowV1::new(&schema, vec![TypedValue::Int8(7)]),
        Err(ResultError::SchemaMismatch)
    );
    assert_eq!(
        TypedResultRowV1::new(
            &schema,
            vec![
                TypedValue::Int8(7),
                TypedValue::Int8(8),
                TypedValue::Int8(9),
            ],
        ),
        Err(ResultError::SchemaMismatch)
    );
    assert_eq!(
        TypedResultRowV1::new(
            &schema,
            vec![TypedValue::Null(ValueType::Int8), TypedValue::Int8(8)],
        ),
        Err(ResultError::WrongType)
    );
    let row = TypedResultRowV1::new(
        &schema,
        vec![TypedValue::Int8(7), TypedValue::Null(ValueType::Int8)],
    )
    .unwrap();
    let foreign = ResultSchemaV1::new(
        vec![field(1, "foreign_id", false), field(2, "payload", true)],
        vec![1],
    )
    .unwrap();
    assert_eq!(row.validate(&foreign), Err(ResultError::SchemaMismatch));
}

#[test]
fn scalar_contract_owns_initial_row_and_singleton_identity() {
    let schema = ResultSchemaV1::new(vec![field(1, "count", false)], vec![]).unwrap();
    let row = TypedResultRowV1::new(&schema, vec![TypedValue::Int8(0)]).unwrap();
    assert!(
        OutputContract::new(schema.clone(), Some(row))
            .unwrap()
            .validate()
            .is_ok()
    );
    assert!(ResultRowKey::scalar(&schema).unwrap().values.is_empty());
    assert!(OutputContract::new(schema, None).is_err());
}
