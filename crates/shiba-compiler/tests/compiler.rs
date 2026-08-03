use core::num::NonZeroU64;

use shiba_compiler::{
    CompilerError, OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1,
    POSTGRES_INT8_TYPE_OID, SourceColumnDescriptor, SourceDescriptor, compile_operator,
    compile_plan,
};
use shiba_operator::{
    CompiledOperatorKind, CompiledPlan, ObjectAddress, OperatorId, OutputContract,
    PlanImplementation, ValueType,
};
use shiba_protocol::SourceId;

fn address(object_id: u32, sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        class_id: 1_259,
        object_id,
        sub_id,
    }
}

fn spec(operation: OperatorOperationV1) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(2).unwrap()),
        source_id: SourceId::new(1).unwrap(),
        operation,
    }
}

fn source(columns: Vec<SourceColumnDescriptor>) -> SourceDescriptor {
    SourceDescriptor {
        source_id: SourceId::new(1).unwrap(),
        relation: address(16_384, 0),
        columns,
    }
}

fn column(name: &str, sub_id: i32, type_oid: u32) -> SourceColumnDescriptor {
    SourceColumnDescriptor {
        name: name.into(),
        address: address(16_384, sub_id),
        type_oid,
        nullable: true,
    }
}

fn required_column(name: &str, sub_id: i32) -> SourceColumnDescriptor {
    SourceColumnDescriptor {
        nullable: false,
        ..column(name, sub_id, POSTGRES_INT8_TYPE_OID)
    }
}

#[test]
fn strict_json_is_canonical_and_rejects_invalid_shapes() {
    let sum = spec(OperatorOperationV1::SumInt8 {
        input_column: "payload".into(),
    });
    let canonical = sum.to_canonical_json().unwrap();
    assert_eq!(
        canonical,
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"sum_int8","input_column":"payload"}}"#
    );
    assert_eq!(OperatorSpecV1::from_json(&canonical).unwrap(), sum);

    for invalid in [
        br#"{"version":2,"operator_id":2,"source_id":1,"operation":{"kind":"count_rows"}}"#.as_slice(),
        br#"{"version":1,"operator_id":0,"source_id":1,"operation":{"kind":"count_rows"}}"#,
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"sum_int8","input_column":" "}}"#,
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"sum_int8","column":"payload"}}"#,
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"count","input_column":"payload"}}"#,
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"count_rows"},"extra":true}"#,
    ] {
        assert!(OperatorSpecV1::from_json(invalid).is_err(), "{invalid:?}");
    }
}

#[test]
fn count_does_not_bind_a_column() {
    let compiled =
        compile_operator(&spec(OperatorOperationV1::CountRows), &source(vec![])).unwrap();
    assert_eq!(compiled.kind, CompiledOperatorKind::CountRows);
}

#[test]
fn sum_resolves_exact_int8_address_once() {
    let operation = OperatorOperationV1::SumInt8 {
        input_column: "payload".into(),
    };
    let compiled = compile_operator(
        &spec(operation.clone()),
        &source(vec![column("payload", 2, POSTGRES_INT8_TYPE_OID)]),
    )
    .unwrap();
    assert_eq!(
        compiled.kind,
        CompiledOperatorKind::SumInt8 {
            input: address(16_384, 2)
        }
    );

    for (columns, error) in [
        (vec![], CompilerError::MissingColumn("payload".into())),
        (
            vec![
                column("payload", 2, POSTGRES_INT8_TYPE_OID),
                column("payload", 3, POSTGRES_INT8_TYPE_OID),
            ],
            CompilerError::DuplicateColumn("payload".into()),
        ),
        (
            vec![column("payload", 2, 25)],
            CompilerError::WrongColumnType {
                column: "payload".into(),
                type_oid: 25,
            },
        ),
    ] {
        assert_eq!(
            compile_operator(&spec(operation.clone()), &source(columns)),
            Err(error)
        );
    }
}

#[test]
fn source_identity_must_match() {
    let mut descriptor = source(vec![]);
    descriptor.source_id = SourceId::new(2).unwrap();
    assert_eq!(
        compile_operator(&spec(OperatorOperationV1::CountRows), &descriptor),
        Err(CompilerError::SourceMismatch)
    );
}

#[test]
fn constructed_invalid_ir_fails_closed() {
    let mut invalid_version = spec(OperatorOperationV1::CountRows);
    invalid_version.version = 2;
    assert_eq!(
        compile_operator(&invalid_version, &source(vec![])),
        Err(CompilerError::UnsupportedVersion(2))
    );
    assert_eq!(
        compile_operator(
            &spec(OperatorOperationV1::SumInt8 {
                input_column: " ".into(),
            }),
            &source(vec![])
        ),
        Err(CompilerError::BlankInputColumn)
    );
}

#[test]
fn generic_plans_are_canonical_bound_and_strict() {
    let descriptor = source(vec![
        required_column("id", 1),
        column("payload", 2, POSTGRES_INT8_TYPE_OID),
    ]);
    let project_spec = spec(OperatorOperationV1::ProjectRows {
        key_column: "id".into(),
        input_column: "payload".into(),
    });
    let first = compile_plan(&project_spec, &descriptor).unwrap();
    let second = compile_plan(&project_spec, &descriptor).unwrap();
    assert_eq!(
        first.canonical_payload,
        br#"{"format_version":1,"operator_id":2,"source_id":1,"inputs":[{"role":"key","address":{"class_id":1259,"object_id":16384,"sub_id":1}},{"role":"payload","address":{"class_id":1259,"object_id":16384,"sub_id":2}}],"state_contract":{"codec_version":1},"output_contract":{"shape":"keyed_rows","key_type":"int8","value_type":"int8","nullable":true},"implementation":{"kind":"project_rows","key":{"class_id":1259,"object_id":16384,"sub_id":1},"value":{"class_id":1259,"object_id":16384,"sub_id":2}}}"#
    );
    assert_eq!(
        first.digest,
        [
            0x50, 0x51, 0x64, 0xec, 0xba, 0xfb, 0xb2, 0x8e, 0x66, 0xcf, 0x1d, 0x1c, 0x58, 0x09,
            0xfd, 0x7e, 0x88, 0x69, 0xd0, 0x35, 0x06, 0x14, 0x48, 0x4d, 0xcc, 0xb2, 0x06, 0x8d,
            0xda, 0x3f, 0x90, 0x93,
        ]
    );
    assert_eq!(first.canonical_payload, second.canonical_payload);
    assert_eq!(first.digest, second.digest);
    assert_eq!(
        first.output_contract,
        OutputContract::KeyedRows {
            key_type: ValueType::Int8,
            value_type: ValueType::Int8,
            nullable: true
        }
    );
    assert_eq!(
        first.implementation,
        PlanImplementation::ProjectRows {
            key: address(16_384, 1),
            value: address(16_384, 2)
        }
    );
    first.validate().unwrap();
    let mut unknown = serde_json::to_value(&first).unwrap();
    unknown
        .as_object_mut()
        .unwrap()
        .insert("unknown".into(), true.into());
    assert!(serde_json::from_value::<CompiledPlan>(unknown).is_err());

    let encoded = project_spec.to_canonical_json().unwrap();
    assert_eq!(OperatorSpecV1::from_json(&encoded).unwrap(), project_spec);
    for invalid in [
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"project_rows","key_column":"","input_column":"payload"}}"#.as_slice(),
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"project_rows","key_column":"id","input_column":" "}}"#,
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"project_rows","key_column":"id","input_column":"payload","alias":"x"}}"#,
    ] {
        assert!(OperatorSpecV1::from_json(invalid).is_err());
    }
}

#[test]
fn project_binding_rejects_missing_wrong_duplicate_and_nullable_key() {
    let operation = OperatorOperationV1::ProjectRows {
        key_column: "id".into(),
        input_column: "payload".into(),
    };
    let cases = [
        (
            vec![column("payload", 2, POSTGRES_INT8_TYPE_OID)],
            CompilerError::MissingColumn("id".into()),
        ),
        (
            vec![required_column("id", 1), column("payload", 2, 25)],
            CompilerError::WrongColumnType {
                column: "payload".into(),
                type_oid: 25,
            },
        ),
        (
            vec![
                required_column("id", 1),
                column("payload", 2, 20),
                column("payload", 3, 20),
            ],
            CompilerError::DuplicateColumn("payload".into()),
        ),
        (
            vec![column("id", 1, 20), column("payload", 2, 20)],
            CompilerError::NullableKey("id".into()),
        ),
    ];
    for (columns, expected) in cases {
        assert_eq!(
            compile_plan(&spec(operation.clone()), &source(columns)),
            Err(expected)
        );
    }
}
