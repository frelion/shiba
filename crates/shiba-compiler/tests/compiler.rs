use core::num::NonZeroU64;

use shiba_compiler::{
    CompilerError, OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1,
    POSTGRES_INT8_TYPE_OID, SourceColumnDescriptor, SourceDescriptor, compile_operator,
};
use shiba_operator::{CompiledOperatorKind, ObjectAddress, OperatorId};
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
