use core::num::NonZeroU64;

use shiba_compiler::{
    CompilerError, OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1,
    POSTGRES_INT8_TYPE_OID, SourceColumnDescriptor, SourceDescriptor, compile_graph, compile_plan,
};
use shiba_operator::{
    Expression, NodeInput, ObjectAddress, OperatorId, OperatorNodeKind, OutputContract,
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
    let compiled = compile_plan(&spec(OperatorOperationV1::CountRows), &source(vec![])).unwrap();
    assert_eq!(compiled.implementation, PlanImplementation::CountRows);
    assert!(compiled.inputs.is_empty());
}

#[test]
fn sum_resolves_exact_int8_address_once() {
    let operation = OperatorOperationV1::SumInt8 {
        input_column: "payload".into(),
    };
    let compiled = compile_plan(
        &spec(operation.clone()),
        &source(vec![column("payload", 2, POSTGRES_INT8_TYPE_OID)]),
    )
    .unwrap();
    assert_eq!(
        compiled.implementation,
        PlanImplementation::SumInt8 {
            input: address(16_384, 2),
            input_slot: 0
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
            compile_plan(&spec(operation.clone()), &source(columns)),
            Err(error)
        );
    }
}

#[test]
fn source_identity_must_match() {
    let mut descriptor = source(vec![]);
    descriptor.source_id = SourceId::new(2).unwrap();
    assert_eq!(
        compile_plan(&spec(OperatorOperationV1::CountRows), &descriptor),
        Err(CompilerError::SourceMismatch)
    );
}

#[test]
fn constructed_invalid_ir_fails_closed() {
    let mut invalid_version = spec(OperatorOperationV1::CountRows);
    invalid_version.version = 2;
    assert_eq!(
        compile_plan(&invalid_version, &source(vec![])),
        Err(CompilerError::UnsupportedVersion(2))
    );
    assert_eq!(
        compile_plan(
            &spec(OperatorOperationV1::SumInt8 {
                input_column: " ".into(),
            }),
            &source(vec![])
        ),
        Err(CompilerError::BlankInputColumn)
    );
}

#[test]
fn project_declaration_compiles_to_canonical_graph_nodes() {
    let descriptor = source(vec![
        required_column("id", 1),
        column("payload", 2, POSTGRES_INT8_TYPE_OID),
    ]);
    let project_spec = spec(OperatorOperationV1::MaterializedProject {
        key_column: "id".into(),
        value_column: "payload".into(),
    });
    let first = compile_graph(&project_spec, &descriptor).unwrap();
    let second = compile_graph(&project_spec, &descriptor).unwrap();
    assert_eq!(first.canonical_payload, second.canonical_payload);
    assert_eq!(first.digest, second.digest);
    assert_eq!(
        first.sources[0]
            .layout
            .iter()
            .map(|binding| binding.address)
            .collect::<Vec<_>>(),
        vec![address(16_384, 1), address(16_384, 2)]
    );
    assert_eq!(first.nodes.len(), 2);
    assert!(matches!(
        first.nodes[0].kind,
        OperatorNodeKind::Project { .. }
    ));
    assert!(matches!(first.nodes[1].input, NodeInput::Node(_)));
    assert!(matches!(
        first.nodes[1].kind,
        OperatorNodeKind::Materialize {
            output: OutputContract::KeyedRows {
                key_type: ValueType::Int8,
                key_nullable: false,
                value_type: ValueType::Int8,
                nullable: true
            },
            ..
        }
    ));
    first.validate().unwrap();
    let mut digest = first.digest;
    digest[0] ^= 1;
    assert!(
        shiba_operator::OperatorGraph::from_canonical_payload(&first.canonical_payload, digest)
            .is_err()
    );

    let encoded = project_spec.to_canonical_json().unwrap();
    assert_eq!(OperatorSpecV1::from_json(&encoded).unwrap(), project_spec);
    for invalid in [
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"materialized_project","key_column":"","value_column":"payload"}}"#.as_slice(),
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"materialized_project","key_column":"id","value_column":" "}}"#,
        br#"{"version":1,"operator_id":2,"source_id":1,"operation":{"kind":"materialized_project","key_column":"id","value_column":"payload","alias":"x"}}"#,
    ] {
        assert!(OperatorSpecV1::from_json(invalid).is_err());
    }
}

#[test]
fn project_binding_rejects_missing_wrong_duplicate_and_nullable_key() {
    let operation = OperatorOperationV1::MaterializedProject {
        key_column: "id".into(),
        value_column: "payload".into(),
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
            compile_graph(&spec(operation.clone()), &source(columns)),
            Err(expected)
        );
    }
}

#[test]
fn graph_uses_the_full_descriptor_order_and_resolved_slots() {
    let graph = compile_graph(
        &spec(OperatorOperationV1::MaterializedProject {
            key_column: "id".into(),
            value_column: "payload".into(),
        }),
        &source(vec![
            column("payload", 2, POSTGRES_INT8_TYPE_OID),
            required_column("id", 1),
            column("label", 3, shiba_compiler::POSTGRES_TEXT_TYPE_OID),
        ]),
    )
    .unwrap();
    assert_eq!(
        graph.sources[0]
            .layout
            .iter()
            .map(|binding| binding.address.sub_id)
            .collect::<Vec<_>>(),
        vec![2, 1, 3]
    );
    assert_eq!(
        graph.nodes[0].kind,
        OperatorNodeKind::Project {
            expressions: vec![
                Expression::Column { slot: 1 },
                Expression::Column { slot: 0 }
            ]
        }
    );
}
