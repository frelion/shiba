use core::num::NonZeroU64;

use shiba_compiler::{
    OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1, SourceColumnDescriptor,
    SourceDescriptor, compile_plan,
};
use shiba_operator::{
    NodeInput, ObjectAddress, OperatorId, OperatorNodeKind, OutputContract, PlanImplementation,
    ValueType,
};
use shiba_protocol::SourceId;

fn address(sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        class_id: 1_259,
        object_id: 16_384,
        sub_id,
    }
}

fn source(key_nullable: bool) -> SourceDescriptor {
    SourceDescriptor {
        source_id: SourceId::new(1).unwrap(),
        relation: address(0),
        columns: vec![
            SourceColumnDescriptor {
                name: "payload".into(),
                address: address(3),
                type_oid: 20,
                nullable: true,
            },
            SourceColumnDescriptor {
                name: "group_key".into(),
                address: address(2),
                type_oid: 20,
                nullable: key_nullable,
            },
            SourceColumnDescriptor {
                name: "id".into(),
                address: address(1),
                type_oid: 20,
                nullable: false,
            },
        ],
    }
}

fn spec(operation: OperatorOperationV1) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(7).unwrap()),
        source_id: SourceId::new(1).unwrap(),
        operation,
    }
}

#[test]
fn grouped_declarations_compile_exact_slots_and_nullable_contracts() {
    for (operation, sum) in [
        (
            OperatorOperationV1::GroupedCount {
                key_column: "group_key".into(),
            },
            false,
        ),
        (
            OperatorOperationV1::GroupedSumInt8 {
                key_column: "group_key".into(),
                input_column: "payload".into(),
            },
            true,
        ),
    ] {
        let first = compile_plan(&spec(operation.clone()), &source(true)).unwrap();
        let second = compile_plan(&spec(operation), &source(true)).unwrap();
        assert_eq!(first.canonical_payload, second.canonical_payload);
        assert_eq!(first.digest, second.digest);
        assert_eq!(
            first.output_contract,
            OutputContract::KeyedRows {
                key_type: ValueType::Int8,
                key_nullable: true,
                value_type: ValueType::Int8,
                nullable: sum,
            }
        );
        let PlanImplementation::Graph { graph } = first.implementation else {
            panic!("expected graph")
        };
        assert_eq!(
            graph.sources[0]
                .layout
                .iter()
                .map(|binding| binding.address.sub_id)
                .collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
        assert!(matches!(
            graph.nodes[0].kind,
            OperatorNodeKind::KeyBy { .. }
        ));
        assert!(matches!(graph.nodes[1].input, NodeInput::Node(_)));
        assert_eq!(graph.nodes[1].state_contract.unwrap().codec_version, 1);
        match graph.nodes[1].kind {
            OperatorNodeKind::GroupedCount { key_slot } => assert_eq!(key_slot, 3),
            OperatorNodeKind::GroupedSumInt8 {
                key_slot,
                value_slot,
            } => assert_eq!((key_slot, value_slot), (3, 0)),
            _ => panic!("expected grouped aggregate"),
        }
    }
}

#[test]
fn grouped_ir_is_strict_and_binding_failures_are_closed() {
    let declaration = spec(OperatorOperationV1::GroupedSumInt8 {
        key_column: "group_key".into(),
        input_column: "payload".into(),
    });
    let json = declaration.to_canonical_json().unwrap();
    assert_eq!(OperatorSpecV1::from_json(&json).unwrap(), declaration);
    for invalid in [
        br#"{"version":1,"operator_id":7,"source_id":1,"operation":{"kind":"grouped_count","key_column":""}}"#.as_slice(),
        br#"{"version":1,"operator_id":7,"source_id":1,"operation":{"kind":"grouped_sum_int8","key_column":"group_key","input_column":" "}}"#,
        br#"{"version":1,"operator_id":7,"source_id":1,"operation":{"kind":"grouped_count","key_column":"group_key","alias":"x"}}"#,
    ] {
        assert!(OperatorSpecV1::from_json(invalid).is_err());
    }
    let mut wrong = source(false);
    wrong.columns[1].type_oid = 25;
    assert!(compile_plan(&declaration, &wrong).is_err());
    let mut duplicate = source(false);
    duplicate.columns.push(duplicate.columns[1].clone());
    assert!(compile_plan(&declaration, &duplicate).is_err());
}
