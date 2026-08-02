use core::num::NonZeroU64;

use shiba_operator::{
    CompiledOperator, CompiledOperatorKind, ObjectAddress, OperatorError, OperatorId, RowEffect,
    RowImage, Value, apply_operator,
};
use shiba_protocol::SourceId;

fn image(value: Value) -> RowImage {
    RowImage {
        source_row_id: Some(1),
        source_row_sub_id: None,
        payload: value,
    }
}

fn operator(kind: CompiledOperatorKind) -> CompiledOperator {
    CompiledOperator {
        operator_id: OperatorId::new(NonZeroU64::new(1).unwrap()),
        source_id: SourceId::new(1).unwrap(),
        kind,
    }
}

#[test]
fn count_rows_reference_model_tracks_row_presence() {
    let effects = [
        RowEffect {
            before: None,
            after: Some(image(Value::Int8(10))),
        },
        RowEffect {
            before: Some(image(Value::Null)),
            after: Some(image(Value::Int8(7))),
        },
        RowEffect {
            before: Some(image(Value::Int8(7))),
            after: None,
        },
    ];
    assert_eq!(
        apply_operator(&operator(CompiledOperatorKind::CountRows), 0, &effects),
        Ok(0)
    );
    assert_eq!(
        apply_operator(
            &operator(CompiledOperatorKind::CountRows),
            0,
            &[RowEffect {
                before: Some(image(Value::Null)),
                after: None,
            }]
        ),
        Err(OperatorError::CountUnderflow)
    );
}

#[test]
fn sum_int8_reference_model_handles_insert_update_delete_and_null() {
    let sum = operator(CompiledOperatorKind::SumInt8 {
        input: ObjectAddress {
            class_id: 1,
            object_id: 2,
            sub_id: 3,
        },
    });
    let effects = [
        RowEffect {
            before: None,
            after: Some(image(Value::Int8(10))),
        },
        RowEffect {
            before: None,
            after: Some(image(Value::Null)),
        },
        RowEffect {
            before: Some(image(Value::Null)),
            after: Some(image(Value::Int8(7))),
        },
        RowEffect {
            before: Some(image(Value::Int8(10))),
            after: Some(image(Value::Null)),
        },
        RowEffect {
            before: Some(image(Value::Int8(7))),
            after: None,
        },
    ];
    assert_eq!(apply_operator(&sum, 0, &effects), Ok(0));
}

#[test]
fn sum_int8_fails_closed_for_bad_values_and_overflow() {
    let sum = operator(CompiledOperatorKind::SumInt8 {
        input: ObjectAddress {
            class_id: 1,
            object_id: 2,
            sub_id: 3,
        },
    });
    for (value, expected) in [
        (Value::Absent, OperatorError::AbsentSumInput),
        (Value::Text("7".into()), OperatorError::InvalidSumInputType),
    ] {
        assert_eq!(
            apply_operator(
                &sum,
                0,
                &[RowEffect {
                    before: None,
                    after: Some(image(value)),
                }]
            ),
            Err(expected)
        );
    }
    assert_eq!(
        apply_operator(
            &sum,
            i64::MAX,
            &[RowEffect {
                before: None,
                after: Some(image(Value::Int8(1))),
            }]
        ),
        Err(OperatorError::ArithmeticOverflow)
    );
}
