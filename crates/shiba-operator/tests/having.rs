use shiba_operator::{AggregateCall, AggregateFunctionV1, HavingExpression, TypedValue, ValueType};

fn calls() -> Vec<AggregateCall> {
    vec![
        AggregateCall {
            ordinal: 1,
            function_version: 1,
            function: AggregateFunctionV1::CountStar,
            expression: None,
        },
        AggregateCall {
            ordinal: 2,
            function_version: 1,
            function: AggregateFunctionV1::SumInt8,
            expression: None,
        },
    ]
}

#[test]
fn having_three_valued_visibility_transitions_are_deterministic() {
    let predicate = HavingExpression::And {
        left: Box::new(HavingExpression::Greater {
            left: Box::new(HavingExpression::Call { ordinal: 1 }),
            right: Box::new(HavingExpression::Int8Literal { value: 1 }),
        }),
        right: Box::new(HavingExpression::Not {
            input: Box::new(HavingExpression::IsNull {
                input: Box::new(HavingExpression::Call { ordinal: 2 }),
            }),
        }),
    };
    assert_eq!(predicate.validate(&calls()).unwrap(), ValueType::Bool);
    assert_eq!(
        predicate
            .evaluate(&[TypedValue::Int8(2), TypedValue::Int8(10)])
            .unwrap(),
        TypedValue::Bool(true)
    );
    assert_eq!(
        predicate
            .evaluate(&[TypedValue::Int8(1), TypedValue::Null(ValueType::Int8)])
            .unwrap(),
        TypedValue::Bool(false)
    );
}

#[test]
fn having_rejects_unknown_call_and_wrong_boolean_type() {
    let unknown = HavingExpression::Call { ordinal: 3 };
    assert!(unknown.validate(&calls()).is_err());
    let wrong = HavingExpression::And {
        left: Box::new(HavingExpression::Int8Literal { value: 1 }),
        right: Box::new(HavingExpression::Int8Literal { value: 2 }),
    };
    assert!(wrong.validate(&calls()).is_err());
}
