use shiba_operator::{
    AggregateCall, AggregateFunctionV1, HavingError, HavingExpression, MAX_HAVING_DEPTH,
    MAX_HAVING_NODES, TypedValue, ValueType,
};

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

fn boolean_tree(level: usize) -> HavingExpression {
    if level == 0 {
        HavingExpression::IsNull {
            input: Box::new(HavingExpression::Call { ordinal: 1 }),
        }
    } else {
        HavingExpression::And {
            left: Box::new(boolean_tree(level - 1)),
            right: Box::new(boolean_tree(level - 1)),
        }
    }
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

#[test]
fn having_evaluate_rejects_zero_out_of_range_and_empty_ordinals() {
    let valid_calls = calls();
    for ordinal in [0, 3] {
        assert_eq!(
            HavingExpression::Call { ordinal }.evaluate(&[TypedValue::Int8(1)]),
            Err(HavingError::InvalidCall)
        );
        assert_eq!(
            HavingExpression::Call { ordinal }.validate(&valid_calls),
            Err(HavingError::InvalidCall)
        );
    }
    assert_eq!(
        HavingExpression::Call { ordinal: 1 }.evaluate(&[]),
        Err(HavingError::InvalidCall)
    );
    assert_eq!(
        HavingExpression::Call { ordinal: 1 }.validate(&[]),
        Err(HavingError::InvalidCall)
    );
}

#[test]
fn having_rejects_depth_node_and_boolean_budget_before_evaluation() {
    let mut deep = HavingExpression::Call { ordinal: 1 };
    for _ in 0..=MAX_HAVING_DEPTH {
        deep = HavingExpression::Not {
            input: Box::new(deep),
        };
    }
    assert_eq!(deep.validate(&calls()), Err(HavingError::DepthLimit));

    let mut many = HavingExpression::Call { ordinal: 1 };
    for _ in 0..MAX_HAVING_NODES {
        many = HavingExpression::IsNull {
            input: Box::new(many),
        };
    }
    assert!(many.validate(&calls()).is_err());

    let booleans = boolean_tree(6);
    assert_eq!(booleans.validate(&calls()), Err(HavingError::BooleanLimit));
}
