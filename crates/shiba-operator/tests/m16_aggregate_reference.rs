#[path = "m16_aggregate_reference/fixtures.rs"]
mod fixtures;
#[path = "m16_aggregate_reference/grouped.rs"]
mod grouped;
#[path = "m16_aggregate_reference/grouped_cases.rs"]
mod grouped_cases;
#[path = "m16_aggregate_reference/model.rs"]
mod model;
#[path = "m16_aggregate_reference/transition.rs"]
mod transition;

use std::collections::BTreeMap;

use fixtures::{all_calls_plan, delete, having_sum_plan, insert, update};
use model::{
    Call, Change, FORMAT_VERSION, FUNCTION_VERSION, Function, MAX_CALLS, MAX_CHANGES,
    MAX_ROW_WIDTH, ModelError, Payload, Plan, STATE_CODEC_VERSION, State, StoredCall, StoredState,
    Value,
};

#[test]
fn count_star_count_expr_sum_min_max_follow_null_and_empty_semantics() {
    let plan = all_calls_plan();
    let mut state = State::empty(&plan).unwrap();
    assert_eq!(
        state.output(&plan),
        Some(vec![
            Value::Int8(0),
            Value::Int8(0),
            Value::Null,
            Value::Null,
            Value::Null,
        ])
    );

    state
        .apply(
            &plan,
            &[
                insert(Value::Int8(7)),
                insert(Value::Null),
                insert(Value::Int8(-2)),
            ],
        )
        .unwrap();
    assert_eq!(
        state.output(&plan),
        Some(vec![
            Value::Int8(3),
            Value::Int8(2),
            Value::Int8(5),
            Value::Int8(-2),
            Value::Int8(7),
        ])
    );
}

#[test]
fn insert_update_delete_retract_exact_multiplicity_and_normalize() {
    let plan = all_calls_plan();
    let mut state = State::empty(&plan).unwrap();
    state
        .apply(
            &plan,
            &[
                insert(Value::Int8(2)),
                insert(Value::Int8(2)),
                insert(Value::Int8(9)),
            ],
        )
        .unwrap();
    state.apply(&plan, &[delete(Value::Int8(2))]).unwrap();
    assert_eq!(state.output(&plan).unwrap()[3], Value::Int8(2));

    state
        .apply(
            &plan,
            &[
                update(Value::Int8(2), Value::Int8(4)),
                update(Value::Int8(9), Value::Int8(4)),
                update(Value::Int8(4), Value::Int8(4)),
            ],
        )
        .unwrap();
    assert_eq!(
        state.output(&plan),
        Some(vec![
            Value::Int8(2),
            Value::Int8(2),
            Value::Int8(8),
            Value::Int8(4),
            Value::Int8(4),
        ])
    );

    let before = state.clone();
    assert_eq!(
        state.apply(&plan, &[delete(Value::Int8(99))]),
        Err(ModelError::RetractMissing)
    );
    assert_eq!(state, before, "failed normalized batch must be atomic");
}

#[test]
fn multi_call_ordinals_are_stable_and_round_trip_exactly() {
    let plan = all_calls_plan();
    let mut state = State::empty(&plan).unwrap();
    state.apply(&plan, &[insert(Value::Int8(5))]).unwrap();
    let encoded = state.encode(&plan);
    assert_eq!(State::decode(&plan, encoded).unwrap(), state);

    let mut reordered = plan.clone();
    reordered.calls.swap(0, 1);
    assert_eq!(reordered.validate(), Err(ModelError::Corrupt));
}

#[test]
fn having_visibility_tracks_null_false_and_true_transitions() {
    let plan = having_sum_plan();
    let mut state = State::empty(&plan).unwrap();
    assert_eq!(state.output(&plan), None, "NULL HAVING is not visible");
    state.apply(&plan, &[insert(Value::Int8(-1))]).unwrap();
    assert_eq!(state.output(&plan), None, "false HAVING is not visible");
    state.apply(&plan, &[insert(Value::Int8(3))]).unwrap();
    assert!(state.output(&plan).is_some(), "true HAVING is visible");
    state.apply(&plan, &[delete(Value::Int8(3))]).unwrap();
    assert_eq!(state.output(&plan), None);
    state
        .apply(&plan, &[update(Value::Int8(-1), Value::Null)])
        .unwrap();
    assert_eq!(state.output(&plan), None, "retraction restores NULL");
}

#[test]
fn schema_row_codec_and_unknown_function_fail_closed() {
    let plan = all_calls_plan();
    let state = State::empty(&plan).unwrap();
    assert_eq!(
        state.clone().apply(&plan, &[insert(Value::Bool(true))]),
        Err(ModelError::Schema)
    );
    let mut wrong_width = all_calls_plan();
    wrong_width.input_width = 2;
    let mut state = State::empty(&wrong_width).unwrap();
    assert_eq!(
        state.apply(&wrong_width, &[insert(Value::Int8(1))]),
        Err(ModelError::Schema)
    );

    let mut corrupt = State::empty(&plan).unwrap().encode(&plan);
    corrupt.codec_version = STATE_CODEC_VERSION + 1;
    assert_eq!(State::decode(&plan, corrupt), Err(ModelError::UnknownCodec));
    let mut corrupt = State::empty(&plan).unwrap().encode(&plan);
    corrupt.calls[0].function_tag = "avg".to_owned();
    assert_eq!(
        State::decode(&plan, corrupt),
        Err(ModelError::UnknownFunction)
    );
    let mut corrupt = State::empty(&plan).unwrap().encode(&plan);
    corrupt.calls[2].payload = Payload::Count(0);
    assert_eq!(
        State::decode(&plan, corrupt),
        Err(ModelError::UnknownFunction)
    );
    let mut corrupt = State::empty(&plan).unwrap().encode(&plan);
    corrupt.calls[1].function_version = FUNCTION_VERSION + 1;
    assert_eq!(
        State::decode(&plan, corrupt),
        Err(ModelError::UnknownVersion)
    );
    let mut unknown_plan = plan.clone();
    unknown_plan.version = FORMAT_VERSION + 1;
    assert_eq!(unknown_plan.validate(), Err(ModelError::UnknownVersion));
}

#[test]
fn checked_arithmetic_and_corrupt_state_are_atomic() {
    let plan = all_calls_plan();
    let mut state = State::empty(&plan).unwrap();
    state
        .apply(&plan, &[insert(Value::Int8(i64::MAX))])
        .unwrap();
    let before = state.clone();
    assert_eq!(
        state.apply(&plan, &[insert(Value::Int8(1))]),
        Err(ModelError::Overflow)
    );
    assert_eq!(state, before);

    let mut count_overflow = State::empty(&plan).unwrap();
    *count_overflow.stored_mut(0) = Payload::Count(i64::MAX);
    assert_eq!(
        count_overflow.apply(&plan, &[insert(Value::Null)]),
        Err(ModelError::Overflow)
    );

    let stored = StoredState {
        codec_version: STATE_CODEC_VERSION,
        calls: vec![
            StoredCall {
                function_tag: "count_star".to_owned(),
                function_version: FUNCTION_VERSION,
                payload: Payload::Count(-1),
            },
            StoredCall {
                function_tag: "count".to_owned(),
                function_version: FUNCTION_VERSION,
                payload: Payload::Count(0),
            },
            StoredCall {
                function_tag: "sum".to_owned(),
                function_version: FUNCTION_VERSION,
                payload: Payload::Sum {
                    non_null: 0,
                    value: 1,
                },
            },
            StoredCall {
                function_tag: "min".to_owned(),
                function_version: FUNCTION_VERSION,
                payload: Payload::Extrema(BTreeMap::new()),
            },
            StoredCall {
                function_tag: "max".to_owned(),
                function_version: FUNCTION_VERSION,
                payload: Payload::Extrema(BTreeMap::new()),
            },
        ],
    };
    assert_eq!(State::decode(&plan, stored), Err(ModelError::Corrupt));
}

#[test]
fn bounds_accept_exact_limit_and_reject_bound_plus_one() {
    let calls = (1..=MAX_CALLS)
        .map(|ordinal| Call {
            ordinal,
            function_version: FUNCTION_VERSION,
            function: Function::CountStar,
        })
        .collect();
    let plan = Plan {
        version: FORMAT_VERSION,
        input_width: MAX_ROW_WIDTH,
        calls,
        having: None,
    };
    let mut state = State::empty(&plan).unwrap();
    let changes = vec![
        Change {
            before: None,
            after: Some(vec![Value::Null; MAX_ROW_WIDTH]),
        };
        MAX_CHANGES
    ];
    state.apply(&plan, &changes).unwrap();

    let mut too_many_calls = plan.clone();
    too_many_calls.calls.push(Call {
        ordinal: MAX_CALLS + 1,
        function_version: FUNCTION_VERSION,
        function: Function::CountStar,
    });
    assert_eq!(too_many_calls.validate(), Err(ModelError::Bound));
    let mut too_wide = plan.clone();
    too_wide.input_width = MAX_ROW_WIDTH + 1;
    assert_eq!(too_wide.validate(), Err(ModelError::Bound));
    let mut state = State::empty(&plan).unwrap();
    assert_eq!(
        state.apply(
            &plan,
            &vec![
                Change {
                    before: None,
                    after: None
                };
                MAX_CHANGES + 1
            ]
        ),
        Err(ModelError::Bound)
    );
}
