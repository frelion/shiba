use super::fixtures::{
    all_calls_plan, delete, grouped_plan, grouped_row, grouped_without_count_plan, update,
};
use super::grouped::{GroupedState, validate_batch_bounds, validate_graph_mutation_bounds};
use super::model::{
    Change, FORMAT_VERSION, FUNCTION_VERSION, MAX_EMITTED_RESULT_IMAGES,
    MAX_GRAPH_OUTPUT_MUTATIONS, MAX_GRAPH_STATE_MUTATIONS, MAX_TOUCHED_GROUPS, ModelError, Payload,
    State, StoredCall, StoredResult, Value,
};

#[test]
fn grouped_creation_deletion_and_key_change_retract_exactly() {
    let plan = grouped_plan();
    let mut state = GroupedState::empty();
    state
        .apply(
            &plan,
            &[
                Change {
                    before: None,
                    after: Some(grouped_row(1, Value::Int8(2))),
                },
                Change {
                    before: None,
                    after: Some(grouped_row(1, Value::Int8(2))),
                },
                Change {
                    before: None,
                    after: Some(grouped_row(2, Value::Null)),
                },
            ],
        )
        .unwrap();
    assert_eq!(
        state.output(&plan).get(&Value::Int8(1)),
        Some(&vec![
            Value::Int8(2),
            Value::Int8(4),
            Value::Int8(2),
            Value::Int8(2),
        ])
    );

    state
        .apply(
            &plan,
            &[Change {
                before: Some(grouped_row(1, Value::Int8(2))),
                after: Some(grouped_row(3, Value::Int8(7))),
            }],
        )
        .unwrap();
    assert_eq!(
        state.output(&plan).get(&Value::Int8(1)).unwrap()[0],
        Value::Int8(1)
    );
    assert!(state.output(&plan).contains_key(&Value::Int8(3)));
    state
        .apply(
            &plan,
            &[Change {
                before: Some(grouped_row(1, Value::Int8(2))),
                after: None,
            }],
        )
        .unwrap();
    assert!(!state.output(&plan).contains_key(&Value::Int8(1)));
}

#[test]
fn kernel_membership_namespace_supports_groups_without_count_star() {
    let plan = grouped_without_count_plan();
    let mut state = GroupedState::empty();
    let row = grouped_row(9, Value::Int8(4));
    state
        .apply(
            &plan,
            &[Change {
                before: None,
                after: Some(row.clone()),
            }],
        )
        .unwrap();
    assert_eq!(
        state.output(&plan).get(&Value::Int8(9)),
        Some(&vec![Value::Int8(4), Value::Int8(4)])
    );
    state
        .apply(
            &plan,
            &[Change {
                before: Some(row),
                after: None,
            }],
        )
        .unwrap();
    assert!(state.output(&plan).is_empty());
}

#[test]
fn result_schema_and_row_codec_reject_truncated_extra_type_nullability_and_version() {
    let plan = all_calls_plan();
    let valid = State::empty(&plan).unwrap().output(&plan).unwrap();
    assert_eq!(
        State::decode_result(
            &plan,
            StoredResult {
                version: FORMAT_VERSION,
                values: valid.clone(),
            }
        )
        .unwrap(),
        valid
    );
    let mut truncated = valid.clone();
    truncated.pop();
    let mut extra = valid.clone();
    extra.push(Value::Null);
    let mut wrong_type = valid.clone();
    wrong_type[2] = Value::Bool(true);
    let mut wrong_nullability = valid;
    wrong_nullability[0] = Value::Null;
    for stored in [
        StoredResult {
            version: FORMAT_VERSION,
            values: truncated,
        },
        StoredResult {
            version: FORMAT_VERSION,
            values: extra,
        },
        StoredResult {
            version: FORMAT_VERSION,
            values: wrong_type,
        },
        StoredResult {
            version: FORMAT_VERSION,
            values: wrong_nullability,
        },
        StoredResult {
            version: FORMAT_VERSION + 1,
            values: vec![Value::Int8(0); 5],
        },
    ] {
        assert!(State::decode_result(&plan, stored).is_err());
    }
    let encoded = State::empty(&plan).unwrap().encode(&plan);
    let mut short = encoded.clone();
    short.calls.pop();
    assert_eq!(State::decode(&plan, short), Err(ModelError::Corrupt));
    let mut long = encoded;
    long.calls.push(StoredCall {
        function_tag: "count_star".to_owned(),
        function_version: FUNCTION_VERSION,
        payload: Payload::Count(0),
    });
    assert_eq!(State::decode(&plan, long), Err(ModelError::Corrupt));
}

#[test]
fn grouped_state_and_output_mutation_bounds_accept_limit_and_reject_plus_one() {
    let plan = grouped_plan();
    let changes = |end| {
        (0..end)
            .map(|key| Change {
                before: None,
                after: Some(grouped_row(i64::try_from(key).unwrap(), Value::Int8(1))),
            })
            .collect::<Vec<_>>()
    };
    let mut state = GroupedState::empty();
    let counts = state.apply(&plan, &changes(MAX_TOUCHED_GROUPS)).unwrap();
    assert_eq!(counts.state, MAX_TOUCHED_GROUPS * (plan.calls.len() + 1));
    assert_eq!(counts.output, MAX_TOUCHED_GROUPS);
    let before = GroupedState::empty();
    let mut state = before.clone();
    assert_eq!(
        state.apply(&plan, &changes(MAX_TOUCHED_GROUPS + 1)),
        Err(ModelError::Bound)
    );
    assert_eq!(state, before);

    assert_eq!(
        validate_batch_bounds(
            MAX_TOUCHED_GROUPS,
            MAX_TOUCHED_GROUPS,
            MAX_EMITTED_RESULT_IMAGES
        ),
        Ok(())
    );
    assert_eq!(
        validate_batch_bounds(
            MAX_TOUCHED_GROUPS,
            MAX_TOUCHED_GROUPS,
            MAX_EMITTED_RESULT_IMAGES + 1
        ),
        Err(ModelError::Bound)
    );
    assert_eq!(
        validate_graph_mutation_bounds(MAX_GRAPH_STATE_MUTATIONS, MAX_GRAPH_OUTPUT_MUTATIONS),
        Ok(())
    );
    assert_eq!(
        validate_graph_mutation_bounds(MAX_GRAPH_STATE_MUTATIONS + 1, MAX_GRAPH_OUTPUT_MUTATIONS),
        Err(ModelError::Bound)
    );
    assert_eq!(
        validate_graph_mutation_bounds(MAX_GRAPH_STATE_MUTATIONS, MAX_GRAPH_OUTPUT_MUTATIONS + 1),
        Err(ModelError::Bound)
    );
}

#[test]
fn fixed_seed_randomized_iud_matches_independent_reference() {
    let plan = all_calls_plan();
    let mut state = State::empty(&plan).unwrap();
    let mut rows = Vec::<Value>::new();
    let mut seed = 0x6d31_365f_6167_6772_u64;
    for _ in 0..1_000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let value = if seed.is_multiple_of(5) {
            Value::Null
        } else {
            Value::Int8(i64::try_from(seed % 101).unwrap() - 50)
        };
        let change = if rows.is_empty() {
            rows.push(value.clone());
            Change {
                before: None,
                after: Some(vec![value]),
            }
        } else {
            match seed % 3 {
                0 => {
                    rows.push(value.clone());
                    Change {
                        before: None,
                        after: Some(vec![value]),
                    }
                }
                1 => {
                    let index = usize::try_from(seed).unwrap() % rows.len();
                    let before = std::mem::replace(&mut rows[index], value.clone());
                    update(before, value)
                }
                _ => {
                    let index = usize::try_from(seed).unwrap() % rows.len();
                    delete(rows.swap_remove(index))
                }
            }
        };
        state.apply(&plan, &[change]).unwrap();
        let non_null = rows
            .iter()
            .filter_map(|value| match value {
                Value::Int8(value) => Some(*value),
                Value::Null => None,
                Value::Bool(_) => unreachable!(),
            })
            .collect::<Vec<_>>();
        let extrema = |select_min: bool| {
            let value = if select_min {
                non_null.iter().min()
            } else {
                non_null.iter().max()
            };
            value.copied().map_or(Value::Null, Value::Int8)
        };
        assert_eq!(
            state.output(&plan),
            Some(vec![
                Value::Int8(i64::try_from(rows.len()).unwrap()),
                Value::Int8(i64::try_from(non_null.len()).unwrap()),
                if non_null.is_empty() {
                    Value::Null
                } else {
                    Value::Int8(non_null.iter().sum())
                },
                extrema(true),
                extrema(false),
            ])
        );
    }
}
