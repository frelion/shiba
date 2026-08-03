use core::num::NonZeroU64;
use std::collections::BTreeMap;

use shiba_operator::{
    CompiledPlan, EncodedOperatorState, InputBinding, InputRole, KernelError, KeyedMutation,
    ObjectAddress, OperatorId, OutputContract, OutputDelta, PlanImplementation, RowEffect,
    RowImage, ScalarValue, Value, ValueType, apply_plan, decode_state, initial_state,
};
use shiba_protocol::SourceId;

fn address(sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        class_id: 1_259,
        object_id: 16_384,
        sub_id,
    }
}

fn plan(id: u64, implementation: PlanImplementation) -> CompiledPlan {
    let output_contract = match &implementation {
        PlanImplementation::ProjectRows { .. } => OutputContract::KeyedRows {
            key_type: ValueType::Int8,
            value_type: ValueType::Int8,
            nullable: true,
        },
        _ => OutputContract::Scalar {
            value_type: ValueType::Int8,
        },
    };
    let inputs = match &implementation {
        PlanImplementation::CountRows => Vec::new(),
        PlanImplementation::SumInt8 { input } => vec![InputBinding {
            role: InputRole::Payload,
            address: *input,
        }],
        PlanImplementation::ProjectRows { key, value } => vec![
            InputBinding {
                role: InputRole::Key,
                address: *key,
            },
            InputBinding {
                role: InputRole::Payload,
                address: *value,
            },
        ],
    };
    CompiledPlan::build(
        OperatorId::new(NonZeroU64::new(id).unwrap()),
        SourceId::new(1).unwrap(),
        inputs,
        output_contract,
        implementation,
    )
    .unwrap()
}

fn image(key: i64, payload: Value) -> RowImage {
    RowImage {
        source_row_id: Some(key),
        source_row_sub_id: None,
        payload,
    }
}

fn apply_scalar(
    plan: &CompiledPlan,
    state: &EncodedOperatorState,
    effects: &[RowEffect],
) -> (EncodedOperatorState, i64) {
    let transition = apply_plan(plan, state, effects).unwrap();
    let OutputDelta::ScalarReplacement {
        value: ScalarValue::Int8(value),
    } = transition.output_delta
    else {
        panic!("expected int8 scalar")
    };
    (transition.next_state, value)
}

#[test]
fn count_and_sum_use_opaque_checked_state() {
    let count = plan(1, PlanImplementation::CountRows);
    let sum = plan(2, PlanImplementation::SumInt8 { input: address(2) });
    let effects = [
        RowEffect {
            before: None,
            after: Some(image(1, Value::Int8(10))),
        },
        RowEffect {
            before: None,
            after: Some(image(2, Value::Null)),
        },
        RowEffect {
            before: Some(image(2, Value::Null)),
            after: Some(image(2, Value::Int8(7))),
        },
        RowEffect {
            before: Some(image(1, Value::Int8(10))),
            after: Some(image(1, Value::Null)),
        },
        RowEffect {
            before: Some(image(2, Value::Int8(7))),
            after: None,
        },
    ];
    let (_, count_value) = apply_scalar(&count, &initial_state(&count).unwrap(), &effects);
    let (_, sum_value) = apply_scalar(&sum, &initial_state(&sum).unwrap(), &effects);
    assert_eq!((count_value, sum_value), (1, 0));

    let max = EncodedOperatorState {
        codec_version: 1,
        payload: i64::MAX.to_be_bytes().to_vec(),
    };
    assert_eq!(
        apply_plan(
            &sum,
            &max,
            &[RowEffect {
                before: None,
                after: Some(image(3, Value::Int8(1)))
            }]
        ),
        Err(KernelError::Overflow)
    );
}

#[test]
fn project_rows_emits_typed_withdrawals_upserts_and_null() {
    let project = plan(
        3,
        PlanImplementation::ProjectRows {
            key: address(1),
            value: address(2),
        },
    );
    let transition = apply_plan(
        &project,
        &initial_state(&project).unwrap(),
        &[
            RowEffect {
                before: None,
                after: Some(image(1, Value::Int8(10))),
            },
            RowEffect {
                before: Some(image(2, Value::Null)),
                after: Some(image(4, Value::Int8(7))),
            },
            RowEffect {
                before: Some(image(5, Value::Int8(9))),
                after: None,
            },
        ],
    )
    .unwrap();
    assert_eq!(
        transition.output_delta,
        OutputDelta::KeyedMutations {
            mutations: vec![
                KeyedMutation::Upsert {
                    key: 1,
                    value: ScalarValue::Int8(10)
                },
                KeyedMutation::Delete { key: 2 },
                KeyedMutation::Upsert {
                    key: 4,
                    value: ScalarValue::Int8(7)
                },
                KeyedMutation::Delete { key: 5 },
            ]
        }
    );
    let null_insert = apply_plan(
        &project,
        &initial_state(&project).unwrap(),
        &[RowEffect {
            before: None,
            after: Some(image(8, Value::Null)),
        }],
    )
    .unwrap();
    assert!(matches!(
        null_insert.output_delta,
        OutputDelta::KeyedMutations { ref mutations }
            if mutations == &[KeyedMutation::Upsert { key: 8, value: ScalarValue::Null }]
    ));
}

#[test]
fn corrupt_plan_state_input_and_output_amplification_fail_closed() {
    let project = plan(
        3,
        PlanImplementation::ProjectRows {
            key: address(1),
            value: address(2),
        },
    );
    let mut corrupt_plan = project.clone();
    corrupt_plan.digest[0] ^= 1;
    assert_eq!(initial_state(&corrupt_plan), Err(KernelError::InvalidPlan));
    assert_eq!(
        CompiledPlan::from_canonical_payload(&project.canonical_payload, project.digest),
        Ok(project.clone())
    );
    let mut wrong_digest = project.digest;
    wrong_digest[0] ^= 1;
    assert!(
        CompiledPlan::from_canonical_payload(&project.canonical_payload, wrong_digest).is_err()
    );
    assert!(
        CompiledPlan::build(
            OperatorId::new(NonZeroU64::new(9).unwrap()),
            SourceId::new(1).unwrap(),
            vec![InputBinding {
                role: InputRole::Payload,
                address: address(2),
            }],
            OutputContract::Scalar {
                value_type: ValueType::Int8,
            },
            PlanImplementation::CountRows,
        )
        .is_err()
    );
    for state in [
        EncodedOperatorState {
            codec_version: 2,
            payload: Vec::new(),
        },
        EncodedOperatorState {
            codec_version: 1,
            payload: vec![0],
        },
    ] {
        assert_eq!(
            decode_state(&project, &state),
            Err(KernelError::InvalidState)
        );
    }
    assert_eq!(
        apply_plan(
            &project,
            &initial_state(&project).unwrap(),
            &[RowEffect {
                before: None,
                after: Some(image(1, Value::Absent))
            }]
        ),
        Err(KernelError::AbsentInput)
    );
    let too_many = (0..10_001)
        .map(|key| RowEffect {
            before: None,
            after: Some(image(key, Value::Null)),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        apply_plan(&project, &initial_state(&project).unwrap(), &too_many),
        Err(KernelError::OutputLimit)
    );
}

#[test]
fn deterministic_random_sequence_matches_reference_models() {
    let count = plan(1, PlanImplementation::CountRows);
    let sum = plan(2, PlanImplementation::SumInt8 { input: address(2) });
    let mut count_state = initial_state(&count).unwrap();
    let mut sum_state = initial_state(&sum).unwrap();
    let mut rows = BTreeMap::<i64, Option<i64>>::new();
    let mut seed = 0x5eed_u64;
    for _ in 0..1_000 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let key = i64::try_from(seed % 32).unwrap();
        let before = rows.get(&key).copied();
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let after = match seed % 3 {
            0 => None,
            1 => Some(None),
            _ => Some(Some(i64::try_from((seed >> 8) % 101).unwrap() - 50)),
        };
        if before.is_none() && after.is_none() {
            continue;
        }
        let effect = RowEffect {
            before: before.map(|value| image(key, value.map_or(Value::Null, Value::Int8))),
            after: after.map(|value| image(key, value.map_or(Value::Null, Value::Int8))),
        };
        let (next_count, _) = apply_scalar(&count, &count_state, std::slice::from_ref(&effect));
        let (next_sum, _) = apply_scalar(&sum, &sum_state, &[effect]);
        count_state = next_count;
        sum_state = next_sum;
        match after {
            Some(value) => {
                rows.insert(key, value);
            }
            None => {
                rows.remove(&key);
            }
        }
        let expected_sum: i64 = rows.values().copied().flatten().sum();
        assert_eq!(
            decode_state(&count, &count_state).unwrap(),
            i64::try_from(rows.len()).unwrap()
        );
        assert_eq!(decode_state(&sum, &sum_state).unwrap(), expected_sum);
    }
}
