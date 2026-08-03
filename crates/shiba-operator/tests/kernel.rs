use core::num::NonZeroU64;
use std::collections::BTreeMap;

use shiba_operator::{
    ColumnBinding, CompiledPlan, DeltaBatch, EffectOrigin, EncodedOperatorState, InputBinding,
    InputRole, KernelError, ObjectAddress, OperatorId, OutputContract, OutputDelta,
    PlanImplementation, RowDelta, TypedRow, TypedValue, ValueType, apply_plan, decode_state,
    initial_state, source_typed_layout,
};
use shiba_protocol::{
    IngressTransactionId, PostgresLsn, SlotGeneration, SourceId, SourceTransactionId,
};

fn address(sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        class_id: 1_259,
        object_id: 16_384,
        sub_id,
    }
}

fn source_id() -> SourceId {
    SourceId::new(1).unwrap()
}

fn plan(id: u64, implementation: PlanImplementation) -> CompiledPlan {
    let inputs = match &implementation {
        PlanImplementation::CountRows => Vec::new(),
        PlanImplementation::SumInt8 { input, .. } => vec![InputBinding {
            role: InputRole::Payload,
            address: *input,
        }],
        PlanImplementation::Graph { .. } => unreachable!("graph tests own graph plans"),
    };
    CompiledPlan::build(
        OperatorId::new(NonZeroU64::new(id).unwrap()),
        source_id(),
        inputs,
        OutputContract::Scalar {
            value_type: ValueType::Int8,
        },
        implementation,
    )
    .unwrap()
}

fn origin() -> EffectOrigin {
    EffectOrigin::Wal(
        SourceTransactionId::new(
            source_id(),
            SlotGeneration::new(1).unwrap(),
            PostgresLsn::from_u64(1),
            IngressTransactionId::new(1).unwrap(),
        )
        .unwrap(),
    )
}

fn layout() -> shiba_operator::TypedLayout {
    source_typed_layout(
        source_id(),
        &[
            ColumnBinding {
                address: address(1),
                value_type: ValueType::Int8,
            },
            ColumnBinding {
                address: address(2),
                value_type: ValueType::Int8,
            },
        ],
    )
    .unwrap()
}

fn row(key: i64, payload: Option<i64>) -> TypedRow {
    let layout = layout();
    TypedRow::new(
        &layout,
        vec![
            TypedValue::Int8(key),
            payload.map_or(TypedValue::Null(ValueType::Int8), TypedValue::Int8),
        ],
    )
    .unwrap()
}

fn batch(delta: RowDelta) -> DeltaBatch {
    DeltaBatch {
        origin: origin(),
        layout_identity: layout().identity,
        rows: vec![delta],
    }
}

fn scalar(plan: &CompiledPlan, state: &EncodedOperatorState, batch: &DeltaBatch) -> i64 {
    let transition = apply_plan(plan, state, batch).unwrap();
    let OutputDelta::ScalarReplacement {
        value: TypedValue::Int8(value),
    } = transition.output_delta
    else {
        panic!("expected int8 scalar")
    };
    value
}

#[test]
fn count_and_sum_use_one_typed_batch_and_checked_state() {
    let count = plan(1, PlanImplementation::CountRows);
    let sum = plan(
        2,
        PlanImplementation::SumInt8 {
            input: address(2),
            input_slot: 1,
        },
    );
    let input = batch(RowDelta {
        before: None,
        after: Some(row(1, Some(10))),
    });
    assert_eq!(scalar(&count, &initial_state(&count).unwrap(), &input), 1);
    assert_eq!(scalar(&sum, &initial_state(&sum).unwrap(), &input), 10);
    let max = EncodedOperatorState {
        codec_version: 1,
        payload: i64::MAX.to_be_bytes().to_vec(),
    };
    assert_eq!(apply_plan(&sum, &max, &input), Err(KernelError::Overflow));
}

#[test]
fn corrupt_scalar_plan_state_and_absent_input_fail_closed() {
    let count = plan(1, PlanImplementation::CountRows);
    let mut corrupt = count.clone();
    corrupt.digest[0] ^= 1;
    assert_eq!(initial_state(&corrupt), Err(KernelError::InvalidPlan));
    let bad_state = EncodedOperatorState {
        codec_version: 1,
        payload: vec![0],
    };
    assert_eq!(
        decode_state(&count, &bad_state),
        Err(KernelError::InvalidState)
    );

    let sum = plan(
        2,
        PlanImplementation::SumInt8 {
            input: address(2),
            input_slot: 1,
        },
    );
    let typed_layout = layout();
    let absent =
        TypedRow::new(&typed_layout, vec![TypedValue::Int8(1), TypedValue::Absent]).unwrap();
    assert_eq!(
        apply_plan(
            &sum,
            &initial_state(&sum).unwrap(),
            &batch(RowDelta {
                before: None,
                after: Some(absent),
            }),
        ),
        Err(KernelError::AbsentInput)
    );
}

#[test]
fn deterministic_random_sequence_matches_count_and_sum_models() {
    let count = plan(1, PlanImplementation::CountRows);
    let sum = plan(
        2,
        PlanImplementation::SumInt8 {
            input: address(2),
            input_slot: 1,
        },
    );
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
        let input = batch(RowDelta {
            before: before.map(|value| row(key, value)),
            after: after.map(|value| row(key, value)),
        });
        count_state = apply_plan(&count, &count_state, &input).unwrap().next_state;
        sum_state = apply_plan(&sum, &sum_state, &input).unwrap().next_state;
        match after {
            Some(value) => {
                rows.insert(key, value);
            }
            None => {
                rows.remove(&key);
            }
        }
        assert_eq!(
            decode_state(&count, &count_state).unwrap(),
            i64::try_from(rows.len()).unwrap()
        );
        assert_eq!(
            decode_state(&sum, &sum_state).unwrap(),
            rows.values().flatten().sum::<i64>()
        );
    }
}
