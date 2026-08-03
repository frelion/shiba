use std::collections::BTreeMap;

use super::model::{Change, Function, ModelError, Payload, Plan, Row, Value};

pub(super) fn normalize(plan: &Plan, changes: &[Change]) -> Result<BTreeMap<Row, i64>, ModelError> {
    let mut normalized = BTreeMap::new();
    for change in changes {
        for (row, sign) in change
            .before
            .iter()
            .map(|row| (row, -1))
            .chain(change.after.iter().map(|row| (row, 1)))
        {
            validate_row(plan, row)?;
            let count = normalized.entry(row.clone()).or_insert(0_i64);
            *count = count.checked_add(sign).ok_or(ModelError::Overflow)?;
        }
    }
    normalized.retain(|_, count| *count != 0);
    Ok(normalized)
}

fn validate_row(plan: &Plan, row: &Row) -> Result<(), ModelError> {
    if row.len() != plan.input_width {
        return Err(ModelError::Schema);
    }
    for call in &plan.calls {
        if let Some(slot) = call.function.slot()
            && !matches!(row.get(slot), Some(Value::Null | Value::Int8(_)))
        {
            return Err(ModelError::Schema);
        }
    }
    Ok(())
}

pub(super) fn payload_matches(function: &Function, payload: &Payload) -> bool {
    matches!(
        (function, payload),
        (
            Function::CountStar | Function::Count { .. },
            Payload::Count(_)
        ) | (Function::Sum { .. }, Payload::Sum { .. })
            | (
                Function::Min { .. } | Function::Max { .. },
                Payload::Extrema(_)
            )
    )
}

pub(super) fn validate_payload(payload: &Payload) -> Result<(), ModelError> {
    match payload {
        Payload::Count(count) if *count < 0 => Err(ModelError::Corrupt),
        Payload::Sum { non_null: 0, value } if *value != 0 => Err(ModelError::Corrupt),
        Payload::Extrema(values) if values.values().any(|count| *count == 0) => {
            Err(ModelError::Corrupt)
        }
        _ => Ok(()),
    }
}

pub(super) fn output_value(function: &Function, payload: &Payload) -> Value {
    match (function, payload) {
        (Function::CountStar | Function::Count { .. }, Payload::Count(value)) => {
            Value::Int8(*value)
        }
        (Function::Sum { .. }, Payload::Sum { non_null: 0, .. }) => Value::Null,
        (Function::Sum { .. }, Payload::Sum { value, .. }) => Value::Int8(*value),
        (Function::Min { .. }, Payload::Extrema(values)) => values
            .first_key_value()
            .map_or(Value::Null, |(value, _)| Value::Int8(*value)),
        (Function::Max { .. }, Payload::Extrema(values)) => values
            .last_key_value()
            .map_or(Value::Null, |(value, _)| Value::Int8(*value)),
        _ => unreachable!("validated plan and state have matching shapes"),
    }
}

pub(super) fn apply_call(
    function: &Function,
    payload: &mut Payload,
    row: &Row,
    multiplicity: i64,
) -> Result<(), ModelError> {
    let selected = function.slot().and_then(|slot| row.get(slot));
    match (function, payload, selected) {
        (Function::CountStar, Payload::Count(count), _)
        | (Function::Count { .. }, Payload::Count(count), Some(Value::Int8(_))) => {
            *count = checked_nonnegative_add(*count, multiplicity)?;
        }
        (Function::Count { .. }, Payload::Count(_), Some(Value::Null))
        | (Function::Sum { .. }, Payload::Sum { .. }, Some(Value::Null))
        | (Function::Min { .. } | Function::Max { .. }, Payload::Extrema(_), Some(Value::Null)) => {
        }
        (Function::Sum { .. }, Payload::Sum { non_null, value }, Some(Value::Int8(input))) => {
            *non_null = checked_u64_delta(*non_null, multiplicity)?;
            let delta = input
                .checked_mul(multiplicity)
                .ok_or(ModelError::Overflow)?;
            *value = value.checked_add(delta).ok_or(ModelError::Overflow)?;
            if *non_null == 0 {
                *value = 0;
            }
        }
        (
            Function::Min { .. } | Function::Max { .. },
            Payload::Extrema(values),
            Some(Value::Int8(input)),
        ) => {
            let old = values.get(input).copied().unwrap_or(0);
            let next = checked_u64_delta(old, multiplicity)?;
            if next == 0 {
                values.remove(input);
            } else {
                values.insert(*input, next);
            }
        }
        _ => return Err(ModelError::Corrupt),
    }
    Ok(())
}

fn checked_nonnegative_add(value: i64, delta: i64) -> Result<i64, ModelError> {
    let next = value.checked_add(delta).ok_or(ModelError::Overflow)?;
    if next < 0 {
        return Err(ModelError::RetractMissing);
    }
    Ok(next)
}

fn checked_u64_delta(value: u64, delta: i64) -> Result<u64, ModelError> {
    if delta >= 0 {
        value
            .checked_add(u64::try_from(delta).map_err(|_| ModelError::Overflow)?)
            .ok_or(ModelError::Overflow)
    } else {
        value
            .checked_sub(delta.unsigned_abs())
            .ok_or(ModelError::RetractMissing)
    }
}
