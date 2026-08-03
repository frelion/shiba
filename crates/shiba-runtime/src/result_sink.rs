use postgres::Transaction;
use shiba_operator::{
    EncodedOperatorState, KeyedMutation, OutputContract, OutputDelta, TypedValue, ValueType,
};

use crate::M2Error;

const MAX_KEYED_MUTATIONS: usize = 20_000;

pub(super) fn persist_state(
    transaction: &mut Transaction<'_>,
    operator_id: i64,
    state: &EncodedOperatorState,
) -> Result<(), M2Error> {
    let codec =
        i32::try_from(state.codec_version).map_err(|_| M2Error::InvalidOperatorDefinition)?;
    if transaction.execute(
        "UPDATE shiba_internal.operator_state SET codec_version = $1, state_payload = $2
         WHERE operator_id = $3",
        &[&codec, &state.payload, &operator_id],
    )? != 1
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

pub(super) fn persist_output(
    transaction: &mut Transaction<'_>,
    operator_id: i64,
    contract: &OutputContract,
    delta: OutputDelta,
    effect_count: usize,
    publish: bool,
) -> Result<(), M2Error> {
    match (contract, delta) {
        (OutputContract::Scalar { .. }, OutputDelta::ScalarReplacement { value }) => {
            if publish {
                update_active_value(transaction, operator_id, scalar_int8(&value)?)?;
            }
        }
        (OutputContract::KeyedRows { nullable, .. }, OutputDelta::KeyedMutations { mutations }) => {
            persist_keyed(transaction, operator_id, mutations, effect_count, *nullable)?;
        }
        _ => return Err(M2Error::InvalidOperatorDefinition),
    }
    Ok(())
}

fn persist_keyed(
    transaction: &mut Transaction<'_>,
    operator_id: i64,
    mutations: Vec<KeyedMutation>,
    effect_count: usize,
    nullable: bool,
) -> Result<(), M2Error> {
    if mutations.len() > MAX_KEYED_MUTATIONS || mutations.len() > effect_count.saturating_mul(2) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    let mut deletes = Vec::new();
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for mutation in mutations {
        match mutation {
            KeyedMutation::Delete {
                key: TypedValue::Int8(key),
            } => deletes.push(key),
            KeyedMutation::Delete { .. } => return Err(M2Error::InvalidOperatorDefinition),
            KeyedMutation::Upsert { key, value } => {
                let TypedValue::Int8(key) = key else {
                    return Err(M2Error::InvalidOperatorDefinition);
                };
                keys.push(key);
                values.push(match value {
                    TypedValue::Null(ValueType::Int8) if nullable => None,
                    TypedValue::Int8(value) => Some(value),
                    _ => return Err(M2Error::InvalidOperatorDefinition),
                });
            }
        }
    }
    if !deletes.is_empty() {
        transaction.execute(
            "DELETE FROM shiba_internal.operator_result_row
             WHERE operator_id = $1 AND result_key_bigint = ANY($2)",
            &[&operator_id, &deletes],
        )?;
    }
    if !keys.is_empty() {
        transaction.execute(
            "INSERT INTO shiba_internal.operator_result_row
                 (operator_id, result_key_bigint, result_value_bigint)
             SELECT $1, input.key, input.value
             FROM unnest($2::bigint[], $3::bigint[]) AS input(key, value)
             ON CONFLICT (operator_id, result_key_bigint)
             DO UPDATE SET result_value_bigint = EXCLUDED.result_value_bigint",
            &[&operator_id, &keys, &values],
        )?;
    }
    Ok(())
}

pub(super) fn scalar_int8(value: &TypedValue) -> Result<i64, M2Error> {
    match value {
        TypedValue::Int8(value) => Ok(*value),
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
}

fn update_active_value(
    transaction: &mut Transaction<'_>,
    operator_id: i64,
    value: i64,
) -> Result<(), M2Error> {
    if transaction.execute(
        "UPDATE shiba.operator_result SET value_bigint = $1
         WHERE operator_id = $2 AND result_status = 'active' AND output_shape = 'scalar'",
        &[&value, &operator_id],
    )? != 1
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

pub(super) fn activate_header(
    transaction: &mut Transaction<'_>,
    operator_id: i64,
    shape: &str,
    value: Option<i64>,
) -> Result<(), M2Error> {
    if transaction.execute(
        "UPDATE shiba.operator_result SET result_status = 'active', value_bigint = $1
         WHERE operator_id = $2 AND result_status = 'building' AND output_shape = $3",
        &[&value, &operator_id, &shape],
    )? != 1
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}
