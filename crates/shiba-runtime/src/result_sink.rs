use std::collections::BTreeSet;

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
        (
            OutputContract::KeyedRows {
                key_nullable,
                nullable,
                ..
            },
            OutputDelta::KeyedMutations { mutations },
        ) => {
            persist_keyed(
                transaction,
                operator_id,
                mutations,
                effect_count,
                *key_nullable,
                *nullable,
            )?;
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
    key_nullable: bool,
    nullable: bool,
) -> Result<(), M2Error> {
    if mutations.len() > MAX_KEYED_MUTATIONS || mutations.len() > effect_count.saturating_mul(2) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    let mut seen = BTreeSet::new();
    let mut deletes = Vec::<Vec<u8>>::new();
    let mut key_payloads = Vec::new();
    let mut keys = Vec::<Option<i64>>::new();
    let mut key_nulls = Vec::new();
    let mut values = Vec::new();
    let mut value_nulls = Vec::new();
    for mutation in mutations {
        match mutation {
            KeyedMutation::Delete { key } => {
                let (payload, _, _) = result_key(&key, key_nullable)?;
                if !seen.insert(payload.clone()) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                deletes.push(payload);
            }
            KeyedMutation::Upsert { key, value } => {
                let (payload, key, key_is_null) = result_key(&key, key_nullable)?;
                if !seen.insert(payload.clone()) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                key_payloads.push(payload);
                keys.push(key);
                key_nulls.push(key_is_null);
                let (value, value_is_null) = match value {
                    TypedValue::Null(ValueType::Int8) if nullable => (None, true),
                    TypedValue::Int8(value) => (Some(value), false),
                    _ => return Err(M2Error::InvalidOperatorDefinition),
                };
                values.push(value);
                value_nulls.push(value_is_null);
            }
        }
    }
    if !deletes.is_empty() {
        let changed = transaction.execute(
            "DELETE FROM shiba_internal.operator_result_row
             WHERE operator_id = $1 AND key_payload = ANY($2)",
            &[&operator_id, &deletes],
        )?;
        if usize::try_from(changed).ok() != Some(deletes.len()) {
            return Err(M2Error::InvalidOperatorDefinition);
        }
    }
    if !keys.is_empty() {
        let changed = transaction.execute(
            "INSERT INTO shiba_internal.operator_result_row
                 (operator_id, key_payload, result_key_is_null,
                  result_key_bigint, result_value_is_null, result_value_bigint)
             SELECT $1, input.key_payload, input.key_is_null,
                    input.key, input.value_is_null, input.value
             FROM unnest($2::bytea[], $3::boolean[], $4::bigint[],
                         $5::boolean[], $6::bigint[])
                  AS input(key_payload, key_is_null, key, value_is_null, value)
             ON CONFLICT (operator_id, key_payload)
             DO UPDATE SET result_key_is_null = EXCLUDED.result_key_is_null,
                           result_key_bigint = EXCLUDED.result_key_bigint,
                           result_value_is_null = EXCLUDED.result_value_is_null,
                           result_value_bigint = EXCLUDED.result_value_bigint",
            &[
                &operator_id,
                &key_payloads,
                &key_nulls,
                &keys,
                &value_nulls,
                &values,
            ],
        )?;
        if usize::try_from(changed).ok() != Some(keys.len()) {
            return Err(M2Error::InvalidOperatorDefinition);
        }
    }
    Ok(())
}

fn result_key(key: &TypedValue, nullable: bool) -> Result<(Vec<u8>, Option<i64>, bool), M2Error> {
    let payload = key
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    match key {
        TypedValue::Int8(value) => Ok((payload, Some(*value), false)),
        TypedValue::Null(ValueType::Int8) if nullable => Ok((payload, None, true)),
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
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
