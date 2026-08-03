use std::collections::BTreeSet;

use postgres::Transaction;
use shiba_operator::{ResultMutation, TypedValue, ValueType};

use super::MAX_GRAPH_RESULT_MUTATIONS;
use crate::M2Error;

type Key = (Vec<u8>, Option<i64>, bool);
type Value = (Vec<u8>, Option<i64>, bool);

pub(super) fn persist(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    mutations: Vec<ResultMutation>,
    key_nullable: bool,
    value_nullable: bool,
) -> Result<(), M2Error> {
    if mutations.len() > MAX_GRAPH_RESULT_MUTATIONS {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    let mut seen = BTreeSet::new();
    let mut deletes = Vec::new();
    let mut upserts = Vec::new();
    for mutation in mutations {
        match mutation {
            ResultMutation::Delete { key } => {
                let key = typed_value(&key, key_nullable)?;
                if !seen.insert(key.0.clone()) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                deletes.push(key.0);
            }
            ResultMutation::Upsert { key, value } => {
                let key = typed_value(&key, key_nullable)?;
                if !seen.insert(key.0.clone()) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                upserts.push((key, typed_value(&value, value_nullable)?));
            }
        }
    }
    delete_rows(transaction, graph_id, result_id, &deletes)?;
    upsert_rows(transaction, graph_id, result_id, &upserts)
}

fn delete_rows(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    keys: &[Vec<u8>],
) -> Result<(), M2Error> {
    if keys.is_empty() {
        return Ok(());
    }
    let changed = transaction.execute(
        "DELETE FROM shiba_internal.graph_result_row
         WHERE graph_id = $1 AND result_id = $2 AND key_payload = ANY($3)",
        &[&graph_id, &result_id, &keys],
    )?;
    if usize::try_from(changed).ok() != Some(keys.len()) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn upsert_rows(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    rows: &[(Key, Value)],
) -> Result<(), M2Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let keys: Vec<Vec<u8>> = rows.iter().map(|row| row.0.0.clone()).collect();
    let value_payloads: Vec<Vec<u8>> = rows.iter().map(|row| row.1.0.clone()).collect();
    let key_values: Vec<Option<i64>> = rows.iter().map(|row| row.0.1).collect();
    let key_nulls: Vec<bool> = rows.iter().map(|row| row.0.2).collect();
    let values: Vec<Option<i64>> = rows.iter().map(|row| row.1.1).collect();
    let value_nulls: Vec<bool> = rows.iter().map(|row| row.1.2).collect();
    let changed = transaction.execute(
        "INSERT INTO shiba_internal.graph_result_row
             (graph_id, result_id, key_payload, result_key_is_null,
              result_key_bigint, value_payload, result_value_is_null,
              result_value_bigint)
         SELECT $1, $2, input.key_payload, input.key_is_null,
                input.key_bigint, input.value_payload, input.value_is_null,
                input.value_bigint
         FROM unnest($3::bytea[], $4::bytea[], $5::boolean[], $6::bigint[],
                     $7::boolean[], $8::bigint[])
              AS input(key_payload, value_payload, key_is_null, key_bigint,
                       value_is_null, value_bigint)
         ON CONFLICT (graph_id, result_id, key_payload) DO UPDATE SET
             result_key_is_null = EXCLUDED.result_key_is_null,
             result_key_bigint = EXCLUDED.result_key_bigint,
             value_payload = EXCLUDED.value_payload,
             result_value_is_null = EXCLUDED.result_value_is_null,
             result_value_bigint = EXCLUDED.result_value_bigint",
        &[
            &graph_id,
            &result_id,
            &keys,
            &value_payloads,
            &key_nulls,
            &key_values,
            &value_nulls,
            &values,
        ],
    )?;
    if usize::try_from(changed).ok() != Some(rows.len()) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn typed_value(value: &TypedValue, nullable: bool) -> Result<Key, M2Error> {
    let payload = value
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    match value {
        TypedValue::Int8(value) => Ok((payload, Some(*value), false)),
        TypedValue::Null(ValueType::Int8) if nullable => Ok((payload, None, true)),
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
}
