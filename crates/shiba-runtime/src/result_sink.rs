use std::collections::{BTreeMap, BTreeSet};

use postgres::Transaction;
use shiba_operator::{ResultDelta, ResultMutation, TypedValue, ValueType};

use crate::M2Error;

const MAX_GRAPH_RESULT_MUTATIONS: usize = 100_000;

#[derive(Clone, Copy)]
enum Shape {
    Scalar,
    Keyed {
        key_nullable: bool,
        value_nullable: bool,
    },
}

pub(crate) struct LockedResults {
    contracts: BTreeMap<i64, Shape>,
}

impl LockedResults {
    pub(crate) fn contract_count(&self) -> usize {
        self.contracts.len()
    }
}

pub(crate) fn lock(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    expected_status: &str,
) -> Result<LockedResults, M2Error> {
    let rows = transaction.query(
        "SELECT result_id, output_shape, output_key_type,
                output_key_nullable, output_value_type,
                output_value_nullable, result_status
         FROM shiba.graph_result WHERE graph_id = $1
         ORDER BY result_id FOR UPDATE",
        &[&graph_id],
    )?;
    if rows.is_empty() {
        return Err(M2Error::MissingSourceOperator);
    }
    let mut contracts = BTreeMap::new();
    for row in rows {
        let result_id: i64 = row.get(0);
        let shape = match row.get::<_, &str>(1) {
            "scalar"
                if row.get::<_, Option<&str>>(2).is_none()
                    && !row.get::<_, bool>(3)
                    && row.get::<_, &str>(4) == "int8"
                    && !row.get::<_, bool>(5) =>
            {
                Shape::Scalar
            }
            "keyed"
                if row.get::<_, Option<&str>>(2) == Some("int8")
                    && row.get::<_, &str>(4) == "int8" =>
            {
                Shape::Keyed {
                    key_nullable: row.get(3),
                    value_nullable: row.get(5),
                }
            }
            _ => return Err(M2Error::InvalidOperatorDefinition),
        };
        if row.get::<_, &str>(6) != expected_status || contracts.insert(result_id, shape).is_some()
        {
            return Err(M2Error::InvalidOperatorDefinition);
        }
    }
    Ok(LockedResults { contracts })
}

pub(crate) fn persist(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    locked: &LockedResults,
    results: Vec<ResultDelta>,
    publish: bool,
    activate: bool,
) -> Result<(), M2Error> {
    let mut seen = BTreeSet::new();
    let mut mutation_count = 0usize;
    for result in results {
        let (node_id, shape) = match &result {
            ResultDelta::Scalar { node_id, .. } => (i64::from(node_id.get()), Shape::Scalar),
            ResultDelta::Keyed { node_id, .. } => (
                i64::from(node_id.get()),
                *locked
                    .contracts
                    .get(&i64::from(node_id.get()))
                    .ok_or(M2Error::InvalidOperatorDefinition)?,
            ),
        };
        if !seen.insert(node_id) || !shape_matches(locked.contracts.get(&node_id), shape) {
            return Err(M2Error::InvalidOperatorDefinition);
        }
        match result {
            ResultDelta::Scalar { value, .. } => {
                if activate {
                    activate_scalar(transaction, graph_id, node_id, &value)?;
                } else if publish {
                    persist_scalar(transaction, graph_id, node_id, &value)?;
                }
            }
            ResultDelta::Keyed { mutations, .. } => {
                mutation_count = mutation_count
                    .checked_add(mutations.len())
                    .ok_or(M2Error::TransactionLimitExceeded)?;
                let Shape::Keyed {
                    key_nullable,
                    value_nullable,
                } = shape
                else {
                    return Err(M2Error::InvalidOperatorDefinition);
                };
                persist_keyed(
                    transaction,
                    graph_id,
                    node_id,
                    mutations,
                    key_nullable,
                    value_nullable,
                )?;
                if activate {
                    activate_keyed(transaction, graph_id, node_id)?;
                }
            }
        }
    }
    if mutation_count > MAX_GRAPH_RESULT_MUTATIONS || seen.len() != locked.contracts.len() {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn activate_scalar(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    value: &TypedValue,
) -> Result<(), M2Error> {
    let bigint = scalar_int8(value)?;
    let payload = value
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    if transaction.execute(
        "UPDATE shiba.graph_result SET result_status = 'active',
                value_payload = $1, value_bigint = $2
         WHERE graph_id = $3 AND result_id = $4
           AND result_status = 'building' AND output_shape = 'scalar'",
        &[&payload, &bigint, &graph_id, &result_id],
    )? != 1
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn activate_keyed(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
) -> Result<(), M2Error> {
    if transaction.execute(
        "UPDATE shiba.graph_result SET result_status = 'active'
         WHERE graph_id = $1 AND result_id = $2
           AND result_status = 'building' AND output_shape = 'keyed'",
        &[&graph_id, &result_id],
    )? != 1
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn shape_matches(expected: Option<&Shape>, actual: Shape) -> bool {
    matches!(
        (expected, actual),
        (Some(Shape::Scalar), Shape::Scalar) | (Some(Shape::Keyed { .. }), Shape::Keyed { .. })
    )
}

fn persist_scalar(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    result_id: i64,
    value: &TypedValue,
) -> Result<(), M2Error> {
    let bigint = scalar_int8(value)?;
    let payload = value
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    if transaction.execute(
        "UPDATE shiba.graph_result SET value_payload = $1, value_bigint = $2
         WHERE graph_id = $3 AND result_id = $4
           AND result_status = 'active' AND output_shape = 'scalar'",
        &[&payload, &bigint, &graph_id, &result_id],
    )? != 1
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn persist_keyed(
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
                let key = result_key(&key, key_nullable)?;
                if !seen.insert(key.0.clone()) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                deletes.push(key.0);
            }
            ResultMutation::Upsert { key, value } => {
                let key = result_key(&key, key_nullable)?;
                if !seen.insert(key.0.clone()) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                let value = result_value(&value, value_nullable)?;
                upserts.push((key, value));
            }
        }
    }
    delete_rows(transaction, graph_id, result_id, &deletes)?;
    upsert_rows(transaction, graph_id, result_id, &upserts)
}

type Key = (Vec<u8>, Option<i64>, bool);
type Value = (Vec<u8>, Option<i64>, bool);

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

fn result_key(value: &TypedValue, nullable: bool) -> Result<Key, M2Error> {
    let payload = value
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    match value {
        TypedValue::Int8(value) => Ok((payload, Some(*value), false)),
        TypedValue::Null(ValueType::Int8) if nullable => Ok((payload, None, true)),
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
}

fn result_value(value: &TypedValue, nullable: bool) -> Result<Value, M2Error> {
    let payload = value
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    match value {
        TypedValue::Int8(value) => Ok((payload, Some(*value), false)),
        TypedValue::Null(ValueType::Int8) if nullable => Ok((payload, None, true)),
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
}

pub(crate) fn scalar_int8(value: &TypedValue) -> Result<i64, M2Error> {
    match value {
        TypedValue::Int8(value) => Ok(*value),
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
}
