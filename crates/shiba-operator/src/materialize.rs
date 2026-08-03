use std::collections::BTreeSet;

use crate::{
    DeltaBatch, GraphError, NodeId, ResultDelta, ResultMutation, TypedLayout, TypedRow, TypedValue,
    ValueType, plan::MAX_KEYED_MUTATIONS,
};

pub(crate) fn materialize(
    node_id: NodeId,
    batch: &DeltaBatch,
    layout: &TypedLayout,
    key_slot: u16,
    value_slot: u16,
    key_nullable: bool,
    value_nullable: bool,
) -> Result<ResultDelta, GraphError> {
    let capacity = batch
        .rows
        .len()
        .checked_mul(2)
        .ok_or(GraphError::OutputLimit)?
        .min(MAX_KEYED_MUTATIONS);
    let mut keys = BTreeSet::new();
    let mut mutations = Vec::with_capacity(capacity);
    for delta in &batch.rows {
        let before = delta
            .before
            .as_ref()
            .map(|row| result_value(row, layout, key_slot, key_nullable))
            .transpose()?;
        let after = delta
            .after
            .as_ref()
            .map(|row| {
                Ok::<_, GraphError>((
                    result_value(row, layout, key_slot, key_nullable)?,
                    result_value(row, layout, value_slot, value_nullable)?,
                ))
            })
            .transpose()?;
        match (before, after) {
            (Some(old_key), Some((new_key, value))) if old_key == new_key => {
                insert_key(&mut keys, &old_key)?;
                push_mutation(
                    &mut mutations,
                    ResultMutation::Upsert {
                        key: old_key,
                        value,
                    },
                )?;
            }
            (Some(old_key), Some((new_key, value))) => {
                insert_key(&mut keys, &old_key)?;
                insert_key(&mut keys, &new_key)?;
                push_mutation(&mut mutations, ResultMutation::Delete { key: old_key })?;
                push_mutation(
                    &mut mutations,
                    ResultMutation::Upsert {
                        key: new_key,
                        value,
                    },
                )?;
            }
            (Some(key), None) => {
                insert_key(&mut keys, &key)?;
                push_mutation(&mut mutations, ResultMutation::Delete { key })?;
            }
            (None, Some((key, value))) => {
                insert_key(&mut keys, &key)?;
                push_mutation(&mut mutations, ResultMutation::Upsert { key, value })?;
            }
            (None, None) => {}
        }
    }
    Ok(ResultDelta::Keyed { node_id, mutations })
}

fn push_mutation(
    mutations: &mut Vec<ResultMutation>,
    mutation: ResultMutation,
) -> Result<(), GraphError> {
    if mutations.len() == MAX_KEYED_MUTATIONS {
        return Err(GraphError::OutputLimit);
    }
    mutations.push(mutation);
    Ok(())
}

fn insert_key(keys: &mut BTreeSet<TypedValue>, key: &TypedValue) -> Result<(), GraphError> {
    if keys.insert(key.clone()) {
        Ok(())
    } else {
        Err(GraphError::ConflictingKey)
    }
}

fn result_value(
    row: &TypedRow,
    layout: &TypedLayout,
    slot: u16,
    nullable: bool,
) -> Result<TypedValue, GraphError> {
    match row.value(layout, slot)? {
        TypedValue::Int8(value) => Ok(TypedValue::Int8(*value)),
        TypedValue::Null(ValueType::Int8) if nullable => Ok(TypedValue::Null(ValueType::Int8)),
        TypedValue::Absent => Err(GraphError::Expression),
        _ => Err(GraphError::WrongType),
    }
}
