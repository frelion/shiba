use std::collections::BTreeSet;

use crate::{
    DeltaBatch, GraphError, NodeId, OutputContract, ResultDelta, ResultMutation, ResultRowKey,
    TypedLayout, TypedResultRowV1, TypedRow, graph::MAX_NODE_DELTA_ROWS,
};

pub(crate) fn materialize(
    node_id: NodeId,
    batch: &DeltaBatch,
    layout: &TypedLayout,
    field_slots: &[u16],
    output: &OutputContract,
) -> Result<ResultDelta, GraphError> {
    let mut budget = crate::graph_budget::GraphBudget::new();
    materialize_with_budget(node_id, batch, layout, field_slots, output, &mut budget)
}

pub(crate) fn materialize_with_budget(
    node_id: NodeId,
    batch: &DeltaBatch,
    layout: &TypedLayout,
    field_slots: &[u16],
    output: &OutputContract,
    budget: &mut crate::graph_budget::GraphBudget,
) -> Result<ResultDelta, GraphError> {
    output.schema.validate().map_err(|_| GraphError::Codec)?;
    if field_slots.len() != output.schema.fields.len() || output.schema.is_scalar() {
        return Err(GraphError::WrongType);
    }
    let capacity = batch
        .rows
        .len()
        .checked_mul(2)
        .ok_or(GraphError::OutputLimit)?
        .min(MAX_NODE_DELTA_ROWS);
    let mut keys = BTreeSet::new();
    let mut mutations = Vec::with_capacity(capacity);
    for delta in &batch.rows {
        let before = delta
            .before
            .as_ref()
            .map(|row| result_row(row, layout, field_slots, output))
            .transpose()?;
        let after = delta
            .after
            .as_ref()
            .map(|row| result_row(row, layout, field_slots, output))
            .transpose()?;
        let before = match before {
            Some(row) => Some((
                ResultRowKey::from_row(&output.schema, &row).map_err(|_| GraphError::WrongType)?,
                row,
            )),
            None => None,
        };
        let after = match after {
            Some(row) => Some((
                ResultRowKey::from_row(&output.schema, &row).map_err(|_| GraphError::WrongType)?,
                row,
            )),
            None => None,
        };
        match (before, after) {
            (Some((old_key, _)), Some((new_key, row))) if old_key == new_key => {
                insert_key(&mut keys, &old_key)?;
                push(
                    &mut mutations,
                    ResultMutation::Upsert { key: old_key, row },
                    budget,
                )?;
            }
            (Some((old_key, _)), Some((new_key, row))) => {
                insert_key(&mut keys, &old_key)?;
                insert_key(&mut keys, &new_key)?;
                push(
                    &mut mutations,
                    ResultMutation::Delete { key: old_key },
                    budget,
                )?;
                push(
                    &mut mutations,
                    ResultMutation::Upsert { key: new_key, row },
                    budget,
                )?;
            }
            (Some((key, _)), None) => {
                insert_key(&mut keys, &key)?;
                push(&mut mutations, ResultMutation::Delete { key }, budget)?;
            }
            (None, Some((key, row))) => {
                insert_key(&mut keys, &key)?;
                push(&mut mutations, ResultMutation::Upsert { key, row }, budget)?;
            }
            (None, None) => {}
        }
    }
    Ok(ResultDelta { node_id, mutations })
}

fn result_row(
    row: &TypedRow,
    layout: &TypedLayout,
    slots: &[u16],
    output: &OutputContract,
) -> Result<TypedResultRowV1, GraphError> {
    let values = slots
        .iter()
        .map(|slot| row.value(layout, *slot).cloned())
        .collect::<Result<Vec<_>, _>>()?;
    TypedResultRowV1::new(&output.schema, values).map_err(|_| GraphError::WrongType)
}

fn push(
    mutations: &mut Vec<ResultMutation>,
    mutation: ResultMutation,
    budget: &mut crate::graph_budget::GraphBudget,
) -> Result<(), GraphError> {
    if mutations.len() == MAX_NODE_DELTA_ROWS {
        return Err(GraphError::OutputLimit);
    }
    budget.charge_result_mutation()?;
    mutations.push(mutation);
    Ok(())
}

fn insert_key(keys: &mut BTreeSet<ResultRowKey>, key: &ResultRowKey) -> Result<(), GraphError> {
    if keys.insert(key.clone()) {
        Ok(())
    } else {
        Err(GraphError::ConflictingKey)
    }
}
