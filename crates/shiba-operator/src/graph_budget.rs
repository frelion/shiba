use std::collections::BTreeSet;

use crate::{
    DeltaBatch, GraphError, GraphTransition, ResultMutation, StateKey, StateMutation, StateReadSet,
    TypedValue,
    graph::{MAX_GRAPH_DELTA_ROWS, MAX_GRAPH_WORK_BYTES, MAX_NODE_DELTA_ROWS},
};

pub const MAX_TOUCHED_GROUPS: usize = 100_000;
pub const MAX_STATE_KEYS: usize = 100_000;
pub const MAX_PARTITION_ENTRIES: usize = 100_000;
pub const MAX_EXTREMA_VALUES: usize = 100_000;
pub const MAX_STATE_MUTATIONS: usize = 100_000;
pub const MAX_RESULT_MUTATIONS: usize = 100_000;
pub const MAX_ESTIMATED_WORK_BYTES: usize = MAX_GRAPH_WORK_BYTES;

pub(crate) struct EvaluationBudget {
    work_bytes: usize,
}

impl EvaluationBudget {
    pub(crate) fn new(input: &DeltaBatch) -> Result<Self, GraphError> {
        Ok(Self {
            work_bytes: batch_bytes(input)?,
        })
    }

    pub(crate) fn check_batch(batch: &DeltaBatch) -> Result<(), GraphError> {
        if batch_bytes(batch)? > MAX_ESTIMATED_WORK_BYTES {
            Err(GraphError::OutputLimit)
        } else {
            Ok(())
        }
    }

    pub(crate) fn charge(
        &mut self,
        batch: &DeltaBatch,
        emitted_rows: &mut usize,
    ) -> Result<(), GraphError> {
        charge_rows(emitted_rows, batch.rows.len())?;
        charge_bytes(&mut self.work_bytes, batch_bytes(batch)?)
    }
}

pub(crate) fn estimated_batch_bytes(batch: &DeltaBatch) -> Result<usize, GraphError> {
    batch_bytes(batch)
}

/// One graph-wide admission budget shared by all stateful and result nodes.
///
/// The budget is deliberately independent of a concrete operator kind. A
/// transition is accepted only when the aggregate of every node remains
/// within these limits.
pub(crate) struct GraphBudget {
    touched_groups: usize,
    state_keys: usize,
    partition_entries: usize,
    state_mutations: usize,
    result_mutations: usize,
    work_bytes: usize,
}

impl GraphBudget {
    pub(crate) fn new() -> Self {
        Self {
            touched_groups: 0,
            state_keys: 0,
            partition_entries: 0,
            state_mutations: 0,
            result_mutations: 0,
            work_bytes: 0,
        }
    }

    pub(crate) fn charge_touched_groups(&mut self, count: usize) -> Result<(), GraphError> {
        self.touched_groups = self
            .touched_groups
            .checked_add(count)
            .ok_or(GraphError::OutputLimit)?;
        if self.touched_groups > MAX_TOUCHED_GROUPS {
            return Err(GraphError::OutputLimit);
        }
        Ok(())
    }

    pub(crate) fn charge_state_key(&mut self) -> Result<(), GraphError> {
        self.state_keys = self
            .state_keys
            .checked_add(1)
            .ok_or(GraphError::OutputLimit)?;
        if self.state_keys > MAX_STATE_KEYS {
            return Err(GraphError::OutputLimit);
        }
        Ok(())
    }

    pub(crate) fn charge_partition_entry(&mut self) -> Result<(), GraphError> {
        self.partition_entries = self
            .partition_entries
            .checked_add(1)
            .ok_or(GraphError::OutputLimit)?;
        if self.partition_entries > MAX_PARTITION_ENTRIES {
            return Err(GraphError::OutputLimit);
        }
        Ok(())
    }

    pub(crate) fn charge_state_mutation(&mut self) -> Result<(), GraphError> {
        self.state_mutations = self
            .state_mutations
            .checked_add(1)
            .ok_or(GraphError::OutputLimit)?;
        if self.state_mutations > MAX_STATE_MUTATIONS {
            return Err(GraphError::OutputLimit);
        }
        Ok(())
    }

    pub(crate) fn charge_result_mutation(&mut self) -> Result<(), GraphError> {
        self.result_mutations = self
            .result_mutations
            .checked_add(1)
            .ok_or(GraphError::OutputLimit)?;
        if self.result_mutations > MAX_RESULT_MUTATIONS {
            return Err(GraphError::OutputLimit);
        }
        Ok(())
    }

    pub(crate) fn charge_read_set(&mut self, read_set: &StateReadSet) -> Result<(), GraphError> {
        self.state_keys = self
            .state_keys
            .checked_add(read_set.keys.len())
            .ok_or(GraphError::OutputLimit)?;
        self.partition_entries = self
            .partition_entries
            .checked_add(read_set.partitions.len())
            .ok_or(GraphError::OutputLimit)?;
        if self.state_keys > MAX_STATE_KEYS || self.partition_entries > MAX_PARTITION_ENTRIES {
            return Err(GraphError::OutputLimit);
        }
        self.charge_work_bytes(read_set_bytes(read_set)?)
    }

    pub(crate) fn charge_read_set_work(
        &mut self,
        read_set: &StateReadSet,
    ) -> Result<(), GraphError> {
        self.charge_work_bytes(read_set_bytes(read_set)?)
    }

    pub(crate) fn charge_work_bytes(&mut self, bytes: usize) -> Result<(), GraphError> {
        self.work_bytes = self
            .work_bytes
            .checked_add(bytes)
            .ok_or(GraphError::OutputLimit)?;
        if self.work_bytes > MAX_ESTIMATED_WORK_BYTES {
            return Err(GraphError::OutputLimit);
        }
        Ok(())
    }

    /// Charges the complete state/result shape once, after all nodes have
    /// produced their local deltas but before the transition is returned.
    pub(crate) fn charge_transition(
        &mut self,
        transition: &GraphTransition,
    ) -> Result<(), GraphError> {
        if transition.state_deltas.len() > MAX_STATE_MUTATIONS {
            return Err(GraphError::OutputLimit);
        }
        let result_mutations = transition
            .results
            .iter()
            .map(|result| result.mutations.len())
            .try_fold(0usize, |total, count| {
                total.checked_add(count).ok_or(GraphError::OutputLimit)
            })?;
        if result_mutations > MAX_RESULT_MUTATIONS {
            return Err(GraphError::OutputLimit);
        }
        let keys = transition
            .state_deltas
            .iter()
            .map(|delta| delta.key.clone())
            .collect::<BTreeSet<StateKey>>();
        let partitions = keys
            .iter()
            .map(|key| (key.node_id, key.namespace, key.partition_key.clone()))
            .collect::<BTreeSet<_>>();
        if keys.len() > MAX_STATE_KEYS || partitions.len() > MAX_PARTITION_ENTRIES {
            return Err(GraphError::OutputLimit);
        }
        self.charge_work_bytes(transition_bytes(transition)?)
    }
}

fn read_set_bytes(read_set: &StateReadSet) -> Result<usize, GraphError> {
    let mut bytes = 0usize;
    for key in &read_set.keys {
        bytes = bytes
            .checked_add(key_bytes(key)?)
            .ok_or(GraphError::OutputLimit)?;
    }
    for partition in &read_set.partitions {
        bytes = bytes
            .checked_add(value_bytes(&partition.partition_key)?)
            .ok_or(GraphError::OutputLimit)?;
    }
    Ok(bytes)
}

fn transition_bytes(transition: &GraphTransition) -> Result<usize, GraphError> {
    let mut bytes = 0usize;
    for delta in &transition.state_deltas {
        bytes = bytes
            .checked_add(key_bytes(&delta.key)?)
            .ok_or(GraphError::OutputLimit)?;
        if let StateMutation::Upsert { state } = &delta.mutation {
            bytes = bytes
                .checked_add(state.payload.len())
                .ok_or(GraphError::OutputLimit)?;
        }
    }
    for result in &transition.results {
        for mutation in &result.mutations {
            let mutation_bytes = match mutation {
                ResultMutation::ReplaceScalar { row } => row
                    .to_canonical_payload()
                    .map_err(|_| GraphError::OutputLimit)?
                    .len(),
                ResultMutation::Delete { key } => key
                    .to_canonical_payload()
                    .map_err(|_| GraphError::OutputLimit)?
                    .len(),
                ResultMutation::Upsert { key, row } => key
                    .to_canonical_payload()
                    .map_err(|_| GraphError::OutputLimit)?
                    .len()
                    .checked_add(
                        row.to_canonical_payload()
                            .map_err(|_| GraphError::OutputLimit)?
                            .len(),
                    )
                    .ok_or(GraphError::OutputLimit)?,
            };
            bytes = bytes
                .checked_add(mutation_bytes)
                .ok_or(GraphError::OutputLimit)?;
        }
    }
    Ok(bytes)
}

fn key_bytes(key: &StateKey) -> Result<usize, GraphError> {
    value_bytes(&key.partition_key)?
        .checked_add(
            key.item_key
                .as_ref()
                .map(value_bytes)
                .transpose()?
                .unwrap_or(0),
        )
        .ok_or(GraphError::OutputLimit)
}

fn value_bytes(value: &TypedValue) -> Result<usize, GraphError> {
    value
        .to_canonical_json()
        .map(|payload| payload.len())
        .map_err(|_| GraphError::OutputLimit)
}

fn charge_bytes(total: &mut usize, bytes: usize) -> Result<(), GraphError> {
    *total = total.checked_add(bytes).ok_or(GraphError::OutputLimit)?;
    if *total > MAX_GRAPH_WORK_BYTES {
        return Err(GraphError::OutputLimit);
    }
    Ok(())
}

fn batch_bytes(batch: &DeltaBatch) -> Result<usize, GraphError> {
    let mut bytes = 64_usize;
    for delta in &batch.rows {
        for row in [delta.before.as_ref(), delta.after.as_ref()]
            .into_iter()
            .flatten()
        {
            bytes = bytes.checked_add(32).ok_or(GraphError::OutputLimit)?;
            for value in &row.values {
                let value_bytes = match value {
                    TypedValue::Text(text) => 16_usize
                        .checked_add(text.len())
                        .ok_or(GraphError::OutputLimit)?,
                    _ => 16,
                };
                bytes = bytes
                    .checked_add(value_bytes)
                    .ok_or(GraphError::OutputLimit)?;
            }
        }
    }
    if bytes > MAX_GRAPH_WORK_BYTES {
        return Err(GraphError::OutputLimit);
    }
    Ok(bytes)
}

fn charge_rows(total: &mut usize, rows: usize) -> Result<(), GraphError> {
    if rows > MAX_NODE_DELTA_ROWS {
        return Err(GraphError::OutputLimit);
    }
    *total = total.checked_add(rows).ok_or(GraphError::OutputLimit)?;
    if *total > MAX_GRAPH_DELTA_ROWS {
        return Err(GraphError::OutputLimit);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_budget_accumulates_across_multiple_stateful_nodes() {
        let mut budget = GraphBudget::new();
        budget
            .charge_touched_groups(MAX_TOUCHED_GROUPS / 2)
            .unwrap();
        budget
            .charge_touched_groups(MAX_TOUCHED_GROUPS - MAX_TOUCHED_GROUPS / 2)
            .unwrap();
        assert_eq!(
            budget.charge_touched_groups(1),
            Err(GraphError::OutputLimit)
        );

        let mut budget = GraphBudget::new();
        for _ in 0..MAX_STATE_KEYS {
            budget.charge_state_key().unwrap();
        }
        assert_eq!(budget.charge_state_key(), Err(GraphError::OutputLimit));

        let mut budget = GraphBudget::new();
        for _ in 0..MAX_PARTITION_ENTRIES {
            budget.charge_partition_entry().unwrap();
        }
        assert_eq!(
            budget.charge_partition_entry(),
            Err(GraphError::OutputLimit)
        );
    }

    #[test]
    fn graph_budget_separates_state_result_and_work_limits() {
        let mut budget = GraphBudget::new();
        for _ in 0..MAX_STATE_MUTATIONS {
            budget.charge_state_mutation().unwrap();
        }
        assert_eq!(budget.charge_state_mutation(), Err(GraphError::OutputLimit));

        let mut budget = GraphBudget::new();
        for _ in 0..MAX_RESULT_MUTATIONS {
            budget.charge_result_mutation().unwrap();
        }
        assert_eq!(
            budget.charge_result_mutation(),
            Err(GraphError::OutputLimit)
        );

        let mut budget = GraphBudget::new();
        budget.charge_work_bytes(MAX_ESTIMATED_WORK_BYTES).unwrap();
        assert_eq!(budget.charge_work_bytes(1), Err(GraphError::OutputLimit));
    }
}
