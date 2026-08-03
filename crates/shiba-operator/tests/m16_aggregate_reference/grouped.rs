use std::collections::{BTreeMap, BTreeSet};

use super::model::{
    Change, MAX_CHANGES, MAX_EMITTED_RESULT_IMAGES, MAX_GRAPH_OUTPUT_MUTATIONS,
    MAX_GRAPH_STATE_MUTATIONS, MAX_TOUCHED_GROUPS, ModelError, Plan, Row, State, Value,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationCounts {
    pub state: usize,
    pub output: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupedState {
    groups: BTreeMap<Value, State>,
    membership: BTreeMap<Value, u64>,
}

impl GroupedState {
    pub fn empty() -> Self {
        Self {
            groups: BTreeMap::new(),
            membership: BTreeMap::new(),
        }
    }

    pub fn apply(&mut self, plan: &Plan, changes: &[Change]) -> Result<MutationCounts, ModelError> {
        let mut touched_keys = BTreeSet::new();
        for change in changes {
            if let Some(before) = &change.before {
                touched_keys.insert(group_key(before)?);
            }
            if let Some(after) = &change.after {
                touched_keys.insert(group_key(after)?);
            }
        }
        if changes.len() > MAX_CHANGES || touched_keys.len() > MAX_TOUCHED_GROUPS {
            return Err(ModelError::Bound);
        }

        let old_output = self.output(plan);
        let mut staged = self.clone();
        for change in changes {
            if let Some(before) = &change.before {
                staged.retract(plan, before)?;
            }
            if let Some(after) = &change.after {
                staged.insert(plan, after)?;
            }
        }
        let next_output = staged.output(plan);
        let output = touched_keys
            .iter()
            .filter(|key| old_output.get(*key) != next_output.get(*key))
            .count();
        let output_images = touched_keys
            .iter()
            .map(|key| {
                usize::from(old_output.contains_key(key))
                    + usize::from(next_output.contains_key(key))
            })
            .sum();
        validate_batch_bounds(changes.len(), touched_keys.len(), output_images)?;
        let state_mutations = touched_keys
            .len()
            .checked_mul(plan.calls.len() + 1)
            .ok_or(ModelError::Overflow)?;
        validate_graph_mutation_bounds(state_mutations, output)?;
        *self = staged;
        Ok(MutationCounts {
            state: state_mutations,
            output,
        })
    }

    pub fn output(&self, plan: &Plan) -> BTreeMap<Value, Row> {
        self.groups
            .iter()
            .filter_map(|(key, state)| state.output(plan).map(|row| (key.clone(), row)))
            .collect()
    }

    fn insert(&mut self, plan: &Plan, row: &Row) -> Result<(), ModelError> {
        let key = group_key(row)?;
        let membership = self.membership.entry(key.clone()).or_insert(0);
        *membership = membership.checked_add(1).ok_or(ModelError::Overflow)?;
        let state = self.groups.entry(key).or_insert(State::empty(plan)?);
        state.apply(
            plan,
            &[Change {
                before: None,
                after: Some(row.clone()),
            }],
        )
    }

    fn retract(&mut self, plan: &Plan, row: &Row) -> Result<(), ModelError> {
        let key = group_key(row)?;
        let membership = self
            .membership
            .get_mut(&key)
            .ok_or(ModelError::RetractMissing)?;
        *membership = membership
            .checked_sub(1)
            .ok_or(ModelError::RetractMissing)?;
        let state = self
            .groups
            .get_mut(&key)
            .ok_or(ModelError::RetractMissing)?;
        state.apply(
            plan,
            &[Change {
                before: Some(row.clone()),
                after: None,
            }],
        )?;
        if *membership == 0 {
            self.membership.remove(&key);
            self.groups.remove(&key);
        }
        Ok(())
    }
}

pub fn validate_batch_bounds(
    changes: usize,
    touched_groups: usize,
    output_images: usize,
) -> Result<(), ModelError> {
    if changes > MAX_CHANGES
        || touched_groups > MAX_TOUCHED_GROUPS
        || output_images > MAX_EMITTED_RESULT_IMAGES
    {
        return Err(ModelError::Bound);
    }
    Ok(())
}

pub fn validate_graph_mutation_bounds(
    state_mutations: usize,
    output_mutations: usize,
) -> Result<(), ModelError> {
    if state_mutations > MAX_GRAPH_STATE_MUTATIONS || output_mutations > MAX_GRAPH_OUTPUT_MUTATIONS
    {
        return Err(ModelError::Bound);
    }
    Ok(())
}

fn group_key(row: &Row) -> Result<Value, ModelError> {
    match row.first() {
        Some(Value::Null | Value::Int8(_)) => Ok(row[0].clone()),
        Some(Value::Bool(_)) | None => Err(ModelError::Schema),
    }
}
