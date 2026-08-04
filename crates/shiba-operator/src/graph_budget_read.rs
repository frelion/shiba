use super::{GraphBudget, key_bytes, value_bytes};
use crate::{GraphError, MAX_PARTITION_ENTRIES, MAX_STATE_KEYS, StateRange, StateReadSet};

impl GraphBudget {
    pub(crate) fn charge_range(&mut self, range: &StateRange) -> Result<(), GraphError> {
        self.charge_partition_entry()?;
        let limit = usize::try_from(range.limit).map_err(|_| GraphError::OutputLimit)?;
        self.state_keys = self
            .state_keys
            .checked_add(limit)
            .ok_or(GraphError::OutputLimit)?;
        if self.state_keys > MAX_STATE_KEYS {
            return Err(GraphError::OutputLimit);
        }
        let bytes = value_bytes(&range.partition_key)?
            .checked_add(16)
            .ok_or(GraphError::OutputLimit)?;
        self.charge_work_bytes(bytes)
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
        for range in &read_set.ranges {
            self.charge_range(range)?;
        }
        self.charge_work_bytes(read_set_bytes(read_set)?)
    }

    pub(crate) fn charge_read_set_work(
        &mut self,
        read_set: &StateReadSet,
    ) -> Result<(), GraphError> {
        self.charge_work_bytes(read_set_bytes(read_set)?)
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
    for range in &read_set.ranges {
        bytes = bytes
            .checked_add(value_bytes(&range.partition_key)?)
            .and_then(|value| value.checked_add(16))
            .ok_or(GraphError::OutputLimit)?;
    }
    Ok(bytes)
}
