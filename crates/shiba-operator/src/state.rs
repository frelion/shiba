use core::fmt;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{EncodedOperatorState, NodeId, TypedValue};

#[path = "state_order.rs"]
mod state_order;
pub use state_order::{
    INT8_ORDER_KEY_VERSION, int8_order_key, state_item_order_key, validate_int8_order_key,
};

pub const MAX_STATE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateKey {
    pub node_id: NodeId,
    pub namespace: u16,
    pub partition_key: TypedValue,
    pub item_key: Option<TypedValue>,
}

impl StateKey {
    pub(crate) fn validate(&self) -> Result<(), StateError> {
        self.partition_key
            .to_canonical_json()
            .map_err(|_| StateError::InvalidKey)?;
        if let Some(item) = &self.item_key {
            item.to_canonical_json()
                .map_err(|_| StateError::InvalidKey)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateReadSet {
    pub keys: Vec<StateKey>,
    pub partitions: Vec<StatePartition>,
    pub ranges: Vec<StateRange>,
}

impl StateReadSet {
    /// Sorts, validates, and deduplicates exact state keys.
    ///
    /// # Errors
    ///
    /// Rejects absent or otherwise non-persistable typed keys.
    pub fn canonical(mut keys: Vec<StateKey>) -> Result<Self, StateError> {
        if keys.len() > crate::MAX_STATE_KEYS {
            return Err(StateError::Limit);
        }
        for key in &keys {
            key.validate()?;
        }
        keys.sort();
        keys.dedup();
        Ok(Self {
            keys,
            partitions: Vec::new(),
            ranges: Vec::new(),
        })
    }

    /// Builds a canonical read set containing exact keys and partition reads.
    ///
    /// # Errors
    ///
    /// Rejects absent or otherwise non-persistable typed keys.
    pub fn with_partitions(
        keys: Vec<StateKey>,
        mut partitions: Vec<StatePartition>,
    ) -> Result<Self, StateError> {
        let mut read_set = Self::canonical(keys)?;
        if partitions.len() > crate::MAX_PARTITION_ENTRIES {
            return Err(StateError::Limit);
        }
        for partition in &partitions {
            partition.validate()?;
        }
        partitions.sort();
        partitions.dedup();
        read_set.partitions = partitions;
        Ok(read_set)
    }

    /// Builds a canonical read set with exact, legacy full-partition, and
    /// bounded ordered reads.
    ///
    /// # Errors
    ///
    /// Rejects invalid keys, ranges, or request bounds.
    pub fn with_ranges(
        keys: Vec<StateKey>,
        mut partitions: Vec<StatePartition>,
        mut ranges: Vec<StateRange>,
    ) -> Result<Self, StateError> {
        let mut read_set = Self::canonical(keys)?;
        if partitions.len() > crate::MAX_PARTITION_ENTRIES
            || ranges.len() > crate::MAX_PARTITION_ENTRIES
        {
            return Err(StateError::Limit);
        }
        for partition in &partitions {
            partition.validate()?;
        }
        for range in &ranges {
            range.validate()?;
        }
        partitions.sort();
        partitions.dedup();
        ranges.sort();
        ranges.dedup();
        read_set.partitions = partitions;
        read_set.ranges = ranges;
        Ok(read_set)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatePartition {
    pub node_id: NodeId,
    pub namespace: u16,
    pub partition_key: TypedValue,
}

impl StatePartition {
    fn validate(&self) -> Result<(), StateError> {
        self.partition_key
            .to_canonical_json()
            .map(|_| ())
            .map_err(|_| StateError::InvalidKey)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateRangeDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateRange {
    pub node_id: NodeId,
    pub namespace: u16,
    pub partition_key: TypedValue,
    pub direction: StateRangeDirection,
    pub limit: u32,
    pub order_key_version: u32,
}

impl StateRange {
    fn validate(&self) -> Result<(), StateError> {
        self.partition_key
            .to_canonical_json()
            .map_err(|_| StateError::InvalidKey)?;
        if self.limit == 0
            || usize::try_from(self.limit).ok() > Some(crate::MAX_STATE_KEYS)
            || self.order_key_version != INT8_ORDER_KEY_VERSION
        {
            return Err(StateError::Limit);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateEntry {
    pub key: StateKey,
    pub state: Option<EncodedOperatorState>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateSnapshot {
    pub entries: Vec<StateEntry>,
}

impl StateSnapshot {
    /// Constructs a snapshot and verifies one ordered entry per requested key.
    ///
    /// # Errors
    ///
    /// Rejects missing, extra, duplicate, or out-of-order entries.
    pub fn new(read_set: &StateReadSet, entries: Vec<StateEntry>) -> Result<Self, StateError> {
        let snapshot = Self { entries };
        snapshot.validate_exact(read_set)?;
        Ok(snapshot)
    }

    /// Verifies one ordered entry per requested key, with no extras or duplicates.
    ///
    /// # Errors
    ///
    /// Rejects missing, extra, duplicate, or out-of-order entries.
    pub fn validate_exact(&self, read_set: &StateReadSet) -> Result<(), StateError> {
        if self
            .entries
            .windows(2)
            .any(|pair| pair[0].key >= pair[1].key)
        {
            return Err(StateError::SnapshotMismatch);
        }
        let entry_keys = self
            .entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<BTreeSet<_>>();
        if !read_set.keys.iter().all(|key| entry_keys.contains(key)) {
            return Err(StateError::SnapshotMismatch);
        }
        let exact_keys = read_set.keys.iter().cloned().collect::<BTreeSet<_>>();
        let partition_coordinates = read_set
            .partitions
            .iter()
            .map(|partition| {
                (
                    partition.node_id,
                    partition.namespace,
                    partition.partition_key.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        let range_coordinates = read_set
            .ranges
            .iter()
            .map(|range| (range.node_id, range.namespace, range.partition_key.clone()))
            .collect::<BTreeSet<_>>();
        if !self.entries.iter().all(|entry| {
            exact_keys.contains(&entry.key)
                || partition_coordinates.contains(&(
                    entry.key.node_id,
                    entry.key.namespace,
                    entry.key.partition_key.clone(),
                ))
                || (matches!(entry.key.item_key, Some(TypedValue::Int8(_)))
                    && range_coordinates.contains(&(
                        entry.key.node_id,
                        entry.key.namespace,
                        entry.key.partition_key.clone(),
                    )))
        }) {
            return Err(StateError::SnapshotMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mutation", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateMutation {
    Delete,
    Upsert { state: EncodedOperatorState },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateDelta {
    pub key: StateKey,
    pub mutation: StateMutation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateError {
    InvalidKey,
    InvalidOrderKey,
    Limit,
    SnapshotMismatch,
}

impl fmt::Display for StateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "operator keyed state rejected: {self:?}")
    }
}

impl std::error::Error for StateError {}
