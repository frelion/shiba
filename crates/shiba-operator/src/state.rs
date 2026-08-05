use core::fmt;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{EncodedOperatorState, NodeId, TypedValue};

#[path = "state_order.rs"]
mod state_order;
#[path = "state_range.rs"]
mod state_range;
pub use state_order::{
    INT8_ORDER_KEY_VERSION, int8_order_key, state_item_order_key, validate_int8_order_key,
};
pub use state_range::StateRangeResult;

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
        let mut range_identities = BTreeSet::new();
        for range in &ranges {
            // A flattened snapshot cannot preserve provenance for two
            // directions of the same partition.  Reject the ambiguity at
            // read-set construction instead of merging or choosing a limit.
            let identity = (range.node_id, range.namespace, range.partition_key.clone());
            if !range_identities.insert(identity) {
                return Err(StateError::AmbiguousRange);
            }
        }
        let partition_identities = partitions
            .iter()
            .map(|partition| {
                (
                    partition.node_id,
                    partition.namespace,
                    partition.partition_key.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        if ranges.iter().any(|range| {
            partition_identities.contains(&(
                range.node_id,
                range.namespace,
                range.partition_key.clone(),
            ))
        }) {
            return Err(StateError::AmbiguousRange);
        }
        partitions.sort();
        partitions.dedup();
        ranges.sort();
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
    AmbiguousRange,
    Limit,
    SnapshotMismatch,
}

impl fmt::Display for StateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "operator keyed state rejected: {self:?}")
    }
}

impl std::error::Error for StateError {}
