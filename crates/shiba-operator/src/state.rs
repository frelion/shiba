use core::fmt;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{EncodedOperatorState, NodeId, TypedValue};

/// Version of the signed-int8 order-key encoding used by ordered state reads.
pub const INT8_ORDER_KEY_VERSION: u32 = 1;
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

/// Encodes an `i64` so bytewise ascending order is signed numeric order.
#[must_use]
pub const fn int8_order_key(value: i64) -> [u8; 8] {
    let mut bytes = value.to_be_bytes();
    bytes[0] ^= 0x80;
    bytes
}

/// Validates one persisted order key against its canonical int8 item key.
///
/// # Errors
///
/// Returns `StateError::InvalidOrderKey` for a non-int8 item or mismatched
/// eight-byte encoding.
pub fn validate_int8_order_key(value: &TypedValue, order_key: &[u8]) -> Result<(), StateError> {
    let TypedValue::Int8(value) = value else {
        return Err(StateError::InvalidOrderKey);
    };
    if order_key == int8_order_key(*value) {
        Ok(())
    } else {
        Err(StateError::InvalidOrderKey)
    }
}

/// Returns the derived order key for a state item, or `None` for unit state.
///
/// # Errors
///
/// Returns [`StateError::InvalidOrderKey`] when a keyed state item is not an
/// `Int8`, because no ordered-key contract exists for other value types.
pub fn state_item_order_key(key: &StateKey) -> Result<Option<Vec<u8>>, StateError> {
    match key.item_key.as_ref() {
        None => Ok(None),
        Some(TypedValue::Int8(value)) => Ok(Some(int8_order_key(*value).to_vec())),
        Some(_) => Err(StateError::InvalidOrderKey),
    }
}

impl std::error::Error for StateError {}

#[cfg(test)]
mod tests {
    use super::*;
    use core::num::NonZeroU32;

    fn key(value: i64) -> StateKey {
        StateKey {
            node_id: NodeId::new(NonZeroU32::new(1).unwrap()),
            namespace: 2,
            partition_key: TypedValue::Int8(7),
            item_key: Some(TypedValue::Int8(value)),
        }
    }

    #[test]
    fn signed_int8_order_key_is_numeric_byte_order() {
        let values = [i64::MIN, -1, 0, 1, i64::MAX];
        let mut encoded = values
            .iter()
            .map(|value| (int8_order_key(*value), *value))
            .collect::<Vec<_>>();
        encoded.sort_by_key(|(order, _)| *order);
        assert_eq!(
            encoded
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>(),
            values
        );
        for value in values {
            validate_int8_order_key(&TypedValue::Int8(value), &int8_order_key(value)).unwrap();
        }
    }

    #[test]
    fn ordered_read_is_bounded_and_snapshot_allows_exact_overlap() {
        let range = StateRange {
            node_id: key(0).node_id,
            namespace: 2,
            partition_key: TypedValue::Int8(7),
            direction: StateRangeDirection::Ascending,
            limit: 2,
            order_key_version: INT8_ORDER_KEY_VERSION,
        };
        let read_set = StateReadSet::with_ranges(vec![key(-1)], vec![], vec![range]).unwrap();
        let snapshot = StateSnapshot::new(
            &read_set,
            vec![
                StateEntry {
                    key: key(-1),
                    state: None,
                },
                StateEntry {
                    key: key(0),
                    state: None,
                },
            ],
        )
        .unwrap();
        assert_eq!(snapshot.entries.len(), 2);
        assert!(
            StateReadSet::with_ranges(
                Vec::new(),
                Vec::new(),
                vec![StateRange {
                    limit: 0,
                    ..read_set.ranges[0].clone()
                }]
            )
            .is_err()
        );
    }

    #[test]
    fn stale_or_non_int8_order_keys_fail_closed() {
        let value = TypedValue::Int8(-1);
        assert!(validate_int8_order_key(&value, &int8_order_key(0)).is_err());
        assert!(validate_int8_order_key(&TypedValue::Text("x".into()), &[0; 8]).is_err());
        assert!(
            state_item_order_key(&StateKey {
                item_key: Some(TypedValue::Text("x".into())),
                ..key(1)
            })
            .is_err()
        );
    }
}
