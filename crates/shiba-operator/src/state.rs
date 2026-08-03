use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{EncodedOperatorState, NodeId, TypedValue};

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
}

impl StateReadSet {
    /// Sorts, validates, and deduplicates exact state keys.
    ///
    /// # Errors
    ///
    /// Rejects absent or otherwise non-persistable typed keys.
    pub fn canonical(mut keys: Vec<StateKey>) -> Result<Self, StateError> {
        for key in &keys {
            key.validate()?;
        }
        keys.sort();
        keys.dedup();
        Ok(Self { keys })
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
        if self.entries.len() != read_set.keys.len()
            || self
                .entries
                .iter()
                .zip(&read_set.keys)
                .any(|(entry, key)| &entry.key != key)
        {
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
    SnapshotMismatch,
}

impl fmt::Display for StateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "operator keyed state rejected: {self:?}")
    }
}

impl std::error::Error for StateError {}
