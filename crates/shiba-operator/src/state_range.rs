use std::collections::BTreeSet;

use crate::{StateEntry, StateError, StateRangeDirection, StateReadSet, StateSnapshot, TypedValue};

/// Transaction-local result for one ordered range request.
///
/// The range index is request provenance, not durable state. It is consumed
/// while constructing a flattened [`StateSnapshot`] so a candidate cannot
/// bypass its declared limit or direction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateRangeResult {
    pub range_index: usize,
    pub entries: Vec<StateEntry>,
}

impl StateSnapshot {
    /// Constructs a snapshot and verifies one ordered entry per requested key.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] when entries are missing, extra, malformed, or
    /// outside the declared range contract.
    pub fn new(read_set: &StateReadSet, entries: Vec<StateEntry>) -> Result<Self, StateError> {
        let range_results = infer_range_results(read_set, &entries)?;
        Self::new_with_ranges(read_set, entries, &range_results)
    }

    /// Constructs a snapshot after validating each range with its source
    /// metadata. Range metadata is deliberately not persisted or serialized.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] when range provenance, ordering, limits, or
    /// flattened entries do not match the read set.
    pub fn new_with_ranges(
        read_set: &StateReadSet,
        entries: Vec<StateEntry>,
        range_results: &[StateRangeResult],
    ) -> Result<Self, StateError> {
        validate_entries(read_set, &entries)?;
        validate_range_results(read_set, &entries, range_results)?;
        Ok(Self { entries })
    }

    /// Verifies one ordered entry per requested key, with no extras or duplicates.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] when the snapshot does not exactly satisfy the
    /// requested keys, partitions, and ranges.
    pub fn validate_exact(&self, read_set: &StateReadSet) -> Result<(), StateError> {
        let range_results = infer_range_results(read_set, &self.entries)?;
        validate_entries(read_set, &self.entries)?;
        validate_range_results(read_set, &self.entries, &range_results)
    }
}

fn infer_range_results(
    read_set: &StateReadSet,
    entries: &[StateEntry],
) -> Result<Vec<StateRangeResult>, StateError> {
    let exact_keys = read_set.keys.iter().cloned().collect::<BTreeSet<_>>();
    let mut coordinates = BTreeSet::new();
    let mut results = Vec::with_capacity(read_set.ranges.len());
    for (range_index, range) in read_set.ranges.iter().enumerate() {
        let coordinate = (range.node_id, range.namespace, range.partition_key.clone());
        if !coordinates.insert(coordinate) {
            return Err(StateError::AmbiguousRange);
        }
        let mut range_entries = entries
            .iter()
            .filter(|entry| {
                !exact_keys.contains(&entry.key)
                    && entry.key.node_id == range.node_id
                    && entry.key.namespace == range.namespace
                    && entry.key.partition_key == range.partition_key
            })
            .cloned()
            .collect::<Vec<_>>();
        range_entries.sort_by(|left, right| {
            let left = range_value(left).unwrap_or_default();
            let right = range_value(right).unwrap_or_default();
            match range.direction {
                StateRangeDirection::Ascending => left.cmp(&right),
                StateRangeDirection::Descending => right.cmp(&left),
            }
        });
        results.push(StateRangeResult {
            range_index,
            entries: range_entries,
        });
    }
    Ok(results)
}

fn validate_entries(read_set: &StateReadSet, entries: &[StateEntry]) -> Result<(), StateError> {
    if entries.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err(StateError::SnapshotMismatch);
    }
    let entry_keys = entries
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
    if !entries.iter().all(|entry| {
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

fn validate_range_results(
    read_set: &StateReadSet,
    entries: &[StateEntry],
    range_results: &[StateRangeResult],
) -> Result<(), StateError> {
    if range_results.len() != read_set.ranges.len() {
        return Err(StateError::SnapshotMismatch);
    }
    let exact_keys = read_set.keys.iter().cloned().collect::<BTreeSet<_>>();
    let entry_keys = entries
        .iter()
        .map(|entry| entry.key.clone())
        .collect::<BTreeSet<_>>();
    let mut seen_ranges = BTreeSet::new();
    let mut range_keys = BTreeSet::new();
    for result in range_results {
        let range = read_set
            .ranges
            .get(result.range_index)
            .ok_or(StateError::SnapshotMismatch)?;
        if !seen_ranges.insert(result.range_index) {
            return Err(StateError::AmbiguousRange);
        }
        let non_exact_count = result
            .entries
            .iter()
            .filter(|entry| !exact_keys.contains(&entry.key))
            .count();
        if non_exact_count > usize::try_from(range.limit).unwrap_or(usize::MAX) {
            return Err(StateError::Limit);
        }
        for pair in result.entries.windows(2) {
            let left = range_value(&pair[0])?;
            let right = range_value(&pair[1])?;
            let ordered = match range.direction {
                StateRangeDirection::Ascending => left < right,
                StateRangeDirection::Descending => left > right,
            };
            if !ordered {
                return Err(StateError::SnapshotMismatch);
            }
        }
        for entry in &result.entries {
            let key = &entry.key;
            if key.node_id != range.node_id
                || key.namespace != range.namespace
                || key.partition_key != range.partition_key
            {
                return Err(StateError::SnapshotMismatch);
            }
            let _ = range_value(entry)?;
            if !entry_keys.contains(key) {
                return Err(StateError::SnapshotMismatch);
            }
            if !exact_keys.contains(key) && !range_keys.insert(key.clone()) {
                return Err(StateError::SnapshotMismatch);
            }
        }
    }
    if !entries.iter().all(|entry| {
        exact_keys.contains(&entry.key)
            || range_keys.contains(&entry.key)
            || read_set.partitions.iter().any(|partition| {
                partition.node_id == entry.key.node_id
                    && partition.namespace == entry.key.namespace
                    && partition.partition_key == entry.key.partition_key
            })
    }) {
        return Err(StateError::SnapshotMismatch);
    }
    if seen_ranges.len() != read_set.ranges.len() {
        return Err(StateError::SnapshotMismatch);
    }
    Ok(())
}

fn range_value(entry: &StateEntry) -> Result<i64, StateError> {
    match &entry.key.item_key {
        Some(TypedValue::Int8(value)) => Ok(*value),
        _ => Err(StateError::SnapshotMismatch),
    }
}
