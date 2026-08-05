use super::StateError;
use crate::TypedValue;

use super::StateKey;

/// Version of the signed-int8 order-key encoding used by ordered state reads.
pub const INT8_ORDER_KEY_VERSION: u32 = 1;

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

#[cfg(test)]
mod tests {
    use super::*;
    use core::num::NonZeroU32;

    use super::super::{
        StateEntry, StateRange, StateRangeDirection, StateRangeResult, StateReadSet, StateSnapshot,
    };
    use crate::NodeId;

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

    #[test]
    fn range_limit_is_checked_per_range_and_exact_overlap_counts_once() {
        let range = StateRange {
            node_id: key(0).node_id,
            namespace: 2,
            partition_key: TypedValue::Int8(7),
            direction: StateRangeDirection::Ascending,
            limit: 1,
            order_key_version: INT8_ORDER_KEY_VERSION,
        };
        let read_set = StateReadSet::with_ranges(vec![key(-1)], vec![], vec![range]).unwrap();
        let entries = vec![
            StateEntry {
                key: key(-1),
                state: None,
            },
            StateEntry {
                key: key(0),
                state: None,
            },
        ];
        StateSnapshot::new_with_ranges(
            &read_set,
            entries.clone(),
            &[StateRangeResult {
                range_index: 0,
                entries: entries.clone(),
            }],
        )
        .unwrap();
        assert!(
            StateSnapshot::new_with_ranges(
                &read_set,
                vec![
                    entries[0].clone(),
                    entries[1].clone(),
                    StateEntry {
                        key: key(1),
                        state: None,
                    },
                ],
                &[StateRangeResult {
                    range_index: 0,
                    entries: vec![
                        entries[0].clone(),
                        entries[1].clone(),
                        StateEntry {
                            key: key(1),
                            state: None,
                        }
                    ],
                }],
            )
            .is_err()
        );
    }

    #[test]
    fn range_provenance_rejects_conflicts_and_wrong_order() {
        let range = StateRange {
            node_id: key(0).node_id,
            namespace: 2,
            partition_key: TypedValue::Int8(7),
            direction: StateRangeDirection::Descending,
            limit: 2,
            order_key_version: INT8_ORDER_KEY_VERSION,
        };
        let read_set =
            StateReadSet::with_ranges(Vec::new(), Vec::new(), vec![range.clone(), range.clone()]);
        assert!(read_set.is_err());

        let opposite_direction = StateRange {
            direction: StateRangeDirection::Ascending,
            ..range.clone()
        };
        assert!(
            StateReadSet::with_ranges(
                Vec::new(),
                Vec::new(),
                vec![range.clone(), opposite_direction]
            )
            .is_err()
        );

        let read_set = StateReadSet::with_ranges(Vec::new(), Vec::new(), vec![range]).unwrap();
        let entries = vec![
            StateEntry {
                key: key(0),
                state: None,
            },
            StateEntry {
                key: key(1),
                state: None,
            },
        ];
        assert!(
            StateSnapshot::new_with_ranges(
                &read_set,
                entries.clone(),
                &[StateRangeResult {
                    range_index: 0,
                    entries,
                }],
            )
            .is_err()
        );
    }

    #[test]
    fn range_identity_allows_distinct_namespace_and_partition_only() {
        let base = StateRange {
            node_id: key(0).node_id,
            namespace: 2,
            partition_key: TypedValue::Int8(7),
            direction: StateRangeDirection::Ascending,
            limit: 1,
            order_key_version: INT8_ORDER_KEY_VERSION,
        };
        let different_namespace = StateRange {
            namespace: 3,
            ..base.clone()
        };
        let different_partition = StateRange {
            partition_key: TypedValue::Int8(8),
            ..base.clone()
        };
        let read_set = StateReadSet::with_ranges(
            Vec::new(),
            Vec::new(),
            vec![base.clone(), different_namespace, different_partition],
        )
        .unwrap();
        assert_eq!(read_set.ranges.len(), 3);

        let duplicate_coordinates = StateReadSet {
            keys: Vec::new(),
            partitions: Vec::new(),
            ranges: vec![
                base.clone(),
                StateRange {
                    direction: StateRangeDirection::Descending,
                    ..base
                },
            ],
        };
        assert_eq!(
            StateSnapshot::new(&duplicate_coordinates, Vec::new()),
            Err(StateError::AmbiguousRange)
        );
    }
}
