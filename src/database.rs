//! Shared decoding for scalar facts returned by PostgreSQL primitives.

use pgrx::prelude::{FromDatum, IntoDatum};
use pgrx::spi::{SpiHeapTupleData, SpiTupleTable};

pub(crate) fn required<T: FromDatum + IntoDatum>(
    row: &SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    row.get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}

pub(crate) fn required_heap<T: FromDatum + IntoDatum>(
    row: &SpiHeapTupleData<'_>,
    ordinal: usize,
    name: &str,
) -> Result<T, String> {
    row.get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("database returned NULL {name}"))
}

pub(crate) fn optional<T: FromDatum + IntoDatum>(
    row: &SpiTupleTable<'_>,
    ordinal: usize,
    name: &str,
) -> Result<Option<T>, String> {
    row.get::<T>(ordinal)
        .map_err(|error| format!("invalid {name}: {error}"))
}

pub(crate) fn require_count(
    rows: &SpiTupleTable<'_>,
    expected: usize,
    name: &str,
) -> Result<(), String> {
    if rows.len() != expected {
        return Err(format!(
            "{name} returned {} rows, expected {expected}",
            rows.len()
        ));
    }
    Ok(())
}

pub(crate) fn nonnegative(value: i64, name: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{name} is negative"))
}

pub(crate) fn database_nonnegative(value: i64, name: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("database returned negative {name}"))
}
