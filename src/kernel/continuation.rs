//! Common authority plumbing for operator-local typed continuations.
//!
//! A continuation relation remains an operator-owned, typed table.  This
//! module deliberately knows nothing about its phase enum or state fields: it
//! only enforces the common one-row ABI and performs the optimistic
//! delete/insert replacement used by every resumable kernel.

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys;
use pgrx::spi::SpiTupleTable;

use super::{RelationRef, StepContext};

pub(crate) fn validate_authority(
    transaction: &mut StepContext<'_, '_>,
    relation: bool,
) -> Result<(), String> {
    transaction.bind_continuation_authority(relation)
}

/// One physical continuation column, including the singleton authority row.
#[derive(Clone, Copy)]
pub(crate) struct Column {
    pub(crate) name: &'static str,
    pub(crate) type_oid: pg_sys::Oid,
    pub(crate) not_null: bool,
    /// A trusted, static SQL type name for scalar representations such as a
    /// lossless numeric transported through SPI as text.
    pub(crate) parameter_cast: Option<&'static str>,
}

impl Column {
    pub(crate) const fn required(name: &'static str, type_oid: pg_sys::Oid) -> Self {
        Self {
            name,
            type_oid,
            not_null: true,
            parameter_cast: None,
        }
    }

    pub(crate) const fn nullable(name: &'static str, type_oid: pg_sys::Oid) -> Self {
        Self {
            name,
            type_oid,
            not_null: false,
            parameter_cast: None,
        }
    }

    pub(crate) const fn nullable_as(
        name: &'static str,
        type_oid: pg_sys::Oid,
        parameter_cast: &'static str,
    ) -> Self {
        Self {
            name,
            type_oid,
            not_null: false,
            parameter_cast: Some(parameter_cast),
        }
    }
}

/// Checks the exact typed ABI; accepting a merely compatible relation would
/// make a stale catalog row authoritative after a migration.
pub(crate) fn validate_abi(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    columns: &[Column],
    owner: &str,
) -> Result<(), String> {
    let attributes = transaction.relation_attributes(relation.oid())?;
    if attributes.len() != columns.len()
        || attributes.iter().zip(columns).any(|(actual, expected)| {
            actual.name != expected.name
                || actual.type_oid != expected.type_oid
                || actual.not_null != expected.not_null
        })
    {
        return Err(format!("{owner} continuation relation has an invalid ABI"));
    }
    Ok(())
}

/// Locks and decodes the optional single authority row.  The decoder retains
/// ownership of typed and phase-specific field decoding.
pub(crate) fn lock_one<T>(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    select: &str,
    owner: &str,
    decode: impl FnOnce(SpiTupleTable<'_>) -> Result<T, String>,
) -> Result<Option<T>, String> {
    let query = format!(
        "SELECT {select} FROM {} WHERE singleton FOR UPDATE",
        relation.sql()
    );
    let rows = transaction.lock(&query, &[])?;
    match rows.len() {
        0 => Ok(None),
        1 => decode(rows).map(Some),
        count => Err(format!(
            "{owner} continuation relation contains {count} rows"
        )),
    }
}

/// Clears the authority row after the caller has locked it with [`lock_one`].
pub(crate) fn clear_locked(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    owner: &str,
) -> Result<(), String> {
    transaction.prepare_continuation_replace(true)?;
    let query = format!(
        "DELETE FROM {} WHERE singleton RETURNING singleton",
        relation.sql()
    );
    if transaction.write(&query, &[])?.len() != 1 {
        return Err(format!("{owner} continuation clear failed"));
    }
    transaction.record_continuation_replace(false);
    Ok(())
}

/// Atomically replaces the one authoritative continuation row.
///
/// `columns` includes `singleton` as its first entry; `old` and `next` carry
/// exactly the remaining values in that order.  The old row is compared field
/// by field so a stale worker can never erase a successor committed by another
/// transaction.
pub(crate) fn replace_cas(
    transaction: &mut StepContext<'_, '_>,
    relation: &RelationRef,
    columns: &[Column],
    old: Option<&[DatumWithOid<'_>]>,
    next: Option<&[DatumWithOid<'_>]>,
    owner: &str,
) -> Result<(), String> {
    transaction.prepare_continuation_replace(old.is_some())?;
    let fields = columns
        .get(1..)
        .ok_or_else(|| "continuation schema omitted singleton".to_string())?;
    for values in [old, next].into_iter().flatten() {
        if values.len() != fields.len() {
            return Err(format!(
                "{owner} continuation field count disagrees with its ABI"
            ));
        }
    }
    if let Some(old) = old {
        let predicate = fields
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "{} IS NOT DISTINCT FROM {}",
                    column.name,
                    parameter_sql(index + 1, column),
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let query = format!(
            "DELETE FROM {} WHERE singleton AND {predicate} RETURNING singleton",
            relation.sql()
        );
        if transaction.write(&query, old)?.len() != 1 {
            return Err(format!("{owner} continuation compare-and-set failed"));
        }
    }
    if let Some(next) = next {
        let names = columns
            .iter()
            .map(|column| column.name)
            .collect::<Vec<_>>()
            .join(",");
        let values = fields
            .iter()
            .enumerate()
            .map(|(index, column)| parameter_sql(index + 1, column))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            "INSERT INTO {}({names}) VALUES(true,{values}) RETURNING singleton",
            relation.sql()
        );
        if transaction.write(&query, next)?.len() != 1 {
            return Err(format!("{owner} continuation insert failed"));
        }
    }
    transaction.record_continuation_replace(next.is_some());
    Ok(())
}

fn parameter_sql(index: usize, column: &Column) -> String {
    match column.parameter_cast {
        Some(cast) => format!("${index}::{cast}"),
        None => format!("${index}"),
    }
}
