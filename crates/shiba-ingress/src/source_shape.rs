use postgres::{GenericClient, Transaction};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::PgoutputSource;

use crate::IngressError;

const INT8_OID: i64 = 20;

pub(crate) fn derive_source(
    transaction: &mut Transaction<'_>,
    source_id: SourceId,
    generation: SlotGeneration,
    relation_class: i64,
    relation_oid: i64,
    published_columns: &[i16],
) -> Result<PgoutputSource, IngressError> {
    let relation = transaction
        .query_opt(
            "SELECT class.relkind::text, class.relreplident::text
             FROM pg_catalog.pg_class AS class
             WHERE class.oid = $1::bigint::oid",
            &[&relation_oid],
        )?
        .ok_or(IngressError::Governance("bound relation is missing"))?;
    if relation.get::<_, &str>(0) != "r" || relation.get::<_, &str>(1) != "d" {
        return Err(IngressError::Governance(
            "relation identity is not admitted",
        ));
    }
    let primary_key: bool = transaction
        .query_one(
            "SELECT count(*) = 1 FROM pg_catalog.pg_index AS index
             WHERE index.indrelid = $1::bigint::oid
               AND index.indisprimary AND index.indnkeyatts = 1
               AND index.indkey[0] = 1
               AND index.indpred IS NULL AND index.indexprs IS NULL",
            &[&relation_oid],
        )?
        .get(0);
    if !primary_key {
        return Err(IngressError::Governance("source requires primary int8 id"));
    }
    let columns = transaction.query(
        "SELECT attribute.attnum::integer, attribute.atttypid::bigint,
                attribute.attnotnull
         FROM pg_catalog.pg_attribute AS attribute
         WHERE attribute.attrelid = $1::bigint::oid
           AND attribute.attnum > 0 AND NOT attribute.attisdropped
         ORDER BY attribute.attnum",
        &[&relation_oid],
    )?;
    let key_only = column_matches(&columns, 0, 1, true) && columns.len() == 1;
    let nullable_payload = columns.len() == 2
        && column_matches(&columns, 0, 1, true)
        && column_matches(&columns, 1, 2, false);
    if !key_only && !nullable_payload {
        return Err(IngressError::Governance(
            "source column shape is not admitted",
        ));
    }
    validate_bindings(
        transaction,
        source_id,
        relation_class,
        relation_oid,
        columns.len(),
    )?;
    let decoded_relation_oid = u32::try_from(relation_oid)
        .map_err(|_| IngressError::Governance("relation OID is out of range"))?;
    if published_columns == [1] {
        Ok(PgoutputSource::new(
            source_id,
            generation,
            decoded_relation_oid,
        ))
    } else if nullable_payload && published_columns == [1, 2] {
        Ok(PgoutputSource::with_nullable_int8_payload(
            source_id,
            generation,
            decoded_relation_oid,
        ))
    } else {
        Err(IngressError::Governance(
            "published column shape is not admitted",
        ))
    }
}

fn column_matches(columns: &[postgres::Row], index: usize, attnum: i32, not_null: bool) -> bool {
    columns.get(index).is_some_and(|column| {
        column.get::<_, i32>(0) == attnum
            && column.get::<_, i64>(1) == INT8_OID
            && column.get::<_, bool>(2) == not_null
    })
}

pub(crate) fn validate_bindings(
    transaction: &mut impl GenericClient,
    source_id: SourceId,
    relation_class: i64,
    relation_oid: i64,
    column_count: usize,
) -> Result<Option<i64>, IngressError> {
    let source_key = i64::try_from(source_id.get())
        .map_err(|_| IngressError::Governance("source ID exceeds bigint"))?;
    let marker = transaction.query_opt(
        "SELECT retired_bootstrap_id, retired_slot_name::text,
                retired_slot_generation
         FROM shiba_internal.source_bootstrap WHERE source_id = $1",
        &[&source_key],
    )?;
    let marker = marker.map(|row| {
        (
            row.get::<_, Option<i64>>(0),
            row.get::<_, Option<String>>(1),
            row.get::<_, Option<i64>>(2),
        )
    });
    let (m12_generation, expected_rows) = match marker {
        None => (false, column_count + 1),
        Some((None, None, None)) => {
            if column_count != 2 {
                return Err(IngressError::Governance("source binding set drifted"));
            }
            (false, 3)
        }
        Some((Some(bootstrap), Some(slot), Some(generation)))
            if bootstrap > 0 && !slot.is_empty() && generation > 0 && column_count == 2 =>
        {
            (true, 4)
        }
        Some((Some(_), Some(_), Some(_))) => {
            return Err(IngressError::Governance("source binding set drifted"));
        }
        Some(_) => return Err(IngressError::Governance("rebuild marker is partial")),
    };
    let rows = transaction.query(
        "SELECT binding.binding_kind, binding.address_classid::bigint,
                binding.address_objid::bigint, binding.address_objsubid
         FROM shiba_internal.source_binding AS binding
         WHERE binding.source_id = $1 ORDER BY binding.address_objsubid",
        &[&source_key],
    )?;
    if rows.len() != expected_rows {
        return Err(IngressError::Governance("source binding set drifted"));
    }
    let mut relation_seen = false;
    let mut columns_seen = vec![false; column_count];
    let mut identity_oid = None;
    for row in rows {
        let kind: &str = row.get(0);
        let classid: i64 = row.get(1);
        let objid: i64 = row.get(2);
        let subid: i32 = row.get(3);
        if classid != relation_class {
            return Err(IngressError::Governance("source binding address drifted"));
        }
        match (kind, subid) {
            ("relation", 0) if objid == relation_oid && !relation_seen => relation_seen = true,
            ("column", value) if objid == relation_oid && value > 0 => {
                let index = usize::try_from(value - 1)
                    .ok()
                    .filter(|index| *index < column_count)
                    .ok_or(IngressError::Governance("source binding address drifted"))?;
                if columns_seen[index] {
                    return Err(IngressError::Governance("source binding address drifted"));
                }
                columns_seen[index] = true;
            }
            ("identity_index", 0) if m12_generation && identity_oid.is_none() => {
                identity_oid = Some(objid);
            }
            _ => return Err(IngressError::Governance("source binding address drifted")),
        }
    }
    if !relation_seen || columns_seen.iter().any(|seen| !seen) {
        return Err(IngressError::Governance("source binding set drifted"));
    }
    if let Some(identity_oid) = identity_oid {
        let exact_primary: bool = transaction
            .query_one(
                "SELECT EXISTS (
                   SELECT 1 FROM pg_catalog.pg_index AS identity
                   WHERE identity.indexrelid = $1::bigint::oid
                     AND identity.indrelid = $2::bigint::oid
                     AND identity.indisprimary AND identity.indisunique
                     AND identity.indisvalid AND identity.indisready
                     AND identity.indnkeyatts = 1 AND identity.indnatts = 1
                     AND identity.indkey[0] = 1
                     AND identity.indexprs IS NULL AND identity.indpred IS NULL)",
                &[&identity_oid, &relation_oid],
            )?
            .get(0);
        if !exact_primary {
            return Err(IngressError::Governance("identity binding drifted"));
        }
        Ok(Some(identity_oid))
    } else if m12_generation {
        Err(IngressError::Governance("identity binding is missing"))
    } else {
        Ok(None)
    }
}
