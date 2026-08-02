use postgres::Transaction;
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

fn validate_bindings(
    transaction: &mut Transaction<'_>,
    source_id: SourceId,
    relation_class: i64,
    relation_oid: i64,
    column_count: usize,
) -> Result<(), IngressError> {
    let source_key = i64::try_from(source_id.get())
        .map_err(|_| IngressError::Governance("source ID exceeds bigint"))?;
    let rows = transaction.query(
        "SELECT binding.binding_kind, binding.address_classid::bigint,
                binding.address_objid::bigint, binding.address_objsubid
         FROM shiba_internal.source_binding AS binding
         WHERE binding.source_id = $1 ORDER BY binding.address_objsubid",
        &[&source_key],
    )?;
    if rows.len() != column_count + 1 {
        return Err(IngressError::Governance("source binding set drifted"));
    }
    for row in rows {
        let kind: &str = row.get(0);
        let classid: i64 = row.get(1);
        let objid: i64 = row.get(2);
        let subid: i32 = row.get(3);
        let valid_subid = subid == 0 && kind == "relation"
            || subid > 0
                && kind == "column"
                && usize::try_from(subid)
                    .ok()
                    .is_some_and(|value| value <= column_count);
        if classid != relation_class || objid != relation_oid || !valid_subid {
            return Err(IngressError::Governance("source binding address drifted"));
        }
    }
    Ok(())
}
