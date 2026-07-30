use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiTupleTable};

use crate::postgres::quote_identifier;

use super::{required_row, required_table as required};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelationRef {
    oid: pg_sys::Oid,
    qualified: String,
}

impl RelationRef {
    pub(crate) const fn oid(&self) -> pg_sys::Oid {
        self.oid
    }

    pub(crate) fn sql(&self) -> &str {
        &self.qualified
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TypeRef {
    oid: pg_sys::Oid,
    qualified: String,
}

impl TypeRef {
    pub(crate) const fn oid(&self) -> pg_sys::Oid {
        self.oid
    }

    pub(crate) fn sql(&self) -> &str {
        &self.qualified
    }
}

/// Build the one SQL identity expression used for typed bag rows.
///
/// A Scan bootstrap starts from a direct PostgreSQL datum, while ingress
/// reconstructs each datum from pgoutput text. Round-tripping the complete
/// named composite through PostgreSQL record text makes both paths use the
/// same type I/O before `record_send`. This normalizes details that pgoutput
/// text cannot carry, such as float NaN payload bits, without losing details
/// that PostgreSQL text does carry, such as array lower bounds.
pub(crate) fn canonical_row_key_sql(row_expression: &str, row_type: &TypeRef) -> String {
    format!(
        "pg_catalog.record_send(\
           (({row_expression})::text)::{}\
         )",
        row_type.sql()
    )
}

/// Logical binary bytes materialized for one scalar state value.
///
/// Wrapping the scalar in a one-field record gives every PostgreSQL type the
/// same detoasted binary accounting path without treating the bytes as row
/// identity.
pub(crate) fn scalar_work_bytes_sql(value_expression: &str) -> String {
    format!(
        "pg_catalog.octet_length(\
           pg_catalog.record_send(ROW({value_expression}))\
         )::bigint"
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PayloadStorage {
    pub(crate) relation: RelationRef,
    pub(crate) row_type: TypeRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AttributeRef {
    pub(crate) number: i16,
    pub(crate) name: String,
    pub(crate) type_oid: pg_sys::Oid,
    pub(crate) typmod: i32,
    pub(crate) collation_oid: pg_sys::Oid,
    pub(crate) not_null: bool,
}

pub(crate) fn payload(
    client: &mut SpiClient<'_>,
    stream_id: i64,
) -> Result<PayloadStorage, String> {
    if stream_id <= 0 {
        return Err("effect stream ID must be positive".into());
    }
    let arguments = unsafe { [DatumWithOid::new(stream_id, pg_sys::INT8OID)] };
    let table = client
        .select(
            r#"
            SELECT relation.oid,
                   relation_namespace.nspname::text,
                   relation.relname::text,
                   row_type.oid,
                   type_namespace.nspname::text,
                   row_type.typname::text
            FROM shiba_internal.effect_streams AS payload
            JOIN pg_catalog.pg_class AS relation
              ON relation.oid = payload.relation_oid
             AND relation.relkind = 'r'
             AND relation.relpersistence = 'p'
            JOIN pg_catalog.pg_namespace AS relation_namespace
              ON relation_namespace.oid = relation.relnamespace
             AND relation_namespace.nspname = 'shiba_internal'
            JOIN pg_catalog.pg_type AS row_type
              ON row_type.oid = payload.row_type_oid
             AND row_type.typtype = 'c'
            JOIN pg_catalog.pg_namespace AS type_namespace
              ON type_namespace.oid = row_type.typnamespace
             AND type_namespace.nspname = 'shiba_internal'
            WHERE payload.stream_id = $1
              AND (
                SELECT pg_catalog.array_agg(
                         attribute.attname::text
                         ORDER BY attribute.attnum
                       )
                FROM pg_catalog.pg_attribute AS attribute
                WHERE attribute.attrelid = relation.oid
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
              ) = ARRAY[
                'stream_id','chunk_seq','row_ordinal','weight','row_value'
              ]::text[]
              AND (
                SELECT pg_catalog.array_agg(
                         attribute.atttypid
                         ORDER BY attribute.attnum
                       )
                FROM pg_catalog.pg_attribute AS attribute
                WHERE attribute.attrelid = relation.oid
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
              ) = ARRAY[
                'bigint'::pg_catalog.regtype::oid,
                'bigint'::pg_catalog.regtype::oid,
                'bigint'::pg_catalog.regtype::oid,
                'bigint'::pg_catalog.regtype::oid,
                row_type.oid
              ]::oid[]
              AND (
                SELECT pg_catalog.array_agg(
                         attribute.attnotnull
                         ORDER BY attribute.attnum
                       )
                FROM pg_catalog.pg_attribute AS attribute
                WHERE attribute.attrelid = relation.oid
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
              ) = ARRAY[true,true,true,true,true]::boolean[]
              AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_attribute AS attribute
                WHERE attribute.attrelid = relation.oid
                  AND attribute.attnum = 5
                  AND attribute.attname = 'row_value'
                  AND attribute.atttypid = row_type.oid
                  AND NOT attribute.attisdropped
              )
              AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint AS constraint_catalog
                WHERE constraint_catalog.conrelid = relation.oid
                  AND constraint_catalog.contype = 'p'
                  AND constraint_catalog.conkey = ARRAY[1,2,3]::smallint[]
              )
              AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint AS constraint_catalog
                WHERE constraint_catalog.conrelid = relation.oid
                  AND constraint_catalog.contype = 'f'
                  AND constraint_catalog.confrelid =
                        'shiba_internal.effect_stream_chunks'::pg_catalog.regclass
                  AND constraint_catalog.conkey = ARRAY[1,2]::smallint[]
                  AND constraint_catalog.confkey = ARRAY[1,2]::smallint[]
                  AND constraint_catalog.confdeltype = 'c'
              )
            "#,
            Some(2),
            &arguments,
        )
        .map_err(|error| format!("could not resolve payload storage: {error}"))?;
    if table.len() != 1 {
        return Err(format!(
            "effect stream {stream_id} has no unique valid typed payload relation"
        ));
    }
    let row = table.first();
    Ok(PayloadStorage {
        relation: relation_ref(&row, 1, 2, 3, "payload")?,
        row_type: type_ref(&row, 4, 5, 6, "payload row")?,
    })
}

pub(crate) fn continuation(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
) -> Result<RelationRef, String> {
    cataloged_operator_relation(
        client,
        "operator_continuation_relations",
        None,
        result_oid,
        stage_id,
    )
}

pub(crate) fn state(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    state_slot: i32,
) -> Result<RelationRef, String> {
    if state_slot < 0 {
        return Err("operator state slot must be non-negative".into());
    }
    cataloged_operator_relation(
        client,
        "operator_state_relations",
        Some(state_slot),
        result_oid,
        stage_id,
    )
}

pub(crate) fn result(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
) -> Result<RelationRef, String> {
    let arguments = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    let table = client
        .select(
            r#"
            SELECT relation.oid,
                   namespace.nspname::text,
                   relation.relname::text
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
             AND namespace.nspname = 'shiba'
            JOIN shiba_internal.dataflows AS dataflow
              ON dataflow.result_oid = relation.oid
             AND dataflow.active
            WHERE relation.oid = $1
              AND relation.relkind = 'r'
              AND relation.relpersistence = 'p'
            "#,
            Some(2),
            &arguments,
        )
        .map_err(|error| format!("could not resolve result relation: {error}"))?;
    if table.len() != 1 {
        return Err(format!(
            "result OID {} has no unique active Shiba relation",
            result_oid.to_u32()
        ));
    }
    relation_ref(&table.first(), 1, 2, 3, "result")
}

pub(crate) fn composite_attributes(
    client: &mut SpiClient<'_>,
    type_: &TypeRef,
) -> Result<Vec<AttributeRef>, String> {
    let arguments = unsafe { [DatumWithOid::new(type_.oid, pg_sys::OIDOID)] };
    load_attributes(
        client,
        r#"
        SELECT attribute.attnum,
               attribute.attname::text,
               attribute.atttypid,
               attribute.atttypmod,
               attribute.attcollation,
               attribute.attnotnull
        FROM pg_catalog.pg_type AS row_type
        JOIN pg_catalog.pg_class AS relation
          ON relation.oid = row_type.typrelid
         AND relation.relkind = 'c'
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
         AND namespace.nspname = 'shiba_internal'
        JOIN pg_catalog.pg_attribute AS attribute
          ON attribute.attrelid = relation.oid
        WHERE row_type.oid = $1
          AND row_type.typtype = 'c'
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
        ORDER BY attribute.attnum
        "#,
        &arguments,
        "composite type",
    )
}

pub(crate) fn relation_attributes(
    client: &mut SpiClient<'_>,
    relation_oid: pg_sys::Oid,
) -> Result<Vec<AttributeRef>, String> {
    let arguments = unsafe { [DatumWithOid::new(relation_oid, pg_sys::OIDOID)] };
    load_attributes(
        client,
        r#"
        SELECT attribute.attnum,
               attribute.attname::text,
               attribute.atttypid,
               attribute.atttypmod,
               attribute.attcollation,
               attribute.attnotnull
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_attribute AS attribute
          ON attribute.attrelid = relation.oid
        WHERE relation.oid = $1
          AND relation.relkind = 'r'
          AND relation.relpersistence = 'p'
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
        ORDER BY attribute.attnum
        "#,
        &arguments,
        "relation",
    )
}

fn cataloged_operator_relation(
    client: &mut SpiClient<'_>,
    catalog: &str,
    state_slot: Option<i32>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
) -> Result<RelationRef, String> {
    if result_oid == pg_sys::InvalidOid || stage_id < 0 {
        return Err("invalid operator storage identity".into());
    }
    let (query, arguments) = match state_slot {
        Some(state_slot) => (
            format!(
                r#"
                SELECT relation.oid,
                       namespace.nspname::text,
                       relation.relname::text
                FROM shiba_internal.{catalog} AS storage
                JOIN pg_catalog.pg_class AS relation
                  ON relation.oid = storage.relation_oid
                 AND relation.relkind = 'r'
                 AND relation.relpersistence = 'p'
                JOIN pg_catalog.pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                 AND namespace.nspname = 'shiba_internal'
                WHERE storage.result_oid = $1
                  AND storage.stage_id = $2
                  AND storage.state_slot = $3
                "#,
            ),
            unsafe {
                vec![
                    DatumWithOid::new(result_oid, pg_sys::OIDOID),
                    DatumWithOid::new(stage_id, pg_sys::INT4OID),
                    DatumWithOid::new(state_slot, pg_sys::INT4OID),
                ]
            },
        ),
        None => (
            format!(
                r#"
                SELECT relation.oid,
                       namespace.nspname::text,
                       relation.relname::text
                FROM shiba_internal.{catalog} AS storage
                JOIN pg_catalog.pg_class AS relation
                  ON relation.oid = storage.relation_oid
                 AND relation.relkind = 'r'
                 AND relation.relpersistence = 'p'
                JOIN pg_catalog.pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                 AND namespace.nspname = 'shiba_internal'
                WHERE storage.result_oid = $1
                  AND storage.stage_id = $2
                "#,
            ),
            unsafe {
                vec![
                    DatumWithOid::new(result_oid, pg_sys::OIDOID),
                    DatumWithOid::new(stage_id, pg_sys::INT4OID),
                ]
            },
        ),
    };
    let table = client
        .select(&query, Some(2), &arguments)
        .map_err(|error| format!("could not resolve operator storage: {error}"))?;
    if table.len() != 1 {
        let slot = state_slot.map_or(String::new(), |slot| format!(" slot {slot}"));
        return Err(format!(
            "operator {}/{}{} has no unique valid typed relation",
            result_oid.to_u32(),
            stage_id,
            slot
        ));
    }
    relation_ref(&table.first(), 1, 2, 3, "operator")
}

fn relation_ref(
    row: &SpiTupleTable<'_>,
    oid_column: usize,
    namespace_column: usize,
    name_column: usize,
    label: &str,
) -> Result<RelationRef, String> {
    let oid = required::<pg_sys::Oid>(row, oid_column, &format!("{label} relation OID"))?;
    let namespace = required::<String>(row, namespace_column, &format!("{label} namespace"))?;
    let name = required::<String>(row, name_column, &format!("{label} relation name"))?;
    Ok(RelationRef {
        oid,
        qualified: format!(
            "{}.{}",
            quote_identifier(&namespace),
            quote_identifier(&name)
        ),
    })
}

fn type_ref(
    row: &SpiTupleTable<'_>,
    oid_column: usize,
    namespace_column: usize,
    name_column: usize,
    label: &str,
) -> Result<TypeRef, String> {
    let oid = required::<pg_sys::Oid>(row, oid_column, &format!("{label} type OID"))?;
    let namespace = required::<String>(row, namespace_column, &format!("{label} type namespace"))?;
    let name = required::<String>(row, name_column, &format!("{label} type name"))?;
    Ok(TypeRef {
        oid,
        qualified: format!(
            "{}.{}",
            quote_identifier(&namespace),
            quote_identifier(&name)
        ),
    })
}

fn load_attributes(
    client: &mut SpiClient<'_>,
    query: &str,
    arguments: &[DatumWithOid<'_>],
    label: &str,
) -> Result<Vec<AttributeRef>, String> {
    let table = client
        .select(query, None, arguments)
        .map_err(|error| format!("could not resolve {label} attributes: {error}"))?;
    if table.is_empty() {
        return Err(format!("{label} has no live user attributes"));
    }
    let mut attributes = Vec::with_capacity(table.len());
    for row in table {
        attributes.push(AttributeRef {
            number: required_row(&row, 1, "attribute number")?,
            name: required_row(&row, 2, "attribute name")?,
            type_oid: required_row(&row, 3, "attribute type OID")?,
            typmod: required_row(&row, 4, "attribute typmod")?,
            collation_oid: required_row(&row, 5, "attribute collation OID")?,
            not_null: required_row(&row, 6, "attribute nullability")?,
        });
    }
    Ok(attributes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_row_key_roundtrips_before_binary_encoding() {
        let row_type = TypeRef {
            oid: pg_sys::Oid::from_u32(42),
            qualified: "shiba_internal.\"payload row\"".into(),
        };
        assert_eq!(
            canonical_row_key_sql("input_row.row_value", &row_type),
            "pg_catalog.record_send(\
             ((input_row.row_value)::text)::shiba_internal.\"payload row\"\
             )"
        );
    }

    #[test]
    fn scalar_work_bytes_use_shared_binary_accounting() {
        assert_eq!(
            scalar_work_bytes_sql("finalized.function_value"),
            "pg_catalog.octet_length(\
             pg_catalog.record_send(ROW(finalized.function_value))\
             )::bigint"
        );
    }
}
