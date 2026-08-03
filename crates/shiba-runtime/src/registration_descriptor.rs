use postgres::Transaction;
use shiba_compiler::{IdentityIndexDescriptor, SourceColumnDescriptor, SourceDescriptor};
use shiba_operator::ObjectAddress;
use shiba_protocol::SourceId;

use crate::M2Error;
use crate::registration::{RebuildSourceTarget, RegistrationError};

pub(super) fn source_descriptor(
    transaction: &mut Transaction<'_>,
    source_id: SourceId,
) -> Result<(SourceDescriptor, Option<IdentityIndexDescriptor>), RegistrationError> {
    let source_key = i64::try_from(source_id.get()).map_err(|_| M2Error::SourceInvalidated)?;
    let relation = transaction
        .query_opt(
            "SELECT address_classid::bigint,address_objid::bigint,address_objsubid
         FROM shiba_internal.source_binding WHERE source_id=$1 AND binding_kind='relation'
         FOR UPDATE",
            &[&source_key],
        )?
        .ok_or(M2Error::SourceBindingMissing)?;
    let relation = address_at(&relation, 0)?;
    let columns = transaction
        .query(
            "SELECT attribute.attname,binding.address_classid::bigint,
                binding.address_objid::bigint,binding.address_objsubid,
                attribute.atttypid::bigint,NOT attribute.attnotnull
         FROM shiba_internal.source_binding AS binding
         JOIN pg_catalog.pg_attribute AS attribute ON attribute.attrelid=binding.address_objid
          AND attribute.attnum=binding.address_objsubid AND NOT attribute.attisdropped
         WHERE binding.source_id=$1 AND binding.binding_kind='column'
         ORDER BY binding.address_objsubid FOR UPDATE OF binding",
            &[&source_key],
        )?
        .into_iter()
        .map(|row| {
            Ok(SourceColumnDescriptor {
                name: row.get(0),
                address: address_at(&row, 1)?,
                type_oid: u32::try_from(row.get::<_, i64>(4))
                    .map_err(|_| M2Error::SourceInvalidated)?,
                nullable: row.get(5),
            })
        })
        .collect::<Result<Vec<_>, RegistrationError>>()?;
    let identity = identity_descriptor(transaction, source_key, relation, &columns)?;
    Ok((
        SourceDescriptor {
            source_id,
            relation,
            columns,
        },
        identity,
    ))
}

fn identity_descriptor(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    relation: ObjectAddress,
    columns: &[SourceColumnDescriptor],
) -> Result<Option<IdentityIndexDescriptor>, RegistrationError> {
    let Some(row) = transaction.query_opt(
        "SELECT binding.address_classid::bigint,binding.address_objid::bigint,
                binding.address_objsubid,index.indisunique,index.indisvalid,index.indisready,
                index.indexprs IS NOT NULL,index.indpred IS NOT NULL,
                (index.indisreplident OR
                 (relation.relreplident='d' AND index.indisprimary)),
                (index.indkey::smallint[])[0]::integer
         FROM shiba_internal.source_binding AS binding JOIN pg_catalog.pg_index AS index
           ON index.indexrelid=binding.address_objid
         JOIN pg_catalog.pg_class AS relation ON relation.oid=index.indrelid
         WHERE binding.source_id=$1 AND binding.binding_kind='identity_index' FOR UPDATE OF binding",
        &[&source_id],
    )? else { return Ok(None) };
    let key_column = columns
        .iter()
        .find(|column| column.address.sub_id == row.get::<_, i32>(9))
        .ok_or(M2Error::SourceInvalidated)?
        .address;
    Ok(Some(IdentityIndexDescriptor {
        address: address_at(&row, 0)?,
        relation,
        key_column,
        unique: row.get(3),
        valid: row.get(4),
        ready: row.get(5),
        has_expression: row.get(6),
        has_predicate: row.get(7),
        effective_replica_identity: row.get(8),
    }))
}

pub(super) fn target_descriptor(
    transaction: &mut Transaction<'_>,
    target: RebuildSourceTarget,
) -> Result<(SourceDescriptor, IdentityIndexDescriptor), RegistrationError> {
    let relation = ObjectAddress {
        class_id: relation_class_id(transaction)?,
        object_id: target.relation_id,
        sub_id: 0,
    };
    let columns = transaction
        .query(
            "SELECT attribute.attname,attribute.attnum::integer,
                attribute.atttypid::bigint,NOT attribute.attnotnull
         FROM pg_catalog.pg_attribute AS attribute JOIN pg_catalog.pg_class AS class
           ON class.oid=attribute.attrelid
         WHERE attribute.attrelid=$1 AND class.relkind='r'
           AND attribute.attnum>0 AND NOT attribute.attisdropped ORDER BY attribute.attnum",
            &[&target.relation_id],
        )?
        .into_iter()
        .map(|row| {
            Ok(SourceColumnDescriptor {
                name: row.get(0),
                address: ObjectAddress {
                    class_id: relation.class_id,
                    object_id: target.relation_id,
                    sub_id: row.get(1),
                },
                type_oid: u32::try_from(row.get::<_, i64>(2))
                    .map_err(|_| M2Error::SourceInvalidated)?,
                nullable: row.get(3),
            })
        })
        .collect::<Result<Vec<_>, RegistrationError>>()?;
    let row = transaction
        .query_opt(
            "SELECT index.indisunique,index.indisvalid,index.indisready,
                index.indexprs IS NOT NULL,index.indpred IS NOT NULL,
                (index.indisreplident OR
                 (relation.relreplident='d' AND index.indisprimary)),
                (index.indkey::smallint[])[0]::integer
         FROM pg_catalog.pg_index AS index
         JOIN pg_catalog.pg_class AS relation ON relation.oid=index.indrelid
         WHERE index.indexrelid=$1 AND index.indrelid=$2",
            &[&target.identity_index_id, &target.relation_id],
        )?
        .ok_or(M2Error::SourceInvalidated)?;
    let key_column = columns
        .iter()
        .find(|column| column.address.sub_id == row.get::<_, i32>(6))
        .ok_or(M2Error::SourceInvalidated)?
        .address;
    Ok((
        SourceDescriptor {
            source_id: target.source_id,
            relation,
            columns,
        },
        IdentityIndexDescriptor {
            address: ObjectAddress {
                class_id: relation.class_id,
                object_id: target.identity_index_id,
                sub_id: 0,
            },
            relation,
            key_column,
            unique: row.get(0),
            valid: row.get(1),
            ready: row.get(2),
            has_expression: row.get(3),
            has_predicate: row.get(4),
            effective_replica_identity: row.get(5),
        },
    ))
}

fn relation_class_id(transaction: &mut Transaction<'_>) -> Result<u32, RegistrationError> {
    u32::try_from(
        transaction
            .query_one("SELECT 'pg_class'::regclass::oid::bigint", &[])?
            .get::<_, i64>(0),
    )
    .map_err(|_| M2Error::SourceInvalidated.into())
}
fn address_at(row: &postgres::Row, offset: usize) -> Result<ObjectAddress, RegistrationError> {
    Ok(ObjectAddress {
        class_id: u32::try_from(row.get::<_, i64>(offset))
            .map_err(|_| M2Error::SourceInvalidated)?,
        object_id: u32::try_from(row.get::<_, i64>(offset + 1))
            .map_err(|_| M2Error::SourceInvalidated)?,
        sub_id: row.get(offset + 2),
    })
}
