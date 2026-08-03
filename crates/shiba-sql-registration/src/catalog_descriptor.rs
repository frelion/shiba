use postgres::Transaction;
use shiba_compiler::{IdentityIndexDescriptor, SourceColumnDescriptor, SourceDescriptor};
use shiba_operator::ObjectAddress;
use shiba_protocol::SourceId;
use shiba_sql_frontend::{ErrorCode, Span};

use crate::SqlRegistrationError;
use crate::catalog::CatalogSource;

pub(crate) fn describe(
    transaction: &mut Transaction<'_>,
    source_id: SourceId,
    relation_oid: u32,
    span: Span,
) -> Result<CatalogSource, SqlRegistrationError> {
    let source_key = i64::try_from(source_id.get())
        .map_err(|_| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, span))?;
    let class_id = relation_class_id(transaction, span)?;
    let columns = transaction
        .query(
            "SELECT attribute.attname,binding.address_objsubid,
                    attribute.atttypid::bigint,NOT attribute.attnotnull
             FROM shiba_internal.source_binding AS binding
             JOIN pg_catalog.pg_attribute AS attribute
               ON attribute.attrelid=binding.address_objid
              AND attribute.attnum=binding.address_objsubid
              AND NOT attribute.attisdropped
             WHERE binding.source_id=$1 AND binding.binding_kind='column'
               AND binding.address_classid=$2::oid AND binding.address_objid=$3::oid
             ORDER BY binding.address_objsubid",
            &[&source_key, &class_id, &relation_oid],
        )
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, span, error)
        })?
        .into_iter()
        .map(|row| {
            Ok(SourceColumnDescriptor {
                name: row.get(0),
                address: ObjectAddress {
                    class_id,
                    object_id: relation_oid,
                    sub_id: row.get(1),
                },
                type_oid: u32::try_from(row.get::<_, i64>(2))
                    .map_err(|_| SqlRegistrationError::catalog(ErrorCode::DdlDrift, span))?,
                nullable: row.get(3),
            })
        })
        .collect::<Result<Vec<_>, SqlRegistrationError>>()?;
    let relation = ObjectAddress {
        class_id,
        object_id: relation_oid,
        sub_id: 0,
    };
    let identity = identity(transaction, source_key, relation, &columns, span)?;
    Ok(CatalogSource {
        descriptor: SourceDescriptor {
            source_id,
            relation,
            columns,
        },
        identity,
    })
}

fn identity(
    transaction: &mut Transaction<'_>,
    source_key: i64,
    relation: ObjectAddress,
    columns: &[SourceColumnDescriptor],
    span: Span,
) -> Result<IdentityIndexDescriptor, SqlRegistrationError> {
    let row = transaction
        .query_opt(
            "SELECT binding.address_classid::bigint,binding.address_objid::bigint,
                    binding.address_objsubid,index.indisunique,index.indisvalid,index.indisready,
                    index.indexprs IS NOT NULL,index.indpred IS NOT NULL,
                    (index.indisreplident OR (class.relreplident='d' AND index.indisprimary)),
                    (index.indkey::smallint[])[0]::integer,index.indnkeyatts
             FROM shiba_internal.source_binding AS binding
             JOIN pg_catalog.pg_index AS index ON index.indexrelid=binding.address_objid
             JOIN pg_catalog.pg_class AS class ON class.oid=index.indrelid
             WHERE binding.source_id=$1 AND binding.binding_kind='identity_index'
               AND index.indrelid=$2::oid",
            &[&source_key, &relation.object_id],
        )
        .map_err(|error| {
            SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, span, error)
        })?
        .ok_or_else(|| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, span))?;
    let key_column = columns
        .iter()
        .find(|column| column.address.sub_id == row.get::<_, i32>(9))
        .ok_or_else(|| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, span))?
        .address;
    Ok(IdentityIndexDescriptor {
        address: ObjectAddress {
            class_id: u32::try_from(row.get::<_, i64>(0))
                .map_err(|_| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, span))?,
            object_id: u32::try_from(row.get::<_, i64>(1))
                .map_err(|_| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, span))?,
            sub_id: row.get(2),
        },
        relation,
        key_column,
        key_arity: u16::try_from(row.get::<_, i16>(10))
            .map_err(|_| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, span))?,
        unique: row.get(3),
        valid: row.get(4),
        ready: row.get(5),
        has_expression: row.get(6),
        has_predicate: row.get(7),
        effective_replica_identity: row.get(8),
    })
}

fn relation_class_id(
    transaction: &mut Transaction<'_>,
    span: Span,
) -> Result<u32, SqlRegistrationError> {
    u32::try_from(
        transaction
            .query_one("SELECT 'pg_class'::regclass::oid::bigint", &[])
            .map_err(|error| {
                SqlRegistrationError::postgres(ErrorCode::RegistrationFailed, span, error)
            })?
            .get::<_, i64>(0),
    )
    .map_err(|_| SqlRegistrationError::catalog(ErrorCode::IdentityMismatch, span))
}
