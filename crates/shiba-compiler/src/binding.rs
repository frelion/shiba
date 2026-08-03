use shiba_operator::{ColumnBinding, ObjectAddress, SourcePort, ValueType};

use crate::{
    CompilerError, IdentityIndexDescriptor, POSTGRES_INT8_TYPE_OID, POSTGRES_TEXT_TYPE_OID,
    SourceColumnDescriptor, SourceDescriptor,
};

pub(crate) fn source(
    sources: &[SourceDescriptor],
    id: shiba_protocol::SourceId,
) -> Result<&SourceDescriptor, CompilerError> {
    sources
        .iter()
        .find(|source| source.source_id == id)
        .ok_or(CompilerError::SourceMismatch)
}

pub(crate) fn source_port(
    source: &SourceDescriptor,
    identity_index: Option<ObjectAddress>,
) -> Result<SourcePort, CompilerError> {
    Ok(SourcePort {
        source_id: source.source_id,
        layout: source
            .columns
            .iter()
            .map(|column| {
                Ok(ColumnBinding {
                    address: column.address,
                    value_type: match column.type_oid {
                        POSTGRES_INT8_TYPE_OID => ValueType::Int8,
                        POSTGRES_TEXT_TYPE_OID => ValueType::Text,
                        type_oid => {
                            return Err(CompilerError::WrongColumnType {
                                column: column.name.clone(),
                                type_oid,
                            });
                        }
                    },
                })
            })
            .collect::<Result<_, _>>()?,
        identity_index,
    })
}

pub(crate) fn identity_for<'a>(
    source: &SourceDescriptor,
    indexes: &'a [IdentityIndexDescriptor],
) -> Result<&'a IdentityIndexDescriptor, CompilerError> {
    let key = source
        .columns
        .first()
        .filter(|column| {
            !column.nullable
                && column.type_oid == POSTGRES_INT8_TYPE_OID
                && column.address.class_id == source.relation.class_id
                && column.address.object_id == source.relation.object_id
                && column.address.sub_id > 0
        })
        .ok_or(CompilerError::InvalidIdentityIndex)?;
    let mut matches = indexes.iter().filter(|index| {
        index.relation == source.relation
            && index.key_column == key.address
            && index.address.class_id == source.relation.class_id
            && index.address.object_id != 0
            && index.address.sub_id == 0
            && index.key_arity > 0
            && index.unique
            && index.valid
            && index.ready
            && !index.has_expression
            && !index.has_predicate
            && index.effective_replica_identity
    });
    let exact = matches.next().ok_or(CompilerError::InvalidIdentityIndex)?;
    if matches.next().is_some() {
        return Err(CompilerError::InvalidIdentityIndex);
    }
    Ok(exact)
}

fn resolve<'a>(
    source: &'a SourceDescriptor,
    name: &str,
) -> Result<(u16, &'a SourceColumnDescriptor), CompilerError> {
    let mut matches = source
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name == name);
    let (slot, column) = matches
        .next()
        .ok_or_else(|| CompilerError::MissingColumn(name.into()))?;
    if matches.next().is_some() {
        return Err(CompilerError::DuplicateColumn(name.into()));
    }
    Ok((
        u16::try_from(slot).map_err(|_| CompilerError::GraphEncoding)?,
        column,
    ))
}

pub(crate) fn int8<'a>(
    source: &'a SourceDescriptor,
    name: &str,
) -> Result<(u16, &'a SourceColumnDescriptor), CompilerError> {
    let found = resolve(source, name)?;
    if found.1.type_oid != POSTGRES_INT8_TYPE_OID {
        return Err(CompilerError::WrongColumnType {
            column: name.into(),
            type_oid: found.1.type_oid,
        });
    }
    Ok(found)
}
