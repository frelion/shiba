use postgres::Transaction;
use shiba_operator::{
    ColumnBinding, DeltaBatch, EffectOrigin, ObjectAddress, RowDelta, TypedLayout, TypedRow,
    TypedValue, ValueType, source_typed_layout,
};
use shiba_protocol::SourceId;

use crate::{M2Error, SourcePayload};

pub(crate) struct SourceLayout {
    typed: TypedLayout,
}

impl SourceLayout {
    pub(crate) fn load(transaction: &mut Transaction<'_>, source_id: i64) -> Result<Self, M2Error> {
        let source_identity = u64::try_from(source_id)
            .ok()
            .and_then(|value| SourceId::new(value).ok())
            .ok_or(M2Error::InvalidSourceRowState)?;
        let rows = transaction.query(
            "SELECT binding.address_classid::bigint,
                    binding.address_objid::bigint, binding.address_objsubid,
                    attribute.atttypid::bigint
             FROM shiba_internal.source_binding AS binding
             JOIN pg_catalog.pg_attribute AS attribute
               ON attribute.attrelid = binding.address_objid
              AND attribute.attnum = binding.address_objsubid
             WHERE binding.source_id = $1 AND binding.binding_kind = 'column'
             ORDER BY binding.address_objsubid",
            &[&source_id],
        )?;
        if rows.len() > 2 {
            return Err(M2Error::InvalidSourceRowState);
        }
        let columns = rows
            .into_iter()
            .map(|row| {
                let address = ObjectAddress {
                    class_id: u32::try_from(row.get::<_, i64>(0))
                        .map_err(|_| M2Error::InvalidSourceRowState)?,
                    object_id: u32::try_from(row.get::<_, i64>(1))
                        .map_err(|_| M2Error::InvalidSourceRowState)?,
                    sub_id: row.get(2),
                };
                let value_type = match row.get::<_, i64>(3) {
                    20 => ValueType::Int8,
                    25 => ValueType::Text,
                    _ => return Err(M2Error::InvalidSourceRowState),
                };
                Ok(ColumnBinding {
                    address,
                    value_type,
                })
            })
            .collect::<Result<Vec<_>, M2Error>>()?;
        if columns
            .first()
            .is_some_and(|column| column.value_type != ValueType::Int8)
        {
            return Err(M2Error::InvalidSourceRowState);
        }
        let typed = source_typed_layout(source_identity, &columns)
            .map_err(|_| M2Error::InvalidSourceRowState)?;
        Ok(Self { typed })
    }

    pub(crate) fn row(
        &self,
        source_row_id: Option<i64>,
        source_row_sub_id: Option<i64>,
        payload: &SourcePayload,
    ) -> Result<TypedRow, M2Error> {
        let values = match self.typed.value_types.as_slice() {
            [] => Vec::new(),
            [ValueType::Int8] => vec![TypedValue::Int8(
                source_row_id.ok_or(M2Error::InvalidSourceRowState)?,
            )],
            [ValueType::Int8, second] => vec![
                TypedValue::Int8(source_row_id.ok_or(M2Error::InvalidSourceRowState)?),
                source_row_sub_id.map_or_else(
                    || typed_payload(payload, *second),
                    |value| {
                        if *second == ValueType::Int8 {
                            Ok(TypedValue::Int8(value))
                        } else {
                            Err(M2Error::InvalidSourceRowState)
                        }
                    },
                )?,
            ],
            _ => return Err(M2Error::InvalidSourceRowState),
        };
        TypedRow::new(&self.typed, values).map_err(|_| M2Error::InvalidSourceRowState)
    }

    pub(crate) fn batch(self, origin: EffectOrigin, rows: Vec<RowDelta>) -> DeltaBatch {
        DeltaBatch {
            origin,
            layout_identity: self.typed.identity,
            rows,
        }
    }
}

fn typed_payload(payload: &SourcePayload, expected: ValueType) -> Result<TypedValue, M2Error> {
    match payload {
        SourcePayload::Absent => Ok(TypedValue::Absent),
        SourcePayload::Null => Ok(TypedValue::Null(expected)),
        SourcePayload::Int8(value) if expected == ValueType::Int8 => Ok(TypedValue::Int8(*value)),
        SourcePayload::Text(value) if expected == ValueType::Text => {
            Ok(TypedValue::Text(value.clone()))
        }
        SourcePayload::Int8(_) | SourcePayload::Text(_) => Err(M2Error::InvalidSourceRowState),
    }
}
