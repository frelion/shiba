use core::num::NonZeroU64;

use postgres::Transaction;
use shiba_operator::{
    CompiledOperator, CompiledOperatorKind, EffectBatch, ObjectAddress, OperatorId, apply_operator,
};
use shiba_protocol::SourceId;

use crate::M2Error;

pub(crate) fn apply_all(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    batch: &EffectBatch,
) -> Result<(), M2Error> {
    let rows = transaction.query(
        "SELECT definition.operator_id, state.value_bigint,
                definition.operator_kind,
                definition.input_classid::bigint,
                definition.input_objid::bigint,
                definition.input_objsubid
         FROM shiba_internal.operator_definition AS definition
         JOIN shiba_internal.operator_state AS state
           USING (operator_id)
         WHERE definition.source_id = $1
         ORDER BY definition.operator_id
         FOR UPDATE OF state",
        &[&source_id],
    )?;
    if rows.is_empty() {
        return Err(M2Error::MissingSourceOperator);
    }
    let definition_count: i64 = transaction
        .query_one(
            "SELECT count(*) FROM shiba_internal.operator_definition
             WHERE source_id = $1",
            &[&source_id],
        )?
        .get(0);
    if usize::try_from(definition_count).ok() != Some(rows.len()) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    let source_id =
        SourceId::new(u64::try_from(source_id).map_err(|_| M2Error::InvalidOperatorDefinition)?)
            .map_err(|_| M2Error::InvalidOperatorDefinition)?;

    for row in rows {
        let raw_operator_id: i64 = row.get(0);
        let operator_id = NonZeroU64::new(
            u64::try_from(raw_operator_id).map_err(|_| M2Error::InvalidOperatorDefinition)?,
        )
        .map(OperatorId::new)
        .ok_or(M2Error::InvalidOperatorDefinition)?;
        let kind = decode_kind(row.get(2), row.get(3), row.get(4), row.get(5))?;
        let operator = CompiledOperator {
            operator_id,
            source_id,
            kind,
        };
        let next = apply_operator(&operator, row.get(1), &batch.effects)?;
        if transaction.execute(
            "UPDATE shiba_internal.operator_state
             SET value_bigint = $1 WHERE operator_id = $2",
            &[&next, &raw_operator_id],
        )? != 1
        {
            return Err(M2Error::InvalidOperatorDefinition);
        }
        if transaction.execute(
            "UPDATE shiba.operator_result
             SET value_bigint = $1 WHERE operator_id = $2",
            &[&next, &raw_operator_id],
        )? != 1
        {
            return Err(M2Error::InvalidOperatorDefinition);
        }
    }
    Ok(())
}

fn decode_kind(
    kind: &str,
    class_id: Option<i64>,
    object_id: Option<i64>,
    sub_id: Option<i32>,
) -> Result<CompiledOperatorKind, M2Error> {
    match (kind, class_id, object_id, sub_id) {
        ("count_rows", None, None, None) => Ok(CompiledOperatorKind::CountRows),
        ("sum_int8", Some(class_id), Some(object_id), Some(sub_id)) => {
            Ok(CompiledOperatorKind::SumInt8 {
                input: ObjectAddress {
                    class_id: u32::try_from(class_id)
                        .map_err(|_| M2Error::InvalidOperatorDefinition)?,
                    object_id: u32::try_from(object_id)
                        .map_err(|_| M2Error::InvalidOperatorDefinition)?,
                    sub_id,
                },
            })
        }
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
}
