use postgres::{Row, Transaction};
use shiba_operator::{EffectBatch, EffectOrigin, RowEffect, RowImage, Value};

use crate::transaction::as_bigint;
use crate::{
    M2Error, SourceChange, SourcePayload, SourceTransaction, SourceUpdate, SourceUpdatePayload,
};

pub(crate) fn apply(
    transaction: &mut Transaction<'_>,
    input: &SourceTransaction,
) -> Result<EffectBatch, M2Error> {
    let identity = input.identity;
    let source_id = as_bigint("source_id", identity.source_id.get())?;
    let mut effects = Vec::with_capacity(input.changes.len());

    for change in &input.changes {
        effects.push(match change {
            SourceChange::Insert(insert) => {
                let payload = operator_value(&insert.source_payload);
                let (payload_present, payload_int8, payload_text) = value_columns(&payload);
                transaction.execute(
                    "INSERT INTO shiba_internal.source_row_state (
                         source_id, source_row_id, source_row_sub_id,
                         payload_present, payload_int8, payload_text
                     ) VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &source_id,
                        &insert.source_row_id,
                        &insert.source_row_sub_id,
                        &payload_present,
                        &payload_int8,
                        &payload_text,
                    ],
                )?;
                RowEffect {
                    before: None,
                    after: Some(RowImage {
                        source_row_id: insert.source_row_id,
                        source_row_sub_id: insert.source_row_sub_id,
                        payload,
                    }),
                }
            }
            SourceChange::Update(update) => apply_update(transaction, source_id, update)?,
            SourceChange::Delete {
                source_row_id,
                source_row_sub_id,
                ..
            } => {
                let before = load_row(transaction, source_id, *source_row_id, *source_row_sub_id)?;
                let changed = transaction.execute(
                    "DELETE FROM shiba_internal.source_row_state
                     WHERE source_id = $1 AND source_row_id = $2
                       AND source_row_sub_id IS NOT DISTINCT FROM $3",
                    &[&source_id, source_row_id, source_row_sub_id],
                )?;
                if changed != 1 {
                    return Err(M2Error::MissingSourceRow);
                }
                RowEffect {
                    before: Some(before),
                    after: None,
                }
            }
        });
    }
    Ok(EffectBatch {
        origin: EffectOrigin::Wal(identity),
        effects,
    })
}

fn apply_update(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    update: &SourceUpdate,
) -> Result<RowEffect, M2Error> {
    let before = load_row(transaction, source_id, update.source_row_id, None)?;
    let after_payload = match &update.source_payload {
        SourceUpdatePayload::Int8(value) => match &before.payload {
            Value::Text(_) => return Err(M2Error::MissingSourceRow),
            _ => value.map_or(Value::Null, Value::Int8),
        },
        SourceUpdatePayload::UnchangedText => match &before.payload {
            Value::Text(value) => Value::Text(value.clone()),
            _ => return Err(M2Error::MissingSourceRow),
        },
        SourceUpdatePayload::Text(value) => match &before.payload {
            Value::Text(_) => Value::Text(value.clone()),
            _ => return Err(M2Error::MissingSourceRow),
        },
    };
    let (payload_present, payload_int8, payload_text) = value_columns(&after_payload);
    let changed = transaction.execute(
        "UPDATE shiba_internal.source_row_state
         SET payload_present = $1, payload_int8 = $2, payload_text = $3
         WHERE source_id = $4 AND source_row_id = $5
           AND source_row_sub_id IS NULL",
        &[
            &payload_present,
            &payload_int8,
            &payload_text,
            &source_id,
            &update.source_row_id,
        ],
    )?;
    if changed != 1 {
        return Err(M2Error::MissingSourceRow);
    }
    Ok(RowEffect {
        before: Some(before),
        after: Some(RowImage {
            source_row_id: Some(update.source_row_id),
            source_row_sub_id: None,
            payload: after_payload,
        }),
    })
}

fn load_row(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    row_id: i64,
    row_sub_id: Option<i64>,
) -> Result<RowImage, M2Error> {
    let row = transaction
        .query_opt(
            "SELECT source_row_id, source_row_sub_id,
                    payload_present, payload_int8, payload_text
             FROM shiba_internal.source_row_state
             WHERE source_id = $1 AND source_row_id = $2
               AND source_row_sub_id IS NOT DISTINCT FROM $3
             FOR UPDATE",
            &[&source_id, &row_id, &row_sub_id],
        )?
        .ok_or(M2Error::MissingSourceRow)?;
    row_image(&row)
}

fn row_image(row: &Row) -> Result<RowImage, M2Error> {
    let present: bool = row.get(2);
    let int8: Option<i64> = row.get(3);
    let text: Option<String> = row.get(4);
    let payload = match (present, int8, text) {
        (false, None, None) => Value::Absent,
        (true, Some(value), None) => Value::Int8(value),
        (true, None, Some(value)) => Value::Text(value),
        (true, None, None) => Value::Null,
        (false, _, _) | (true, Some(_), Some(_)) => {
            return Err(M2Error::InvalidSourceRowState);
        }
    };
    Ok(RowImage {
        source_row_id: row.get(0),
        source_row_sub_id: row.get(1),
        payload,
    })
}

fn operator_value(payload: &SourcePayload) -> Value {
    match payload {
        SourcePayload::Absent => Value::Absent,
        SourcePayload::Null => Value::Null,
        SourcePayload::Int8(value) => Value::Int8(*value),
        SourcePayload::Text(value) => Value::Text(value.clone()),
    }
}

fn value_columns(value: &Value) -> (bool, Option<i64>, Option<&str>) {
    match value {
        Value::Absent => (false, None, None),
        Value::Null => (true, None, None),
        Value::Int8(value) => (true, Some(*value), None),
        Value::Text(value) => (true, None, Some(value.as_str())),
    }
}
