use postgres::{Row, Transaction};
use shiba_operator::{EffectOrigin, RowDelta};

use crate::source_batch::{SourceBatch, SourceLayout};
use crate::transaction::as_bigint;
use crate::{
    M2Error, SourceChange, SourcePayload, SourceTransaction, SourceUpdate, SourceUpdatePayload,
};

pub(crate) fn apply(
    transaction: &mut Transaction<'_>,
    input: &SourceTransaction,
) -> Result<SourceBatch, M2Error> {
    let identity = input.identity;
    let source_id = as_bigint("source_id", identity.source_id.get())?;
    let layout = SourceLayout::load(transaction, source_id)?;
    let mut rows = Vec::with_capacity(input.changes.len());

    for change in &input.changes {
        rows.push(match change {
            SourceChange::Insert(insert) => {
                let (payload_present, payload_int8, payload_text) =
                    value_columns(&insert.source_payload);
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
                RowDelta {
                    before: None,
                    after: Some(layout.row(
                        insert.source_row_id,
                        insert.source_row_sub_id,
                        &insert.source_payload,
                    )?),
                }
            }
            SourceChange::Update(update) => apply_update(transaction, source_id, &layout, update)?,
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
                RowDelta {
                    before: Some(layout.row(
                        before.source_row_id,
                        before.source_row_sub_id,
                        &before.payload,
                    )?),
                    after: None,
                }
            }
        });
    }
    Ok(layout.batch(EffectOrigin::Wal(identity), rows))
}

fn apply_update(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    layout: &SourceLayout,
    update: &SourceUpdate,
) -> Result<RowDelta, M2Error> {
    let before = load_row(transaction, source_id, update.source_row_id, None)?;
    let after_payload = match &update.source_payload {
        SourceUpdatePayload::Int8(value) => match &before.payload {
            SourcePayload::Text(_) => return Err(M2Error::MissingSourceRow),
            _ => value.map_or(SourcePayload::Null, SourcePayload::Int8),
        },
        SourceUpdatePayload::UnchangedText => match &before.payload {
            SourcePayload::Text(value) => SourcePayload::Text(value.clone()),
            _ => return Err(M2Error::MissingSourceRow),
        },
        SourceUpdatePayload::Text(value) => match &before.payload {
            SourcePayload::Text(_) => SourcePayload::Text(value.clone()),
            _ => return Err(M2Error::MissingSourceRow),
        },
    };
    let (payload_present, payload_int8, payload_text) = value_columns(&after_payload);
    let changed = if update.source_row_id == update.new_source_row_id {
        transaction.execute(
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
        )?
    } else {
        let deleted = transaction.execute(
            "DELETE FROM shiba_internal.source_row_state
             WHERE source_id = $1 AND source_row_id = $2
               AND source_row_sub_id IS NULL",
            &[&source_id, &update.source_row_id],
        )?;
        if deleted != 1 {
            return Err(M2Error::MissingSourceRow);
        }
        transaction.execute(
            "INSERT INTO shiba_internal.source_row_state (
                 source_id, source_row_id, source_row_sub_id,
                 payload_present, payload_int8, payload_text
             ) VALUES ($1, $2, NULL, $3, $4, $5)",
            &[
                &source_id,
                &update.new_source_row_id,
                &payload_present,
                &payload_int8,
                &payload_text,
            ],
        )?
    };
    if changed != 1 {
        return Err(M2Error::MissingSourceRow);
    }
    Ok(RowDelta {
        before: Some(layout.row(
            before.source_row_id,
            before.source_row_sub_id,
            &before.payload,
        )?),
        after: Some(layout.row(Some(update.new_source_row_id), None, &after_payload)?),
    })
}

struct StoredRow {
    source_row_id: Option<i64>,
    source_row_sub_id: Option<i64>,
    payload: SourcePayload,
}

fn load_row(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    row_id: i64,
    row_sub_id: Option<i64>,
) -> Result<StoredRow, M2Error> {
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

fn row_image(row: &Row) -> Result<StoredRow, M2Error> {
    let present: bool = row.get(2);
    let int8: Option<i64> = row.get(3);
    let text: Option<String> = row.get(4);
    let payload = match (present, int8, text) {
        (false, None, None) => SourcePayload::Absent,
        (true, Some(value), None) => SourcePayload::Int8(value),
        (true, None, Some(value)) => SourcePayload::Text(value),
        (true, None, None) => SourcePayload::Null,
        (false, _, _) | (true, Some(_), Some(_)) => {
            return Err(M2Error::InvalidSourceRowState);
        }
    };
    Ok(StoredRow {
        source_row_id: row.get(0),
        source_row_sub_id: row.get(1),
        payload,
    })
}

fn value_columns(value: &SourcePayload) -> (bool, Option<i64>, Option<&str>) {
    match value {
        SourcePayload::Absent => (false, None, None),
        SourcePayload::Null => (true, None, None),
        SourcePayload::Int8(value) => (true, Some(*value), None),
        SourcePayload::Text(value) => (true, None, Some(value.as_str())),
    }
}
