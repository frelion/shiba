use std::collections::BTreeMap;

use postgres::{Row, Transaction};
use shiba_operator::{
    EffectOrigin, GraphEffectOrigin, MultiInputBatch, RowDelta, SourceDeltaBatch,
};
use shiba_protocol::SourceTransactionId;

use crate::source_batch::SourceLayout;
use crate::transaction::as_bigint;
use crate::{
    GraphTransaction, M2Error, SourceChange, SourcePayload, SourceUpdate, SourceUpdatePayload,
};

pub(crate) fn apply(
    transaction: &mut Transaction<'_>,
    graph: &shiba_operator::OperatorGraph,
    input: &GraphTransaction,
) -> Result<MultiInputBatch, M2Error> {
    lock_existing_rows(transaction, input)?;
    let mut changes = BTreeMap::new();
    for tagged in &input.changes {
        changes
            .entry(tagged.source_id)
            .or_insert_with(Vec::new)
            .push(&tagged.change);
    }
    if changes.keys().any(|source_id| {
        !graph
            .sources
            .iter()
            .any(|source| source.source_id == *source_id)
    }) {
        return Err(M2Error::InvalidOperatorDefinition);
    }

    let mut sources = Vec::with_capacity(graph.sources.len());
    for port in &graph.sources {
        let source_id = as_bigint("source_id", port.source_id.get())?;
        let layout = SourceLayout::load(transaction, source_id)?;
        let source_identity = SourceTransactionId::new(
            port.source_id,
            input.identity.slot_generation,
            input.identity.commit_lsn,
            input.identity.ingress_transaction_id,
        )
        .map_err(|_| M2Error::IdentityConflict)?;
        let source_changes = changes.remove(&port.source_id).unwrap_or_default();
        let mut rows = Vec::with_capacity(source_changes.len());
        for change in source_changes {
            rows.push(apply_change(transaction, source_id, &layout, change)?);
        }
        sources.push(SourceDeltaBatch {
            source_id: port.source_id,
            delta: layout.batch(EffectOrigin::Wal(source_identity), rows),
        });
    }
    Ok(MultiInputBatch {
        origin: GraphEffectOrigin::Wal(input.identity),
        sources,
    })
}

fn lock_existing_rows(
    transaction: &mut Transaction<'_>,
    input: &GraphTransaction,
) -> Result<(), M2Error> {
    let mut coordinates = input
        .changes
        .iter()
        .filter_map(|tagged| match &tagged.change {
            SourceChange::Update(update) => {
                Some((tagged.source_id.get(), update.source_row_id, None))
            }
            SourceChange::Delete {
                source_row_id,
                source_row_sub_id,
                ..
            } => Some((tagged.source_id.get(), *source_row_id, *source_row_sub_id)),
            SourceChange::Insert(_) => None,
        })
        .map(|(source_id, row_id, sub_id)| Ok((as_bigint("source_id", source_id)?, row_id, sub_id)))
        .collect::<Result<Vec<_>, M2Error>>()?;
    coordinates.sort();
    if coordinates.is_empty() {
        return Ok(());
    }
    let source_ids: Vec<i64> = coordinates.iter().map(|coordinate| coordinate.0).collect();
    let row_ids: Vec<i64> = coordinates.iter().map(|coordinate| coordinate.1).collect();
    let sub_ids: Vec<Option<i64>> = coordinates.iter().map(|coordinate| coordinate.2).collect();
    let locked = transaction.query(
        "SELECT state.source_id, state.source_row_id, state.source_row_sub_id
         FROM unnest($1::bigint[], $2::bigint[], $3::bigint[])
              AS requested(source_id, source_row_id, source_row_sub_id)
         JOIN shiba_internal.source_row_state AS state
           ON state.source_id = requested.source_id
          AND state.source_row_id = requested.source_row_id
          AND state.source_row_sub_id IS NOT DISTINCT FROM requested.source_row_sub_id
         ORDER BY state.source_id, state.source_row_id, state.source_row_sub_id
         FOR UPDATE OF state",
        &[&source_ids, &row_ids, &sub_ids],
    )?;
    if locked.len() != coordinates.len() {
        return Err(M2Error::MissingSourceRow);
    }
    Ok(())
}

fn apply_change(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    layout: &SourceLayout,
    change: &SourceChange,
) -> Result<RowDelta, M2Error> {
    Ok(match change {
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
        SourceChange::Update(update) => apply_update(transaction, source_id, layout, update)?,
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
    })
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
