use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

use crate::postgres::{format_lsn, parse_lsn};

use super::{
    nonnegative, required_table, InputState, OutputFacts, RelationRef, StepContext, WorkUsage,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChunkKind {
    Data,
    Frontier,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChunkMeta {
    pub(crate) stream_id: i64,
    pub(crate) sequence: i64,
    pub(crate) kind: ChunkKind,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    pub(crate) lsn: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PayloadFacts {
    pub(crate) rows: u64,
    pub(crate) first_ordinal: Option<i64>,
    pub(crate) last_ordinal: Option<i64>,
    pub(crate) bytes: u64,
}

pub(crate) fn next_chunk(
    transaction: &mut StepContext<'_, '_>,
    port: u16,
) -> Result<Option<ChunkMeta>, String> {
    let input = transaction.input(port)?.clone();
    chunk(transaction, &input, input.next_chunk_seq)
}

pub(crate) fn chunk(
    transaction: &mut StepContext<'_, '_>,
    input: &InputState,
    sequence: i64,
) -> Result<Option<ChunkMeta>, String> {
    if sequence <= 0 {
        return Err("effect chunk sequence must be positive".into());
    }
    let arguments = unsafe {
        [
            DatumWithOid::new(input.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(sequence, pg_sys::INT8OID),
        ]
    };
    let table = transaction.read(
        r#"
        SELECT chunk.chunk_kind,
               chunk.row_count,
               chunk.payload_bytes,
               chunk.chunk_lsn::text
        FROM shiba_internal.effect_stream_chunks AS chunk
        WHERE chunk.stream_id = $1
          AND chunk.chunk_seq = $2
        "#,
        &arguments,
    )?;
    match table.len() {
        0 => Ok(None),
        1 => {
            let table = table.first();
            let kind = match required_table::<String>(&table, 1, "chunk kind")?.as_str() {
                "data" => ChunkKind::Data,
                "frontier" => ChunkKind::Frontier,
                kind => {
                    return Err(format!(
                        "effect stream contains invalid chunk kind {kind:?}"
                    ));
                }
            };
            let rows = nonnegative(
                required_table::<i64>(&table, 2, "chunk row count")?,
                "chunk row count",
            )?;
            let bytes = nonnegative(
                required_table::<i64>(&table, 3, "chunk byte count")?,
                "chunk byte count",
            )?;
            match kind {
                ChunkKind::Data if rows == 0 || bytes == 0 => {
                    return Err("data chunk has no payload".into());
                }
                ChunkKind::Frontier if rows != 0 || bytes != 0 => {
                    return Err("frontier chunk has payload".into());
                }
                _ => {}
            }
            let lsn_text = required_table::<String>(&table, 4, "chunk LSN")?;
            let lsn =
                parse_lsn(&lsn_text).map_err(|error| format!("invalid chunk LSN: {error}"))?;
            Ok(Some(ChunkMeta {
                stream_id: input.stream_id,
                sequence,
                kind,
                rows,
                bytes,
                lsn,
            }))
        }
        count => Err(format!(
            "effect stream chunk identity returned {count} rows"
        )),
    }
}

pub(crate) fn payload_facts(
    transaction: &mut StepContext<'_, '_>,
    storage: &RelationRef,
    chunk: &ChunkMeta,
) -> Result<PayloadFacts, String> {
    let query = format!(
        r#"
        SELECT count(*)::bigint,
               min(payload.row_ordinal)::bigint,
               max(payload.row_ordinal)::bigint,
               coalesce(
                 sum(shiba_internal.effect_row_bytes(payload.row_value)),
                 0
               )::bigint
        FROM {} AS payload
        WHERE payload.stream_id = $1
          AND payload.chunk_seq = $2
        "#,
        storage.sql()
    );
    let arguments = unsafe {
        [
            DatumWithOid::new(chunk.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(chunk.sequence, pg_sys::INT8OID),
        ]
    };
    let table = transaction.read(&query, &arguments)?.first();
    let facts = PayloadFacts {
        rows: nonnegative(
            required_table::<i64>(&table, 1, "payload row count")?,
            "payload row count",
        )?,
        first_ordinal: table.get::<i64>(2).map_err(|error| error.to_string())?,
        last_ordinal: table.get::<i64>(3).map_err(|error| error.to_string())?,
        bytes: nonnegative(
            required_table::<i64>(&table, 4, "payload byte count")?,
            "payload byte count",
        )?,
    };
    if facts.rows != chunk.rows
        || facts.bytes != chunk.bytes
        || (facts.rows == 0 && (facts.first_ordinal.is_some() || facts.last_ordinal.is_some()))
        || (facts.rows > 0
            && (facts.first_ordinal != Some(0)
                || facts.last_ordinal != Some(i64_from_u64(facts.rows)? - 1)))
    {
        return Err(format!(
            "chunk {}/{} payload does not match immutable metadata",
            chunk.stream_id, chunk.sequence
        ));
    }
    Ok(facts)
}

pub(crate) fn append_frontier(
    transaction: &mut StepContext<'_, '_>,
    frontier_lsn: u64,
) -> Result<OutputFacts, String> {
    let output = transaction.output()?.clone();
    let frontier = format_lsn(frontier_lsn);
    let arguments = unsafe {
        [
            DatumWithOid::new(output.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(output.next_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(frontier.as_str(), pg_sys::TEXTOID),
        ]
    };
    let appended = transaction
        .write(
            r#"
            SELECT append.outcome,
                   append.appended_chunk_seq
            FROM shiba_internal.append_effect_stream_chunk(
              $1, $2, 'frontier', 0, 0, $3::pg_lsn
            ) AS append
            "#,
            &arguments,
        )?
        .first();
    let outcome = required_table::<String>(&appended, 1, "frontier append outcome")?;
    if outcome != "appended" {
        return Err(format!(
            "locked output stream returned frontier append outcome {outcome:?}"
        ));
    }
    let sequence = required_table::<i64>(&appended, 2, "frontier chunk sequence")?;
    if sequence != output.next_chunk_seq {
        return Err("frontier append returned an unexpected chunk sequence".into());
    }
    transaction.record_frontier_output(sequence, frontier_lsn)?;
    Ok(OutputFacts::Frontier {
        chunk_seq: sequence,
    })
}

pub(crate) fn advance_input(
    transaction: &mut StepContext<'_, '_>,
    port: u16,
    new_next_chunk_seq: i64,
    new_frontier_lsn: u64,
    consumed_range: WorkUsage,
) -> Result<(), String> {
    let input = transaction.input(port)?.clone();
    if new_next_chunk_seq < input.next_chunk_seq
        || new_frontier_lsn < input.consumed_frontier_lsn
        || (new_next_chunk_seq == input.next_chunk_seq
            && new_frontier_lsn == input.consumed_frontier_lsn)
    {
        return Err("input cursor did not advance".into());
    }
    let expected_frontier = format_lsn(input.consumed_frontier_lsn);
    let new_frontier = format_lsn(new_frontier_lsn);
    let chunk_limit = new_next_chunk_seq
        .checked_sub(input.next_chunk_seq)
        .ok_or_else(|| "input chunk cursor moved backwards".to_string())?
        .max(1);
    let chunk_limit =
        i32::try_from(chunk_limit).map_err(|_| "input chunk advance exceeds integer")?;
    let row_limit = i64_from_u64(consumed_range.input_rows)?;
    let byte_limit = i64_from_u64(consumed_range.input_bytes)?;
    let arguments = unsafe {
        [
            DatumWithOid::new(input.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(transaction.result_oid(), pg_sys::OIDOID),
            DatumWithOid::new(transaction.stage_id(), pg_sys::INT4OID),
            DatumWithOid::new(i32::from(port), pg_sys::INT4OID),
            DatumWithOid::new(input.next_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(new_next_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(expected_frontier.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(new_frontier.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(chunk_limit, pg_sys::INT4OID),
            DatumWithOid::new(row_limit, pg_sys::INT8OID),
            DatumWithOid::new(byte_limit, pg_sys::INT8OID),
        ]
    };
    let advanced = transaction.write(
        r#"
        SELECT consumer.next_chunk_seq,
               consumer.consumed_frontier_lsn::text
        FROM shiba_internal.advance_effect_stream_consumer(
          $1,$2::oid,$3,$4,$5,$6,$7::pg_lsn,$8::pg_lsn,$9,$10,$11
        ) AS consumer
        "#,
        &arguments,
    )?;
    if advanced.len() != 1 {
        return Err("input cursor advance returned no row".into());
    }
    let advanced = advanced.first();
    let actual_chunk = required_table::<i64>(&advanced, 1, "advanced input cursor")?;
    let actual_frontier = required_table::<String>(&advanced, 2, "advanced input frontier")?;
    let actual_frontier_lsn = parse_lsn(&actual_frontier)
        .map_err(|error| format!("invalid advanced input frontier: {error}"))?;
    if actual_chunk != new_next_chunk_seq || actual_frontier_lsn != new_frontier_lsn {
        return Err("input cursor advance returned unexpected state".into());
    }
    transaction.record_input_advance();
    Ok(())
}

fn i64_from_u64(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "resource count exceeds bigint".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_kinds_do_not_encode_operator_phases() {
        assert_ne!(ChunkKind::Data, ChunkKind::Frontier);
    }
}
