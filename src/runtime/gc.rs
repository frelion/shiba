//! Runtime garbage collection.

use crate::config;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use std::time::Duration;

pub(crate) const GC_INTERVAL: Duration = Duration::from_millis(250);
pub(crate) const GC_MAX_TRANSACTIONS_PER_ROUND: i32 = 64;
pub(crate) const GC_MAX_EFFECT_STREAMS_PER_ROUND: i32 = 64;
pub(crate) const GC_MAX_EFFECT_CHUNKS_PER_STREAM: i32 = 64;

pub(crate) fn collect(generation: i64, cursor: &mut Option<i64>) -> i64 {
    gc_change_log(generation) + gc_effect_streams(cursor)
}

pub(crate) fn gc_change_log(generation: i64) -> i64 {
    let generation_argument = unsafe { [DatumWithOid::new(generation, pg_sys::INT8OID)] };
    Spi::run_with_args(
        "SELECT shiba_internal.reconcile_ingress_replay_safe($1)",
        &generation_argument,
    )
    .expect("Shiba could not reconcile its logical-slot replay-safe LSN");
    let collected_transactions =
        Spi::get_one_with_args::<i64>("SELECT shiba._gc_change_log($1)", unsafe {
            &[DatumWithOid::new(
                GC_MAX_TRANSACTIONS_PER_ROUND,
                pg_sys::INT4OID,
            )]
        })
        .expect("Shiba could not garbage-collect its change log")
        .unwrap_or(0);
    Spi::run(
        "WITH garbage AS (
             SELECT replay.slot_generation
             FROM shiba_internal.ingress_replay_state AS replay
             WHERE replay.state = 'retired'
               AND replay.retired_at < clock_timestamp()
                   - current_setting('shiba.ingress_retention')::interval
               AND NOT EXISTS (
                       SELECT 1
                       FROM shiba_internal.ingress_transactions AS txn
                       WHERE txn.slot_generation = replay.slot_generation
                   )
               AND NOT EXISTS (
                       SELECT 1
                       FROM shiba_internal.effect_streams AS stream
                       WHERE stream.producer_kind = 'source'
                         AND stream.slot_generation = replay.slot_generation
                   )
             ORDER BY replay.slot_generation
             LIMIT 8
         )
         DELETE FROM shiba_internal.ingress_replay_state AS replay
         USING garbage
         WHERE replay.slot_generation = garbage.slot_generation",
    )
    .expect("Shiba could not garbage-collect retired ingress generations");
    collected_transactions
}

pub(crate) fn gc_effect_streams(cursor: &mut Option<i64>) -> i64 {
    let row_limit = i64::try_from(config::batch_rows()).expect("batch row budget exceeds bigint");
    let byte_limit =
        i64::try_from(config::batch_bytes()).expect("batch byte budget exceeds bigint");
    let stream_ids = gc_effect_stream_ids(*cursor);
    let next_cursor = stream_ids.last().copied();
    // The fair rotation may wrap from high IDs back to low IDs. Locking in
    // that order would invert StepTxn's global stream-ID order, so resolve the
    // still-existing candidates and lock them in ascending order first.
    let locked_stream_ids = lock_effect_streams_for_gc(&stream_ids);
    let mut deleted_chunks = 0_i64;
    for stream_id in locked_stream_ids {
        let arguments = unsafe {
            [
                DatumWithOid::new(stream_id, pg_sys::INT8OID),
                DatumWithOid::new(GC_MAX_EFFECT_CHUNKS_PER_STREAM, pg_sys::INT4OID),
                DatumWithOid::new(row_limit, pg_sys::INT8OID),
                DatumWithOid::new(byte_limit, pg_sys::INT8OID),
            ]
        };
        let deleted = Spi::get_one_with_args::<i64>(
            "SELECT deleted_chunks
             FROM shiba_internal.gc_effect_stream(
               $1::bigint, $2::integer, $3::bigint, $4::bigint
             )",
            &arguments,
        )
        .expect("Shiba could not garbage-collect a durable effect stream")
        .expect("Shiba effect-stream garbage collection returned NULL");
        deleted_chunks = deleted_chunks
            .checked_add(deleted)
            .expect("Shiba effect-stream garbage collection count overflowed");
    }
    if let Some(next_cursor) = next_cursor {
        *cursor = Some(next_cursor);
    }
    deleted_chunks
}

fn lock_effect_streams_for_gc(stream_ids: &[i64]) -> Vec<i64> {
    if stream_ids.is_empty() {
        return Vec::new();
    }
    let arguments = unsafe { [DatumWithOid::new(stream_ids.to_vec(), pg_sys::INT8ARRAYOID)] };
    Spi::connect_mut(|client| {
        client
            .update(
                "SELECT stream.stream_id
                 FROM shiba_internal.effect_streams AS stream
                 WHERE stream.stream_id = ANY($1::bigint[])
                 ORDER BY stream.stream_id
                 FOR UPDATE OF stream",
                None,
                &arguments,
            )
            .expect("Shiba could not lock effect streams for garbage collection")
            .map(|row| {
                row.get::<i64>(1)
                    .expect("invalid locked effect-stream ID")
                    .expect("NULL locked effect-stream ID")
            })
            .collect()
    })
}

fn gc_effect_stream_ids(cursor: Option<i64>) -> Vec<i64> {
    let mut stream_ids =
        gc_effect_stream_ids_in_range(cursor, true, GC_MAX_EFFECT_STREAMS_PER_ROUND);
    let limit = usize::try_from(GC_MAX_EFFECT_STREAMS_PER_ROUND)
        .expect("effect-stream GC round limit exceeds usize");
    if cursor.is_some() && stream_ids.len() < limit {
        let remaining = i32::try_from(limit - stream_ids.len())
            .expect("remaining effect-stream GC limit exceeds integer");
        stream_ids.extend(gc_effect_stream_ids_in_range(cursor, false, remaining));
    }
    stream_ids
}

fn gc_effect_stream_ids_in_range(cursor: Option<i64>, after_cursor: bool, limit: i32) -> Vec<i64> {
    Spi::connect_mut(|client| {
        let (predicate, arguments) = match cursor {
            Some(cursor) => {
                let comparison = if after_cursor { ">" } else { "<=" };
                (
                    format!("AND stream.stream_id {comparison} $1::bigint"),
                    unsafe {
                        vec![
                            DatumWithOid::new(cursor, pg_sys::INT8OID),
                            DatumWithOid::new(limit, pg_sys::INT4OID),
                        ]
                    },
                )
            }
            None => (String::new(), unsafe {
                vec![DatumWithOid::new(limit, pg_sys::INT4OID)]
            }),
        };
        let limit_parameter = if cursor.is_some() { "$2" } else { "$1" };
        let query = format!(
            "SELECT stream.stream_id
             FROM shiba_internal.effect_streams AS stream
             WHERE stream.first_retained_chunk_seq < stream.next_chunk_seq
               {predicate}
               AND (
                 (
                   stream.producer_kind = 'source'
                   AND NOT EXISTS (
                     SELECT 1
                     FROM shiba_internal.effect_stream_consumers AS consumer
                     WHERE consumer.stream_id = stream.stream_id
                   )
                 )
                 OR stream.first_retained_chunk_seq < (
                   SELECT min(consumer.next_chunk_seq)
                   FROM shiba_internal.effect_stream_consumers AS consumer
                   WHERE consumer.stream_id = stream.stream_id
                 )
               )
             ORDER BY stream.stream_id
             LIMIT {limit_parameter}"
        );
        client
            .update(&query, None, &arguments)
            .expect("Shiba could not discover effect streams ready for garbage collection")
            .map(|row| {
                row.get::<i64>(1)
                    .expect("invalid effect-stream GC stream ID")
                    .expect("NULL effect-stream GC stream ID")
            })
            .collect()
    })
}
