//! Durable source-task publication.
//!
//! Rust owns the bounded transaction protocol: choose and lock one causal
//! source task, append at most one typed chunk, validate the returned facts,
//! and advance the task cursor. PostgreSQL remains authoritative for every
//! durable row and performs the typed conversion plus append atomically.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::database::{optional, require_count, required as required_table};
use crate::kernel::resolve_payload_storage;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourcePublicationOutcome {
    Idle,
    Blocked,
    Appended,
    Completed,
    Discarded,
}

impl SourcePublicationOutcome {
    #[cfg(any(test, feature = "pg_test"))]
    fn sql(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Blocked => "blocked",
            Self::Appended => "appended",
            Self::Completed => "completed",
            Self::Discarded => "discarded",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourcePublication {
    pub(crate) outcome: SourcePublicationOutcome,
    pub(crate) ingress_txn_id: Option<i64>,
    pub(crate) batch_ordinal: Option<i64>,
    pub(crate) source_oid: Option<pg_sys::Oid>,
    pub(crate) final_lsn: Option<String>,
    pub(crate) chunk_seq: Option<i64>,
    pub(crate) has_pending: bool,
}

impl SourcePublication {
    fn without_task(outcome: SourcePublicationOutcome, has_pending: bool) -> Self {
        Self {
            outcome,
            ingress_txn_id: None,
            batch_ordinal: None,
            source_oid: None,
            final_lsn: None,
            chunk_seq: None,
            has_pending,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Candidate {
    ingress_txn_id: i64,
    batch_ordinal: i64,
    source_oid: pg_sys::Oid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Task {
    ingress_txn_id: i64,
    batch_ordinal: i64,
    source_oid: pg_sys::Oid,
    first_input_seq: i64,
    last_input_seq: i64,
    next_input_seq: i64,
    final_lsn: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Stream {
    stream_id: i64,
    next_chunk_seq: i64,
    backpressured: bool,
    target_chunk_rows: i64,
    target_chunk_bytes: i64,
    has_eligible_consumer: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AppendFacts {
    selected_rows: i64,
    selected_bytes: i64,
    selected_last_input_seq: Option<i64>,
    append_outcome: String,
    appended_chunk_seq: Option<i64>,
    inserted_rows: i64,
    inserted_bytes: i64,
    next_input_seq: Option<i64>,
}

pub(crate) fn publish_source_batch(slot_generation: i64) -> Result<SourcePublication, String> {
    if slot_generation <= 0 {
        return Err("source publication generation must be positive".into());
    }
    Spi::connect_mut(|client| publish_locked(client, slot_generation))
}

fn publish_locked(
    client: &mut SpiClient<'_>,
    slot_generation: i64,
) -> Result<SourcePublication, String> {
    lock_generation(client, slot_generation)?;
    let Some(candidate) = choose_candidate(client, slot_generation)? else {
        let has_pending = has_pending(client, slot_generation)?;
        let outcome = if has_pending {
            SourcePublicationOutcome::Blocked
        } else {
            SourcePublicationOutcome::Idle
        };
        return Ok(SourcePublication::without_task(outcome, has_pending));
    };
    let task = lock_task(client, slot_generation, candidate)?;
    let stream = lock_stream(client, slot_generation, &task)?;

    let (outcome, chunk_seq, next_input_seq) = match stream {
        None => (SourcePublicationOutcome::Discarded, None, None),
        Some(stream) if !stream.has_eligible_consumer => {
            (SourcePublicationOutcome::Discarded, None, None)
        }
        Some(stream) if stream.backpressured => (SourcePublicationOutcome::Blocked, None, None),
        Some(stream) => append_typed_prefix(client, &task, stream)?,
    };

    if matches!(
        outcome,
        SourcePublicationOutcome::Appended
            | SourcePublicationOutcome::Completed
            | SourcePublicationOutcome::Discarded
    ) {
        advance_task(client, &task, outcome, next_input_seq)?;
    }

    Ok(SourcePublication {
        outcome,
        ingress_txn_id: Some(task.ingress_txn_id),
        batch_ordinal: Some(task.batch_ordinal),
        source_oid: Some(task.source_oid),
        final_lsn: Some(task.final_lsn),
        chunk_seq,
        has_pending: has_pending(client, slot_generation)?,
    })
}

fn lock_generation(client: &mut SpiClient<'_>, slot_generation: i64) -> Result<(), String> {
    let arguments = unsafe { [DatumWithOid::new(slot_generation, pg_sys::INT8OID)] };
    let rows = client
        .update(
            "SELECT 1
               FROM shiba_internal.ingress_replay_state AS replay
              WHERE replay.slot_generation = $1
                AND replay.state = 'active'
              FOR UPDATE",
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not lock ingress generation: {error}"))?;
    require_count(&rows, 1, "active ingress generation")
}

fn choose_candidate(
    client: &mut SpiClient<'_>,
    slot_generation: i64,
) -> Result<Option<Candidate>, String> {
    let arguments = unsafe { [DatumWithOid::new(slot_generation, pg_sys::INT8OID)] };
    let rows = client
        .select(
            r#"
            SELECT publication.ingress_txn_id,
                   publication.batch_ordinal,
                   publication.source_oid
              FROM shiba_internal.source_publications AS publication
              JOIN shiba_internal.ingress_transactions AS txn
                ON txn.ingress_txn_id = publication.ingress_txn_id
             WHERE txn.slot_generation = $1
               AND txn.status = 'committed'
               AND publication.next_input_seq IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1
                     FROM shiba_internal.source_publications AS earlier
                     JOIN shiba_internal.ingress_transactions AS earlier_txn
                       ON earlier_txn.ingress_txn_id = earlier.ingress_txn_id
                    WHERE earlier_txn.slot_generation = txn.slot_generation
                      AND earlier_txn.status = 'committed'
                      AND earlier.source_oid = publication.source_oid
                      AND earlier.next_input_seq IS NOT NULL
                      AND (
                          earlier_txn.final_lsn,
                          earlier.ingress_txn_id,
                          earlier.batch_ordinal
                      ) < (
                          txn.final_lsn,
                          publication.ingress_txn_id,
                          publication.batch_ordinal
                      )
               )
               AND NOT EXISTS (
                   SELECT 1
                     FROM shiba_internal.effect_streams AS stream
                    WHERE stream.producer_kind = 'source'
                      AND stream.slot_generation = txn.slot_generation
                      AND stream.source_oid = publication.source_oid
                      AND stream.backpressured
                      AND EXISTS (
                          SELECT 1
                            FROM shiba_internal.effect_stream_consumers AS consumer
                           WHERE consumer.stream_id = stream.stream_id
                             AND consumer.activation_lsn < txn.final_lsn
                      )
               )
             ORDER BY txn.final_lsn,
                      publication.ingress_txn_id,
                      publication.batch_ordinal,
                      publication.source_oid
             LIMIT 1
            "#,
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not choose source publication task: {error}"))?;
    if rows.is_empty() {
        return Ok(None);
    }
    let row = rows.first();
    Ok(Some(Candidate {
        ingress_txn_id: required_table(&row, 1, "publication transaction ID")?,
        batch_ordinal: required_table(&row, 2, "publication batch ordinal")?,
        source_oid: required_table(&row, 3, "publication source OID")?,
    }))
}

fn lock_task(
    client: &mut SpiClient<'_>,
    slot_generation: i64,
    candidate: Candidate,
) -> Result<Task, String> {
    let transaction_arguments = unsafe {
        [
            DatumWithOid::new(candidate.ingress_txn_id, pg_sys::INT8OID),
            DatumWithOid::new(slot_generation, pg_sys::INT8OID),
        ]
    };
    let transaction = client
        .update(
            "SELECT txn.final_lsn::text
               FROM shiba_internal.ingress_transactions AS txn
              WHERE txn.ingress_txn_id = $1
                AND txn.slot_generation = $2
                AND txn.status = 'committed'
              FOR UPDATE",
            Some(1),
            &transaction_arguments,
        )
        .map_err(|error| format!("could not lock publication transaction: {error}"))?;
    require_count(&transaction, 1, "committed publication transaction")?;
    let final_lsn = required_table(&transaction.first(), 1, "publication final LSN")?;

    let task_arguments = unsafe {
        [
            DatumWithOid::new(candidate.ingress_txn_id, pg_sys::INT8OID),
            DatumWithOid::new(candidate.batch_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(candidate.source_oid, pg_sys::OIDOID),
        ]
    };
    let rows = client
        .update(
            "SELECT publication.first_input_seq,
                    publication.last_input_seq,
                    publication.next_input_seq
               FROM shiba_internal.source_publications AS publication
              WHERE publication.ingress_txn_id = $1
                AND publication.batch_ordinal = $2
                AND publication.source_oid = $3
                AND publication.next_input_seq IS NOT NULL
              FOR UPDATE",
            Some(1),
            &task_arguments,
        )
        .map_err(|error| format!("could not lock source publication task: {error}"))?;
    require_count(&rows, 1, "pending source publication task")?;
    let row = rows.first();
    Ok(Task {
        ingress_txn_id: candidate.ingress_txn_id,
        batch_ordinal: candidate.batch_ordinal,
        source_oid: candidate.source_oid,
        first_input_seq: required_table(&row, 1, "publication first input sequence")?,
        last_input_seq: required_table(&row, 2, "publication last input sequence")?,
        next_input_seq: required_table(&row, 3, "publication next input sequence")?,
        final_lsn,
    })
}

fn lock_stream(
    client: &mut SpiClient<'_>,
    slot_generation: i64,
    task: &Task,
) -> Result<Option<Stream>, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(slot_generation, pg_sys::INT8OID),
            DatumWithOid::new(task.source_oid, pg_sys::OIDOID),
            DatumWithOid::new(task.final_lsn.as_str(), pg_sys::TEXTOID),
        ]
    };
    let rows = client
        .update(
            r#"
            SELECT stream.stream_id,
                   stream.next_chunk_seq,
                   stream.backpressured,
                   stream.target_chunk_rows,
                   stream.target_chunk_bytes,
                   EXISTS (
                       SELECT 1
                         FROM shiba_internal.effect_stream_consumers AS consumer
                        WHERE consumer.stream_id = stream.stream_id
                          AND consumer.activation_lsn < $3::pg_lsn
                   )
              FROM shiba_internal.effect_streams AS stream
             WHERE stream.producer_kind = 'source'
               AND stream.slot_generation = $1
               AND stream.source_oid = $2
             FOR UPDATE OF stream
            "#,
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not lock source effect stream: {error}"))?;
    if rows.is_empty() {
        return Ok(None);
    }
    let row = rows.first();
    Ok(Some(Stream {
        stream_id: required_table(&row, 1, "source stream ID")?,
        next_chunk_seq: required_table(&row, 2, "source stream chunk sequence")?,
        backpressured: required_table(&row, 3, "source stream backpressure")?,
        target_chunk_rows: required_table(&row, 4, "source stream row target")?,
        target_chunk_bytes: required_table(&row, 5, "source stream byte target")?,
        has_eligible_consumer: required_table(&row, 6, "source stream consumer eligibility")?,
    }))
}

fn append_typed_prefix(
    client: &mut SpiClient<'_>,
    task: &Task,
    stream: Stream,
) -> Result<(SourcePublicationOutcome, Option<i64>, Option<i64>), String> {
    let storage = resolve_payload_storage(client, stream.stream_id)?;
    let query = format!(
        r#"
        WITH candidates AS MATERIALIZED (
            SELECT event.input_seq,
                   event.weight,
                   event.payload
              FROM shiba_internal.change_log AS event
             WHERE event.ingress_txn_id = $3
               AND event.source_oid = $4
               AND event.input_seq BETWEEN $5 AND $6
               AND NOT EXISTS (
                   SELECT 1
                     FROM shiba_internal.ingress_aborted_subtransactions AS aborted
                    WHERE aborted.ingress_txn_id = event.ingress_txn_id
                      AND aborted.source_subxid = event.source_subxid
               )
             ORDER BY event.input_seq
             LIMIT $7
        ),
        converted AS MATERIALIZED (
            SELECT candidate.input_seq,
                   candidate.weight,
                   pg_catalog.row_number() OVER (
                       ORDER BY candidate.input_seq
                   ) AS ordinal,
                   pg_catalog.jsonb_populate_record(
                       NULL::{row_type},
                       candidate.payload
                   ) AS row_value
              FROM candidates AS candidate
        ),
        measured AS MATERIALIZED (
            SELECT converted.*,
                   shiba_internal.effect_row_bytes(
                       converted.row_value
                   ) AS row_bytes
              FROM converted
        ),
        running AS MATERIALIZED (
            SELECT measured.*,
                   sum(measured.row_bytes) OVER (
                       ORDER BY measured.input_seq
                       ROWS UNBOUNDED PRECEDING
                   ) AS running_bytes
              FROM measured
        ),
        selected AS MATERIALIZED (
            SELECT running.*
              FROM running
             WHERE running.ordinal = 1
                OR (
                    running.ordinal <= $7
                    AND running.running_bytes <= $8
                )
        ),
        selected_facts AS MATERIALIZED (
            SELECT count(*)::bigint AS selected_rows,
                   coalesce(sum(selected.row_bytes), 0)::bigint AS selected_bytes,
                   max(selected.input_seq) AS selected_last_input_seq
              FROM selected
        ),
        append_input AS MATERIALIZED (
            SELECT selected_facts.*
              FROM selected_facts
             WHERE selected_facts.selected_rows > 0
               AND selected_facts.selected_bytes > 0
        ),
        append AS MATERIALIZED (
            SELECT result.outcome,
                   result.appended_chunk_seq
              FROM append_input
              CROSS JOIN LATERAL shiba_internal.append_effect_stream_chunk(
                  $1,
                  $2,
                  'data',
                  append_input.selected_rows,
                  append_input.selected_bytes,
                  $9::pg_lsn
              ) AS result
        ),
        inserted AS (
            INSERT INTO {payload_relation} (
                stream_id,
                chunk_seq,
                row_ordinal,
                weight,
                row_value
            )
            SELECT $1,
                   append.appended_chunk_seq,
                   (selected.ordinal - 1)::bigint,
                   selected.weight,
                   selected.row_value
              FROM selected
              CROSS JOIN append
             WHERE append.outcome = 'appended'
             ORDER BY selected.input_seq
            RETURNING row_value
        ),
        next_input AS MATERIALIZED (
            SELECT min(event.input_seq)::bigint AS next_input_seq
              FROM selected_facts
              JOIN append ON append.outcome = 'appended'
              JOIN shiba_internal.change_log AS event
                ON event.ingress_txn_id = $3
               AND event.source_oid = $4
               AND event.input_seq > selected_facts.selected_last_input_seq
               AND event.input_seq <= $6
             WHERE NOT EXISTS (
                 SELECT 1
                   FROM shiba_internal.ingress_aborted_subtransactions AS aborted
                  WHERE aborted.ingress_txn_id = event.ingress_txn_id
                    AND aborted.source_subxid = event.source_subxid
             )
        )
        SELECT selected_facts.selected_rows,
               selected_facts.selected_bytes,
               selected_facts.selected_last_input_seq,
               coalesce(
                   (SELECT append.outcome FROM append),
                   'empty'
               ),
               (SELECT append.appended_chunk_seq FROM append),
               (SELECT count(*)::bigint FROM inserted),
               (
                   SELECT coalesce(
                       sum(shiba_internal.effect_row_bytes(inserted.row_value)),
                       0
                   )::bigint
                     FROM inserted
               ),
               (SELECT next_input.next_input_seq FROM next_input)
          FROM selected_facts
        "#,
        row_type = storage.row_type.sql(),
        payload_relation = storage.relation.sql(),
    );
    let first_input_seq = task.next_input_seq.max(task.first_input_seq);
    let arguments = unsafe {
        [
            DatumWithOid::new(stream.stream_id, pg_sys::INT8OID),
            DatumWithOid::new(stream.next_chunk_seq, pg_sys::INT8OID),
            DatumWithOid::new(task.ingress_txn_id, pg_sys::INT8OID),
            DatumWithOid::new(task.source_oid, pg_sys::OIDOID),
            DatumWithOid::new(first_input_seq, pg_sys::INT8OID),
            DatumWithOid::new(task.last_input_seq, pg_sys::INT8OID),
            DatumWithOid::new(stream.target_chunk_rows, pg_sys::INT8OID),
            DatumWithOid::new(stream.target_chunk_bytes, pg_sys::INT8OID),
            DatumWithOid::new(task.final_lsn.as_str(), pg_sys::TEXTOID),
        ]
    };
    let rows = client
        .update(&query, Some(1), &arguments)
        .map_err(|error| format!("could not append typed source prefix: {error}"))?;
    require_count(&rows, 1, "typed source append")?;
    let row = rows.first();
    let facts = AppendFacts {
        selected_rows: required_table(&row, 1, "selected source rows")?,
        selected_bytes: required_table(&row, 2, "selected source bytes")?,
        selected_last_input_seq: optional(&row, 3, "selected last input sequence")?,
        append_outcome: required_table(&row, 4, "source append outcome")?,
        appended_chunk_seq: optional(&row, 5, "appended source chunk sequence")?,
        inserted_rows: required_table(&row, 6, "inserted source rows")?,
        inserted_bytes: required_table(&row, 7, "inserted source bytes")?,
        next_input_seq: optional(&row, 8, "next source input sequence")?,
    };
    validate_append_facts(task, facts)
}

fn validate_append_facts(
    task: &Task,
    facts: AppendFacts,
) -> Result<(SourcePublicationOutcome, Option<i64>, Option<i64>), String> {
    if facts.selected_rows < 0
        || facts.selected_bytes < 0
        || facts.inserted_rows < 0
        || facts.inserted_bytes < 0
    {
        return Err("source append returned negative resource facts".into());
    }
    if facts.selected_rows == 0 {
        if facts.selected_bytes != 0
            || facts.selected_last_input_seq.is_some()
            || facts.append_outcome != "empty"
            || facts.appended_chunk_seq.is_some()
            || facts.inserted_rows != 0
            || facts.inserted_bytes != 0
            || facts.next_input_seq.is_some()
        {
            return Err(format!(
                "source publication {}/{}/{} returned inconsistent empty append facts",
                task.ingress_txn_id,
                task.batch_ordinal,
                task.source_oid.to_u32()
            ));
        }
        return Ok((SourcePublicationOutcome::Completed, None, None));
    }
    if facts.selected_bytes < 1 || facts.selected_last_input_seq.is_none() {
        return Err(format!(
            "source publication {}/{}/{} selected rows without typed payload",
            task.ingress_txn_id,
            task.batch_ordinal,
            task.source_oid.to_u32()
        ));
    }

    match facts.append_outcome.as_str() {
        "appended" => {
            let chunk_seq = facts.appended_chunk_seq.ok_or_else(|| {
                "source append returned no chunk sequence after append".to_string()
            })?;
            if facts.inserted_rows != facts.selected_rows
                || facts.inserted_bytes != facts.selected_bytes
            {
                return Err(format!(
                    "source publication {}/{}/{} selected {}/{}, inserted {}/{}",
                    task.ingress_txn_id,
                    task.batch_ordinal,
                    task.source_oid.to_u32(),
                    facts.selected_rows,
                    facts.selected_bytes,
                    facts.inserted_rows,
                    facts.inserted_bytes
                ));
            }
            let outcome = if facts.next_input_seq.is_some() {
                SourcePublicationOutcome::Appended
            } else {
                SourcePublicationOutcome::Completed
            };
            Ok((outcome, Some(chunk_seq), facts.next_input_seq))
        }
        "blocked" | "discarded" => {
            if facts.appended_chunk_seq.is_some()
                || facts.inserted_rows != 0
                || facts.inserted_bytes != 0
                || facts.next_input_seq.is_some()
            {
                return Err(format!(
                    "source publication {}/{}/{} wrote payload after {}",
                    task.ingress_txn_id,
                    task.batch_ordinal,
                    task.source_oid.to_u32(),
                    facts.append_outcome
                ));
            }
            let outcome = if facts.append_outcome == "blocked" {
                SourcePublicationOutcome::Blocked
            } else {
                SourcePublicationOutcome::Discarded
            };
            Ok((outcome, None, None))
        }
        unexpected => Err(format!("unknown source stream append outcome {unexpected}")),
    }
}

fn advance_task(
    client: &mut SpiClient<'_>,
    task: &Task,
    outcome: SourcePublicationOutcome,
    next_input_seq: Option<i64>,
) -> Result<(), String> {
    if matches!(outcome, SourcePublicationOutcome::Appended) != next_input_seq.is_some() {
        return Err("source task cursor does not match publication outcome".into());
    }
    let arguments = unsafe {
        [
            DatumWithOid::new(next_input_seq, pg_sys::INT8OID),
            DatumWithOid::new(task.ingress_txn_id, pg_sys::INT8OID),
            DatumWithOid::new(task.batch_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(task.source_oid, pg_sys::OIDOID),
            DatumWithOid::new(task.next_input_seq, pg_sys::INT8OID),
        ]
    };
    let rows = client
        .update(
            "UPDATE shiba_internal.source_publications AS publication
                SET next_input_seq = $1
              WHERE publication.ingress_txn_id = $2
                AND publication.batch_ordinal = $3
                AND publication.source_oid = $4
                AND publication.next_input_seq = $5
              RETURNING 1",
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not advance source publication task: {error}"))?;
    require_count(&rows, 1, "source publication cursor compare-and-set")?;

    if matches!(
        outcome,
        SourcePublicationOutcome::Completed | SourcePublicationOutcome::Discarded
    ) {
        let arguments = unsafe { [DatumWithOid::new(task.ingress_txn_id, pg_sys::INT8OID)] };
        let rows = client
            .update(
                "UPDATE shiba_internal.ingress_transactions AS txn
                    SET pending_publications = txn.pending_publications - 1
                  WHERE txn.ingress_txn_id = $1
                    AND txn.pending_publications > 0
                  RETURNING txn.pending_publications",
                Some(1),
                &arguments,
            )
            .map_err(|error| format!("could not complete source publication task: {error}"))?;
        require_count(&rows, 1, "source publication pending counter")?;
    }
    Ok(())
}

fn has_pending(client: &mut SpiClient<'_>, slot_generation: i64) -> Result<bool, String> {
    let arguments = unsafe { [DatumWithOid::new(slot_generation, pg_sys::INT8OID)] };
    let rows = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                   FROM shiba_internal.ingress_transactions AS txn
                  WHERE txn.slot_generation = $1
                    AND txn.status = 'committed'
                    AND txn.pending_publications > 0
             )",
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not read source publication backlog: {error}"))?;
    require_count(&rows, 1, "source publication backlog")?;
    required_table(&rows.first(), 1, "source publication backlog")
}

// The production Runtime calls the Rust protocol directly. This SQL surface
// exists only for the integration gate's synthetic generations and rollback
// assertions.
#[cfg(any(test, feature = "pg_test"))]
#[allow(clippy::type_complexity)]
#[pg_extern(
    schema = "shiba_internal",
    name = "test_publish_source_batch",
    security_definer,
    volatile,
    requires = ["shiba_catalog"]
)]
#[search_path(pg_catalog, shiba_internal)]
fn test_publish_source_batch(
    slot_generation: i64,
) -> TableIterator<
    'static,
    (
        name!(outcome, String),
        name!(ingress_txn_id, Option<i64>),
        name!(batch_ordinal, Option<i64>),
        name!(source_oid, Option<pg_sys::Oid>),
        name!(final_lsn, Option<String>),
        name!(chunk_seq, Option<i64>),
        name!(has_pending, bool),
    ),
> {
    let publication = publish_source_batch(slot_generation)
        .unwrap_or_else(|error| error!("Shiba could not publish source batch: {error}"));
    TableIterator::once((
        publication.outcome.sql().to_owned(),
        publication.ingress_txn_id,
        publication.batch_ordinal,
        publication.source_oid,
        publication.final_lsn,
        publication.chunk_seq,
        publication.has_pending,
    ))
}
