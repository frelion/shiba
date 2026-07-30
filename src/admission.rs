//! Durable admission of one bounded pgoutput event batch.
//!
//! Rust owns validation, replay classification, counter allocation, and
//! outcome checks. PostgreSQL executes one data-modifying CTE that commits the
//! change rows, source tasks, staging bytes, and transaction counters together.

use std::collections::{BTreeMap, HashSet};

use pgrx::datum::DatumWithOid;
#[cfg(any(test, feature = "pg_test"))]
use pgrx::datum::JsonB;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
#[cfg(any(test, feature = "pg_test"))]
use serde::Deserialize;
use serde_json::Value;

use crate::database::{optional, require_count, required as required_table};
use crate::ingress::IngressEvent;
#[cfg(any(test, feature = "pg_test"))]
use crate::postgres::parse_lsn;
use crate::postgres::{format_lsn, quote_identifier};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionFacts {
    pub(crate) inserted_count: i64,
    pub(crate) replayed_count: i64,
    pub(crate) first_input_seq: Option<i64>,
    pub(crate) last_input_seq: Option<i64>,
}

#[derive(Clone, Debug)]
struct Event {
    change_lsn: u64,
    change_ordinal: u64,
    image_ordinal: u32,
    source_subxid: u32,
    source_oid: u32,
    weight: i64,
    payload: Value,
}

#[cfg(any(test, feature = "pg_test"))]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestEvent {
    change_lsn: String,
    change_ordinal: u64,
    image_ordinal: u32,
    source_subxid: u32,
    source_oid: u32,
    weight: i64,
    payload: Value,
}

#[derive(Clone, Debug)]
struct Header {
    slot_generation: i64,
    status: String,
    event_count: i64,
    payload_bytes: i64,
    batch_count: i64,
    pending_publications: i64,
}

#[derive(Clone, Copy, Debug)]
struct ReplayFacts {
    inserted_count: i64,
    replayed_count: i64,
    last_replayed_ordinal: Option<i64>,
    first_new_ordinal: Option<i64>,
    first_input_seq: Option<i64>,
    last_input_seq: Option<i64>,
    identity_conflict: bool,
    batch_payload_bytes: i64,
}

#[derive(Clone, Copy, Debug)]
struct Allocation {
    first_input_seq: i64,
    last_input_seq: i64,
    batch_ordinal: i64,
    task_count: i64,
    event_count: i64,
    payload_bytes: i64,
    batch_count: i64,
    pending_publications: i64,
}

#[derive(Clone, Copy, Debug)]
struct WriteFacts {
    inserted_count: i64,
    first_input_seq: Option<i64>,
    last_input_seq: Option<i64>,
    payload_bytes: i64,
    task_count: i64,
    replay_updates: i64,
    header_updates: i64,
    event_count: Option<i64>,
    total_payload_bytes: Option<i64>,
    batch_count: Option<i64>,
    pending_publications: Option<i64>,
}

pub(crate) fn insert_ingress_events(
    ingress_txn_id: i64,
    events: &[IngressEvent],
) -> Result<AdmissionFacts, String> {
    let events = events
        .iter()
        .map(|event| Event {
            change_lsn: event.change_lsn,
            change_ordinal: event.change_ordinal,
            image_ordinal: event.image_ordinal,
            source_subxid: event.source_subxid,
            source_oid: event.source_oid,
            weight: event.weight,
            payload: event.payload.clone(),
        })
        .collect::<Vec<_>>();
    admit(ingress_txn_id, &events)
}

fn admit(ingress_txn_id: i64, events: &[Event]) -> Result<AdmissionFacts, String> {
    if ingress_txn_id <= 0 {
        return Err("ingress transaction ID must be positive".into());
    }
    validate_events(events)?;
    let encoded = encode_events(events);
    Spi::connect_mut(|client| {
        let header = lock_header(client, ingress_txn_id)?;
        // Hold the generation and header authorities before consulting source
        // metadata, matching the global ingress lock order used by finalizers.
        validate_source_rows(client, &encoded, events)?;
        let replay = classify_replay(client, ingress_txn_id, &encoded)?;
        validate_replay(ingress_txn_id, &header, replay)?;

        if header.status != "open" || replay.inserted_count == 0 {
            return Ok(AdmissionFacts {
                inserted_count: replay.inserted_count,
                replayed_count: replay.replayed_count,
                first_input_seq: replay.first_input_seq,
                last_input_seq: replay.last_input_seq,
            });
        }

        let replayed = usize::try_from(replay.replayed_count)
            .map_err(|_| "replayed event count exceeds usize")?;
        let task_count = i64::try_from(
            events[replayed..]
                .iter()
                .map(|event| event.source_oid)
                .collect::<HashSet<_>>()
                .len(),
        )
        .map_err(|_| "ingress publication task count exceeds bigint")?;
        if task_count < 1 {
            return Err("new ingress batch has no source publication task".into());
        }
        let allocation = allocate(&header, replay, task_count)?;
        let written = write_batch(
            client,
            ingress_txn_id,
            &encoded,
            &header,
            replay,
            allocation,
        )?;
        validate_write(ingress_txn_id, replay, allocation, written)?;

        Ok(AdmissionFacts {
            inserted_count: replay.inserted_count,
            replayed_count: replay.replayed_count,
            first_input_seq: min_option(replay.first_input_seq, Some(allocation.first_input_seq)),
            last_input_seq: max_option(replay.last_input_seq, Some(allocation.last_input_seq)),
        })
    })
}

fn validate_events(events: &[Event]) -> Result<(), String> {
    let mut identities = HashSet::with_capacity(events.len());
    for event in events {
        if event.change_ordinal > i64::MAX as u64
            || event.image_ordinal > i32::MAX as u32
            || event.source_oid == 0
            || !matches!(event.weight, -1 | 1)
            || !event.payload.is_object()
        {
            return Err("ingress event batch contains an invalid value".into());
        }
        if !identities.insert((event.change_lsn, event.change_ordinal, event.image_ordinal)) {
            return Err("ingress event batch repeats a stable event identity".into());
        }
    }
    i64::try_from(events.len()).map_err(|_| "ingress event batch exceeds bigint".to_string())?;
    Ok(())
}

fn encode_events(events: &[Event]) -> String {
    Value::Array(
        events
            .iter()
            .map(|event| {
                serde_json::json!({
                    "change_lsn": format_lsn(event.change_lsn),
                    "change_ordinal": event.change_ordinal,
                    "image_ordinal": event.image_ordinal,
                    "source_subxid": event.source_subxid,
                    "source_oid": event.source_oid,
                    "weight": event.weight,
                    "payload": event.payload,
                })
            })
            .collect(),
    )
    .to_string()
}

fn validate_source_rows(
    client: &mut SpiClient<'_>,
    encoded: &str,
    events: &[Event],
) -> Result<(), String> {
    let mut sources = BTreeMap::<u32, i64>::new();
    for event in events {
        *sources.entry(event.source_oid).or_default() += 1;
    }
    for (source_oid, expected_count) in sources {
        let oid = pg_sys::Oid::from(source_oid);
        let arguments = unsafe { [DatumWithOid::new(oid, pg_sys::OIDOID)] };
        let rows = client
            .select(
                "SELECT namespace.nspname::text,
                        relation.relname::text
                   FROM pg_catalog.pg_class AS relation
                   JOIN pg_catalog.pg_namespace AS namespace
                     ON namespace.oid = relation.relnamespace
                  WHERE relation.oid = $1
                    AND relation.relkind IN ('r', 'p')
                    AND relation.relpersistence = 'p'",
                Some(1),
                &arguments,
            )
            .map_err(|error| format!("could not resolve ingress source {source_oid}: {error}"))?;
        require_count(&rows, 1, "persistent ingress source")?;
        let row = rows.first();
        let namespace: String = required_table(&row, 1, "ingress source namespace")?;
        let relation: String = required_table(&row, 2, "ingress source relation")?;
        let source_type = format!(
            "{}.{}",
            quote_identifier(&namespace),
            quote_identifier(&relation)
        );
        let query = format!(
            "SELECT count(
                        pg_catalog.jsonb_populate_record(
                            NULL::{source_type},
                            item.value -> 'payload'
                        )
                    )::bigint
               FROM pg_catalog.jsonb_array_elements($1::jsonb) AS item(value)
              WHERE (item.value ->> 'source_oid')::oid = $2"
        );
        let arguments = unsafe {
            [
                DatumWithOid::new(encoded, pg_sys::TEXTOID),
                DatumWithOid::new(oid, pg_sys::OIDOID),
            ]
        };
        let rows = client
            .select(&query, Some(1), &arguments)
            .map_err(|error| format!("could not validate typed source {source_oid}: {error}"))?;
        require_count(&rows, 1, "typed ingress validation")?;
        let actual: i64 = required_table(&rows.first(), 1, "typed ingress row count")?;
        if actual != expected_count {
            return Err(format!(
                "typed ingress validation for source {source_oid} returned {actual} rows, expected {expected_count}"
            ));
        }
    }
    Ok(())
}

fn lock_header(client: &mut SpiClient<'_>, ingress_txn_id: i64) -> Result<Header, String> {
    let arguments = unsafe { [DatumWithOid::new(ingress_txn_id, pg_sys::INT8OID)] };
    let rows = client
        .select(
            "SELECT txn.slot_generation
               FROM shiba_internal.ingress_transactions AS txn
              WHERE txn.ingress_txn_id = $1",
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not resolve ingress transaction: {error}"))?;
    require_count(&rows, 1, "ingress transaction")?;
    let slot_generation: i64 = required_table(&rows.first(), 1, "ingress slot generation")?;

    let generation_arguments = unsafe { [DatumWithOid::new(slot_generation, pg_sys::INT8OID)] };
    let rows = client
        .update(
            "SELECT 1
               FROM shiba_internal.ingress_replay_state AS replay
              WHERE replay.slot_generation = $1
                AND replay.state = 'active'
              FOR UPDATE",
            Some(1),
            &generation_arguments,
        )
        .map_err(|error| format!("could not lock ingress generation: {error}"))?;
    require_count(&rows, 1, "active ingress generation")?;

    let rows = client
        .update(
            "SELECT txn.status,
                    txn.event_count,
                    txn.payload_bytes,
                    txn.batch_count,
                    txn.pending_publications
               FROM shiba_internal.ingress_transactions AS txn
              WHERE txn.ingress_txn_id = $1
              FOR UPDATE",
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not lock ingress transaction: {error}"))?;
    require_count(&rows, 1, "locked ingress transaction")?;
    let row = rows.first();
    Ok(Header {
        slot_generation,
        status: required_table(&row, 1, "ingress transaction status")?,
        event_count: required_table(&row, 2, "ingress event count")?,
        payload_bytes: required_table(&row, 3, "ingress payload bytes")?,
        batch_count: required_table(&row, 4, "ingress batch count")?,
        pending_publications: required_table(&row, 5, "pending publication count")?,
    })
}

fn classify_replay(
    client: &mut SpiClient<'_>,
    ingress_txn_id: i64,
    encoded: &str,
) -> Result<ReplayFacts, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(ingress_txn_id, pg_sys::INT8OID),
            DatumWithOid::new(encoded, pg_sys::TEXTOID),
        ]
    };
    let rows = client
        .select(
            r#"
            WITH incoming AS MATERIALIZED (
                SELECT item.ordinality::bigint AS ordinal,
                       (item.value ->> 'change_lsn')::pg_lsn AS change_lsn,
                       (item.value ->> 'change_ordinal')::bigint AS change_ordinal,
                       (item.value ->> 'image_ordinal')::integer AS image_ordinal,
                       (item.value ->> 'source_subxid')::bigint AS source_subxid,
                       (item.value ->> 'source_oid')::oid AS source_oid,
                       (item.value ->> 'weight')::bigint AS weight,
                       item.value -> 'payload' AS payload
                  FROM pg_catalog.jsonb_array_elements($2::jsonb)
                       WITH ORDINALITY AS item(value, ordinality)
            ),
            matched AS MATERIALIZED (
                SELECT incoming.*,
                       existing.input_seq,
                       existing.source_subxid AS existing_source_subxid,
                       existing.source_oid AS existing_source_oid,
                       existing.weight AS existing_weight,
                       existing.payload AS existing_payload
                  FROM incoming
                  LEFT JOIN shiba_internal.change_log AS existing
                    ON existing.ingress_txn_id = $1
                   AND existing.change_lsn = incoming.change_lsn
                   AND existing.change_ordinal = incoming.change_ordinal
                   AND existing.image_ordinal = incoming.image_ordinal
            )
            SELECT count(*) FILTER (WHERE input_seq IS NULL)::bigint,
                   count(*) FILTER (WHERE input_seq IS NOT NULL)::bigint,
                   max(ordinal) FILTER (WHERE input_seq IS NOT NULL),
                   min(ordinal) FILTER (WHERE input_seq IS NULL),
                   min(input_seq),
                   max(input_seq),
                   coalesce(
                       bool_or(
                           input_seq IS NOT NULL
                           AND (
                               existing_source_oid IS DISTINCT FROM source_oid
                               OR existing_source_subxid IS DISTINCT FROM source_subxid
                               OR existing_weight IS DISTINCT FROM weight
                               OR existing_payload IS DISTINCT FROM payload
                           )
                       ),
                       false
                   ),
                   coalesce(
                       sum(pg_catalog.octet_length(
                           pg_catalog.jsonb_send(payload)
                       )) FILTER (WHERE input_seq IS NULL),
                       0
                   )::bigint
              FROM matched
            "#,
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not classify ingress replay: {error}"))?;
    require_count(&rows, 1, "ingress replay classification")?;
    let row = rows.first();
    Ok(ReplayFacts {
        inserted_count: required_table(&row, 1, "new ingress event count")?,
        replayed_count: required_table(&row, 2, "replayed ingress event count")?,
        last_replayed_ordinal: optional(&row, 3, "last replayed ordinal")?,
        first_new_ordinal: optional(&row, 4, "first new ordinal")?,
        first_input_seq: optional(&row, 5, "first replayed input sequence")?,
        last_input_seq: optional(&row, 6, "last replayed input sequence")?,
        identity_conflict: required_table(&row, 7, "ingress identity conflict")?,
        batch_payload_bytes: required_table(&row, 8, "new ingress payload bytes")?,
    })
}

fn validate_replay(
    ingress_txn_id: i64,
    header: &Header,
    replay: ReplayFacts,
) -> Result<(), String> {
    if replay.inserted_count < 0 || replay.replayed_count < 0 || replay.batch_payload_bytes < 0 {
        return Err("ingress replay returned negative facts".into());
    }
    if replay.identity_conflict {
        return Err(format!(
            "ingress event identity conflict for transaction {ingress_txn_id}"
        ));
    }
    if replay.first_new_ordinal.is_some()
        && replay.last_replayed_ordinal.is_some()
        && replay.first_new_ordinal < replay.last_replayed_ordinal
    {
        return Err(format!(
            "ingress replay for transaction {ingress_txn_id} is not an existing prefix"
        ));
    }
    if header.status != "open" && replay.inserted_count > 0 {
        return Err(format!(
            "replay added events to ingress transaction {ingress_txn_id} in terminal state {}",
            header.status
        ));
    }
    Ok(())
}

fn allocate(header: &Header, replay: ReplayFacts, task_count: i64) -> Result<Allocation, String> {
    let first_input_seq = header
        .event_count
        .checked_add(1)
        .ok_or("ingress event sequence exhausted bigint range")?;
    let event_count = header
        .event_count
        .checked_add(replay.inserted_count)
        .ok_or("ingress transaction event count exhausted bigint range")?;
    let payload_bytes = header
        .payload_bytes
        .checked_add(replay.batch_payload_bytes)
        .ok_or("ingress transaction payload summary exhausted bigint range")?;
    let batch_count = header
        .batch_count
        .checked_add(1)
        .ok_or("ingress apply batch count exhausted bigint range")?;
    let pending_publications = header
        .pending_publications
        .checked_add(task_count)
        .ok_or("ingress publication task count exhausted bigint range")?;
    Ok(Allocation {
        first_input_seq,
        last_input_seq: event_count,
        batch_ordinal: batch_count,
        task_count,
        event_count,
        payload_bytes,
        batch_count,
        pending_publications,
    })
}

fn write_batch(
    client: &mut SpiClient<'_>,
    ingress_txn_id: i64,
    encoded: &str,
    header: &Header,
    replay: ReplayFacts,
    allocation: Allocation,
) -> Result<WriteFacts, String> {
    let arguments = unsafe {
        vec![
            DatumWithOid::new(ingress_txn_id, pg_sys::INT8OID),
            DatumWithOid::new(encoded, pg_sys::TEXTOID),
            DatumWithOid::new(replay.replayed_count, pg_sys::INT8OID),
            DatumWithOid::new(allocation.first_input_seq, pg_sys::INT8OID),
            DatumWithOid::new(allocation.last_input_seq, pg_sys::INT8OID),
            DatumWithOid::new(allocation.batch_ordinal, pg_sys::INT8OID),
            DatumWithOid::new(header.slot_generation, pg_sys::INT8OID),
            DatumWithOid::new(replay.batch_payload_bytes, pg_sys::INT8OID),
            DatumWithOid::new(allocation.task_count, pg_sys::INT8OID),
            DatumWithOid::new(allocation.event_count, pg_sys::INT8OID),
            DatumWithOid::new(allocation.payload_bytes, pg_sys::INT8OID),
            DatumWithOid::new(allocation.batch_count, pg_sys::INT8OID),
            DatumWithOid::new(allocation.pending_publications, pg_sys::INT8OID),
            DatumWithOid::new(header.event_count, pg_sys::INT8OID),
            DatumWithOid::new(header.payload_bytes, pg_sys::INT8OID),
            DatumWithOid::new(header.batch_count, pg_sys::INT8OID),
            DatumWithOid::new(header.pending_publications, pg_sys::INT8OID),
        ]
    };
    let rows = client
        .update(WRITE_BATCH_SQL, Some(1), &arguments)
        .map_err(|error| format!("could not write bounded ingress batch: {error}"))?;
    require_count(&rows, 1, "bounded ingress write")?;
    let row = rows.first();
    Ok(WriteFacts {
        inserted_count: required_table(&row, 1, "written ingress event count")?,
        first_input_seq: optional(&row, 2, "written first input sequence")?,
        last_input_seq: optional(&row, 3, "written last input sequence")?,
        payload_bytes: required_table(&row, 4, "written ingress payload bytes")?,
        task_count: required_table(&row, 5, "written source task count")?,
        replay_updates: required_table(&row, 6, "updated ingress replay rows")?,
        header_updates: required_table(&row, 7, "updated ingress header rows")?,
        event_count: optional(&row, 8, "updated ingress event count")?,
        total_payload_bytes: optional(&row, 9, "updated ingress payload total")?,
        batch_count: optional(&row, 10, "updated ingress batch count")?,
        pending_publications: optional(&row, 11, "updated pending publication count")?,
    })
}

const WRITE_BATCH_SQL: &str = r#"
WITH incoming AS MATERIALIZED (
    SELECT item.ordinality::bigint AS ordinal,
           (item.value ->> 'change_lsn')::pg_lsn AS change_lsn,
           (item.value ->> 'change_ordinal')::bigint AS change_ordinal,
           (item.value ->> 'image_ordinal')::integer AS image_ordinal,
           (item.value ->> 'source_subxid')::bigint AS source_subxid,
           (item.value ->> 'source_oid')::oid AS source_oid,
           (item.value ->> 'weight')::bigint AS weight,
           item.value -> 'payload' AS payload
      FROM pg_catalog.jsonb_array_elements($2::jsonb)
           WITH ORDINALITY AS item(value, ordinality)
),
new_events AS MATERIALIZED (
    SELECT incoming.*,
           $4 + pg_catalog.row_number() OVER (
               ORDER BY incoming.ordinal
           ) - 1 AS input_seq
      FROM incoming
     WHERE incoming.ordinal > $3
),
inserted AS (
    INSERT INTO shiba_internal.change_log (
        ingress_txn_id,
        change_lsn,
        change_ordinal,
        image_ordinal,
        source_subxid,
        input_seq,
        source_oid,
        weight,
        payload
    )
    SELECT $1,
           new_events.change_lsn,
           new_events.change_ordinal,
           new_events.image_ordinal,
           new_events.source_subxid,
           new_events.input_seq,
           new_events.source_oid,
           new_events.weight,
           new_events.payload
      FROM new_events
     ORDER BY new_events.ordinal
    RETURNING input_seq, source_oid, payload
),
inserted_facts AS MATERIALIZED (
    SELECT count(*)::bigint AS inserted_count,
           min(input_seq) AS first_input_seq,
           max(input_seq) AS last_input_seq,
           coalesce(
               sum(pg_catalog.octet_length(pg_catalog.jsonb_send(payload))),
               0
           )::bigint AS payload_bytes
      FROM inserted
),
publications AS (
    INSERT INTO shiba_internal.source_publications (
        ingress_txn_id,
        batch_ordinal,
        source_oid,
        first_input_seq,
        last_input_seq,
        next_input_seq
    )
    SELECT $1,
           $6,
           inserted.source_oid,
           $4,
           $5,
           min(inserted.input_seq)
      FROM inserted
      CROSS JOIN inserted_facts
     WHERE inserted_facts.inserted_count = $5 - $4 + 1
       AND inserted_facts.first_input_seq = $4
       AND inserted_facts.last_input_seq = $5
       AND inserted_facts.payload_bytes = $8
     GROUP BY inserted.source_oid
    RETURNING 1
),
publication_facts AS MATERIALIZED (
    SELECT count(*)::bigint AS task_count
      FROM publications
),
replay_updated AS (
    UPDATE shiba_internal.ingress_replay_state AS replay
       SET open_payload_bytes = replay.open_payload_bytes + $8,
           updated_at = clock_timestamp()
      FROM inserted_facts,
           publication_facts
     WHERE replay.slot_generation = $7
       AND inserted_facts.inserted_count = $5 - $4 + 1
       AND inserted_facts.first_input_seq = $4
       AND inserted_facts.last_input_seq = $5
       AND inserted_facts.payload_bytes = $8
       AND publication_facts.task_count = $9
       AND replay.open_payload_bytes
             <= pg_catalog.pg_size_bytes(
                    pg_catalog.current_setting('shiba.ingress_staging_limit')
                ) - $8
    RETURNING 1
),
header_updated AS (
    UPDATE shiba_internal.ingress_transactions AS txn
       SET event_count = $10,
           payload_bytes = $11,
           batch_count = $12,
           pending_publications = $13
      FROM replay_updated
     WHERE txn.ingress_txn_id = $1
       AND txn.status = 'open'
       AND txn.event_count = $14
       AND txn.payload_bytes = $15
       AND txn.batch_count = $16
       AND txn.pending_publications = $17
    RETURNING txn.event_count,
              txn.payload_bytes,
              txn.batch_count,
              txn.pending_publications
)
SELECT inserted_facts.inserted_count,
       inserted_facts.first_input_seq,
       inserted_facts.last_input_seq,
       inserted_facts.payload_bytes,
       publication_facts.task_count,
       (SELECT count(*)::bigint FROM replay_updated),
       (SELECT count(*)::bigint FROM header_updated),
       (SELECT header_updated.event_count FROM header_updated),
       (SELECT header_updated.payload_bytes FROM header_updated),
       (SELECT header_updated.batch_count FROM header_updated),
       (SELECT header_updated.pending_publications FROM header_updated)
  FROM inserted_facts
  CROSS JOIN publication_facts
"#;

fn validate_write(
    ingress_txn_id: i64,
    replay: ReplayFacts,
    allocation: Allocation,
    written: WriteFacts,
) -> Result<(), String> {
    let expected = (
        replay.inserted_count,
        Some(allocation.first_input_seq),
        Some(allocation.last_input_seq),
        replay.batch_payload_bytes,
        allocation.task_count,
        1,
        1,
        Some(allocation.event_count),
        Some(allocation.payload_bytes),
        Some(allocation.batch_count),
        Some(allocation.pending_publications),
    );
    let actual = (
        written.inserted_count,
        written.first_input_seq,
        written.last_input_seq,
        written.payload_bytes,
        written.task_count,
        written.replay_updates,
        written.header_updates,
        written.event_count,
        written.total_payload_bytes,
        written.batch_count,
        written.pending_publications,
    );
    if actual != expected {
        return Err(format!(
            "ingress transaction {ingress_txn_id} write facts {actual:?} did not match {expected:?}"
        ));
    }
    Ok(())
}

fn min_option(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

fn max_option(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[allow(clippy::type_complexity)]
#[pg_extern(
    schema = "shiba_internal",
    name = "test_insert_ingress_events",
    volatile,
    requires = ["shiba_catalog"]
)]
fn test_insert_ingress_events(
    ingress_txn_id: i64,
    events: JsonB,
) -> TableIterator<
    'static,
    (
        name!(inserted_count, i64),
        name!(replayed_count, i64),
        name!(first_input_seq, Option<i64>),
        name!(last_input_seq, Option<i64>),
    ),
> {
    let events = serde_json::from_value::<Vec<TestEvent>>(events.0)
        .unwrap_or_else(|error| error!("invalid ingress event batch: {error}"))
        .into_iter()
        .map(|event| {
            let change_lsn = parse_lsn(&event.change_lsn)
                .unwrap_or_else(|error| error!("invalid ingress event LSN: {error}"));
            Event {
                change_lsn,
                change_ordinal: event.change_ordinal,
                image_ordinal: event.image_ordinal,
                source_subxid: event.source_subxid,
                source_oid: event.source_oid,
                weight: event.weight,
                payload: event.payload,
            }
        })
        .collect::<Vec<_>>();
    let facts = admit(ingress_txn_id, &events)
        .unwrap_or_else(|error| error!("Shiba could not insert ingress events: {error}"));
    TableIterator::once((
        facts.inserted_count,
        facts.replayed_count,
        facts.first_input_seq,
        facts.last_input_seq,
    ))
}
