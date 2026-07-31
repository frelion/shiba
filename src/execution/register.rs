//! Rust-owned dataflow registration and typed storage provisioning.
//!
//! Registration is control flow, so it belongs beside the operator
//! dispatcher. PostgreSQL still creates and owns every typed relation. Rust
//! chooses the objects to create, records their OIDs, and never reconstructs
//! those names while executing a dataflow.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use pgrx::datum::{DatumWithOid, JsonB};
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiHeapTupleData, SpiTupleTable};
use serde::Deserialize;

use crate::config;
use crate::planner::model::{
    DataflowPlan, DataflowStage, OperatorSpec, OutputSlot, ScanSpec, SlotType,
};
use crate::postgres::quote_identifier;

use super::storage;

#[derive(Clone, Debug)]
struct ResultIdentity {
    qualified: String,
}

#[derive(Clone, Debug)]
struct Registration {
    result_oid: pg_sys::Oid,
    creator_oid: pg_sys::Oid,
    activation_lsn: String,
    slot_generation: i64,
    source_streams: BTreeMap<u32, i64>,
    stage_streams: Vec<Option<i64>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PayloadField {
    slot_id: Option<u32>,
    attnum: Option<i16>,
    name: String,
    type_oid: u32,
    typmod: i32,
    collation_oid: u32,
    nullable: bool,
}

#[pg_extern(
    name = "_lock_dataflow_sources",
    security_definer,
    volatile,
    requires = ["shiba_catalog"]
)]
#[search_path(pg_catalog, shiba_internal)]
pub(crate) fn lock_dataflow_sources(mut source_oids: Vec<pg_sys::Oid>) {
    source_oids.sort_unstable_by_key(|oid| oid.to_u32());
    if source_oids.is_empty()
        || source_oids.contains(&pg_sys::InvalidOid)
        || source_oids.windows(2).any(|pair| pair[0] == pair[1])
    {
        error!("Shiba received invalid or duplicate source OIDs");
    }
    Spi::connect_mut(|client| {
        for source_oid in source_oids {
            validate_source(client, source_oid)?;
            let relation = resolve_qualified_relation(client, source_oid, "source table")?;
            client
                .update(
                    &format!("LOCK TABLE {relation} IN SHARE ROW EXCLUSIVE MODE"),
                    None,
                    &[],
                )
                .map_err(|error| {
                    format!(
                        "could not lock source {} for activation: {error}",
                        source_oid.to_u32()
                    )
                })?;
        }
        Ok::<(), String>(())
    })
    .unwrap_or_else(|error| error!("Shiba could not lock dataflow sources: {error}"));
}

#[pg_extern(
    schema = "shiba_internal",
    name = "create_effect_stream_payload",
    security_definer,
    volatile,
    requires = ["shiba_catalog"]
)]
#[search_path(pg_catalog, shiba_internal)]
pub(crate) fn create_effect_stream_payload(stream_id: i64, schema: JsonB) {
    let fields = decode_payload_fields(schema.0)
        .unwrap_or_else(|error| error!("invalid effect stream payload schema: {error}"));
    Spi::connect_mut(|client| create_payload(client, stream_id, &fields))
        .unwrap_or_else(|error| error!("could not create effect stream payload: {error}"));
}

#[pg_extern(
    schema = "shiba_internal",
    name = "validate_effect_stream_payload",
    security_definer,
    volatile,
    requires = ["shiba_catalog"]
)]
#[search_path(pg_catalog, shiba_internal)]
pub(crate) fn validate_effect_stream_payload(stream_id: i64, schema: JsonB) {
    let fields = decode_payload_fields(schema.0)
        .unwrap_or_else(|error| error!("invalid effect stream payload schema: {error}"));
    Spi::connect_mut(|client| validate_payload(client, stream_id, &fields))
        .unwrap_or_else(|error| error!("effect stream payload validation failed: {error}"));
}

#[pg_extern(
    schema = "shiba_internal",
    name = "prepare_dataflow_source",
    security_definer,
    volatile,
    requires = ["shiba_catalog"]
)]
#[search_path(pg_catalog, shiba_internal)]
pub(crate) fn prepare_dataflow_source(source_oid: pg_sys::Oid) {
    Spi::connect_mut(|client| prepare_source(client, source_oid))
        .unwrap_or_else(|error| error!("could not prepare dataflow source: {error}"));
}

/// The only dataflow registration entry point.
///
/// The utility hook has already lowered and validated the analyzed PostgreSQL
/// `Query`. This SECURITY DEFINER boundary reparses the exact serialized plan
/// that is persisted in the catalog, validates it again, and provisions all
/// durable storage in the surrounding CTAS transaction.
#[pg_extern(
    name = "_register_dataflow",
    security_definer,
    volatile,
    requires = ["shiba_catalog"]
)]
#[search_path(pg_catalog, shiba_internal)]
pub(crate) fn register_dataflow(result_oid: pg_sys::Oid, serialized_plan: &str) {
    let plan: DataflowPlan = serde_json::from_str(serialized_plan)
        .unwrap_or_else(|error| error!("Shiba received an invalid dataflow plan: {error}"));
    plan.validate()
        .unwrap_or_else(|error| error!("Shiba rejected its persisted dataflow plan: {error}"));
    Spi::connect_mut(|client| register(client, result_oid, serialized_plan, &plan))
        .unwrap_or_else(|error| error!("Shiba failed to register the dataflow: {error}"));
}

fn register(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    serialized_plan: &str,
    plan: &DataflowPlan,
) -> Result<(), String> {
    if result_oid == pg_sys::InvalidOid {
        return Err("result OID is invalid".into());
    }
    let result = resolve_new_result(client, result_oid)?;
    validate_slot_types(client, plan)?;
    validate_scalar_catalog(plan)?;
    let source_oids = source_oids(plan)?;
    let creator_oid = crate::index_management::invoker_oid();
    let slot_generation = active_slot_generation(client)?;
    let activation_lsn = required_scalar::<String>(
        client,
        "SELECT pg_catalog.pg_current_wal_lsn()::text",
        &[],
        "registration LSN",
    )?;

    // Transfer ownership before cataloging the result. The lifecycle event
    // trigger intentionally rejects ALTER TABLE on a live Shiba result, so
    // this one-time provisioning step must happen before the dataflow row is
    // visible to that guard.
    transfer_result_ownership(client, &result)?;
    insert_dataflow(client, result_oid, serialized_plan, &activation_lsn)?;
    insert_checkpoints(client, result_oid, plan.stages.len())?;

    let mut registration = Registration {
        result_oid,
        creator_oid,
        activation_lsn,
        slot_generation,
        source_streams: BTreeMap::new(),
        stage_streams: vec![None; plan.stages.len()],
    };
    provision_sources(client, &mut registration, &source_oids)?;
    provision_output_streams(client, &mut registration, plan)?;
    attach_inputs(client, &registration, plan)?;
    provision_operator_storage(client, &registration, plan)?;
    protect_result(client, &registration, &result)?;

    client
        .update("SELECT shiba._ensure_runtime()", Some(1), &[])
        .map_err(|error| format!("could not request the Runtime: {error}"))?;
    Ok(())
}

fn validate_scalar_catalog(plan: &DataflowPlan) -> Result<(), String> {
    for (stage_id, stage) in plan.stages.iter().enumerate() {
        for expression in stage.spec.expressions() {
            crate::planner::scalar_sql::validate_scalar_catalog(expression).map_err(|error| {
                format!("stage {stage_id} has an untrusted scalar catalog reference: {error}")
            })?;
        }
    }
    Ok(())
}

fn resolve_new_result(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
) -> Result<ResultIdentity, String> {
    let arguments = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    let table = client
        .select(
            r#"
            SELECT pg_catalog.format('%I.%I', namespace.nspname, relation.relname)
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE relation.oid = $1
              AND relation.relkind = 'r'
              AND relation.relpersistence = 'p'
              AND namespace.nspname = 'shiba'
              AND NOT EXISTS (
                SELECT 1
                FROM shiba_internal.dataflows AS dataflow
                WHERE dataflow.result_oid = relation.oid
              )
            "#,
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not resolve the new result table: {error}"))?;
    if table.len() != 1 {
        return Err("result is not one new permanent table in schema shiba".into());
    }
    Ok(ResultIdentity {
        qualified: required_table(&table.first(), 1, "qualified result name")?,
    })
}

fn validate_slot_types(client: &mut SpiClient<'_>, plan: &DataflowPlan) -> Result<(), String> {
    let mut unique = BTreeSet::new();
    for stage in &plan.stages {
        unique.extend(stage.schema.inputs.iter().map(|slot| {
            (
                slot.type_.type_oid,
                slot.type_.typmod,
                slot.type_.collation_oid,
            )
        }));
        unique.extend(stage.schema.outputs.iter().map(|slot| {
            (
                slot.type_.type_oid,
                slot.type_.typmod,
                slot.type_.collation_oid,
            )
        }));
    }
    for (type_oid, typmod, collation_oid) in unique {
        let type_oid = oid(type_oid, "slot type")?;
        let collation_oid = oid_allow_invalid(collation_oid);
        let arguments = unsafe {
            [
                DatumWithOid::new(type_oid, pg_sys::OIDOID),
                DatumWithOid::new(typmod, pg_sys::INT4OID),
                DatumWithOid::new(collation_oid, pg_sys::OIDOID),
            ]
        };
        let valid = required_scalar::<bool>(
            client,
            r#"
            SELECT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_type AS type_catalog
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = type_catalog.typnamespace
              WHERE type_catalog.oid = $1
                AND type_catalog.typtype <> 'p'
                AND namespace.nspname = 'pg_catalog'
                AND pg_catalog.format_type(type_catalog.oid, $2) IS NOT NULL
                AND (
                  $3 IS NULL
                  OR (
                    type_catalog.typcollation <> 0::oid
                    AND EXISTS (
                      SELECT 1
                      FROM pg_catalog.pg_collation AS collation_catalog
                      JOIN pg_catalog.pg_namespace AS collation_namespace
                        ON collation_namespace.oid =
                           collation_catalog.collnamespace
                      WHERE collation_catalog.oid = $3
                        AND collation_namespace.nspname = 'pg_catalog'
                    )
                  )
                )
            )
            "#,
            &arguments,
            "slot type validation",
        )?;
        if !valid {
            return Err(format!(
                "slot type {}/{} with collation {} is not a live pg_catalog type",
                type_oid.to_u32(),
                typmod,
                collation_oid.to_u32()
            ));
        }
    }
    Ok(())
}

fn source_oids(plan: &DataflowPlan) -> Result<Vec<u32>, String> {
    let sources = plan
        .stages
        .iter()
        .filter_map(|stage| match &stage.spec {
            OperatorSpec::Scan(scan) => Some(scan.source_oid),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    if sources.is_empty() || sources.contains(&0) {
        return Err("dataflow has no valid Scan source".into());
    }
    Ok(sources.into_iter().collect())
}

fn active_slot_generation(client: &mut SpiClient<'_>) -> Result<i64, String> {
    required_update_scalar(
        client,
        r#"
        SELECT replay.slot_generation
        FROM shiba_internal.ingress_replay_state AS replay
        WHERE replay.database_oid = (
          SELECT database.oid
          FROM pg_catalog.pg_database AS database
          WHERE database.datname = pg_catalog.current_database()
        )
          AND replay.slot_name = shiba_internal.slot_name()
          AND replay.state = 'active'
        FOR SHARE
        "#,
        &[],
        "active replication-slot generation",
    )
}

fn insert_dataflow(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    serialized_plan: &str,
    activation_lsn: &str,
) -> Result<(), String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(serialized_plan, pg_sys::TEXTOID),
            DatumWithOid::new(activation_lsn, pg_sys::TEXTOID),
        ]
    };
    require_one(
        client
            .update(
                r#"
                INSERT INTO shiba_internal.dataflows(
                  result_oid, plan, activation_lsn
                )
                VALUES($1, $2::jsonb, $3::pg_lsn)
                RETURNING result_oid
                "#,
                Some(1),
                &arguments,
            )
            .map_err(|error| format!("could not insert the dataflow: {error}"))?,
        "dataflow insertion",
    )
}

fn insert_checkpoints(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_count: usize,
) -> Result<(), String> {
    if stage_count < 2 || stage_count > i32::MAX as usize {
        return Err("dataflow stage count is outside PostgreSQL integer".into());
    }
    let arguments = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(stage_count as i32, pg_sys::INT4OID),
        ]
    };
    let rows = client
        .update(
            r#"
            INSERT INTO shiba_internal.operator_checkpoints(result_oid,stage_id)
            SELECT $1, ordinal - 1
            FROM pg_catalog.generate_series(1,$2) AS stage(ordinal)
            RETURNING stage_id
            "#,
            None,
            &arguments,
        )
        .map_err(|error| format!("could not insert operator checkpoints: {error}"))?
        .len();
    if rows != stage_count {
        return Err(format!(
            "checkpoint insertion returned {rows} rows, expected {stage_count}"
        ));
    }
    Ok(())
}

fn provision_sources(
    client: &mut SpiClient<'_>,
    registration: &mut Registration,
    source_oids: &[u32],
) -> Result<(), String> {
    let target_rows = i64_from_usize(config::batch_rows(), "ingress row target")?;
    let target_bytes = i64_from_usize(config::batch_bytes(), "ingress byte target")?;
    for &source_oid in source_oids {
        let source = oid(source_oid, "source")?;
        validate_source(client, source)?;
        let stream_id = ensure_source_stream(
            client,
            registration.slot_generation,
            source,
            target_rows,
            target_bytes,
        )?;
        ensure_source_payload(client, stream_id, source)?;
        prepare_source(client, source)?;
        registration.source_streams.insert(source_oid, stream_id);
    }

    for &source_oid in source_oids {
        let arguments = unsafe {
            [
                DatumWithOid::new(registration.result_oid, pg_sys::OIDOID),
                DatumWithOid::new(oid(source_oid, "source")?, pg_sys::OIDOID),
            ]
        };
        require_one(
            client
                .update(
                    r#"
                    INSERT INTO shiba_internal.dataflow_sources(result_oid,source_oid)
                    VALUES($1,$2)
                    RETURNING source_oid
                    "#,
                    Some(1),
                    &arguments,
                )
                .map_err(|error| format!("could not catalog source {source_oid}: {error}"))?,
            "dataflow source insertion",
        )?;
    }
    Ok(())
}

fn validate_source(client: &mut SpiClient<'_>, source_oid: pg_sys::Oid) -> Result<(), String> {
    let arguments = unsafe { [DatumWithOid::new(source_oid, pg_sys::OIDOID)] };
    client
        .update(
            "SELECT shiba._validate_source_table($1)",
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("source table validation failed: {error}"))?;
    let valid = required_scalar::<bool>(
        client,
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM pg_catalog.pg_attribute AS attribute
          WHERE attribute.attrelid = $1
            AND attribute.attnum > 0
            AND NOT attribute.attisdropped
        )
        AND NOT EXISTS (
          SELECT 1
          FROM pg_catalog.pg_attribute AS attribute
          WHERE attribute.attrelid = $1
            AND attribute.attnum > 0
            AND NOT attribute.attisdropped
            AND attribute.attgenerated <> ''
        )
        "#,
        &arguments,
        "source column validation",
    )?;
    if !valid {
        return Err(format!(
            "source {} has no columns or contains a generated column",
            source_oid.to_u32()
        ));
    }
    Ok(())
}

fn decode_payload_fields(value: serde_json::Value) -> Result<Vec<PayloadField>, String> {
    let values = value
        .as_array()
        .ok_or_else(|| "schema is not an array".to_string())?;
    if values.is_empty() {
        return Err("schema has no fields".into());
    }
    let required = [
        "slot_id",
        "attnum",
        "name",
        "type_oid",
        "typmod",
        "collation_oid",
        "nullable",
    ];
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let object = value
                .as_object()
                .ok_or_else(|| format!("field {index} is not an object"))?;
            if object.len() != required.len()
                || required.iter().any(|name| !object.contains_key(*name))
            {
                return Err(format!("field {index} does not have the exact schema"));
            }
            serde_json::from_value::<PayloadField>(value.clone())
                .map_err(|error| format!("field {index} is invalid: {error}"))
        })
        .collect()
}

fn validate_payload_fields(
    client: &mut SpiClient<'_>,
    stream_id: i64,
    fields: &[PayloadField],
    lock: bool,
) -> Result<String, String> {
    if stream_id <= 0 || fields.is_empty() {
        return Err("payload storage identity is invalid".into());
    }
    let arguments = unsafe { [DatumWithOid::new(stream_id, pg_sys::INT8OID)] };
    let stream_kind = required_update_scalar::<String>(
        client,
        if lock {
            "SELECT producer_kind
             FROM shiba_internal.effect_streams
             WHERE stream_id=$1
             FOR UPDATE"
        } else {
            "SELECT producer_kind
             FROM shiba_internal.effect_streams
             WHERE stream_id=$1
             FOR SHARE"
        },
        &arguments,
        "effect stream producer kind",
    )?;
    let max_identifier = required_scalar::<i32>(
        client,
        "SELECT pg_catalog.current_setting('max_identifier_length')::integer",
        &[],
        "maximum identifier length",
    )?;
    let mut names = BTreeSet::new();
    let mut slots = BTreeSet::new();
    let mut attnums = BTreeSet::new();
    for field in fields {
        if field.name.is_empty()
            || field.name.len()
                > usize::try_from(max_identifier)
                    .map_err(|_| "maximum identifier length is negative")?
            || !names.insert(field.name.clone())
        {
            return Err("payload field has an invalid or duplicate name".into());
        }
        match stream_kind.as_str() {
            "source"
                if field.slot_id.is_none()
                    && field.attnum.is_some_and(|attnum| attnum > 0)
                    && attnums.insert(field.attnum.expect("checked")) => {}
            "operator"
                if field.attnum.is_none()
                    && field.slot_id.is_some()
                    && slots.insert(field.slot_id.expect("checked")) => {}
            "source" | "operator" => {
                return Err(format!(
                    "payload field {} does not match {stream_kind} identity",
                    field.name
                ));
            }
            _ => {
                return Err(format!(
                    "unknown effect stream producer kind {stream_kind:?}"
                ));
            }
        }
        let _ = field.nullable;
        column_sql(
            client,
            &SlotType {
                type_oid: field.type_oid,
                typmod: field.typmod,
                collation_oid: field.collation_oid,
                nullable: field.nullable,
            },
        )?;
    }
    Ok(stream_kind)
}

fn create_payload(
    client: &mut SpiClient<'_>,
    stream_id: i64,
    fields: &[PayloadField],
) -> Result<(), String> {
    validate_payload_fields(client, stream_id, fields, true)?;
    let arguments = unsafe { [DatumWithOid::new(stream_id, pg_sys::INT8OID)] };
    let absent = required_scalar::<bool>(
        client,
        r#"
        SELECT NOT EXISTS (
          SELECT 1
          FROM shiba_internal.effect_streams
          WHERE stream_id=$1 AND relation_oid IS NOT NULL AND row_type_oid IS NOT NULL
        )
        "#,
        &arguments,
        "payload storage absence",
    )?;
    if !absent {
        return Err(format!(
            "effect stream {stream_id} already has payload storage"
        ));
    }

    let row_type_name = format!("effect_row_s{stream_id}");
    let payload_name = format!("effect_payload_s{stream_id}");
    let row_type = qualified_internal(&row_type_name);
    let payload = qualified_internal(&payload_name);
    let definitions = fields
        .iter()
        .map(|field| {
            Ok(format!(
                "{} {}",
                quote_identifier(&field.name),
                column_sql(
                    client,
                    &SlotType {
                        type_oid: field.type_oid,
                        typmod: field.typmod,
                        collation_oid: field.collation_oid,
                        nullable: field.nullable,
                    },
                )?
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    client
        .update(
            &format!("CREATE TYPE {row_type} AS ({})", definitions.join(",")),
            None,
            &[],
        )
        .map_err(|error| format!("could not create payload row type: {error}"))?;
    client
        .update(
            &format!(
                "CREATE TABLE {payload}(
                   stream_id bigint NOT NULL CHECK(stream_id={stream_id}),
                   chunk_seq bigint NOT NULL CHECK(chunk_seq > 0),
                   row_ordinal bigint NOT NULL CHECK(row_ordinal >= 0),
                   weight bigint NOT NULL CHECK(weight <> 0),
                   row_value {row_type} NOT NULL,
                   PRIMARY KEY(stream_id,chunk_seq,row_ordinal),
                   FOREIGN KEY(stream_id,chunk_seq)
                     REFERENCES shiba_internal.effect_stream_chunks(
                       stream_id,chunk_seq
                     ) ON DELETE CASCADE
                       DEFERRABLE INITIALLY DEFERRED
                 )"
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create payload relation: {error}"))?;
    client
        .update(
            &format!("REVOKE ALL ON TABLE {payload} FROM PUBLIC"),
            None,
            &[],
        )
        .map_err(|error| format!("could not protect payload relation: {error}"))?;
    let relation_oid = resolve_relation_oid(client, &payload)?;
    let row_type_oid = resolve_type_oid(client, &row_type)?;
    let catalog_arguments = unsafe {
        [
            DatumWithOid::new(stream_id, pg_sys::INT8OID),
            DatumWithOid::new(relation_oid, pg_sys::OIDOID),
            DatumWithOid::new(row_type_oid, pg_sys::OIDOID),
        ]
    };
    require_one(
        client
            .update(
                r#"
                UPDATE shiba_internal.effect_streams
                SET relation_oid=$2,row_type_oid=$3
                WHERE stream_id=$1
                  AND relation_oid IS NULL
                  AND row_type_oid IS NULL
                RETURNING stream_id
                "#,
                Some(1),
                &catalog_arguments,
            )
            .map_err(|error| format!("could not catalog payload storage: {error}"))?,
        "payload catalog insertion",
    )
}

fn validate_payload(
    client: &mut SpiClient<'_>,
    stream_id: i64,
    fields: &[PayloadField],
) -> Result<(), String> {
    validate_payload_fields(client, stream_id, fields, false)?;
    let payload = storage::payload(client, stream_id)?;
    let actual = storage::composite_attributes(client, &payload.row_type)?;
    if actual.len() != fields.len() {
        return Err(format!(
            "effect stream {stream_id} payload field count changed"
        ));
    }
    for (actual, expected) in actual.iter().zip(fields) {
        if actual.name != expected.name
            || actual.type_oid.to_u32() != expected.type_oid
            || actual.typmod != expected.typmod
            || actual.collation_oid.to_u32() != expected.collation_oid
        {
            return Err(format!(
                "effect stream {stream_id} payload field {} changed identity",
                expected.name
            ));
        }
    }
    Ok(())
}

fn prepare_source(client: &mut SpiClient<'_>, source_oid: pg_sys::Oid) -> Result<(), String> {
    validate_source(client, source_oid)?;
    let arguments = unsafe { [DatumWithOid::new(source_oid, pg_sys::OIDOID)] };
    client
        .update(
            "SELECT shiba._ensure_replica_identity_full($1)",
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not set source replica identity: {error}"))?;
    let relation = resolve_qualified_relation(client, source_oid, "source table")?;
    let published = required_scalar::<bool>(
        client,
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM pg_catalog.pg_publication AS publication
          JOIN pg_catalog.pg_publication_rel AS member
            ON member.prpubid=publication.oid
          WHERE publication.pubname='shiba_publication'
            AND member.prrelid=$1
        )
        "#,
        &arguments,
        "source publication membership",
    )?;
    if !published {
        client
            .update(
                &format!("ALTER PUBLICATION shiba_publication ADD TABLE {relation}"),
                None,
                &[],
            )
            .map_err(|error| format!("could not add source to publication: {error}"))?;
    }
    for (trigger, event, function) in [
        (
            "shiba_wakeup",
            "AFTER INSERT OR UPDATE OR DELETE",
            "shiba._request_runtime()",
        ),
        (
            "shiba_no_truncate",
            "BEFORE TRUNCATE",
            "shiba._reject_source_truncate()",
        ),
    ] {
        let trigger_arguments = unsafe {
            [
                DatumWithOid::new(source_oid, pg_sys::OIDOID),
                DatumWithOid::new(trigger, pg_sys::TEXTOID),
            ]
        };
        let exists = required_scalar::<bool>(
            client,
            r#"
            SELECT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_trigger
              WHERE tgrelid=$1
                AND tgname=$2
                AND NOT tgisinternal
            )
            "#,
            &trigger_arguments,
            "source trigger presence",
        )?;
        if !exists {
            client
                .update(
                    &format!(
                        "CREATE TRIGGER {} {event} ON {relation}
                         FOR EACH STATEMENT EXECUTE FUNCTION {function}",
                        quote_identifier(trigger),
                    ),
                    None,
                    &[],
                )
                .map_err(|error| format!("could not create source trigger {trigger}: {error}"))?;
        }
    }
    Ok(())
}

fn ensure_source_stream(
    client: &mut SpiClient<'_>,
    slot_generation: i64,
    source_oid: pg_sys::Oid,
    target_rows: i64,
    target_bytes: i64,
) -> Result<i64, String> {
    let high_rows = target_rows
        .checked_mul(8)
        .ok_or_else(|| "source high row watermark exceeds bigint".to_string())?;
    let high_bytes = target_bytes
        .checked_mul(8)
        .ok_or_else(|| "source high byte watermark exceeds bigint".to_string())?;
    let low_rows = target_rows
        .checked_mul(2)
        .ok_or_else(|| "source low row watermark exceeds bigint".to_string())?;
    let low_bytes = target_bytes
        .checked_mul(2)
        .ok_or_else(|| "source low byte watermark exceeds bigint".to_string())?;
    let arguments = unsafe {
        [
            DatumWithOid::new(slot_generation, pg_sys::INT8OID),
            DatumWithOid::new(source_oid, pg_sys::OIDOID),
            DatumWithOid::new(target_rows, pg_sys::INT8OID),
            DatumWithOid::new(target_bytes, pg_sys::INT8OID),
            DatumWithOid::new(high_rows, pg_sys::INT8OID),
            DatumWithOid::new(high_bytes, pg_sys::INT8OID),
            DatumWithOid::new(low_rows, pg_sys::INT8OID),
            DatumWithOid::new(low_bytes, pg_sys::INT8OID),
        ]
    };
    required_update_scalar(
        client,
        r#"
        WITH proposed AS (
          INSERT INTO shiba_internal.effect_streams(
            producer_kind,slot_generation,source_oid,
            target_chunk_rows,target_chunk_bytes,
            high_chunks,high_rows,high_bytes,
            low_chunks,low_rows,low_bytes
          )
          VALUES('source',$1,$2,$3,$4,8,$5,$6,2,$7,$8)
          ON CONFLICT DO NOTHING
          RETURNING stream_id
        )
        SELECT stream_id FROM proposed
        UNION ALL
        SELECT existing.stream_id
        FROM shiba_internal.effect_streams AS existing
        WHERE existing.producer_kind = 'source'
          AND existing.slot_generation = $1
          AND existing.source_oid = $2
          AND NOT EXISTS (SELECT 1 FROM proposed)
        LIMIT 1
        "#,
        &arguments,
        "source stream ID",
    )
}

fn ensure_source_payload(
    client: &mut SpiClient<'_>,
    stream_id: i64,
    source_oid: pg_sys::Oid,
) -> Result<(), String> {
    let fields = storage::relation_attributes(client, source_oid)?
        .into_iter()
        .map(|attribute| PayloadField {
            slot_id: None,
            attnum: Some(attribute.number),
            name: attribute.name,
            type_oid: attribute.type_oid.to_u32(),
            typmod: attribute.typmod,
            collation_oid: attribute.collation_oid.to_u32(),
            nullable: !attribute.not_null,
        })
        .collect::<Vec<_>>();
    let arguments = unsafe { [DatumWithOid::new(stream_id, pg_sys::INT8OID)] };
    let has_payload = required_scalar::<bool>(
        client,
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM shiba_internal.effect_streams
          WHERE stream_id = $1
            AND relation_oid IS NOT NULL
            AND row_type_oid IS NOT NULL
        )
        "#,
        &arguments,
        "source payload presence",
    )?;
    if has_payload {
        validate_payload(client, stream_id, &fields)
    } else {
        create_payload(client, stream_id, &fields)
    }
}

fn provision_output_streams(
    client: &mut SpiClient<'_>,
    registration: &mut Registration,
    plan: &DataflowPlan,
) -> Result<(), String> {
    let target_rows = i64_from_usize(config::batch_rows(), "operator row target")?;
    let target_bytes = i64_from_usize(config::batch_bytes(), "operator byte target")?;
    for (stage_index, stage) in plan.stages.iter().enumerate() {
        if matches!(stage.spec, OperatorSpec::Sink) {
            continue;
        }
        let stage_id =
            i32::try_from(stage_index).map_err(|_| "operator stage ID exceeds integer")?;
        let stream_id = insert_operator_stream(
            client,
            registration.result_oid,
            stage_id,
            target_rows,
            target_bytes,
        )?;
        create_operator_payload(client, stream_id, &stage.schema.outputs)?;
        registration.stage_streams[stage_index] = Some(stream_id);
    }
    Ok(())
}

fn insert_operator_stream(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    target_rows: i64,
    target_bytes: i64,
) -> Result<i64, String> {
    let high_rows = target_rows
        .checked_mul(8)
        .ok_or_else(|| "operator high row watermark exceeds bigint".to_string())?;
    let high_bytes = target_bytes
        .checked_mul(8)
        .ok_or_else(|| "operator high byte watermark exceeds bigint".to_string())?;
    let low_rows = target_rows
        .checked_mul(2)
        .ok_or_else(|| "operator low row watermark exceeds bigint".to_string())?;
    let low_bytes = target_bytes
        .checked_mul(2)
        .ok_or_else(|| "operator low byte watermark exceeds bigint".to_string())?;
    let arguments = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(stage_id, pg_sys::INT4OID),
            DatumWithOid::new(target_rows, pg_sys::INT8OID),
            DatumWithOid::new(target_bytes, pg_sys::INT8OID),
            DatumWithOid::new(high_rows, pg_sys::INT8OID),
            DatumWithOid::new(high_bytes, pg_sys::INT8OID),
            DatumWithOid::new(low_rows, pg_sys::INT8OID),
            DatumWithOid::new(low_bytes, pg_sys::INT8OID),
        ]
    };
    required_update_scalar(
        client,
        r#"
        INSERT INTO shiba_internal.effect_streams(
          producer_kind,producer_result_oid,producer_stage_id,
          target_chunk_rows,target_chunk_bytes,
          high_chunks,high_rows,high_bytes,
          low_chunks,low_rows,low_bytes
        )
        VALUES('operator',$1,$2,$3,$4,8,$5,$6,2,$7,$8)
        RETURNING stream_id
        "#,
        &arguments,
        "operator stream ID",
    )
}

fn create_operator_payload(
    client: &mut SpiClient<'_>,
    stream_id: i64,
    outputs: &[OutputSlot],
) -> Result<(), String> {
    if outputs.is_empty() {
        return Err("non-Sink stage has no output schema".into());
    }
    let fields = outputs
        .iter()
        .map(|output| PayloadField {
            slot_id: Some(output.slot.0),
            attnum: None,
            name: format!("slot_{}", output.slot.0),
            type_oid: output.type_.type_oid,
            typmod: output.type_.typmod,
            collation_oid: output.type_.collation_oid,
            nullable: output.type_.nullable,
        })
        .collect::<Vec<_>>();
    create_payload(client, stream_id, &fields)
}

fn attach_inputs(
    client: &mut SpiClient<'_>,
    registration: &Registration,
    plan: &DataflowPlan,
) -> Result<(), String> {
    for (stage_index, stage) in plan.stages.iter().enumerate() {
        let stage_id =
            i32::try_from(stage_index).map_err(|_| "operator stage ID exceeds integer")?;
        match &stage.spec {
            OperatorSpec::Scan(scan) => {
                let stream_id = registration
                    .source_streams
                    .get(&scan.source_oid)
                    .copied()
                    .ok_or_else(|| "Scan source stream was not provisioned".to_string())?;
                attach_input(client, registration, stage_id, 0, stream_id)?;
            }
            _ => {
                for (port, input) in stage.inputs.iter().enumerate() {
                    let upstream = usize::try_from(input.upstream_stage_id)
                        .map_err(|_| "upstream stage ID exceeds usize")?;
                    if upstream >= stage_index {
                        return Err(format!("stage {stage_id} input {port} is not upstream"));
                    }
                    let stream_id = registration
                        .stage_streams
                        .get(upstream)
                        .copied()
                        .flatten()
                        .ok_or_else(|| {
                            format!("stage {stage_id} input {port} has no producer stream")
                        })?;
                    attach_input(
                        client,
                        registration,
                        stage_id,
                        i32::try_from(port).map_err(|_| "input port exceeds integer")?,
                        stream_id,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn attach_input(
    client: &mut SpiClient<'_>,
    registration: &Registration,
    stage_id: i32,
    port: i32,
    stream_id: i64,
) -> Result<(), String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(stream_id, pg_sys::INT8OID),
            DatumWithOid::new(registration.result_oid, pg_sys::OIDOID),
            DatumWithOid::new(stage_id, pg_sys::INT4OID),
            DatumWithOid::new(port, pg_sys::INT4OID),
            DatumWithOid::new(registration.activation_lsn.as_str(), pg_sys::TEXTOID),
        ]
    };
    client
        .update(
            r#"
            SELECT shiba_internal.attach_effect_stream_consumer(
              $1,$2,$3,$4,$5::pg_lsn
            )
            "#,
            Some(1),
            &arguments,
        )
        .map_err(|error| format!("could not attach stage {stage_id} input {port}: {error}"))?;
    Ok(())
}

fn provision_operator_storage(
    client: &mut SpiClient<'_>,
    registration: &Registration,
    plan: &DataflowPlan,
) -> Result<(), String> {
    for (stage_index, stage) in plan.stages.iter().enumerate() {
        let stage_id =
            i32::try_from(stage_index).map_err(|_| "operator stage ID exceeds integer")?;
        let inputs = input_streams(client, registration.result_oid, stage_id)?;
        match &stage.spec {
            OperatorSpec::Scan(scan) => {
                provision_scan(client, registration, stage_id, stage, scan, &inputs)?
            }
            OperatorSpec::Filter(_) => {
                provision_linear_continuation(
                    client,
                    registration.result_oid,
                    stage_id,
                    &inputs,
                    LinearContinuation::Transform,
                )?;
            }
            OperatorSpec::Project(_) => {
                provision_linear_continuation(
                    client,
                    registration.result_oid,
                    stage_id,
                    &inputs,
                    LinearContinuation::Transform,
                )?;
            }
            OperatorSpec::Sink => {
                provision_linear_continuation(
                    client,
                    registration.result_oid,
                    stage_id,
                    &inputs,
                    LinearContinuation::Sink,
                )?;
            }
            OperatorSpec::Distinct(_) => super::distinct::provision(
                client,
                registration.result_oid,
                stage_id,
                stage,
                &inputs,
                stage_output_stream(registration, stage_id, "Distinct")?,
            )?,
            OperatorSpec::Join(_) => {
                super::join::provision(
                    client,
                    registration.result_oid,
                    stage_id,
                    stage,
                    &inputs,
                    stage_output_stream(registration, stage_id, "Join")?,
                )?;
            }
            OperatorSpec::Aggregate(_) => {
                super::aggregate::provision(
                    client,
                    registration.result_oid,
                    stage_id,
                    stage,
                    &inputs,
                    stage_output_stream(registration, stage_id, "Aggregate")?,
                )?;
            }
            OperatorSpec::Window(_) => super::window::provision(
                client,
                registration.result_oid,
                stage_id,
                stage,
                &inputs,
                stage_output_stream(registration, stage_id, "Window")?,
            )?,
            OperatorSpec::TopN(_) => super::topn::provision(
                client,
                registration.result_oid,
                stage_id,
                stage,
                &inputs,
                stage_output_stream(registration, stage_id, "TopN")?,
            )?,
        }
    }
    Ok(())
}

fn stage_output_stream(
    registration: &Registration,
    stage_id: i32,
    operator: &str,
) -> Result<i64, String> {
    registration
        .stage_streams
        .get(usize::try_from(stage_id).map_err(|_| "negative operator stage ID")?)
        .copied()
        .flatten()
        .ok_or_else(|| format!("{operator} stage {stage_id} has no output stream"))
}

fn input_streams(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
) -> Result<Vec<i64>, String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(stage_id, pg_sys::INT4OID),
        ]
    };
    let table = client
        .select(
            r#"
            SELECT consumer.stream_id
            FROM shiba_internal.effect_stream_consumers AS consumer
            WHERE consumer.result_oid = $1
              AND consumer.consumer_stage_id = $2
            ORDER BY consumer.input_port
            "#,
            None,
            &arguments,
        )
        .map_err(|error| format!("could not resolve operator inputs: {error}"))?;
    table
        .into_iter()
        .map(|row| required_row(&row, 1, "operator input stream"))
        .collect()
}

#[derive(Clone, Copy)]
enum LinearContinuation {
    Scan,
    Transform,
    Sink,
}

fn provision_linear_continuation(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    input_streams: &[i64],
    kind: LinearContinuation,
) -> Result<pg_sys::Oid, String> {
    if input_streams.len() != 1 {
        return Err(format!(
            "linear stage {stage_id} has {} durable inputs",
            input_streams.len()
        ));
    }
    let name = format!("continuation_r{}_s{stage_id}", result_oid.to_u32());
    let relation = qualified_internal(&name);
    let body = match kind {
        LinearContinuation::Scan => {
            r#"
              singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
              phase smallint NOT NULL CHECK(phase BETWEEN 1 AND 4),
              input_stream_id bigint NOT NULL,
              input_chunk_seq bigint,
              next_row_ordinal bigint,
              next_bootstrap_seq bigint,
              pending_frontier_lsn pg_lsn,
              CHECK(
                (
                  phase = 1
                  AND input_chunk_seq IS NULL
                  AND next_row_ordinal IS NULL
                  AND next_bootstrap_seq IS NOT NULL
                  AND next_bootstrap_seq > 0
                  AND pending_frontier_lsn IS NULL
                )
                OR (
                  phase = 2
                  AND input_chunk_seq IS NULL
                  AND next_row_ordinal IS NULL
                  AND next_bootstrap_seq IS NULL
                  AND pending_frontier_lsn IS NOT NULL
                )
                OR (
                  phase = 3
                  AND input_chunk_seq IS NOT NULL
                  AND input_chunk_seq > 0
                  AND next_row_ordinal IS NOT NULL
                  AND next_row_ordinal >= 0
                  AND next_bootstrap_seq IS NULL
                  AND pending_frontier_lsn IS NULL
                )
                OR (
                  phase = 4
                  AND input_chunk_seq IS NULL
                  AND next_row_ordinal IS NULL
                  AND next_bootstrap_seq IS NULL
                  AND pending_frontier_lsn IS NOT NULL
                )
              ),
              FOREIGN KEY(input_stream_id,input_chunk_seq)
                REFERENCES shiba_internal.effect_stream_chunks(
                  stream_id,chunk_seq
                ) ON DELETE RESTRICT
            "#
        }
        LinearContinuation::Transform => {
            r#"
              singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
              input_stream_id bigint NOT NULL,
              input_chunk_seq bigint NOT NULL CHECK(input_chunk_seq > 0),
              next_row_ordinal bigint NOT NULL CHECK(next_row_ordinal >= 0),
              FOREIGN KEY(input_stream_id,input_chunk_seq)
                REFERENCES shiba_internal.effect_stream_chunks(
                  stream_id,chunk_seq
                ) ON DELETE RESTRICT
            "#
        }
        LinearContinuation::Sink => {
            r#"
              singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
              input_stream_id bigint NOT NULL,
              input_chunk_seq bigint NOT NULL CHECK(input_chunk_seq > 0),
              row_ordinal bigint NOT NULL CHECK(row_ordinal >= 0),
              remaining_weight bigint CHECK(remaining_weight <> 0),
              FOREIGN KEY(input_stream_id,input_chunk_seq)
                REFERENCES shiba_internal.effect_stream_chunks(
                  stream_id,chunk_seq
                ) ON DELETE RESTRICT
            "#
        }
    };
    client
        .update(&format!("CREATE TABLE {relation}({body})"), None, &[])
        .map_err(|error| format!("could not create stage {stage_id} continuation: {error}"))?;
    client
        .update(
            &format!("REVOKE ALL ON TABLE {relation} FROM PUBLIC"),
            None,
            &[],
        )
        .map_err(|error| format!("could not protect stage {stage_id} continuation: {error}"))?;
    let relation_oid = resolve_relation_oid(client, &relation)?;
    catalog_continuation(client, result_oid, stage_id, relation_oid)?;
    Ok(relation_oid)
}

fn provision_scan(
    client: &mut SpiClient<'_>,
    registration: &Registration,
    stage_id: i32,
    stage: &DataflowStage,
    scan: &ScanSpec,
    input_streams: &[i64],
) -> Result<(), String> {
    if input_streams.len() != 1 {
        return Err(format!("Scan stage {stage_id} has no unique source input"));
    }
    let output_stream = registration
        .stage_streams
        .get(usize::try_from(stage_id).map_err(|_| "negative Scan stage ID")?)
        .copied()
        .flatten()
        .ok_or_else(|| format!("Scan stage {stage_id} has no output stream"))?;
    let output_storage = storage::payload(client, output_stream)?;
    let output_attributes = storage::composite_attributes(client, &output_storage.row_type)?;
    if output_attributes.len() != stage.schema.outputs.len() {
        return Err("Scan output payload does not match its plan schema".into());
    }
    let source_oid = oid(scan.source_oid, "Scan source")?;
    let source_attributes = storage::relation_attributes(client, source_oid)?;
    let source_by_number = source_attributes
        .iter()
        .map(|attribute| (attribute.number, attribute))
        .collect::<HashMap<_, _>>();
    let scan_by_slot = scan
        .columns
        .iter()
        .map(|column| (column.output, column.attnum))
        .collect::<HashMap<_, _>>();
    let mut source_expressions = Vec::with_capacity(stage.schema.outputs.len());
    for (output, actual) in stage.schema.outputs.iter().zip(&output_attributes) {
        validate_attribute_type(&output.type_, actual, "Scan output")?;
        let attnum = scan_by_slot
            .get(&output.slot)
            .copied()
            .ok_or_else(|| format!("Scan output slot {} has no source column", output.slot.0))?;
        let source = source_by_number
            .get(&attnum)
            .copied()
            .ok_or_else(|| format!("Scan source attribute {attnum} is not live"))?;
        validate_attribute_type(&output.type_, source, "Scan source")?;
        source_expressions.push(format!("source_row.{}", quote_identifier(&source.name)));
    }
    if source_expressions.len() != scan.columns.len() {
        return Err("Scan columns are not a one-to-one output mapping".into());
    }

    let bootstrap_name = format!(
        "scan_bootstrap_r{}_s{stage_id}",
        registration.result_oid.to_u32()
    );
    let bootstrap = qualified_internal(&bootstrap_name);
    client
        .update(
            &format!(
                "CREATE TABLE {bootstrap}(
                   bootstrap_seq bigint PRIMARY KEY CHECK(bootstrap_seq > 0),
                   row_value {} NOT NULL
                 )",
                output_storage.row_type.sql()
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not create Scan bootstrap storage: {error}"))?;
    client
        .update(
            &format!("REVOKE ALL ON TABLE {bootstrap} FROM PUBLIC"),
            None,
            &[],
        )
        .map_err(|error| format!("could not protect Scan bootstrap storage: {error}"))?;
    let bootstrap_oid = resolve_relation_oid(client, &bootstrap)?;
    catalog_state(client, registration.result_oid, stage_id, 0, bootstrap_oid)?;

    let source_relation = resolve_qualified_relation(client, source_oid, "Scan source")?;
    client
        .update(
            &format!(
                "INSERT INTO {bootstrap}(bootstrap_seq,row_value)
                 SELECT pg_catalog.row_number() OVER (ORDER BY source_row.ctid),
                        ROW({})::{}
                 FROM {source_relation} AS source_row",
                source_expressions.join(","),
                output_storage.row_type.sql()
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not spool Scan activation snapshot: {error}"))?;

    provision_linear_continuation(
        client,
        registration.result_oid,
        stage_id,
        input_streams,
        LinearContinuation::Scan,
    )?;
    let continuation = qualified_internal(&format!(
        "continuation_r{}_s{stage_id}",
        registration.result_oid.to_u32()
    ));
    let arguments = unsafe { [DatumWithOid::new(input_streams[0], pg_sys::INT8OID)] };
    require_one(
        client
            .update(
                &format!(
                    "INSERT INTO {continuation}(
                       singleton,phase,input_stream_id,input_chunk_seq,
                       next_row_ordinal,next_bootstrap_seq,pending_frontier_lsn
                     )
                     VALUES(true,1,$1,NULL,NULL,1,NULL)
                     RETURNING singleton"
                ),
                Some(1),
                &arguments,
            )
            .map_err(|error| format!("could not initialize Scan continuation: {error}"))?,
        "Scan continuation initialization",
    )?;
    let checkpoint_arguments = unsafe {
        [
            DatumWithOid::new(registration.result_oid, pg_sys::OIDOID),
            DatumWithOid::new(stage_id, pg_sys::INT4OID),
        ]
    };
    require_one(
        client
            .update(
                r#"
                UPDATE shiba_internal.operator_checkpoints
                SET has_continuation = true
                WHERE result_oid = $1
                  AND stage_id = $2
                  AND revision = 0
                  AND NOT has_continuation
                RETURNING stage_id
                "#,
                Some(1),
                &checkpoint_arguments,
            )
            .map_err(|error| format!("could not initialize Scan checkpoint: {error}"))?,
        "Scan checkpoint initialization",
    )
}

fn protect_result(
    client: &mut SpiClient<'_>,
    registration: &Registration,
    result: &ResultIdentity,
) -> Result<(), String> {
    client
        .update(
            &format!(
                "CREATE TRIGGER shiba_result_guard
                 BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE
                 ON {}
                 FOR EACH STATEMENT
                 EXECUTE FUNCTION shiba_internal.reject_result_write()",
                result.qualified
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not protect the result table: {error}"))?;
    let creator = role_name(client, registration.creator_oid)?;
    client
        .update(
            &format!(
                "GRANT SELECT ON {} TO {}",
                result.qualified,
                quote_identifier(&creator)
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not grant result SELECT: {error}"))?;
    Ok(())
}

fn transfer_result_ownership(
    client: &mut SpiClient<'_>,
    result: &ResultIdentity,
) -> Result<(), String> {
    let owner = required_scalar::<String>(
        client,
        "SELECT shiba_internal.extension_owner()::text",
        &[],
        "extension owner",
    )?;
    client
        .update(
            &format!(
                "ALTER TABLE {} OWNER TO {}",
                result.qualified,
                quote_identifier(&owner)
            ),
            None,
            &[],
        )
        .map_err(|error| format!("could not transfer result ownership: {error}"))?;
    Ok(())
}

fn role_name(client: &mut SpiClient<'_>, role_oid: pg_sys::Oid) -> Result<String, String> {
    let arguments = unsafe { [DatumWithOid::new(role_oid, pg_sys::OIDOID)] };
    required_scalar(
        client,
        "SELECT pg_catalog.pg_get_userbyid($1)::text",
        &arguments,
        "registration creator",
    )
}

pub(super) fn resolve_relation_oid(
    client: &mut SpiClient<'_>,
    qualified: &str,
) -> Result<pg_sys::Oid, String> {
    let arguments = unsafe { [DatumWithOid::new(qualified, pg_sys::TEXTOID)] };
    required_scalar(
        client,
        "SELECT pg_catalog.to_regclass($1)::oid",
        &arguments,
        "created relation OID",
    )
}

fn resolve_type_oid(client: &mut SpiClient<'_>, qualified: &str) -> Result<pg_sys::Oid, String> {
    let arguments = unsafe { [DatumWithOid::new(qualified, pg_sys::TEXTOID)] };
    required_scalar(
        client,
        "SELECT pg_catalog.to_regtype($1)::oid",
        &arguments,
        "created type OID",
    )
}

fn resolve_qualified_relation(
    client: &mut SpiClient<'_>,
    relation_oid: pg_sys::Oid,
    label: &str,
) -> Result<String, String> {
    let arguments = unsafe { [DatumWithOid::new(relation_oid, pg_sys::OIDOID)] };
    required_scalar(
        client,
        r#"
        SELECT pg_catalog.format('%I.%I',namespace.nspname,relation.relname)
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE relation.oid = $1
          AND relation.relkind = 'r'
          AND relation.relpersistence = 'p'
        "#,
        &arguments,
        label,
    )
}

pub(super) fn catalog_state(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    slot: i32,
    relation_oid: pg_sys::Oid,
) -> Result<(), String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(stage_id, pg_sys::INT4OID),
            DatumWithOid::new(slot, pg_sys::INT4OID),
            DatumWithOid::new(relation_oid, pg_sys::OIDOID),
        ]
    };
    require_one(
        client
            .update(
                r#"
                INSERT INTO shiba_internal.operator_state_relations(
                  result_oid,stage_id,state_slot,relation_oid
                )
                VALUES($1,$2,$3,$4)
                RETURNING relation_oid
                "#,
                Some(1),
                &arguments,
            )
            .map_err(|error| format!("could not catalog operator state: {error}"))?,
        "operator state catalog insertion",
    )
}

pub(super) fn catalog_continuation(
    client: &mut SpiClient<'_>,
    result_oid: pg_sys::Oid,
    stage_id: i32,
    relation_oid: pg_sys::Oid,
) -> Result<(), String> {
    let arguments = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(stage_id, pg_sys::INT4OID),
            DatumWithOid::new(relation_oid, pg_sys::OIDOID),
        ]
    };
    require_one(
        client
            .update(
                r#"
                INSERT INTO shiba_internal.operator_continuation_relations(
                  result_oid,stage_id,relation_oid
                )
                VALUES($1,$2,$3)
                RETURNING relation_oid
                "#,
                Some(1),
                &arguments,
            )
            .map_err(|error| format!("could not catalog operator continuation: {error}"))?,
        "operator continuation catalog insertion",
    )
}

pub(super) fn qualified_internal(name: &str) -> String {
    format!("shiba_internal.{}", quote_identifier(name))
}

pub(super) fn type_sql(client: &mut SpiClient<'_>, type_: &SlotType) -> Result<String, String> {
    let type_oid = oid(type_.type_oid, "state column type")?;
    let arguments = unsafe {
        [
            DatumWithOid::new(type_oid, pg_sys::OIDOID),
            DatumWithOid::new(type_.typmod, pg_sys::INT4OID),
        ]
    };
    required_scalar(
        client,
        r#"
        SELECT pg_catalog.format_type(type_catalog.oid,$2)
        FROM pg_catalog.pg_type AS type_catalog
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = type_catalog.typnamespace
        WHERE type_catalog.oid = $1
          AND type_catalog.typtype <> 'p'
          AND namespace.nspname = 'pg_catalog'
        "#,
        &arguments,
        "state column type",
    )
}

pub(super) fn column_sql(client: &mut SpiClient<'_>, type_: &SlotType) -> Result<String, String> {
    let mut sql = type_sql(client, type_)?;
    if type_.collation_oid == 0 {
        return Ok(sql);
    }
    let arguments = unsafe {
        [DatumWithOid::new(
            oid(type_.collation_oid, "state column collation")?,
            pg_sys::OIDOID,
        )]
    };
    let collation = required_scalar::<String>(
        client,
        r#"
        SELECT pg_catalog.format(
          '%I.%I',namespace.nspname,collation_catalog.collname
        )
        FROM pg_catalog.pg_collation AS collation_catalog
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = collation_catalog.collnamespace
        WHERE collation_catalog.oid = $1
          AND namespace.nspname = 'pg_catalog'
        "#,
        &arguments,
        "state column collation",
    )?;
    sql.push_str(" COLLATE ");
    sql.push_str(&collation);
    Ok(sql)
}

fn validate_attribute_type(
    expected: &SlotType,
    actual: &storage::AttributeRef,
    label: &str,
) -> Result<(), String> {
    if actual.type_oid.to_u32() != expected.type_oid
        || actual.typmod != expected.typmod
        || actual.collation_oid.to_u32() != expected.collation_oid
    {
        return Err(format!("{label} type metadata changed identity"));
    }
    Ok(())
}

fn oid(value: u32, label: &str) -> Result<pg_sys::Oid, String> {
    if value == 0 {
        Err(format!("{label} OID is invalid"))
    } else {
        Ok(pg_sys::Oid::from(value))
    }
}

fn oid_allow_invalid(value: u32) -> pg_sys::Oid {
    pg_sys::Oid::from(value)
}

fn i64_from_usize(value: usize, label: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{label} exceeds bigint"))
}

fn required_scalar<T: FromDatum + IntoDatum>(
    client: &mut SpiClient<'_>,
    query: &str,
    arguments: &[DatumWithOid<'_>],
    label: &str,
) -> Result<T, String> {
    let table = client
        .select(query, Some(1), arguments)
        .map_err(|error| format!("could not read {label}: {error}"))?;
    if table.len() != 1 {
        return Err(format!("{label} returned {} rows, expected 1", table.len()));
    }
    required_table(&table.first(), 1, label)
}

fn required_update_scalar<T: FromDatum + IntoDatum>(
    client: &mut SpiClient<'_>,
    query: &str,
    arguments: &[DatumWithOid<'_>],
    label: &str,
) -> Result<T, String> {
    let table = client
        .update(query, Some(1), arguments)
        .map_err(|error| format!("could not update {label}: {error}"))?;
    if table.len() != 1 {
        return Err(format!("{label} returned {} rows, expected 1", table.len()));
    }
    required_table(&table.first(), 1, label)
}

fn require_one(table: SpiTupleTable<'_>, label: &str) -> Result<(), String> {
    if table.len() != 1 {
        return Err(format!("{label} changed {} rows, expected 1", table.len()));
    }
    Ok(())
}

fn required_table<T: FromDatum + IntoDatum>(
    row: &SpiTupleTable<'_>,
    ordinal: usize,
    label: &str,
) -> Result<T, String> {
    row.get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("{label} returned NULL"))
}

fn required_row<T: FromDatum + IntoDatum>(
    row: &SpiHeapTupleData<'_>,
    ordinal: usize,
    label: &str,
) -> Result<T, String> {
    row.get::<T>(ordinal)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("{label} returned NULL"))
}
