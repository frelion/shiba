//! Database lifecycle transitions whose authority spans the Runtime, logical
//! slot, and durable catalog.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

#[pg_extern(
    name = "_ensure_runtime",
    volatile,
    requires = ["shiba_catalog"]
)]
#[search_path(pg_catalog, shiba_internal)]
pub(crate) fn ensure_runtime() {
    let generation = Spi::connect_mut(prepare_runtime_launch)
        .unwrap_or_else(|error| error!("Shiba could not prepare its Runtime: {error}"));
    if generation.is_some_and(|generation| !crate::worker::start_runtime(generation)) {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
            "Shiba could not start its Runtime background worker; increase max_worker_processes"
        );
    }
}

fn prepare_runtime_launch(client: &mut SpiClient<'_>) -> Result<Option<i64>, String> {
    if !scalar_bool(
        client,
        "SELECT pg_try_advisory_xact_lock(
            shiba_internal.identity_lock_namespace(), 0
         )",
    )? {
        return Ok(None);
    }

    client
        .update(
            "UPDATE shiba_internal.runtime_state
                SET owner_pid = NULL,
                    started_at = NULL,
                    last_heartbeat = NULL,
                    launch_generation = launch_generation + 1,
                    pending_launch_xid = pg_current_xact_id(),
                    pending_since = clock_timestamp()
              WHERE singleton
                AND active
                AND (
                    pending_launch_xid IS NULL
                    OR (
                        pending_launch_xid <> pg_current_xact_id()
                        AND pending_since <= clock_timestamp() - interval '5 seconds'
                    )
                )
          RETURNING launch_generation",
            None,
            &[],
        )
        .map_err(|error| format!("could not claim a Runtime generation: {error}"))?
        .next()
        .map(|row| row.get::<i64>(1))
        .transpose()
        .map_err(|error| format!("Runtime generation has invalid type: {error}"))?
        .flatten()
        .map_or(Ok(None), |generation| {
            if generation <= 0 {
                Err("Runtime generation is not positive".into())
            } else {
                Ok(Some(generation))
            }
        })
}

#[pg_extern(
    security_definer,
    volatile,
    requires = ["shiba_ingress"]
)]
#[search_path(pg_catalog, shiba, shiba_internal)]
pub fn activate() -> bool {
    Spi::connect_mut(activate_database)
        .unwrap_or_else(|error| error!("Shiba could not activate: {error}"));
    ensure_runtime();
    true
}

fn activate_database(client: &mut SpiClient<'_>) -> Result<(), String> {
    read(
        client,
        "SELECT shiba._lock_database_lifecycle()",
        &[],
        "acquire the lifecycle lock",
    )?;
    if !scalar_bool(
        client,
        "SELECT nullif(current_setting('shiba.replication_conninfo', true), '') IS NOT NULL",
    )? {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            "shiba.replication_conninfo must be configured before activation"
        );
    }
    if !scalar_bool(
        client,
        "SELECT EXISTS (
            SELECT 1 FROM pg_publication WHERE pubname='shiba_publication'
         )",
    )? {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            "publication shiba_publication is missing; recreate the extension-owned publication before activation"
        );
    }

    // Slot creation is an external side effect. It must precede every
    // transactional ingress-catalog write so a retry can reconcile it.
    let slot_created = !scalar_bool(
        client,
        "SELECT EXISTS (
            SELECT 1 FROM pg_replication_slots
            WHERE slot_name=shiba_internal.slot_name()::text
         )",
    )?;
    if slot_created {
        read(
            client,
            "SELECT pg_create_logical_replication_slot(
                shiba_internal.slot_name(), 'pgoutput'
             )",
            &[],
            "create the logical slot",
        )?;
    }
    let slot_created_argument = unsafe { [DatumWithOid::new(slot_created, pg_sys::BOOLOID)] };
    run(
        client,
        "SELECT shiba_internal.ensure_ingress_generation(
            shiba_internal.slot_name(), $1::boolean
         )",
        &slot_created_argument,
        "ensure the ingress generation",
    )?;
    if scalar_bool(
        client,
        "SELECT pubtruncate
         FROM pg_publication
         WHERE pubname='shiba_publication'",
    )? {
        run(
            client,
            "ALTER PUBLICATION shiba_publication
             SET (publish = 'insert, update, delete')",
            &[],
            "remove TRUNCATE from the publication",
        )?;
    }
    run(
        client,
        "UPDATE shiba_internal.runtime_state
            SET active=true, last_heartbeat=NULL,
                pending_launch_xid=NULL, pending_since=NULL
          WHERE singleton AND NOT active",
        &[],
        "activate Runtime state",
    )?;
    run(
        client,
        "UPDATE shiba_internal.dataflows SET active=true WHERE NOT active",
        &[],
        "activate dataflows",
    )?;
    for relation in ["ingress_transactions", "change_log", "source_publications"] {
        run(
            client,
            &format!("ANALYZE shiba_internal.{relation}"),
            &[],
            "seed queue statistics",
        )?;
    }
    Ok(())
}

/// Deactivate Shiba in the caller's transaction.
///
/// The SQL helpers own generated-relation validation and the Runtime identity
/// lock. Rust owns the transition itself so its ordered catalog mutations are
/// explicit and there is one execution path for slot retirement.
#[pg_extern(security_definer, volatile, requires = ["shiba_lifecycle"])]
#[search_path(pg_catalog, shiba, shiba_internal)]
pub fn deactivate() {
    Spi::connect_mut(deactivate_database)
        .unwrap_or_else(|error| error!("Shiba could not deactivate: {error}"));
}

fn deactivate_database(client: &mut SpiClient<'_>) -> Result<(), String> {
    run(
        client,
        "SELECT shiba._lock_database_lifecycle()",
        &[],
        "acquire the lifecycle lock",
    )?;
    let has_dataflows = scalar_bool(
        client,
        "SELECT EXISTS (SELECT 1 FROM shiba_internal.dataflows)",
    )?;
    if has_dataflows {
        return Err("drop all Shiba result tables before deactivation".into());
    }

    // This obtains the Runtime's session-identity lock before slot teardown;
    // holding it in this transaction prevents a replacement Runtime launch.
    run(
        client,
        "SELECT shiba_internal.stop_runtime_for_deactivation()",
        &[],
        "stop the Runtime",
    )?;

    let active_generation = client
        .update(
            "SELECT slot_generation
             FROM shiba_internal.ingress_replay_state
             WHERE database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())
               AND slot_name=shiba_internal.slot_name()
               AND state='active'
             FOR UPDATE",
            None,
            &[],
        )
        .map_err(|error| format!("could not lock the active ingress generation: {error}"))?
        .next()
        .map(|row| row.get::<i64>(1))
        .transpose()
        .map_err(|error| format!("active ingress generation is NULL or invalid: {error}"))?
        .flatten();

    if let Some(generation) = active_generation {
        let generation_argument = unsafe { [DatumWithOid::new(generation, pg_sys::INT8OID)] };
        run(
            client,
            "DELETE FROM shiba_internal.ingress_transactions
             WHERE slot_generation=$1::bigint",
            &generation_argument,
            "delete ingress transactions",
        )?;

        let stream_ids = client
            .update(
                "SELECT stream_id
                 FROM shiba_internal.effect_streams
                 WHERE producer_kind='source' AND slot_generation=$1::bigint
                 ORDER BY stream_id",
                None,
                &generation_argument,
            )
            .map_err(|error| format!("could not list source streams: {error}"))?
            .map(|row| {
                row.get::<i64>(1)
                    .map_err(|error| format!("source stream ID has invalid type: {error}"))?
                    .ok_or_else(|| "source stream ID is NULL".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        for stream_id in stream_ids {
            let stream_argument = unsafe { [DatumWithOid::new(stream_id, pg_sys::INT8OID)] };
            run(
                client,
                "SELECT shiba_internal.drop_effect_stream_payload($1::bigint)",
                &stream_argument,
                "drop source stream payload",
            )?;
            run(
                client,
                "DELETE FROM shiba_internal.effect_streams WHERE stream_id=$1::bigint",
                &stream_argument,
                "delete source stream",
            )?;
        }
    }

    run(
        client,
        "UPDATE shiba_internal.runtime_state
         SET active=false, owner_pid=NULL, started_at=NULL,
             last_heartbeat=NULL, pending_launch_xid=NULL, pending_since=NULL
         WHERE singleton",
        &[],
        "deactivate Runtime state",
    )?;
    run(
        client,
        "SELECT shiba_internal.retire_ingress_generation(shiba_internal.slot_name())",
        &[],
        "retire ingress generation",
    )?;
    run(
        client,
        "SELECT pg_drop_replication_slot(shiba_internal.slot_name())
         WHERE EXISTS (
           SELECT 1 FROM pg_replication_slots
           WHERE slot_name=shiba_internal.slot_name()::text
         )",
        &[],
        "drop logical slot",
    )?;
    Ok(())
}

fn run(
    client: &mut SpiClient<'_>,
    query: &str,
    arguments: &[DatumWithOid<'_>],
    action: &str,
) -> Result<(), String> {
    client
        .update(query, None, arguments)
        .map(|_| ())
        .map_err(|error| format!("could not {action}: {error}"))
}

fn read(
    client: &SpiClient<'_>,
    query: &str,
    arguments: &[DatumWithOid<'_>],
    action: &str,
) -> Result<(), String> {
    client
        .select(query, None, arguments)
        .map(|_| ())
        .map_err(|error| format!("could not {action}: {error}"))
}

fn scalar_bool(client: &mut SpiClient<'_>, query: &str) -> Result<bool, String> {
    client
        .select(query, None, &[])
        .map_err(|error| error.to_string())?
        .next()
        .ok_or_else(|| "query returned no row".to_owned())?
        .get::<bool>(1)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "query returned NULL".to_owned())
}
