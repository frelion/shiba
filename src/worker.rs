//! Background workers: one database-level WAL Router and one executor per DAG.

use crate::{logical, pgoutput};
use pgrx::bgworkers::*;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug)]
struct InboxEvent {
    commit_lsn: String,
    source_oid: i32,
    delta: i32,
    row_data: String,
}

/// Start the one per-database worker that owns the logical replication slot.
#[pg_extern]
pub fn start_worker() -> bool {
    let database_name = current_database_name();
    BackgroundWorkerBuilder::new("shiba worker")
        .set_library("shiba")
        .set_function("shiba_background_worker_main")
        .set_extra(&database_name)
        .enable_spi_access()
        .load_dynamic()
        .is_ok()
}

/// Start one executor for a result DAG.  Its input is the durable dag_inbox,
/// never the logical replication slot itself.
#[pg_extern]
pub fn start_view_worker(result_oid: i32) -> bool {
    let worker_extra = format!("{}:{result_oid}", current_database_name());
    BackgroundWorkerBuilder::new("shiba dag worker")
        .set_library("shiba")
        .set_function("shiba_view_worker_main")
        .set_extra(&worker_extra)
        .enable_spi_access()
        .load_dynamic()
        .is_ok()
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn shiba_background_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let database_name = BackgroundWorker::get_extra().to_owned();
    BackgroundWorker::connect_worker_to_spi(Some(&database_name), None);

    log!("Shiba WAL Router started for database {database_name}");
    while BackgroundWorker::wait_latch(Some(Duration::from_millis(100))) {
        let routed_through = BackgroundWorker::transaction(|| {
            if !router_is_active() {
                return None;
            }
            let routed_through = peek_and_route_wal_changes();
            let _ = Spi::run("SELECT shiba._ensure_dag_workers()");
            let _ = Spi::run(
                "UPDATE shiba_internal.worker_state
                 SET last_heartbeat = clock_timestamp()
                 WHERE singleton
                   AND (last_heartbeat IS NULL OR last_heartbeat < clock_timestamp() - interval '1 second')",
            );
            Some(routed_through)
        });
        match routed_through {
            None => break,
            Some(Some(commit_lsn)) => {
                BackgroundWorker::transaction(|| advance_slot_through(&commit_lsn));
            }
            Some(None) => {}
        }
    }
    log!("Shiba WAL Router stopped for database {database_name}");
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn shiba_view_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let extra = BackgroundWorker::get_extra().to_owned();
    let (database_name, result_oid) = extra
        .rsplit_once(':')
        .and_then(|(database, oid)| oid.parse::<i32>().ok().map(|oid| (database, oid)))
        .expect("Shiba DAG worker received invalid startup data");
    BackgroundWorker::connect_worker_to_spi(Some(database_name), None);

    log!("Shiba DAG executor started for result {result_oid}");
    while BackgroundWorker::wait_latch(Some(Duration::from_millis(25))) {
        let keep_running = BackgroundWorker::transaction(|| {
            if !dag_is_active(result_oid) {
                return false;
            }
            let _ = process_next_dag_transaction(result_oid);
            let heartbeat = unsafe { [DatumWithOid::new(result_oid, pg_sys::INT4OID)] };
            let _ = Spi::run_with_args(
                "UPDATE shiba_internal.dag_worker_state
                 SET last_heartbeat = clock_timestamp()
                 WHERE result_oid = $1::oid
                   AND (last_heartbeat IS NULL OR last_heartbeat < clock_timestamp() - interval '1 second')",
                &heartbeat,
            );
            true
        });
        if !keep_running {
            break;
        }
    }
    log!("Shiba DAG executor stopped for result {result_oid}");
}

fn current_database_name() -> String {
    Spi::get_one::<String>("SELECT current_database()::text")
        .expect("Shiba could not identify the current database")
        .expect("current_database() returned NULL")
}

fn router_is_active() -> bool {
    Spi::get_one::<bool>(
        "SELECT to_regclass('shiba_internal.worker_state') IS NOT NULL
          AND EXISTS (SELECT 1 FROM shiba_internal.worker_state WHERE singleton AND active)",
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

fn dag_is_active(result_oid: i32) -> bool {
    let arguments = unsafe { [DatumWithOid::new(result_oid, pg_sys::INT4OID)] };
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (
             SELECT 1 FROM shiba_internal.worker_state router
             JOIN shiba_internal.dag_worker_state dag ON true
             WHERE router.singleton AND router.active AND dag.result_oid = $1::oid AND dag.active
         )",
        &arguments,
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

fn peek_and_route_wal_changes() -> Option<String> {
    let messages: Vec<Vec<u8>> = Spi::connect_mut(|client| {
        client
            .update(
                "SELECT data FROM pg_logical_slot_peek_binary_changes(
                 shiba_internal.slot_name(), NULL, 2048,
                 'proto_version', '1', 'publication_names', 'shiba_publication')",
                None,
                &[],
            )
            .expect("Shiba could not read its logical replication slot")
            .map(|row| {
                row.get::<Vec<u8>>(1)
                    .expect("invalid bytea")
                    .expect("NULL message")
            })
            .collect()
    });

    let mut relations: HashMap<u32, Vec<String>> = HashMap::new();
    let mut transaction: Vec<(u32, i32, Value)> = Vec::new();
    let mut routed_through = None;
    for bytes in messages {
        match pgoutput::parse(&bytes).expect("Shiba received an unsupported pgoutput message") {
            pgoutput::Message::Begin { .. } => {
                if !transaction.is_empty() {
                    panic!("Shiba received a new logical transaction before commit");
                }
            }
            pgoutput::Message::Relation { relid, columns } => {
                relations.insert(relid, columns);
            }
            pgoutput::Message::Insert { relid, row } => transaction.push((
                relid,
                1,
                tuple_to_json(relid, row, &relations).expect("invalid inserted tuple"),
            )),
            pgoutput::Message::Update { relid, old, new } => {
                transaction.push((
                    relid,
                    -1,
                    tuple_to_json(relid, old, &relations).expect("invalid old tuple"),
                ));
                transaction.push((
                    relid,
                    1,
                    tuple_to_json(relid, new, &relations).expect("invalid new tuple"),
                ));
            }
            pgoutput::Message::Delete { relid, old } => transaction.push((
                relid,
                -1,
                tuple_to_json(relid, old, &relations).expect("invalid deleted tuple"),
            )),
            pgoutput::Message::Commit {
                commit_lsn,
                end_lsn,
            } => {
                route_transaction(commit_lsn, &mut transaction);
                routed_through = Some(format_lsn(end_lsn));
            }
        }
    }
    routed_through
}

fn advance_slot_through(commit_lsn: &str) {
    let arguments = unsafe { [DatumWithOid::new(commit_lsn, pg_sys::TEXTOID)] };
    Spi::connect_mut(|client| {
        client
            .update(
                "SELECT 1 FROM pg_logical_slot_get_binary_changes(
                   shiba_internal.slot_name(), $1::pg_lsn, NULL,
                   'proto_version', '1', 'publication_names', 'shiba_publication')",
                None,
                &arguments,
            )
            .expect("Shiba could not advance its durable logical replication checkpoint")
            .for_each(drop);
    });
}

fn route_transaction(commit_lsn: u64, transaction: &mut Vec<(u32, i32, Value)>) {
    let lsn = format_lsn(commit_lsn);
    let checkpoint = unsafe { [DatumWithOid::new(lsn.as_str(), pg_sys::TEXTOID)] };
    let is_new = Spi::get_one_with_args::<bool>(
        "SELECT shiba._begin_route_transaction($1::pg_lsn)",
        &checkpoint,
    )
    .expect("Shiba could not checkpoint a routed transaction")
    .expect("Shiba routing checkpoint returned NULL");
    if !is_new {
        transaction.clear();
        return;
    }
    for (index, (relid, delta, row)) in transaction.drain(..).enumerate() {
        let sequence = i32::try_from(index + 1).expect("Shiba transaction has too many row deltas");
        let arguments = unsafe {
            [
                DatumWithOid::new(relid as i32, pg_sys::OIDOID),
                DatumWithOid::new(row.to_string(), pg_sys::TEXTOID),
                DatumWithOid::new(delta, pg_sys::INT4OID),
                DatumWithOid::new(lsn.as_str(), pg_sys::TEXTOID),
                DatumWithOid::new(sequence, pg_sys::INT4OID),
            ]
        };
        Spi::run_with_args(
            "SELECT shiba._route_wal_delta($1, $2::jsonb, $3, $4, $5)",
            &arguments,
        )
        .expect("Shiba could not route a logical WAL delta");
    }
}

fn process_next_dag_transaction(result_oid: i32) -> bool {
    let result = unsafe { [DatumWithOid::new(result_oid, pg_sys::INT4OID)] };
    Spi::run_with_args("SELECT pg_advisory_xact_lock($1::bigint)", &result)
        .expect("Shiba could not acquire the DAG execution lock");
    if !dag_is_active(result_oid) {
        return false;
    }
    let events: Vec<InboxEvent> = Spi::connect_mut(|client| {
        client
            .update(
                "SELECT commit_lsn::text, source_oid::integer, delta, row_data::text
             FROM shiba_internal.dag_inbox
             WHERE result_oid = $1::oid
               AND commit_lsn = (
                   SELECT min(commit_lsn) FROM shiba_internal.dag_inbox WHERE result_oid = $1::oid
               )
             ORDER BY sequence
             FOR UPDATE",
                None,
                &result,
            )
            .expect("Shiba could not lock DAG inbox rows")
            .map(|row| InboxEvent {
                commit_lsn: row
                    .get::<String>(1)
                    .expect("invalid inbox LSN")
                    .expect("NULL inbox LSN"),
                source_oid: row
                    .get::<i32>(2)
                    .expect("invalid inbox source")
                    .expect("NULL inbox source"),
                delta: row
                    .get::<i32>(3)
                    .expect("invalid inbox delta")
                    .expect("NULL inbox delta"),
                row_data: row
                    .get::<String>(4)
                    .expect("invalid inbox data")
                    .expect("NULL inbox data"),
            })
            .collect()
    });
    let Some(commit_lsn) = events.first().map(|event| event.commit_lsn.as_str()) else {
        return false;
    };
    let runtime = logical::DagRuntime::load(pg_sys::Oid::from(result_oid as u32))
        .expect("Shiba could not load the persisted logical DAG");
    for event in &events {
        let row = serde_json::from_str(&event.row_data).expect("invalid inbox JSON");
        runtime
            .apply_source_delta(
                pg_sys::Oid::from(event.source_oid as u32),
                &event.commit_lsn,
                row,
                event.delta,
            )
            .expect("Shiba could not execute a DAG inbox delta");
    }
    let delete = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(commit_lsn, pg_sys::TEXTOID),
        ]
    };
    Spi::run_with_args(
        "DELETE FROM shiba_internal.dag_inbox WHERE result_oid = $1 AND commit_lsn = $2::pg_lsn",
        &delete,
    )
    .expect("Shiba could not acknowledge a DAG inbox transaction");
    true
}

fn tuple_to_json(
    relid: u32,
    tuple: pgoutput::Tuple,
    relations: &HashMap<u32, Vec<String>>,
) -> Result<Value, &'static str> {
    let columns = relations
        .get(&relid)
        .ok_or("no relation message preceded this tuple")?;
    if columns.len() != tuple.len() {
        return Err("tuple column count does not match relation metadata");
    }
    let mut object = Map::with_capacity(columns.len());
    for (column, value) in columns.iter().zip(tuple) {
        object.insert(column.clone(), value.map_or(Value::Null, Value::String));
    }
    Ok(Value::Object(object))
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tuple_to_json_preserves_column_order_and_nulls() {
        let relations = HashMap::from([(
            42,
            vec![
                "id".to_string(),
                "label".to_string(),
                "optional".to_string(),
            ],
        )]);

        assert_eq!(
            tuple_to_json(
                42,
                vec![Some("7".into()), Some("shiba".into()), None],
                &relations,
            ),
            Ok(json!({"id": "7", "label": "shiba", "optional": null}))
        );
    }

    #[test]
    fn tuple_to_json_rejects_missing_relation_metadata() {
        assert_eq!(
            tuple_to_json(42, vec![], &HashMap::new()),
            Err("no relation message preceded this tuple")
        );
    }

    #[test]
    fn tuple_to_json_rejects_short_and_long_tuples() {
        let relations = HashMap::from([(42, vec!["id".to_string()])]);
        assert_eq!(
            tuple_to_json(42, vec![], &relations),
            Err("tuple column count does not match relation metadata")
        );
        assert_eq!(
            tuple_to_json(42, vec![None, None], &relations),
            Err("tuple column count does not match relation metadata")
        );
    }

    #[test]
    fn tuple_to_json_handles_empty_relation() {
        let relations = HashMap::from([(42, vec![])]);
        assert_eq!(
            tuple_to_json(42, vec![], &relations),
            Ok(Value::Object(Map::new()))
        );
    }

    #[test]
    fn format_lsn_covers_word_boundaries() {
        assert_eq!(format_lsn(0), "0/0");
        assert_eq!(format_lsn(1), "0/1");
        assert_eq!(format_lsn(u32::MAX as u64), "0/FFFFFFFF");
        assert_eq!(format_lsn(1_u64 << 32), "1/0");
        assert_eq!(format_lsn(u64::MAX), "FFFFFFFF/FFFFFFFF");
    }
}
