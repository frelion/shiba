//! Background workers: one database-level WAL Router and one executor per DAG.

use crate::{logical, pgoutput};
use pgrx::bgworkers::*;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const ROUTER_IDLE_WAIT: Duration = Duration::from_millis(100);
const ROUTER_MAX_BATCHES_PER_ROUND: usize = 16;
const ROUTER_DRAIN_BUDGET: Duration = Duration::from_millis(50);
const DAG_IDLE_WAIT: Duration = Duration::from_millis(25);
const DAG_MAX_COMMITS_PER_ROUND: usize = 64;
const DAG_DRAIN_BUDGET: Duration = Duration::from_millis(50);

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
    let mut idle = true;
    'worker: loop {
        let wait = if idle {
            ROUTER_IDLE_WAIT
        } else {
            Duration::ZERO
        };
        if !BackgroundWorker::wait_latch(Some(wait)) {
            break;
        }

        let started = Instant::now();
        let mut batches = 0;
        let mut run_maintenance = true;
        idle = false;
        while drain_has_capacity(
            batches,
            started.elapsed(),
            ROUTER_MAX_BATCHES_PER_ROUND,
            ROUTER_DRAIN_BUDGET,
        ) {
            let routed_through = BackgroundWorker::transaction(|| {
                if !router_is_active() {
                    return None;
                }
                let routed_through = peek_and_route_wal_changes();
                if run_maintenance {
                    let _ = Spi::run("SELECT shiba._ensure_dag_workers()");
                    update_router_heartbeat();
                }
                Some(routed_through)
            });
            run_maintenance = false;
            match routed_through {
                None => break 'worker,
                Some(Some(commit_lsn)) => {
                    // Routing and slot advancement deliberately remain separate
                    // transactions.  A crash between them safely replays through
                    // the durable routing checkpoint.
                    #[cfg(any(test, feature = "pg_test"))]
                    {
                        let failpoint = BackgroundWorker::transaction(|| {
                            test_failpoints::claim("router_before_slot_advance", None, None)
                        });
                        if let Some(pause) = failpoint {
                            std::thread::sleep(pause);
                            panic!(
                                "Shiba test failpoint: router exited after routing and before slot advancement"
                            );
                        }
                    }
                    BackgroundWorker::transaction(|| advance_slot_through(&commit_lsn));
                    batches += 1;
                }
                Some(None) => {
                    idle = true;
                    break;
                }
            }
        }
        if !idle {
            // The next round is immediate, but yield after each bounded burst so
            // a continuously busy router cannot monopolize its scheduler.
            std::thread::yield_now();
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
    let runtime = BackgroundWorker::transaction(|| {
        logical::DagRuntime::load(pg_sys::Oid::from(result_oid as u32))
            .expect("Shiba could not load the persisted logical DAG")
    });
    let mut idle = true;
    'worker: loop {
        let wait = if idle { DAG_IDLE_WAIT } else { Duration::ZERO };
        if !BackgroundWorker::wait_latch(Some(wait)) {
            break;
        }

        let started = Instant::now();
        let mut commits = 0;
        let mut run_maintenance = true;
        idle = false;
        while drain_has_capacity(
            commits,
            started.elapsed(),
            DAG_MAX_COMMITS_PER_ROUND,
            DAG_DRAIN_BUDGET,
        ) {
            let step = BackgroundWorker::transaction(|| {
                if !dag_is_active(result_oid) {
                    return DagStep::Inactive;
                }
                let processed = process_next_dag_transaction(result_oid, &runtime);
                if run_maintenance {
                    update_dag_heartbeat(result_oid);
                }
                if processed {
                    DagStep::Processed
                } else {
                    DagStep::Idle
                }
            });
            run_maintenance = false;
            match step {
                DagStep::Inactive => break 'worker,
                DagStep::Processed => commits += 1,
                DagStep::Idle => {
                    idle = true;
                    break;
                }
            }
        }
        if !idle {
            // Preserve low backlog latency while giving other PostgreSQL
            // backends a scheduling opportunity between bounded bursts.
            std::thread::yield_now();
        }
    }
    log!("Shiba DAG executor stopped for result {result_oid}");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DagStep {
    Inactive,
    Processed,
    Idle,
}

fn drain_has_capacity(
    processed: usize,
    elapsed: Duration,
    max_items: usize,
    time_budget: Duration,
) -> bool {
    processed < max_items && elapsed < time_budget
}

fn update_router_heartbeat() {
    let _ = Spi::run(
        "UPDATE shiba_internal.worker_state
         SET last_heartbeat = clock_timestamp()
         WHERE singleton
           AND (last_heartbeat IS NULL OR last_heartbeat < clock_timestamp() - interval '1 second')",
    );
}

fn update_dag_heartbeat(result_oid: i32) {
    let heartbeat = unsafe { [DatumWithOid::new(result_oid, pg_sys::INT4OID)] };
    let _ = Spi::run_with_args(
        "UPDATE shiba_internal.dag_worker_state
         SET last_heartbeat = clock_timestamp()
         WHERE result_oid = $1::oid
           AND (last_heartbeat IS NULL OR last_heartbeat < clock_timestamp() - interval '1 second')",
        &heartbeat,
    );
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

fn process_next_dag_transaction(result_oid: i32, runtime: &logical::DagRuntime) -> bool {
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
    let Some(commit_lsn) = events.first().map(|event| event.commit_lsn.clone()) else {
        return false;
    };
    let rows = events
        .iter()
        .map(|event| logical::DeltaRow {
            input: event.source_oid.to_string(),
            row: serde_json::from_str(&event.row_data).expect("invalid inbox JSON"),
            diff: i64::from(event.delta),
        })
        .collect();
    runtime
        .apply_batch(logical::DeltaBatch {
            epoch: commit_lsn.clone(),
            rows,
        })
        .expect("Shiba could not execute a DAG inbox transaction");
    #[cfg(any(test, feature = "pg_test"))]
    if let Some(pause) = test_failpoints::claim(
        "executor_before_ack",
        Some(result_oid),
        Some(commit_lsn.as_str()),
    ) {
        log!(
            "Shiba test failpoint reached: executor_before_ack result {result_oid} commit {commit_lsn}"
        );
        std::thread::sleep(pause);
        panic!(
            "Shiba test failpoint: executor exited after applying commit {commit_lsn} and before acknowledgement"
        );
    }
    let delete = unsafe {
        [
            DatumWithOid::new(result_oid, pg_sys::OIDOID),
            DatumWithOid::new(commit_lsn.as_str(), pg_sys::TEXTOID),
        ]
    };
    Spi::run_with_args(
        "DELETE FROM shiba_internal.dag_inbox WHERE result_oid = $1 AND commit_lsn = $2::pg_lsn",
        &delete,
    )
    .expect("Shiba could not acknowledge a DAG inbox transaction");
    true
}

/// Deterministic crash injection used only by pgrx and recovery-test builds.
///
/// Tests create `public.shiba_worker_failpoints` themselves. Keeping both the
/// code and its catalog contract behind `pg_test` means production workers
/// have no failpoint branch, SPI lookup, shared state, or runtime overhead.
#[cfg(any(test, feature = "pg_test"))]
mod test_failpoints {
    use super::*;

    pub(super) fn claim(
        kind: &str,
        result_oid: Option<i32>,
        commit_lsn: Option<&str>,
    ) -> Option<Duration> {
        let available = Spi::get_one::<bool>(
            "SELECT to_regclass('public.shiba_worker_failpoints') IS NOT NULL",
        )
        .ok()
        .flatten()
        .unwrap_or(false);
        if !available {
            return None;
        }

        let result_oid = result_oid.unwrap_or_default();
        let commit_lsn = commit_lsn.unwrap_or("0/0");
        let arguments = unsafe {
            [
                DatumWithOid::new(kind, pg_sys::TEXTOID),
                DatumWithOid::new(result_oid, pg_sys::INT4OID),
                DatumWithOid::new(commit_lsn, pg_sys::TEXTOID),
            ]
        };
        let pause_ms = Spi::get_one_with_args::<i32>(
            "SELECT max(pause_ms)
             FROM public.shiba_worker_failpoints
             WHERE kind = $1
               AND NOT fired
               AND (worker_pid IS NULL OR worker_pid = pg_backend_pid())
               AND (result_oid IS NULL OR result_oid = $2::oid)
               AND (commit_lsn IS NULL OR commit_lsn = $3::pg_lsn)",
            &arguments,
        )
        .expect("Shiba could not inspect its test worker failpoint");
        if pause_ms.is_some() {
            Spi::run_with_args(
                "UPDATE public.shiba_worker_failpoints
                 SET worker_pid = pg_backend_pid(), fired = true
                 WHERE kind = $1
                   AND NOT fired
                   AND (worker_pid IS NULL OR worker_pid = pg_backend_pid())
                   AND (result_oid IS NULL OR result_oid = $2::oid)
                   AND (commit_lsn IS NULL OR commit_lsn = $3::pg_lsn)",
                &arguments,
            )
            .expect("Shiba could not claim its test worker failpoint");
        }
        pause_ms.map(|milliseconds| {
            Duration::from_millis(
                u64::try_from(milliseconds).expect("negative Shiba test failpoint pause"),
            )
        })
    }
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

    #[test]
    fn drain_budget_stops_at_item_limit() {
        assert!(drain_has_capacity(
            63,
            Duration::ZERO,
            64,
            Duration::from_millis(50)
        ));
        assert!(!drain_has_capacity(
            64,
            Duration::ZERO,
            64,
            Duration::from_millis(50)
        ));
    }

    #[test]
    fn drain_budget_stops_at_time_limit() {
        assert!(drain_has_capacity(
            0,
            Duration::from_millis(49),
            64,
            Duration::from_millis(50)
        ));
        assert!(!drain_has_capacity(
            0,
            Duration::from_millis(50),
            64,
            Duration::from_millis(50)
        ));
    }
}
