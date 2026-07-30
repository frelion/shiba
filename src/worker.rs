//! PostgreSQL adapter for the database-level Runtime background worker.

use crate::runtime::scheduler;
use crate::runtime::wakeup;
use crate::runtime::wakeup::PENDING_RUNTIME_WAKE_PID;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Start the one Runtime process for the current database.
#[pg_extern]
pub fn start_runtime(launch_generation: i64) -> bool {
    if launch_generation <= 0 {
        return false;
    }
    let database_name = scheduler::current_database_name();
    let launch_xid = scheduler::current_transaction_id();
    let worker_extra = format!("{database_name}:{launch_xid}:{launch_generation}");
    BackgroundWorkerBuilder::new("shiba runtime")
        .set_library("shiba")
        .set_function("shiba_runtime_main")
        .set_extra(&worker_extra)
        .set_restart_time(Some(Duration::from_secs(1)))
        .enable_spi_access()
        .load_dynamic()
        .is_ok()
}

/// Schedule a latch wakeup only after the caller's source transaction commits.
#[pg_extern]
pub fn wake_runtime_on_commit(owner_pid: i32) -> bool {
    if owner_pid <= 0 {
        return false;
    }
    PENDING_RUNTIME_WAKE_PID.store(owner_pid, Ordering::Release);
    true
}

pub unsafe fn install_runtime_wakeup_callback() {
    wakeup::install_runtime_wakeup_callback();
}

#[cfg_attr(not(test), unsafe(no_mangle))]
#[cfg_attr(not(test), pg_guard)]
pub extern "C-unwind" fn shiba_runtime_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGINT);
    // Use PostgreSQL's normal backend SIGTERM handler. The pgrx deferred
    // handler only wakes our outer latch; if SIGTERM arrives inside logical
    // decoding, fast shutdown can wait forever because that C call never sees
    // ProcDiePending. `die` marks the current transaction for safe abort at
    // PostgreSQL's next interrupt check.
    unsafe {
        pg_sys::pqsignal(pg_sys::SIGTERM as i32, Some(wakeup::runtime_sigterm));
    }
    let extra = BackgroundWorker::get_extra().to_owned();
    let (database_and_xid, launch_generation) = extra
        .rsplit_once(':')
        .expect("Shiba Runtime received invalid startup data");
    let (database_name, launch_xid) = database_and_xid
        .rsplit_once(':')
        .expect("Shiba Runtime received invalid startup data");
    let launch_generation = launch_generation
        .parse::<i64>()
        .expect("Shiba Runtime received invalid launch generation");

    scheduler::run(database_name, launch_xid, launch_generation);
}

#[cfg(any(test, feature = "pg_test"))]
mod worker_catalog_tests {
    use super::*;
    use crate::runtime::gc;

    #[pg_test(schema = "tests")]
    fn effect_stream_gc_advances_past_a_full_page_of_still_eligible_streams() {
        Spi::run(
            r#"
            CREATE TABLE tests.effect_stream_gc_result (
                marker integer
            );

            INSERT INTO shiba_internal.dataflows(
                result_oid,
                plan,
                activation_lsn,
                active
            )
            VALUES (
                'tests.effect_stream_gc_result'::regclass,
                '{}'::jsonb,
                '0/0',
                false
            );

            INSERT INTO shiba_internal.operator_checkpoints(
                result_oid,
                stage_id
            )
            SELECT 'tests.effect_stream_gc_result'::regclass,
                   stage_id
            FROM generate_series(0, 259) AS stage_id;

            INSERT INTO shiba_internal.effect_streams(
                producer_kind,
                producer_result_oid,
                producer_stage_id,
                next_chunk_seq,
                first_retained_chunk_seq,
                buffered_chunks,
                buffered_rows,
                buffered_bytes,
                target_chunk_rows,
                target_chunk_bytes,
                high_chunks,
                high_rows,
                high_bytes,
                low_chunks,
                low_rows,
                low_bytes
            )
            SELECT 'operator',
                   'tests.effect_stream_gc_result'::regclass,
                   producer_stage_id,
                   CASE WHEN producer_stage_id < 64 THEN 66 ELSE 2 END,
                   1,
                   CASE WHEN producer_stage_id < 64 THEN 65 ELSE 1 END,
                   CASE WHEN producer_stage_id < 64 THEN 65 ELSE 1 END,
                   CASE WHEN producer_stage_id < 64 THEN 65 ELSE 1 END,
                   1,
                   1,
                   1024,
                   1024,
                   1024,
                   0,
                   0,
                   0
            FROM generate_series(0, 129) AS producer_stage_id
            ORDER BY producer_stage_id;

            INSERT INTO shiba_internal.effect_stream_chunks(
                stream_id,
                chunk_seq,
                chunk_kind,
                row_count,
                payload_bytes,
                chunk_lsn
            )
            SELECT stream.stream_id,
                   chunk_seq,
                   'data',
                   1,
                   1,
                   '0/1'
            FROM shiba_internal.effect_streams AS stream
            CROSS JOIN LATERAL generate_series(
                1,
                CASE WHEN stream.producer_stage_id < 64 THEN 65 ELSE 1 END
            ) AS chunk_seq
            WHERE stream.producer_result_oid
                    = 'tests.effect_stream_gc_result'::regclass;

            INSERT INTO shiba_internal.effect_stream_consumers(
                stream_id,
                result_oid,
                consumer_stage_id,
                input_port,
                next_chunk_seq,
                activation_lsn,
                consumed_frontier_lsn
            )
            SELECT stream.stream_id,
                   stream.producer_result_oid,
                   stream.producer_stage_id + 130,
                   0,
                   stream.next_chunk_seq,
                   '0/0',
                   '0/0'
            FROM shiba_internal.effect_streams AS stream
            WHERE stream.producer_result_oid
                    = 'tests.effect_stream_gc_result'::regclass;
            "#,
        )
        .expect("effect-stream GC fairness fixture should be created");

        let stream_ids = Spi::connect_mut(|client| {
            client
                .update(
                    "SELECT stream_id
                     FROM shiba_internal.effect_streams
                     WHERE producer_result_oid
                             = 'tests.effect_stream_gc_result'::regclass
                     ORDER BY producer_stage_id",
                    None,
                    &[],
                )
                .expect("effect-stream GC fixture should be queryable")
                .map(|row| {
                    row.get::<i64>(1)
                        .expect("invalid effect-stream ID")
                        .expect("NULL effect-stream ID")
                })
                .collect::<Vec<_>>()
        });
        assert_eq!(stream_ids.len(), 130);

        let mut cursor = Some(
            stream_ids[0]
                .checked_sub(1)
                .expect("effect-stream IDs are positive"),
        );
        assert_eq!(gc::gc_effect_streams(&mut cursor), 64 * 64);
        assert_eq!(cursor, Some(stream_ids[63]));
        assert_eq!(
            Spi::get_one::<bool>(&format!(
                "SELECT stream.first_retained_chunk_seq
                          < min(consumer.next_chunk_seq)
                 FROM shiba_internal.effect_streams AS stream
                 JOIN shiba_internal.effect_stream_consumers AS consumer
                   USING (stream_id)
                 WHERE stream.stream_id = {}
                 GROUP BY stream.first_retained_chunk_seq",
                stream_ids[0]
            ))
            .expect("low-ID stream eligibility should be queryable"),
            Some(true),
        );

        assert_eq!(gc::gc_effect_streams(&mut cursor), 64);
        assert_eq!(cursor, Some(stream_ids[127]));

        let low_stream_chunks = Spi::get_one::<i64>(&format!(
            "SELECT count(*)
             FROM shiba_internal.effect_stream_chunks
             WHERE stream_id = {}",
            stream_ids[0]
        ))
        .expect("low-ID stream chunks should be queryable")
        .expect("low-ID stream chunk count should not be NULL");
        let later_stream_chunks = Spi::get_one::<i64>(&format!(
            "SELECT count(*)
             FROM shiba_internal.effect_stream_chunks
             WHERE stream_id = {}",
            stream_ids[64]
        ))
        .expect("later stream chunks should be queryable")
        .expect("later stream chunk count should not be NULL");
        assert_eq!(low_stream_chunks, 1);
        assert_eq!(later_stream_chunks, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication;
    use crate::runtime::ingress::{
        reduce_feedback, replication_status_due, FeedbackOperation, FeedbackState,
        REPLICATION_STATUS_INTERVAL,
    };
    use crate::runtime::scheduler::{drain_has_capacity, rotate_after_cursor, DeterministicLru};

    #[test]
    fn replication_status_is_periodic_but_reply_requests_bypass_the_interval() {
        let last_update = std::time::Instant::now();
        assert!(!replication_status_due(
            last_update,
            last_update + REPLICATION_STATUS_INTERVAL - Duration::from_millis(1),
            false,
        ));
        assert!(replication_status_due(
            last_update,
            last_update + REPLICATION_STATUS_INTERVAL,
            false,
        ));
        assert!(replication_status_due(
            last_update,
            last_update + Duration::from_millis(1),
            true,
        ));
    }

    #[test]
    fn feedback_reducer_preserves_order_across_nonblocking_writes() {
        let started = std::time::Instant::now();
        let initial = FeedbackState {
            recorded_feedback_lsn: 0,
            pending_feedback: Some(200),
            queued_feedback: Some(100),
            last_status_update: started,
        };

        let first_wait = reduce_feedback(
            initial,
            FeedbackOperation::FlushQueued,
            replication::WriteStatus::PendingFlush,
            started + Duration::from_millis(1),
        );
        assert_eq!(first_wait.state, initial);
        assert_eq!(first_wait.catalog_record, None);
        let second_wait = reduce_feedback(
            first_wait.state,
            FeedbackOperation::FlushQueued,
            replication::WriteStatus::PendingFlush,
            started + Duration::from_millis(2),
        );
        assert_eq!(second_wait, first_wait);

        let first_flush = reduce_feedback(
            second_wait.state,
            FeedbackOperation::FlushQueued,
            replication::WriteStatus::Flushed,
            started + Duration::from_millis(3),
        );
        assert_eq!(first_flush.state.recorded_feedback_lsn, 100);
        assert_eq!(first_flush.state.queued_feedback, None);
        assert_eq!(first_flush.state.pending_feedback, Some(200));
        assert_eq!(first_flush.catalog_record, Some(100));

        let send_blocked = reduce_feedback(
            first_flush.state,
            FeedbackOperation::SendPending,
            replication::WriteStatus::WouldBlock,
            started + Duration::from_millis(4),
        );
        assert_eq!(send_blocked.state, first_flush.state);
        assert_eq!(send_blocked.catalog_record, None);
        let still_blocked = reduce_feedback(
            send_blocked.state,
            FeedbackOperation::SendPending,
            replication::WriteStatus::WouldBlock,
            started + Duration::from_millis(5),
        );
        assert_eq!(still_blocked, send_blocked);
        let send_queued = reduce_feedback(
            still_blocked.state,
            FeedbackOperation::SendPending,
            replication::WriteStatus::PendingFlush,
            started + Duration::from_millis(6),
        );
        assert_eq!(send_queued.state.recorded_feedback_lsn, 100);
        assert_eq!(send_queued.state.pending_feedback, None);
        assert_eq!(send_queued.state.queued_feedback, Some(200));
        assert_eq!(
            send_queued.state.last_status_update,
            started + Duration::from_millis(6)
        );
        assert_eq!(send_queued.catalog_record, None);

        let second_flush = reduce_feedback(
            send_queued.state,
            FeedbackOperation::FlushQueued,
            replication::WriteStatus::Flushed,
            started + Duration::from_millis(7),
        );
        assert_eq!(second_flush.state.recorded_feedback_lsn, 200);
        assert_eq!(second_flush.state.queued_feedback, None);
        assert_eq!(second_flush.catalog_record, Some(200));

        let mut heartbeat = second_flush.state;
        heartbeat.queue(200);
        let repeated = reduce_feedback(
            heartbeat,
            FeedbackOperation::SendPending,
            replication::WriteStatus::Flushed,
            started + Duration::from_millis(8),
        );
        assert_eq!(repeated.state.recorded_feedback_lsn, 200);
        assert_eq!(repeated.state.pending_feedback, None);
        assert_eq!(repeated.catalog_record, None);
    }

    #[test]
    fn runtime_wakeup_is_deduplicated_and_prepare_clears_it() {
        PENDING_RUNTIME_WAKE_PID.store(0, Ordering::Release);
        assert!(wake_runtime_on_commit(101));
        assert!(wake_runtime_on_commit(202));
        assert_eq!(PENDING_RUNTIME_WAKE_PID.load(Ordering::Acquire), 202);
        unsafe {
            wakeup::runtime_wakeup_xact_callback(
                pg_sys::XactEvent::XACT_EVENT_PREPARE,
                std::ptr::null_mut(),
            );
        }
        assert_eq!(PENDING_RUNTIME_WAKE_PID.load(Ordering::Acquire), 0);
    }

    #[test]
    fn drain_budget_stops_at_item_limit() {
        assert!(drain_has_capacity(
            15,
            Duration::ZERO,
            16,
            Duration::from_millis(50)
        ));
        assert!(!drain_has_capacity(
            16,
            Duration::ZERO,
            16,
            Duration::from_millis(50)
        ));
    }

    #[test]
    fn drain_budget_stops_at_time_limit() {
        assert!(drain_has_capacity(
            0,
            Duration::from_millis(49),
            16,
            Duration::from_millis(50)
        ));
        assert!(!drain_has_capacity(
            0,
            Duration::from_millis(50),
            16,
            Duration::from_millis(50)
        ));
    }

    #[test]
    fn ready_results_rotate_after_previous_cursor() {
        let mut result_oids: Vec<_> = [10, 20, 30, 40]
            .into_iter()
            .map(pg_sys::Oid::from)
            .collect();
        rotate_after_cursor(&mut result_oids, Some(pg_sys::Oid::from(20)));
        assert_eq!(
            result_oids,
            [30, 40, 10, 20]
                .into_iter()
                .map(pg_sys::Oid::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn ready_result_rotation_handles_missing_cursor() {
        let mut result_oids: Vec<_> = [10, 20, 30].into_iter().map(pg_sys::Oid::from).collect();
        rotate_after_cursor(&mut result_oids, Some(pg_sys::Oid::from(25)));
        assert_eq!(
            result_oids,
            [30, 10, 20]
                .into_iter()
                .map(pg_sys::Oid::from)
                .collect::<Vec<_>>()
        );

        rotate_after_cursor(&mut result_oids, None);
        assert_eq!(
            result_oids,
            [30, 10, 20]
                .into_iter()
                .map(pg_sys::Oid::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn runtime_cache_evicts_the_least_recently_used_dag() {
        let mut cache = DeterministicLru::new(2);
        assert!(cache.insert(20_u32, "twenty").is_empty());
        assert!(cache.insert(10_u32, "ten").is_empty());
        assert_eq!(cache.get(&20), Some(&"twenty"));

        assert_eq!(cache.insert(30, "thirty"), vec![(10, "ten")]);
        assert!(cache.contains_key(&20));
        assert!(cache.contains_key(&30));
        assert!(!cache.contains_key(&10));
    }

    #[test]
    fn runtime_cache_capacity_shrink_is_deterministic() {
        let mut cache = DeterministicLru::new(4);
        assert!(cache.insert(30_u32, "thirty").is_empty());
        assert!(cache.insert(10_u32, "ten").is_empty());
        assert!(cache.insert(20_u32, "twenty").is_empty());
        assert_eq!(cache.get(&30), Some(&"thirty"));

        assert_eq!(cache.set_capacity(1), vec![(10, "ten"), (20, "twenty")]);
        assert_eq!(cache.get(&30), Some(&"thirty"));
    }

    #[test]
    fn runtime_cache_replacement_refreshes_recency() {
        let mut cache = DeterministicLru::new(2);
        assert!(cache.insert(10_u32, "old").is_empty());
        assert!(cache.insert(20_u32, "twenty").is_empty());
        assert!(cache.insert(10_u32, "new").is_empty());

        assert_eq!(cache.insert(30, "thirty"), vec![(20, "twenty")]);
        assert_eq!(cache.get(&10), Some(&"new"));
    }
}
