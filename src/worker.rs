//! One database-level Runtime background worker.
//!
//! WAL ingestion, source publication, relational operator execution, and
//! garbage collection are bounded phases of one SPI-connected PostgreSQL
//! backend. Loaded dataflows are plan metadata, never processes or threads.

use crate::postgres::{format_lsn, parse_lsn};
use crate::{admission, config, ingress, logical, publication, replication};
use pgrx::bgworkers::*;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};

const RUNTIME_IDLE_WAIT: Duration = Duration::from_millis(25);
const OPERATOR_MAX_STEPS_PER_ROUND: usize = 64;
const OPERATOR_TIME_BUDGET: Duration = Duration::from_millis(50);
const OPERATOR_MAX_TRANSITIONS_PER_TRANSACTION: usize = 64;
const GC_MAX_TRANSACTIONS_PER_ROUND: i32 = 64;
const GC_MAX_EFFECT_STREAMS_PER_ROUND: i32 = 64;
const GC_MAX_EFFECT_CHUNKS_PER_STREAM: i32 = 64;
const GC_INTERVAL: Duration = Duration::from_millis(250);
const REPLICATION_STATUS_INTERVAL: Duration = Duration::from_millis(250);
const INGRESS_POLL_TIME_BUDGET: Duration = Duration::from_millis(50);
const LAUNCH_TRANSACTION_WAIT: Duration = Duration::from_millis(10);
#[cfg(not(test))]
const PROC_ARRAY_LWLOCK_INDEX_PG17: usize = 4;

// Backend-local: one pending wakeup per top-level source transaction,
// regardless of statement count or how many DAG triggers share a source.
static PENDING_RUNTIME_WAKE_PID: AtomicI32 = AtomicI32::new(0);

unsafe extern "C-unwind" fn runtime_sigterm(signal: i32) {
    unsafe {
        pg_sys::die(signal);
    }
}

/// Start the one Runtime process for the current database.
#[pg_extern]
pub fn start_runtime(launch_generation: i64) -> bool {
    if launch_generation <= 0 {
        return false;
    }
    let database_name = current_database_name();
    let launch_xid = current_transaction_id();
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
///
/// The SQL wrapper supplies the current Runtime owner PID from protected
/// catalog state. No row payload crosses this process-only signal.
#[pg_extern]
pub fn wake_runtime_on_commit(owner_pid: i32) -> bool {
    if owner_pid <= 0 {
        return false;
    }
    PENDING_RUNTIME_WAKE_PID.store(owner_pid, Ordering::Release);
    true
}

pub unsafe fn install_runtime_wakeup_callback() {
    pg_sys::RegisterXactCallback(Some(runtime_wakeup_xact_callback), std::ptr::null_mut());
}

#[cfg_attr(not(test), pg_guard)]
unsafe extern "C-unwind" fn runtime_wakeup_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_COMMIT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => {
            let owner_pid = PENDING_RUNTIME_WAKE_PID.swap(0, Ordering::AcqRel);
            if owner_pid > 0 {
                wake_backend_latch(owner_pid);
            }
        }
        pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PREPARE => {
            // COMMIT PREPARED can run in another backend. The Runtime's
            // bounded idle poll is the recovery path for that uncommon case.
            PENDING_RUNTIME_WAKE_PID.store(0, Ordering::Release);
        }
        _ => {}
    }
}

#[cfg(not(test))]
unsafe fn wake_backend_latch(owner_pid: i32) {
    // PostgreSQL 17's generated lwlocknames.h assigns ProcArrayLock index 4.
    // Shiba supports PG17 only; hold that lock from PID lookup through
    // SetLatch so a retiring PGPROC cannot be reused underneath the signal.
    let proc_array_lock =
        std::ptr::addr_of_mut!((*pg_sys::MainLWLockArray.add(PROC_ARRAY_LWLOCK_INDEX_PG17)).lock);
    pg_sys::LWLockAcquire(proc_array_lock, pg_sys::LWLockMode::LW_SHARED);
    let process = pg_sys::BackendPidGetProcWithLock(owner_pid);
    if !process.is_null() {
        pg_sys::SetLatch(std::ptr::addr_of_mut!((*process).procLatch));
    }
    pg_sys::LWLockRelease(proc_array_lock);
}

// Plain Rust unit-test executables are not loaded by a PostgreSQL postmaster
// and therefore cannot resolve or exercise its MainLWLockArray symbol.
#[cfg(test)]
unsafe fn wake_backend_latch(_owner_pid: i32) {}

#[cfg_attr(not(test), pg_guard)]
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C-unwind" fn shiba_runtime_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGINT);
    // Use PostgreSQL's normal backend SIGTERM handler. The pgrx deferred
    // handler only wakes our outer latch; if SIGTERM arrives inside logical
    // decoding, fast shutdown can wait forever because that C call never sees
    // ProcDiePending. `die` marks the current transaction for safe abort at
    // PostgreSQL's next interrupt check.
    unsafe {
        pg_sys::pqsignal(pg_sys::SIGTERM as i32, Some(runtime_sigterm));
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

    BackgroundWorker::connect_worker_to_spi(Some(database_name), None);
    if !wait_for_launch_transaction(launch_xid)
        || !wait_to_claim_runtime_identity(launch_xid, launch_generation)
    {
        return;
    }
    BackgroundWorker::transaction(configure_runtime_session);
    let mut ingress_runtime = initialize_ingress();

    log!("Shiba Runtime started for database {database_name}");
    let mut loaded_dataflows = DeterministicLru::<pg_sys::Oid, logical::LoadedDataflow>::new(
        config::max_cached_dataflows(),
    );
    let mut result_cursor = None;
    let mut effect_stream_gc_cursor = None;
    let mut idle = false;
    let mut next_gc = Instant::now();

    'runtime: loop {
        let wait = if idle {
            RUNTIME_IDLE_WAIT
        } else {
            Duration::ZERO
        };
        if !BackgroundWorker::wait_latch(Some(wait)) || BackgroundWorker::sigint_received() {
            break;
        }
        reload_config_if_requested();

        let Some(ingress_work) = ingest_and_publish_once(&mut ingress_runtime) else {
            break 'runtime;
        };

        let Some(ready_results) = BackgroundWorker::transaction(|| {
            if !runtime_is_active() {
                return None;
            }
            update_runtime_heartbeat();
            Some(ready_result_oids(result_cursor))
        }) else {
            break 'runtime;
        };
        let _ = loaded_dataflows.set_capacity(config::max_cached_dataflows());

        let operator_work = match step_ready_operators_bounded(
            &mut loaded_dataflows,
            &mut result_cursor,
            ready_results,
        ) {
            None => break 'runtime,
            Some(work) => work,
        };

        let collected = if Instant::now() >= next_gc {
            let count = BackgroundWorker::transaction(AssertUnwindSafe(|| {
                gc_change_log(ingress_runtime.generation)
                    + gc_effect_streams(&mut effect_stream_gc_cursor)
            }));
            next_gc = Instant::now() + GC_INTERVAL;
            count
        } else {
            0
        };
        idle = ingress_work == 0 && operator_work == 0 && collected == 0;

        if !idle {
            // Each phase is bounded and each source transaction has already
            // committed. Give other PostgreSQL backends a scheduling chance.
            std::thread::yield_now();
        }
    }

    clear_runtime_owner();
    log!("Shiba Runtime stopped for database {database_name}");
}

struct IngressRuntime {
    generation: i64,
    ingress: ingress::ReplicationIngress,
    persisted_lsn: u64,
    feedback: FeedbackState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FeedbackState {
    // Periodic status messages may repeat persisted_lsn. Keep the catalog
    // watermark in memory so those transport heartbeats stay catalog-free.
    recorded_feedback_lsn: u64,
    pending_feedback: Option<u64>,
    queued_feedback: Option<u64>,
    last_status_update: Instant,
}

impl FeedbackState {
    fn queue(&mut self, feedback_lsn: u64) {
        self.pending_feedback = Some(
            self.pending_feedback
                .map_or(feedback_lsn, |pending| pending.max(feedback_lsn)),
        );
    }

    fn advance_recorded_feedback(&mut self, feedback_lsn: u64) -> Option<u64> {
        if feedback_lsn == 0 || feedback_lsn <= self.recorded_feedback_lsn {
            return None;
        }
        self.recorded_feedback_lsn = feedback_lsn;
        Some(feedback_lsn)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FeedbackOperation {
    FlushQueued,
    SendPending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FeedbackTransition {
    state: FeedbackState,
    catalog_record: Option<u64>,
}

struct IngressBootstrap {
    generation: i64,
    slot_name: String,
    start_lsn: u64,
    confirmed_lsn: u64,
}

fn initialize_ingress() -> IngressRuntime {
    let conninfo = config::replication_conninfo()
        .unwrap_or_else(|| panic!("shiba.replication_conninfo must be configured for the Runtime"));
    let conninfo = conninfo
        .to_str()
        .expect("shiba.replication_conninfo is not valid UTF-8");
    let bootstrap = BackgroundWorker::transaction(bootstrap_ingress);
    let mut transport = replication::ReplicationTransport::connect(conninfo)
        .unwrap_or_else(|error| panic!("Shiba could not connect its replication client: {error}"));
    transport
        .start_replication(replication::StartReplicationOptions {
            slot: &bootstrap.slot_name,
            start_lsn: bootstrap.start_lsn,
            publication_names: &["shiba_publication"],
        })
        .unwrap_or_else(|error| panic!("Shiba could not start logical replication: {error}"));
    let now = Instant::now();
    IngressRuntime {
        generation: bootstrap.generation,
        ingress: ingress::ReplicationIngress::new(transport, config::max_cached_relations()),
        persisted_lsn: bootstrap.start_lsn,
        feedback: FeedbackState {
            recorded_feedback_lsn: bootstrap.confirmed_lsn,
            pending_feedback: None,
            queued_feedback: None,
            last_status_update: now.checked_sub(REPLICATION_STATUS_INTERVAL).unwrap_or(now),
        },
    }
}

fn bootstrap_ingress() -> IngressBootstrap {
    let slot_name = Spi::get_one::<String>("SELECT shiba_internal.slot_name()::text")
        .expect("Shiba could not read its logical slot name")
        .expect("Shiba logical slot name is NULL");
    let slot_argument = unsafe { [DatumWithOid::new(slot_name.as_str(), pg_sys::TEXTOID)] };
    let generation = Spi::get_one_with_args::<i64>(
        "SELECT slot_generation
         FROM shiba_internal.ensure_ingress_generation($1::name)",
        &slot_argument,
    )
    .expect("Shiba could not initialize its ingress generation")
    .expect("ingress generation initialization returned no row");
    let generation_argument = unsafe { [DatumWithOid::new(generation, pg_sys::INT8OID)] };
    let (persisted_lsn, confirmed_lsn) = Spi::get_two_with_args::<String, String>(
        "SELECT coalesce(persisted_lsn, '0/0'::pg_lsn)::text,
                coalesce(confirmed_lsn, '0/0'::pg_lsn)::text
         FROM shiba_internal.ingress_feedback_upper_bound($1)",
        &generation_argument,
    )
    .expect("Shiba could not read its durable ingress position");
    IngressBootstrap {
        generation,
        slot_name,
        start_lsn: parse_lsn(
            &persisted_lsn.expect("ingress generation has no durable feedback state"),
        )
        .expect("invalid durable ingress LSN"),
        confirmed_lsn: parse_lsn(
            &confirmed_lsn.expect("ingress generation has no confirmed feedback state"),
        )
        .expect("invalid confirmed ingress LSN"),
    }
}

fn ingest_and_publish_once(runtime: &mut IngressRuntime) -> Option<usize> {
    if !BackgroundWorker::transaction(runtime_is_active) {
        return None;
    }

    maintain_ingress_feedback(runtime, false);
    let publication = publish_source_once(runtime.generation);
    advance_ingress_publication_frontier(runtime.generation);
    let publication_work = usize::from(matches!(
        publication.outcome,
        publication::SourcePublicationOutcome::Appended
            | publication::SourcePublicationOutcome::Completed
            | publication::SourcePublicationOutcome::Discarded
    ));

    // A durable source task is always drained before more WAL is read. The
    // status heartbeat above still keeps the full-duplex replication
    // connection alive while this branch applies backpressure. If
    // every source-local head is backpressured, operator scheduling below can
    // consume already-published chunks and GC can release the low watermark.
    // This bounds change_log staging to the current ingress batch instead of
    // buffering an unbounded replication stream behind a slow DAG.
    if publication.has_pending {
        return Some(publication_work);
    }

    let budget = ingress::IngressBudget {
        max_events: config::batch_rows(),
        max_wire_bytes: config::batch_bytes(),
        max_poll_time: INGRESS_POLL_TIME_BUDGET,
    };
    let ingested = match runtime
        .ingress
        .poll_batch(budget)
        .unwrap_or_else(|error| panic!("Shiba ingress failed: {error}"))
    {
        ingress::IngressPoll::Batch(batch) => {
            let feedback_lsn =
                BackgroundWorker::transaction(|| persist_ingress_batch(runtime.generation, &batch));
            #[cfg(any(test, feature = "pg_test"))]
            if feedback_lsn.is_none() {
                if let Some(pause) = BackgroundWorker::transaction(|| {
                    let decode_lsn = format_lsn(batch.decode_end_lsn);
                    test_failpoints::claim(
                        "runtime_ingress_after_partial_batch",
                        None,
                        None,
                        Some(&decode_lsn),
                    )
                }) {
                    std::thread::sleep(pause);
                    panic!("Shiba test failpoint: Runtime exited after a partial ingress batch");
                }
            }
            if let Some(feedback_lsn) = feedback_lsn {
                runtime.persisted_lsn = runtime.persisted_lsn.max(feedback_lsn);
                runtime.feedback.queue(runtime.persisted_lsn);
                maintain_ingress_feedback(runtime, false);
            }
            1
        }
        ingress::IngressPoll::NoBatch {
            reply_requested,
            progressed,
        } => {
            if reply_requested {
                maintain_ingress_feedback(runtime, true);
            }
            usize::from(progressed)
        }
        ingress::IngressPoll::End => {
            panic!("Shiba replication connection ended unexpectedly")
        }
    };

    Some(ingested + publication_work)
}

fn publish_source_once(generation: i64) -> publication::SourcePublication {
    let publication = BackgroundWorker::transaction(|| {
        let publication = publication::publish_source_batch(generation).unwrap_or_else(|error| {
            panic!("Shiba could not publish a bounded source batch: {error}")
        });
        if matches!(
            publication.outcome,
            publication::SourcePublicationOutcome::Appended
                | publication::SourcePublicationOutcome::Completed
        ) && publication.final_lsn.is_none()
        {
            panic!("appended source chunk returned NULL causal LSN");
        }

        #[cfg(any(test, feature = "pg_test"))]
        if matches!(
            publication.outcome,
            publication::SourcePublicationOutcome::Appended
                | publication::SourcePublicationOutcome::Completed
        ) {
            if let Some(pause) = test_failpoints::claim(
                "source_publication_before_commit",
                None,
                None,
                publication.final_lsn.as_deref(),
            ) {
                log!(
                    "Shiba test failpoint reached: source_publication_before_commit at {}",
                    publication.final_lsn.as_deref().unwrap_or("unknown LSN")
                );
                std::thread::sleep(pause);
                panic!("Shiba test failpoint: Runtime exited before source publication commit");
            }
        }
        publication
    });

    #[cfg(any(test, feature = "pg_test"))]
    if matches!(
        publication.outcome,
        publication::SourcePublicationOutcome::Appended
            | publication::SourcePublicationOutcome::Completed
    ) {
        let pause = BackgroundWorker::transaction(|| {
            test_failpoints::claim(
                "source_publication_after_commit",
                None,
                None,
                publication.final_lsn.as_deref(),
            )
        });
        if let Some(pause) = pause {
            log!(
                "Shiba test failpoint reached: source_publication_after_commit at {}",
                publication.final_lsn.as_deref().unwrap_or("unknown LSN")
            );
            std::thread::sleep(pause);
            panic!("Shiba test failpoint: Runtime exited after source publication commit");
        }
    }

    publication
}

fn advance_ingress_publication_frontier(generation: i64) {
    let generation_argument = unsafe { [DatumWithOid::new(generation, pg_sys::INT8OID)] };
    BackgroundWorker::transaction(|| {
        Spi::run_with_args(
            "SELECT shiba_internal.advance_ingress_publication_frontier($1)",
            &generation_argument,
        )
        .expect("Shiba could not advance its source publication frontier");
    });
}

fn maintain_ingress_feedback(runtime: &mut IngressRuntime, reply_requested: bool) {
    let now = Instant::now();
    if runtime.feedback.queued_feedback.is_some() {
        let status = runtime
            .ingress
            .transport_mut()
            .flush()
            .unwrap_or_else(|error| panic!("Shiba could not flush replication feedback: {error}"));
        let transition = reduce_feedback(
            runtime.feedback,
            FeedbackOperation::FlushQueued,
            status,
            now,
        );
        apply_feedback_transition(runtime, transition);
        if status != replication::WriteStatus::Flushed {
            return;
        }
    }

    if replication_status_due(runtime.feedback.last_status_update, now, reply_requested) {
        runtime.feedback.queue(runtime.persisted_lsn);
    }

    let Some(feedback_lsn) = runtime.feedback.pending_feedback else {
        return;
    };
    let status = runtime
        .ingress
        .transport_mut()
        // Durable ingress is the receiver's write/flush point.  It is not the
        // DAG apply point; reporting it as apply could incorrectly satisfy a
        // synchronous_commit=remote_apply source transaction.
        .send_standby_status(feedback_lsn, feedback_lsn, 0, false)
        .unwrap_or_else(|error| panic!("Shiba could not send replication feedback: {error}"));
    let transition = reduce_feedback(
        runtime.feedback,
        FeedbackOperation::SendPending,
        status,
        now,
    );
    apply_feedback_transition(runtime, transition);
}

fn replication_status_due(
    last_status_update: Instant,
    now: Instant,
    reply_requested: bool,
) -> bool {
    reply_requested
        || now.saturating_duration_since(last_status_update) >= REPLICATION_STATUS_INTERVAL
}

fn reduce_feedback(
    mut state: FeedbackState,
    operation: FeedbackOperation,
    status: replication::WriteStatus,
    now: Instant,
) -> FeedbackTransition {
    let feedback_lsn = match operation {
        FeedbackOperation::FlushQueued => state
            .queued_feedback
            .expect("feedback reducer cannot flush an empty queue"),
        FeedbackOperation::SendPending => {
            assert!(
                state.queued_feedback.is_none(),
                "feedback reducer cannot send behind an unflushed status"
            );
            state
                .pending_feedback
                .expect("feedback reducer cannot send empty pending feedback")
        }
    };
    let mut catalog_record = None;
    match (operation, status) {
        (FeedbackOperation::FlushQueued, replication::WriteStatus::Flushed) => {
            state.queued_feedback = None;
            catalog_record = state.advance_recorded_feedback(feedback_lsn);
        }
        (FeedbackOperation::FlushQueued, replication::WriteStatus::PendingFlush) => {}
        (FeedbackOperation::FlushQueued, replication::WriteStatus::WouldBlock) => {
            panic!("libpq flush returned an impossible WouldBlock status")
        }
        (FeedbackOperation::SendPending, replication::WriteStatus::Flushed) => {
            state.pending_feedback = None;
            state.last_status_update = now;
            catalog_record = state.advance_recorded_feedback(feedback_lsn);
        }
        (FeedbackOperation::SendPending, replication::WriteStatus::PendingFlush) => {
            state.pending_feedback = None;
            state.queued_feedback = Some(feedback_lsn);
            state.last_status_update = now;
        }
        (FeedbackOperation::SendPending, replication::WriteStatus::WouldBlock) => {}
    }
    FeedbackTransition {
        state,
        catalog_record,
    }
}

fn apply_feedback_transition(runtime: &mut IngressRuntime, transition: FeedbackTransition) {
    if let Some(feedback_lsn) = transition.catalog_record {
        record_ingress_feedback(runtime.generation, feedback_lsn);
    }
    runtime.feedback = transition.state;
}

fn record_ingress_feedback(generation: i64, feedback_lsn: u64) {
    // Before the first decoded commit, a keepalive reply may flush PostgreSQL's
    // sentinel 0/0 position. It acknowledges no WAL and therefore has no
    // durable ingress fact to record.
    if feedback_lsn == 0 {
        return;
    }
    let feedback_lsn = format_lsn(feedback_lsn);
    BackgroundWorker::transaction(|| {
        let arguments = unsafe {
            [
                DatumWithOid::new(generation, pg_sys::INT8OID),
                DatumWithOid::new(feedback_lsn.as_str(), pg_sys::TEXTOID),
            ]
        };
        Spi::run_with_args(
            "SELECT shiba_internal.record_ingress_feedback($1, $2::pg_lsn)",
            &arguments,
        )
        .expect("Shiba could not record replication feedback");
    });
}

fn persist_ingress_batch(generation: i64, batch: &ingress::IngressBatch) -> Option<u64> {
    let transaction_start_lsn = format_lsn(batch.transaction_start_lsn);
    let claim_arguments = unsafe {
        [
            DatumWithOid::new(generation, pg_sys::INT8OID),
            DatumWithOid::new(i64::from(batch.source_xid), pg_sys::INT8OID),
            DatumWithOid::new(transaction_start_lsn.as_str(), pg_sys::TEXTOID),
        ]
    };
    let ingress_txn_id = Spi::get_one_with_args::<i64>(
        "SELECT ingress_txn_id
         FROM shiba_internal.claim_ingress_transaction($1, $2, $3::pg_lsn)",
        &claim_arguments,
    )
    .expect("Shiba could not claim a ingress transaction")
    .expect("ingress transaction claim returned no row");

    admission::insert_ingress_events(ingress_txn_id, &batch.events)
        .unwrap_or_else(|error| panic!("Shiba could not persist a bounded ingress batch: {error}"));

    let feedback_lsn = batch.boundary.as_ref().and_then(|boundary| match boundary {
        ingress::IngressBoundary::Commit { end_lsn, .. } => Some(*end_lsn),
        ingress::IngressBoundary::AbortTransaction { .. }
        | ingress::IngressBoundary::AbortSubtransaction { .. } => None,
    });

    match &batch.boundary {
        Some(ingress::IngressBoundary::Commit {
            commit_lsn,
            end_lsn,
        }) => {
            let commit_lsn = format_lsn(*commit_lsn);
            let end_lsn = format_lsn(*end_lsn);
            let arguments = unsafe {
                [
                    DatumWithOid::new(ingress_txn_id, pg_sys::INT8OID),
                    DatumWithOid::new(commit_lsn.as_str(), pg_sys::TEXTOID),
                    DatumWithOid::new(end_lsn.as_str(), pg_sys::TEXTOID),
                ]
            };
            Spi::run_with_args(
                "SELECT shiba_internal.commit_ingress_transaction(
                     $1, $2::pg_lsn, $3::pg_lsn
                 )",
                &arguments,
            )
            .expect("Shiba could not finalize a committed ingress transaction");
        }
        Some(ingress::IngressBoundary::AbortTransaction { abort_lsn }) => {
            let abort_lsn = format_lsn(*abort_lsn);
            let arguments = unsafe {
                [
                    DatumWithOid::new(ingress_txn_id, pg_sys::INT8OID),
                    DatumWithOid::new(abort_lsn.as_str(), pg_sys::TEXTOID),
                ]
            };
            Spi::run_with_args(
                "SELECT shiba_internal.abort_ingress_transaction($1, $2::pg_lsn)",
                &arguments,
            )
            .expect("Shiba could not finalize an aborted ingress transaction");
        }
        Some(ingress::IngressBoundary::AbortSubtransaction { source_subxid }) => {
            let arguments = unsafe {
                [
                    DatumWithOid::new(ingress_txn_id, pg_sys::INT8OID),
                    DatumWithOid::new(i64::from(*source_subxid), pg_sys::INT8OID),
                ]
            };
            Spi::run_with_args(
                "SELECT shiba_internal.abort_ingress_subtransaction($1, $2)",
                &arguments,
            )
            .expect("Shiba could not record an aborted source subtransaction");
        }
        None => {}
    }

    feedback_lsn
}

fn step_ready_operators_bounded(
    loaded_dataflows: &mut DeterministicLru<pg_sys::Oid, logical::LoadedDataflow>,
    result_cursor: &mut Option<pg_sys::Oid>,
    mut ready_results: Vec<pg_sys::Oid>,
) -> Option<usize> {
    rotate_after_cursor(&mut ready_results, *result_cursor);
    let mut ready_results = VecDeque::from(ready_results);
    let started = Instant::now();
    let mut attempted = 0;
    let mut worked = 0;

    while drain_has_capacity(
        attempted,
        started.elapsed(),
        OPERATOR_MAX_STEPS_PER_ROUND,
        OPERATOR_TIME_BUDGET,
    ) {
        let Some(result_oid) = ready_results.pop_front() else {
            break;
        };
        if !BackgroundWorker::wait_latch(Some(Duration::ZERO))
            || BackgroundWorker::sigint_received()
        {
            return None;
        }
        reload_config_if_requested();

        *result_cursor = Some(result_oid);
        attempted += 1;
        let (outcome, has_more) = step_one_operator(loaded_dataflows, result_oid)?;
        if matches!(
            outcome,
            logical::StepOutcome::Progress | logical::StepOutcome::Yield
        ) {
            worked += 1;
        }
        if has_more {
            ready_results.push_back(result_oid);
        }
    }

    Some(worked)
}

fn step_one_operator(
    loaded_dataflows: &mut DeterministicLru<pg_sys::Oid, logical::LoadedDataflow>,
    result_oid: pg_sys::Oid,
) -> Option<(logical::StepOutcome, bool)> {
    // A panic terminates this single-threaded worker, so its backend-local
    // cache cannot be observed in a partially updated state.
    let committed = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        if !runtime_is_active() {
            return None;
        }
        if !try_lock_dataflow_for_step(result_oid) {
            return Some((None, false, None::<u32>));
        }
        if !dataflow_is_active(result_oid) {
            loaded_dataflows.remove(&result_oid);
            return Some((None, false, None::<u32>));
        }
        let stage_id = loaded_dataflows
            .get_or_load(result_oid)
            .expect("Shiba could not load a dataflow from durable operator state")
            .and_then(|dataflow| next_ready_stage(result_oid, dataflow.stage_cursor()));
        let step = stage_id.map(|stage_id| {
            let budget = logical::WorkBudget::new(
                config::batch_rows(),
                config::batch_bytes(),
                config::batch_rows(),
                config::batch_bytes(),
            );
            loaded_dataflows
                .get_mut(&result_oid)
                .expect("Shiba loaded-dataflow cache lost an active dataflow")
                .step_quantum(
                    result_oid,
                    stage_id,
                    budget,
                    OPERATOR_MAX_TRANSITIONS_PER_TRANSACTION,
                )
                .expect("Shiba operator transaction quantum did not complete")
        });
        #[cfg(any(test, feature = "pg_test"))]
        if let Some(step) = step.filter(|step| step.transitions > 0) {
            let stage_id = i32::try_from(step.stage_id).expect("operator stage ID exceeds integer");
            if let Some(pause) = test_failpoints::claim(
                "operator_step_before_commit",
                Some(result_oid),
                Some(stage_id),
                None,
            ) {
                log!(
                    "Shiba test failpoint reached: operator_step_before_commit result {result_oid} stage {stage_id}"
                );
                std::thread::sleep(pause);
                panic!(
                    "Shiba test failpoint: Runtime exited before committing result {result_oid} stage {stage_id}"
                );
            }
        }
        let committed_stage = step
            .filter(|step| step.transitions > 0)
            .map(|step| step.stage_id);
        Some((
            step.map(|step| step.outcome),
            next_ready_stage(
                result_oid,
                loaded_dataflows
                    .get_mut(&result_oid)
                    .and_then(|dataflow| dataflow.stage_cursor()),
            )
            .is_some(),
            committed_stage,
        ))
    }))?;

    let (outcome, has_more, committed_stage) = committed;
    let Some(outcome) = outcome else {
        return Some((logical::StepOutcome::Idle, has_more));
    };

    #[cfg(any(test, feature = "pg_test"))]
    if let Some(stage_id) = committed_stage {
        let stage_id = i32::try_from(stage_id).expect("operator stage ID exceeds integer");
        let pause = BackgroundWorker::transaction(|| {
            test_failpoints::claim(
                "operator_step_after_commit",
                Some(result_oid),
                Some(stage_id),
                None,
            )
        });
        if let Some(pause) = pause {
            log!(
                "Shiba test failpoint reached: operator_step_after_commit result {result_oid} stage {stage_id}"
            );
            std::thread::sleep(pause);
            panic!(
                "Shiba test failpoint: Runtime exited after committing result {result_oid} stage {stage_id}"
            );
        }
    }
    #[cfg(not(any(test, feature = "pg_test")))]
    let _ = committed_stage;

    Some((outcome, has_more))
}

fn gc_change_log(generation: i64) -> i64 {
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

fn gc_effect_streams(cursor: &mut Option<i64>) -> i64 {
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

fn rotate_after_cursor(result_oids: &mut [pg_sys::Oid], cursor: Option<pg_sys::Oid>) {
    let Some(cursor) = cursor else {
        return;
    };
    let split = result_oids.partition_point(|result_oid| result_oid.to_u32() <= cursor.to_u32());
    result_oids.rotate_left(split);
}

fn reload_config_if_requested() {
    if BackgroundWorker::sighup_received() {
        unsafe {
            pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
        }
        BackgroundWorker::transaction(configure_runtime_session);
    }
}

pub(crate) fn configure_runtime_session() {
    let work_mem = config::format_kilobytes(config::runtime_work_mem_kb());
    let temp_file_limit = config::format_kilobytes(config::runtime_temp_file_limit_kb());
    let arguments = unsafe {
        [
            DatumWithOid::new(work_mem.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(temp_file_limit.as_str(), pg_sys::TEXTOID),
        ]
    };
    Spi::run_with_args(
        "SELECT set_config('plan_cache_mode', 'force_generic_plan', false),
                set_config('work_mem', $1, false),
                set_config('temp_file_limit', $2, false),
                set_config('hash_mem_multiplier', '1', false)",
        &arguments,
    )
    .expect("Shiba Runtime could not configure its PostgreSQL session");
}

struct CachedValue<V> {
    value: V,
    last_used: u64,
}

/// A deterministic backend-local LRU.
///
/// The monotonically increasing access sequence uniquely defines recency,
/// making capacity shrink and defensive clock rollover stable.
struct DeterministicLru<K, V> {
    entries: HashMap<K, CachedValue<V>>,
    capacity: usize,
    access_sequence: u64,
}

impl<K, V> DeterministicLru<K, V>
where
    K: Copy + Eq + Hash,
{
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "LRU capacity must be positive");
        Self {
            entries: HashMap::new(),
            capacity,
            access_sequence: 0,
        }
    }

    fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    #[cfg(test)]
    fn get(&mut self, key: &K) -> Option<&V> {
        let sequence = self.next_sequence();
        let cached = self.entries.get_mut(key)?;
        cached.last_used = sequence;
        Some(&cached.value)
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let sequence = self.next_sequence();
        let cached = self.entries.get_mut(key)?;
        cached.last_used = sequence;
        Some(&mut cached.value)
    }

    fn insert(&mut self, key: K, value: V) -> Vec<(K, V)> {
        let sequence = self.next_sequence();
        self.entries.insert(
            key,
            CachedValue {
                value,
                last_used: sequence,
            },
        );
        self.evict_to_capacity()
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        self.entries.remove(key).map(|cached| cached.value)
    }

    fn set_capacity(&mut self, capacity: usize) -> Vec<(K, V)> {
        assert!(capacity > 0, "LRU capacity must be positive");
        self.capacity = capacity;
        self.evict_to_capacity()
    }

    fn next_sequence(&mut self) -> u64 {
        if self.access_sequence == u64::MAX {
            let mut recency = self
                .entries
                .iter()
                .map(|(key, cached)| (*key, cached.last_used))
                .collect::<Vec<_>>();
            recency.sort_by_key(|(_, last_used)| *last_used);
            for (index, (key, _)) in recency.into_iter().enumerate() {
                self.entries
                    .get_mut(&key)
                    .expect("LRU key disappeared during clock normalization")
                    .last_used = u64::try_from(index + 1).expect("LRU cache is too large");
            }
            self.access_sequence =
                u64::try_from(self.entries.len()).expect("LRU cache is too large");
        }
        self.access_sequence += 1;
        self.access_sequence
    }

    fn evict_to_capacity(&mut self) -> Vec<(K, V)> {
        let mut evicted = Vec::new();
        while self.entries.len() > self.capacity {
            let key = self
                .entries
                .iter()
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(key, _)| *key)
                .expect("over-capacity LRU had no eviction candidate");
            let value = self
                .remove(&key)
                .expect("LRU eviction candidate disappeared");
            evicted.push((key, value));
        }
        evicted
    }
}

impl DeterministicLru<pg_sys::Oid, logical::LoadedDataflow> {
    fn get_or_load(
        &mut self,
        result_oid: pg_sys::Oid,
    ) -> Result<Option<&mut logical::LoadedDataflow>, String> {
        if !self.contains_key(&result_oid) {
            let dataflow = logical::LoadedDataflow::load(result_oid)?;
            let _ = self.insert(result_oid, dataflow);
        }
        Ok(self.get_mut(&result_oid))
    }
}

fn drain_has_capacity(
    processed: usize,
    elapsed: Duration,
    max_items: usize,
    time_budget: Duration,
) -> bool {
    processed < max_items && elapsed < time_budget
}

fn current_transaction_id() -> String {
    Spi::get_one::<String>("SELECT pg_current_xact_id()::text")
        .expect("Shiba could not identify the launch transaction")
        .expect("pg_current_xact_id() returned NULL")
}

fn wait_for_launch_transaction(launch_xid: &str) -> bool {
    let arguments = unsafe { [DatumWithOid::new(launch_xid, pg_sys::TEXTOID)] };
    loop {
        let status = BackgroundWorker::transaction(|| {
            Spi::get_one_with_args::<String>("SELECT pg_xact_status($1::xid8)", &arguments)
                .expect("Shiba could not inspect its launch transaction")
                .expect("pg_xact_status() returned NULL")
        });
        match status.as_str() {
            "committed" => return true,
            "aborted" => return false,
            "in progress" => {}
            unexpected => panic!("Shiba received unknown launch transaction status {unexpected}"),
        }
        if !BackgroundWorker::wait_latch(Some(LAUNCH_TRANSACTION_WAIT)) {
            return false;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IdentityClaim {
    Claimed,
    Retry,
    Rejected,
}

fn wait_to_claim_runtime_identity(launch_xid: &str, launch_generation: i64) -> bool {
    loop {
        match claim_runtime_identity(launch_xid, launch_generation) {
            IdentityClaim::Claimed => return true,
            IdentityClaim::Rejected => return false,
            IdentityClaim::Retry => {}
        }
        if !BackgroundWorker::wait_latch(Some(LAUNCH_TRANSACTION_WAIT)) {
            return false;
        }
    }
}

fn claim_runtime_identity(launch_xid: &str, launch_generation: i64) -> IdentityClaim {
    let arguments = unsafe {
        [
            DatumWithOid::new(launch_xid, pg_sys::TEXTOID),
            DatumWithOid::new(launch_generation, pg_sys::INT8OID),
        ]
    };
    BackgroundWorker::transaction(|| {
        let claimed = Spi::get_one::<bool>(
            "SELECT pg_try_advisory_lock(
                 shiba_internal.identity_lock_namespace(), 0
             )",
        )
        .expect("Shiba could not acquire the Runtime process identity")
        .expect("Runtime process identity lock returned NULL");
        if !claimed {
            let still_launchable = Spi::get_one_with_args::<bool>(
                "SELECT active
                        AND owner_pid IS NULL
                        AND launch_generation = $2
                        AND (
                          pending_launch_xid = $1::xid8
                          OR pending_launch_xid IS NULL
                        )
                 FROM shiba_internal.runtime_state
                 WHERE singleton",
                &arguments,
            )
            .expect("Shiba could not inspect the busy Runtime identity")
            .unwrap_or(false);
            // A live owner already holds the session lock, or a newer launch
            // generation superseded this automatic BGW restart. Do not leave
            // the stale registration waiting forever behind the singleton.
            return if still_launchable {
                IdentityClaim::Retry
            } else {
                IdentityClaim::Rejected
            };
        }
        // A replacement process from this dynamic registration carries the
        // original generation after the initial owner cleared pending_launch_xid.
        // A newer registration increments the generation and therefore rejects
        // this old process even after it acquires the released session lock.
        let recorded = Spi::get_one_with_args::<bool>(
            "UPDATE shiba_internal.runtime_state
             SET owner_pid = pg_backend_pid(),
                 started_at = clock_timestamp(),
                 last_heartbeat = clock_timestamp(),
                 pending_launch_xid = NULL,
                 pending_since = NULL
             WHERE singleton
               AND active
               AND launch_generation = $2
               AND (
                   pending_launch_xid = $1::xid8
                   OR pending_launch_xid IS NULL
               )
             RETURNING true",
            &arguments,
        )
        .expect("Shiba could not record the Runtime owner")
        .unwrap_or(false);
        if recorded {
            IdentityClaim::Claimed
        } else {
            let _ = Spi::run(
                "SELECT pg_advisory_unlock(
                     shiba_internal.identity_lock_namespace(), 0
                 )",
            );
            IdentityClaim::Rejected
        }
    })
}

fn clear_runtime_owner() {
    BackgroundWorker::transaction(|| {
        let _ = Spi::run(
            "UPDATE shiba_internal.runtime_state
             SET owner_pid = NULL, started_at = NULL, last_heartbeat = NULL
             WHERE singleton AND owner_pid = pg_backend_pid()",
        );
    });
}

fn update_runtime_heartbeat() {
    let _ = Spi::run(
        "UPDATE shiba_internal.runtime_state
         SET last_heartbeat = clock_timestamp()
         WHERE singleton
           AND owner_pid = pg_backend_pid()
           AND (last_heartbeat IS NULL
                OR last_heartbeat < clock_timestamp() - interval '1 second')",
    );
}

fn current_database_name() -> String {
    Spi::get_one::<String>("SELECT current_database()::text")
        .expect("Shiba could not identify the current database")
        .expect("current_database() returned NULL")
}

fn runtime_is_active() -> bool {
    Spi::get_one::<bool>(
        "SELECT to_regclass('shiba_internal.runtime_state') IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM shiba_internal.runtime_state
              WHERE singleton AND active
          )",
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

fn dataflow_is_active(result_oid: pg_sys::Oid) -> bool {
    let arguments = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM shiba_internal.dataflows
             WHERE result_oid = $1::oid AND active
         )",
        &arguments,
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

fn try_lock_dataflow_for_step(result_oid: pg_sys::Oid) -> bool {
    let arguments = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    Spi::get_one_with_args::<bool>(
        "SELECT pg_try_advisory_xact_lock(
           shiba_internal.dataflow_lock_key($1::oid)
         )",
        &arguments,
    )
    .expect("Shiba could not acquire the dataflow step lock")
    .expect("Shiba dataflow step lock returned NULL")
}

// This is the sole durable readiness predicate. It is deliberately used both
// to find runnable dataflows and to choose their next stage; Rust remembers
// only rotation cursors, never a shadow frontier or ready queue.
const DURABLE_STAGE_READY: &str = "
    (checkpoint.has_continuation OR EXISTS (
      SELECT 1
      FROM shiba_internal.effect_stream_consumers AS consumer
      JOIN shiba_internal.effect_streams AS input_stream
        ON input_stream.stream_id=consumer.stream_id
      WHERE consumer.result_oid=checkpoint.result_oid
        AND consumer.consumer_stage_id=checkpoint.stage_id
        AND (
          consumer.next_chunk_seq < input_stream.next_chunk_seq
          OR (input_stream.producer_kind='source' AND EXISTS (
            SELECT 1
            FROM shiba_internal.ingress_replay_state AS publication
            WHERE publication.slot_generation=input_stream.slot_generation
              AND publication.published_lsn IS NOT NULL
              AND consumer.consumed_frontier_lsn < publication.published_lsn
          ))
        )
    ))
    AND NOT EXISTS (
      SELECT 1
      FROM shiba_internal.effect_streams AS output_stream
      WHERE output_stream.producer_kind='operator'
        AND output_stream.producer_result_oid=checkpoint.result_oid
        AND output_stream.producer_stage_id=checkpoint.stage_id
        AND output_stream.backpressured
    )";

fn next_ready_stage(result_oid: pg_sys::Oid, cursor: Option<u32>) -> Option<u32> {
    let stage = next_ready_stage_in_range(result_oid, cursor, true).or_else(|| {
        cursor.and_then(|cursor| next_ready_stage_in_range(result_oid, Some(cursor), false))
    });
    stage.map(|stage| u32::try_from(stage).expect("ready stage ID is negative"))
}

fn next_ready_stage_in_range(
    result_oid: pg_sys::Oid,
    cursor: Option<u32>,
    after_cursor: bool,
) -> Option<i32> {
    let (cursor_predicate, arguments) = match cursor {
        Some(cursor) => {
            let comparison = if after_cursor { ">" } else { "<=" };
            (
                format!("AND checkpoint.stage_id {comparison} $2::integer"),
                unsafe {
                    vec![
                        DatumWithOid::new(result_oid, pg_sys::OIDOID),
                        DatumWithOid::new(
                            i32::try_from(cursor).expect("stage cursor exceeds integer"),
                            pg_sys::INT4OID,
                        ),
                    ]
                },
            )
        }
        None => (String::new(), unsafe {
            vec![DatumWithOid::new(result_oid, pg_sys::OIDOID)]
        }),
    };
    let query = format!(
        "SELECT checkpoint.stage_id
         FROM shiba_internal.operator_checkpoints AS checkpoint
         WHERE checkpoint.result_oid=$1::oid
           {cursor_predicate}
           AND {DURABLE_STAGE_READY}
         ORDER BY checkpoint.stage_id
         LIMIT 1"
    );
    Spi::connect_mut(|client| {
        let mut rows = client
            .update(&query, Some(1), &arguments)
            .expect("Shiba could not select a ready durable stage");
        rows.next().map(|row| {
            row.get::<i32>(1)
                .expect("invalid ready stage ID")
                .expect("NULL ready stage ID")
        })
    })
}

fn ready_result_oids(cursor: Option<pg_sys::Oid>) -> Vec<pg_sys::Oid> {
    let limit = i32::try_from(OPERATOR_MAX_STEPS_PER_ROUND)
        .expect("operator step round limit exceeds integer");
    let mut ready = ready_result_oids_in_range(cursor, true, limit);
    if cursor.is_some() && ready.len() < OPERATOR_MAX_STEPS_PER_ROUND {
        let remaining = i32::try_from(OPERATOR_MAX_STEPS_PER_ROUND - ready.len())
            .expect("remaining ready result limit exceeds integer");
        ready.extend(ready_result_oids_in_range(cursor, false, remaining));
    }
    ready
}

fn ready_result_oids_in_range(
    cursor: Option<pg_sys::Oid>,
    after_cursor: bool,
    limit: i32,
) -> Vec<pg_sys::Oid> {
    Spi::connect_mut(|client| {
        let (predicate, arguments) = match cursor {
            Some(cursor) => {
                let comparison = if after_cursor { ">" } else { "<=" };
                (
                    format!("AND dataflow.result_oid {comparison} $1::oid"),
                    unsafe {
                        vec![
                            DatumWithOid::new(cursor, pg_sys::OIDOID),
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
            "SELECT dataflow.result_oid
             FROM shiba_internal.dataflows dataflow
             WHERE dataflow.active
               {predicate}
               AND EXISTS (
                 SELECT 1
                 FROM shiba_internal.operator_checkpoints checkpoint
                 WHERE checkpoint.result_oid = dataflow.result_oid
                   AND {DURABLE_STAGE_READY}
               )
             ORDER BY dataflow.result_oid
             LIMIT {limit_parameter}"
        );
        client
            .update(&query, None, &arguments)
            .expect("Shiba could not discover ready operator graphs")
            .map(|row| {
                row.get::<pg_sys::Oid>(1)
                    .expect("invalid ready result OID")
                    .expect("NULL ready result OID")
            })
            .collect()
    })
}

/// Deterministic crash injection used only by pgrx and recovery-test builds.
#[cfg(any(test, feature = "pg_test"))]
mod test_failpoints {
    use super::*;

    pub(super) fn claim(
        kind: &str,
        result_oid: Option<pg_sys::Oid>,
        stage_id: Option<i32>,
        commit_lsn: Option<&str>,
    ) -> Option<Duration> {
        let available = Spi::get_one::<bool>(
            "SELECT to_regclass('public.shiba_runtime_failpoints') IS NOT NULL",
        )
        .ok()
        .flatten()
        .unwrap_or(false);
        if !available {
            return None;
        }

        let result_oid = result_oid.unwrap_or(pg_sys::InvalidOid);
        let stage_id = stage_id.unwrap_or(-1);
        let has_commit_lsn = commit_lsn.is_some();
        let commit_lsn = commit_lsn.unwrap_or("0/0");
        let arguments = unsafe {
            [
                DatumWithOid::new(kind, pg_sys::TEXTOID),
                DatumWithOid::new(result_oid, pg_sys::OIDOID),
                DatumWithOid::new(stage_id, pg_sys::INT4OID),
                DatumWithOid::new(commit_lsn, pg_sys::TEXTOID),
                DatumWithOid::new(has_commit_lsn, pg_sys::BOOLOID),
            ]
        };
        let pause_ms = Spi::get_one_with_args::<i32>(
            "SELECT max(pause_ms)
             FROM public.shiba_runtime_failpoints
             WHERE kind = $1
               AND NOT fired
               AND (runtime_pid IS NULL OR runtime_pid = pg_backend_pid())
               AND (result_oid IS NULL OR result_oid = $2::oid)
               AND (stage_id IS NULL OR stage_id = $3::integer)
               AND (
                 NOT $5::boolean
                 OR commit_lsn IS NULL
                 OR commit_lsn = $4::pg_lsn
               )",
            &arguments,
        )
        .expect("Shiba could not inspect its test worker failpoint");
        if pause_ms.is_some() {
            Spi::run_with_args(
                "UPDATE public.shiba_runtime_failpoints
                 SET runtime_pid = pg_backend_pid(),
                     stage_id = COALESCE(stage_id, NULLIF($3::integer, -1)),
                     commit_lsn = CASE
                       WHEN $5::boolean
                         THEN COALESCE(commit_lsn, $4::pg_lsn)
                       ELSE commit_lsn
                     END,
                     fired = true
                 WHERE kind = $1
                   AND NOT fired
                   AND (runtime_pid IS NULL OR runtime_pid = pg_backend_pid())
                   AND (result_oid IS NULL OR result_oid = $2::oid)
                   AND (stage_id IS NULL OR stage_id = $3::integer)
                   AND (
                     NOT $5::boolean
                     OR commit_lsn IS NULL
                     OR commit_lsn = $4::pg_lsn
                   )",
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

#[cfg(any(test, feature = "pg_test"))]
mod worker_catalog_tests {
    use super::*;

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
        assert_eq!(gc_effect_streams(&mut cursor), 64 * 64);
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

        assert_eq!(gc_effect_streams(&mut cursor), 64);
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

    #[test]
    fn replication_status_is_periodic_but_reply_requests_bypass_the_interval() {
        let last_update = Instant::now();
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
        let started = Instant::now();
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
            runtime_wakeup_xact_callback(
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
