//! Runtime ingress state and feedback reduction.

use crate::postgres::{format_lsn, parse_lsn};
use crate::{admission, config, ingress, publication, replication};
use pgrx::bgworkers::*;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use std::time::{Duration, Instant};

pub(crate) const REPLICATION_STATUS_INTERVAL: Duration = Duration::from_millis(250);

pub(crate) struct IngressRuntime {
    pub(crate) generation: i64,
    pub(crate) ingress: crate::ingress::ReplicationIngress,
    pub(crate) persisted_lsn: u64,
    pub(crate) feedback: FeedbackState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FeedbackState {
    // Periodic status messages may repeat persisted_lsn. Keep the catalog
    // watermark in memory so those transport heartbeats stay catalog-free.
    pub(crate) recorded_feedback_lsn: u64,
    pub(crate) pending_feedback: Option<u64>,
    pub(crate) queued_feedback: Option<u64>,
    pub(crate) last_status_update: Instant,
}

impl FeedbackState {
    pub(crate) fn queue(&mut self, feedback_lsn: u64) {
        self.pending_feedback = Some(
            self.pending_feedback
                .map_or(feedback_lsn, |pending| pending.max(feedback_lsn)),
        );
    }

    pub(crate) fn advance_recorded_feedback(&mut self, feedback_lsn: u64) -> Option<u64> {
        if feedback_lsn == 0 || feedback_lsn <= self.recorded_feedback_lsn {
            return None;
        }
        self.recorded_feedback_lsn = feedback_lsn;
        Some(feedback_lsn)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FeedbackOperation {
    FlushQueued,
    SendPending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FeedbackTransition {
    pub(crate) state: FeedbackState,
    pub(crate) catalog_record: Option<u64>,
}

pub(crate) fn replication_status_due(
    last_status_update: Instant,
    now: Instant,
    reply_requested: bool,
) -> bool {
    reply_requested
        || now.saturating_duration_since(last_status_update) >= REPLICATION_STATUS_INTERVAL
}

pub(crate) fn reduce_feedback(
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

struct IngressBootstrap {
    generation: i64,
    slot_name: String,
    start_lsn: u64,
    confirmed_lsn: u64,
}

pub(crate) fn initialize() -> IngressRuntime {
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
    Spi::run_with_args(
        "SELECT shiba_internal.reconcile_postmaster_restart($1)",
        &generation_argument,
    )
    .expect("Shiba could not reconcile ingress state after postmaster restart");
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

pub(crate) fn ingest_and_publish_once(runtime: &mut IngressRuntime) -> Option<usize> {
    if !BackgroundWorker::transaction(super::scheduler::runtime_is_active) {
        return None;
    }

    maintain_feedback(runtime, false);
    let publication = publish_source_once(runtime.generation);
    advance_publication_frontier(runtime.generation);
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
        max_poll_time: Duration::from_millis(50),
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
                    super::test_failpoints::claim(
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
                maintain_feedback(runtime, false);
            }
            1
        }
        ingress::IngressPoll::NoBatch {
            reply_requested,
            progressed,
        } => {
            if reply_requested {
                maintain_feedback(runtime, true);
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
            if let Some(pause) = super::test_failpoints::claim(
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
            super::test_failpoints::claim(
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

fn advance_publication_frontier(generation: i64) {
    let generation_argument = unsafe { [DatumWithOid::new(generation, pg_sys::INT8OID)] };
    BackgroundWorker::transaction(|| {
        Spi::run_with_args(
            "SELECT shiba_internal.advance_ingress_publication_frontier($1)",
            &generation_argument,
        )
        .expect("Shiba could not advance its source publication frontier");
    });
}

fn maintain_feedback(runtime: &mut IngressRuntime, reply_requested: bool) {
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
