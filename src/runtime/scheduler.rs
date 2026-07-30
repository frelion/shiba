//! Runtime scheduler and operator execution.

use crate::runtime::{gc, ingress};
use crate::{config, planner};
use pgrx::bgworkers::*;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

const RUNTIME_IDLE_WAIT: Duration = Duration::from_millis(25);
const OPERATOR_MAX_STEPS_PER_ROUND: usize = 64;
const OPERATOR_TIME_BUDGET: Duration = Duration::from_millis(50);
const OPERATOR_MAX_TRANSITIONS_PER_TRANSACTION: usize = 64;
const LAUNCH_TRANSACTION_WAIT: Duration = Duration::from_millis(10);

pub(crate) fn run(database_name: &str, launch_xid: &str, launch_generation: i64) {
    BackgroundWorker::connect_worker_to_spi(Some(database_name), None);
    if !wait_for_launch_transaction(launch_xid)
        || !wait_to_claim_runtime_identity(launch_xid, launch_generation)
    {
        return;
    }
    BackgroundWorker::transaction(configure_runtime_session);
    let mut ingress_runtime = ingress::initialize();

    log!("Shiba Runtime started for database {database_name}");
    let mut loaded_dataflows = DeterministicLru::<pg_sys::Oid, planner::LoadedDataflow>::new(
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

        let Some(ingress_work) = ingress::ingest_and_publish_once(&mut ingress_runtime) else {
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
                gc::collect(ingress_runtime.generation, &mut effect_stream_gc_cursor)
            }));
            next_gc = Instant::now() + gc::GC_INTERVAL;
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

pub(crate) fn current_transaction_id() -> String {
    Spi::get_one::<String>("SELECT pg_current_xact_id()::text")
        .expect("Shiba could not identify the launch transaction")
        .expect("pg_current_xact_id() returned NULL")
}

pub(crate) fn current_database_name() -> String {
    Spi::get_one::<String>("SELECT current_database()::text")
        .expect("Shiba could not identify the current database")
        .expect("current_database() returned NULL")
}

fn step_ready_operators_bounded(
    loaded_dataflows: &mut DeterministicLru<pg_sys::Oid, planner::LoadedDataflow>,
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
            planner::StepOutcome::Progress | planner::StepOutcome::Yield
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
    loaded_dataflows: &mut DeterministicLru<pg_sys::Oid, planner::LoadedDataflow>,
    result_oid: pg_sys::Oid,
) -> Option<(planner::StepOutcome, bool)> {
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
            let budget = planner::WorkBudget::new(
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
            if let Some(pause) = super::test_failpoints::claim(
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
        return Some((planner::StepOutcome::Idle, has_more));
    };

    #[cfg(any(test, feature = "pg_test"))]
    if let Some(stage_id) = committed_stage {
        let stage_id = i32::try_from(stage_id).expect("operator stage ID exceeds integer");
        let pause = BackgroundWorker::transaction(|| {
            super::test_failpoints::claim(
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

pub(crate) fn rotate_after_cursor(result_oids: &mut [pg_sys::Oid], cursor: Option<pg_sys::Oid>) {
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
pub(crate) struct DeterministicLru<K, V> {
    entries: HashMap<K, CachedValue<V>>,
    capacity: usize,
    access_sequence: u64,
}

impl<K, V> DeterministicLru<K, V>
where
    K: Copy + Eq + Hash,
{
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "LRU capacity must be positive");
        Self {
            entries: HashMap::new(),
            capacity,
            access_sequence: 0,
        }
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    #[cfg(test)]
    pub(crate) fn get(&mut self, key: &K) -> Option<&V> {
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

    pub(crate) fn insert(&mut self, key: K, value: V) -> Vec<(K, V)> {
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

    pub(crate) fn set_capacity(&mut self, capacity: usize) -> Vec<(K, V)> {
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

impl DeterministicLru<pg_sys::Oid, planner::LoadedDataflow> {
    fn get_or_load(
        &mut self,
        result_oid: pg_sys::Oid,
    ) -> Result<Option<&mut planner::LoadedDataflow>, String> {
        if !self.contains_key(&result_oid) {
            let dataflow = planner::LoadedDataflow::load(result_oid)?;
            let _ = self.insert(result_oid, dataflow);
        }
        Ok(self.get_mut(&result_oid))
    }
}

pub(crate) fn drain_has_capacity(
    processed: usize,
    elapsed: Duration,
    max_items: usize,
    time_budget: Duration,
) -> bool {
    processed < max_items && elapsed < time_budget
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

pub(crate) fn runtime_is_active() -> bool {
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
