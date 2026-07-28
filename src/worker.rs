//! One database-level Runtime background worker.
//!
//! WAL routing, DAG scheduling, relational operator execution, and change-log
//! garbage collection are bounded phases of one SPI-connected PostgreSQL
//! backend. A DAG runtime is plan metadata, never a process or thread.

use crate::{config, logical, pgoutput};
use pgrx::bgworkers::*;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use serde_json::{Map, Value};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const RUNTIME_IDLE_WAIT: Duration = Duration::from_millis(25);
const ROUTE_MAX_TRANSACTIONS_PER_ROUND: usize = 64;
const ROUTE_MAX_DECODED_CHANGES: i32 = 2048;
const ROUTE_TIME_BUDGET: Duration = Duration::from_millis(50);
const APPLY_MAX_TRANSACTIONS_PER_ROUND: usize = 64;
const APPLY_TIME_BUDGET: Duration = Duration::from_millis(50);
const GC_MAX_TRANSACTIONS_PER_ROUND: i32 = 64;
const GC_INTERVAL: Duration = Duration::from_millis(250);
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

#[pg_guard]
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

#[pg_guard]
#[unsafe(no_mangle)]
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

    log!("Shiba Runtime started for database {database_name}");
    let runtimes = Mutex::new(DeterministicLru::<pg_sys::Oid, logical::DagRuntime>::new(
        config::max_cached_dags(),
    ));
    let mut round_robin_cursor = None;
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

        let routed = match route_bounded_burst() {
            RuntimePhase::Inactive => break 'runtime,
            RuntimePhase::Worked(count) => count,
        };

        let Some(ready_dags) = BackgroundWorker::transaction(|| {
            if !runtime_is_active() {
                return None;
            }
            update_runtime_heartbeat();
            Some(ready_dag_oids(round_robin_cursor))
        }) else {
            break 'runtime;
        };
        let obsolete_runtimes = {
            let mut runtimes = runtimes
                .lock()
                .expect("Shiba DAG runtime cache mutex was poisoned");
            runtimes
                .set_capacity(config::max_cached_dags())
                .into_iter()
                .map(|(result_oid, runtime)| (result_oid, runtime.generation().to_owned()))
                .collect::<Vec<_>>()
        };
        if !obsolete_runtimes.is_empty() {
            BackgroundWorker::transaction(|| {
                for (result_oid, generation) in &obsolete_runtimes {
                    logical::release_physical_programs(*result_oid, generation)
                        .expect("Shiba could not release an obsolete physical program");
                }
            });
        }

        let apply_phase = apply_ready_dags_bounded(&runtimes, &mut round_robin_cursor, ready_dags);
        match apply_phase {
            ApplyPhase::RuntimeInactive | ApplyPhase::SignalReceived => break 'runtime,
            ApplyPhase::Worked | ApplyPhase::Idle => {}
        }

        let collected = if Instant::now() >= next_gc {
            let count = BackgroundWorker::transaction(gc_change_log);
            next_gc = Instant::now() + GC_INTERVAL;
            count
        } else {
            0
        };
        idle = routed == 0 && apply_phase != ApplyPhase::Worked && collected == 0;

        if !idle {
            // Each phase is bounded and each source transaction has already
            // committed. Give other PostgreSQL backends a scheduling chance.
            std::thread::yield_now();
        }
    }

    clear_runtime_owner();
    log!("Shiba Runtime stopped for database {database_name}");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimePhase {
    Inactive,
    Worked(usize),
}

fn route_bounded_burst() -> RuntimePhase {
    let started = Instant::now();
    let mut routed = 0;
    while drain_has_capacity(
        routed,
        started.elapsed(),
        ROUTE_MAX_TRANSACTIONS_PER_ROUND,
        ROUTE_TIME_BUDGET,
    ) {
        let next = BackgroundWorker::transaction(|| {
            if !runtime_is_active() {
                return None;
            }
            update_runtime_heartbeat();
            Some(peek_and_route_wal_transactions(
                ROUTE_MAX_TRANSACTIONS_PER_ROUND - routed,
            ))
        });
        match next {
            None => return RuntimePhase::Inactive,
            Some(Some(routed_burst)) => {
                // Routing and slot advancement are separate transactions. A
                // crash between them replays through routed_transactions.
                #[cfg(any(test, feature = "pg_test"))]
                if let Some(pause) = BackgroundWorker::transaction(|| {
                    test_failpoints::claim(
                        "runtime_route_before_slot_advance",
                        None,
                        Some(routed_burst.commit_lsn.as_str()),
                    )
                }) {
                    std::thread::sleep(pause);
                    panic!(
                        "Shiba test failpoint: runtime exited after routing and before slot advancement"
                    );
                }
                BackgroundWorker::transaction(|| advance_slot_through(&routed_burst.end_lsn));
                routed += routed_burst.transaction_count;
            }
            Some(None) => break,
        }
    }
    RuntimePhase::Worked(routed)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DagStep {
    RuntimeInactive,
    Inactive,
    Retry,
    Processed { has_more: bool },
    ResourceBlocked,
    Quarantined,
    Idle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApplyPhase {
    RuntimeInactive,
    SignalReceived,
    Worked,
    Idle,
}

fn apply_ready_dags_bounded(
    runtimes: &Mutex<DeterministicLru<pg_sys::Oid, logical::DagRuntime>>,
    round_robin_cursor: &mut Option<pg_sys::Oid>,
    mut ready_dags: Vec<pg_sys::Oid>,
) -> ApplyPhase {
    rotate_after_cursor(&mut ready_dags, *round_robin_cursor);
    let mut ready_dags = VecDeque::from(ready_dags);
    let started = Instant::now();
    let mut attempted = 0;
    let mut worked = 0;

    while drain_has_capacity(
        attempted,
        started.elapsed(),
        APPLY_MAX_TRANSACTIONS_PER_ROUND,
        APPLY_TIME_BUDGET,
    ) {
        let Some(result_oid) = ready_dags.pop_front() else {
            break;
        };
        if !BackgroundWorker::wait_latch(Some(Duration::ZERO))
            || BackgroundWorker::sigint_received()
        {
            return ApplyPhase::SignalReceived;
        }
        reload_config_if_requested();

        *round_robin_cursor = Some(result_oid);
        attempted += 1;
        let step = apply_one_dag_transaction(runtimes, result_oid);
        match step {
            DagStep::RuntimeInactive => return ApplyPhase::RuntimeInactive,
            DagStep::Processed { has_more } => {
                worked += 1;
                if has_more {
                    // Routing does not run during this bounded phase. Requeue a
                    // DAG with known backlog at the tail to preserve fairness
                    // without opening a later empty apply transaction.
                    ready_dags.push_back(result_oid);
                }
            }
            DagStep::ResourceBlocked | DagStep::Quarantined => {
                worked += 1;
            }
            // Retry once in this round, then give other DAGs and Runtime phases
            // a chance before taking a fresh transaction snapshot.
            DagStep::Retry | DagStep::Inactive | DagStep::Idle => {}
        }
    }

    if worked == 0 {
        ApplyPhase::Idle
    } else {
        ApplyPhase::Worked
    }
}

fn apply_one_dag_transaction(
    runtimes: &Mutex<DeterministicLru<pg_sys::Oid, logical::DagRuntime>>,
    result_oid: pg_sys::Oid,
) -> DagStep {
    let step = BackgroundWorker::transaction(|| {
        let mut runtimes = runtimes
            .lock()
            .expect("Shiba DAG runtime cache mutex was poisoned");
        if !runtime_is_active() {
            return DagStep::RuntimeInactive;
        }
        if !dag_is_active(result_oid) {
            if let Some(runtime) = runtimes.remove(&result_oid) {
                runtime
                    .release_physical_programs()
                    .expect("Shiba could not release an inactive physical program");
            }
            return DagStep::Inactive;
        }
        let generation =
            dag_generation(result_oid).expect("active Shiba DAG has no runtime generation");
        if runtimes
            .peek(&result_oid)
            .is_some_and(|runtime| !runtime.matches_generation(&generation))
        {
            if let Some(runtime) = runtimes.remove(&result_oid) {
                runtime
                    .release_physical_programs()
                    .expect("Shiba could not release a superseded physical program");
            }
        }
        if !runtimes.contains_key(&result_oid) {
            match logical::DagRuntime::load(result_oid) {
                Ok(logical::LoadOutcome::Loaded(runtime)) => {
                    for (_, evicted) in runtimes.insert(result_oid, runtime) {
                        evicted
                            .release_physical_programs()
                            .expect("Shiba could not release an evicted physical program");
                    }
                }
                Ok(logical::LoadOutcome::Retry) => return DagStep::Retry,
                Ok(logical::LoadOutcome::Quarantined) => return DagStep::Quarantined,
                Err(error) => {
                    logical::DagRuntime::quarantine(result_oid, &error)
                        .expect("Shiba could not quarantine an unloadable DAG");
                    return DagStep::Quarantined;
                }
            }
        }
        let outcome = {
            let runtime = runtimes
                .get(&result_oid)
                .expect("Shiba DAG runtime cache lost a loaded DAG");
            process_next_dag_transaction(result_oid, runtime)
        };
        if matches!(
            outcome,
            DagStep::Inactive | DagStep::ResourceBlocked | DagStep::Quarantined
        ) {
            if let Some(runtime) = runtimes.remove(&result_oid) {
                runtime
                    .release_physical_programs()
                    .expect("Shiba could not release a stopped physical program");
            }
        }
        outcome
    });

    if matches!(step, DagStep::RuntimeInactive) {
        runtimes
            .lock()
            .expect("Shiba DAG runtime cache mutex was poisoned")
            .remove(&result_oid);
    }
    step
}

fn process_next_dag_transaction(result_oid: pg_sys::Oid, runtime: &logical::DagRuntime) -> DagStep {
    let apply_result = runtime
        .apply_next_transaction()
        .expect("Shiba could not execute the next DAG inbox transaction");
    match apply_result.outcome {
        logical::NextApplyOutcome::Retry => return DagStep::Retry,
        logical::NextApplyOutcome::ResourceBlocked => return DagStep::ResourceBlocked,
        logical::NextApplyOutcome::Quarantined => return DagStep::Quarantined,
        logical::NextApplyOutcome::Inactive => return DagStep::Inactive,
        logical::NextApplyOutcome::Idle => return DagStep::Idle,
        logical::NextApplyOutcome::Applied => {}
    }
    #[cfg(any(test, feature = "pg_test"))]
    {
        let commit_lsn = apply_result
            .commit_lsn
            .as_deref()
            .expect("applied Shiba DAG transaction returned no commit LSN");
        if let Some(pause) = test_failpoints::claim(
            "runtime_apply_before_ack",
            Some(result_oid),
            Some(commit_lsn),
        ) {
            log!(
                "Shiba test failpoint reached: runtime_apply_before_ack result {result_oid} commit {commit_lsn}"
            );
            std::thread::sleep(pause);
            panic!(
                "Shiba test failpoint: runtime exited after applying commit {commit_lsn} and before acknowledgement"
            );
        }
    }

    let result = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    let has_more = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM shiba_internal.dag_inbox
             WHERE result_oid = $1::oid
         )",
        &result,
    )
    .expect("Shiba could not inspect the remaining DAG inbox backlog")
    .expect("Shiba DAG inbox backlog check returned NULL");
    update_dag_last_scheduled(result_oid);
    DagStep::Processed { has_more }
}

fn gc_change_log() -> i64 {
    let collected = Spi::get_one_with_args::<i64>("SELECT shiba._gc_change_log($1)", unsafe {
        &[DatumWithOid::new(
            GC_MAX_TRANSACTIONS_PER_ROUND,
            pg_sys::INT4OID,
        )]
    })
    .expect("Shiba could not garbage-collect its change log")
    .unwrap_or(0);
    Spi::run("SELECT shiba_internal._compact_shared_fold_stages()")
        .expect("Shiba could not compact its empty shared fold Stages");
    collected
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

    fn peek(&self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|cached| &cached.value)
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        let sequence = self.next_sequence();
        let cached = self.entries.get_mut(key)?;
        cached.last_used = sequence;
        Some(&cached.value)
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

fn update_dag_last_scheduled(result_oid: pg_sys::Oid) {
    let result = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    Spi::run_with_args(
        "UPDATE shiba_internal.dag_runtime_state
         SET last_scheduled_at = clock_timestamp()
         WHERE result_oid = $1::oid",
        &result,
    )
    .expect("Shiba could not update DAG scheduling state");
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

fn dag_is_active(result_oid: pg_sys::Oid) -> bool {
    let arguments = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM shiba_internal.dag_runtime_state
             WHERE result_oid = $1::oid AND active
         )",
        &arguments,
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

fn dag_generation(result_oid: pg_sys::Oid) -> Option<String> {
    let arguments = unsafe { [DatumWithOid::new(result_oid, pg_sys::OIDOID)] };
    Spi::get_one_with_args::<String>(
        "SELECT physical.plan_id::text
         FROM shiba_internal.dag_runtime_state runtime
         JOIN shiba_internal.physical_plans physical USING(result_oid)
         WHERE runtime.result_oid = $1::oid AND runtime.active",
        &arguments,
    )
    .expect("Shiba could not inspect the DAG runtime generation")
}

fn ready_dag_oids(cursor: Option<pg_sys::Oid>) -> Vec<pg_sys::Oid> {
    let limit = i32::try_from(APPLY_MAX_TRANSACTIONS_PER_ROUND)
        .expect("apply transaction round limit exceeds integer");
    let mut ready = ready_dag_oids_in_range(cursor, true, limit);
    if cursor.is_some() && ready.len() < APPLY_MAX_TRANSACTIONS_PER_ROUND {
        let remaining = i32::try_from(APPLY_MAX_TRANSACTIONS_PER_ROUND - ready.len())
            .expect("remaining ready DAG limit exceeds integer");
        ready.extend(ready_dag_oids_in_range(cursor, false, remaining));
    }
    ready
}

fn ready_dag_oids_in_range(
    cursor: Option<pg_sys::Oid>,
    after_cursor: bool,
    limit: i32,
) -> Vec<pg_sys::Oid> {
    Spi::connect_mut(|client| {
        let (predicate, arguments) = match cursor {
            Some(cursor) => {
                let comparison = if after_cursor { ">" } else { "<=" };
                (
                    format!("AND runtime.result_oid {comparison} $1::oid"),
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
            "SELECT runtime.result_oid
             FROM shiba_internal.dag_runtime_state runtime
             WHERE runtime.active
               {predicate}
               AND EXISTS (
                   SELECT 1
                   FROM shiba_internal.dag_inbox inbox
                   WHERE inbox.result_oid = runtime.result_oid
               )
             ORDER BY runtime.result_oid
             LIMIT {limit_parameter}"
        );
        client
            .update(&query, None, &arguments)
            .expect("Shiba could not discover ready DAGs")
            .map(|row| {
                row.get::<pg_sys::Oid>(1)
                    .expect("invalid ready DAG OID")
                    .expect("NULL ready DAG OID")
            })
            .collect()
    })
}

/// Peek and durably route at most one complete source transaction.
///
/// The SPI cursor is detached between one-row fetches so pgrx can release each
/// tuple table. `Begin.final_lsn` gives the transaction LSN before its payload,
/// allowing each delta to be inserted directly into `change_log` without a
/// transaction-sized Rust collection.
fn peek_and_route_wal_transactions(max_transactions: usize) -> Option<RoutedBurst> {
    debug_assert!(max_transactions > 0);
    let cursor_name = Spi::connect_mut(|client| {
        client
            .open_cursor_mut(
                "SELECT data
                 FROM pg_logical_slot_peek_binary_changes(
                     shiba_internal.slot_name(), NULL, $1,
                     'proto_version', '1',
                     'publication_names', 'shiba_publication'
                 )",
                unsafe {
                    &[DatumWithOid::new(
                        ROUTE_MAX_DECODED_CHANGES,
                        pg_sys::INT4OID,
                    )]
                },
            )
            .detach_into_name()
    });

    let mut relations = HashMap::<u32, Vec<String>>::new();
    let mut route = None::<RouteTransaction>;
    let mut routed_through = None::<RoutedBurst>;
    let mut transaction_count = 0;

    while let Some(bytes) = fetch_cursor_message(&cursor_name) {
        match pgoutput::parse(&bytes).expect("Shiba received an unsupported pgoutput message") {
            pgoutput::Message::Begin { final_lsn, .. } => {
                if route.is_some() {
                    panic!("Shiba received a new logical transaction before commit");
                }
                let commit_lsn = format_lsn(final_lsn);
                let is_new = begin_route_transaction(&commit_lsn);
                route = Some(RouteTransaction {
                    commit_lsn,
                    next_sequence: 1,
                    is_new,
                });
            }
            pgoutput::Message::Relation { relid, columns } => {
                relations.insert(relid, columns);
            }
            pgoutput::Message::Insert { relid, row } => {
                let row = tuple_to_json(relid, row, &relations).expect("invalid inserted tuple");
                route_delta(&mut route, relid, 1, row);
            }
            pgoutput::Message::Update { relid, old, new } => {
                let old = tuple_to_json(relid, old, &relations).expect("invalid old tuple");
                route_delta(&mut route, relid, -1, old);
                let new = tuple_to_json(relid, new, &relations).expect("invalid new tuple");
                route_delta(&mut route, relid, 1, new);
            }
            pgoutput::Message::Delete { relid, old } => {
                let row = tuple_to_json(relid, old, &relations).expect("invalid deleted tuple");
                route_delta(&mut route, relid, -1, row);
            }
            pgoutput::Message::Commit {
                commit_lsn,
                end_lsn,
            } => {
                let transaction = route
                    .take()
                    .expect("Shiba received a logical commit without begin");
                assert_eq!(
                    transaction.commit_lsn,
                    format_lsn(commit_lsn),
                    "Shiba pgoutput begin/commit LSN mismatch"
                );
                if transaction.is_new {
                    canonicalize_route_transaction(&transaction.commit_lsn);
                }
                routed_through = Some(RoutedBurst {
                    #[cfg(any(test, feature = "pg_test"))]
                    commit_lsn: transaction.commit_lsn,
                    end_lsn: format_lsn(end_lsn),
                    transaction_count: transaction_count + 1,
                });
                transaction_count += 1;
                if transaction_count >= max_transactions {
                    break;
                }
            }
        }
    }
    if route.is_some() {
        panic!("Shiba logical decoding cursor ended before transaction commit");
    }
    routed_through
}

struct RouteTransaction {
    commit_lsn: String,
    next_sequence: i32,
    is_new: bool,
}

struct RoutedBurst {
    #[cfg(any(test, feature = "pg_test"))]
    commit_lsn: String,
    end_lsn: String,
    transaction_count: usize,
}

fn fetch_cursor_message(cursor_name: &str) -> Option<Vec<u8>> {
    Spi::connect_mut(|client| {
        let mut cursor = client
            .find_cursor(cursor_name)
            .expect("Shiba could not resume its logical decoding cursor");
        let message = {
            let table = cursor
                .fetch(1)
                .expect("Shiba could not fetch a logical decoding message");
            if table.is_empty() {
                None
            } else {
                table
                    .first()
                    .get_one::<Vec<u8>>()
                    .expect("Shiba received an invalid logical decoding bytea")
            }
        };
        if message.is_some() {
            cursor.detach_into_name();
        }
        message
    })
}

fn begin_route_transaction(commit_lsn: &str) -> bool {
    let checkpoint = unsafe { [DatumWithOid::new(commit_lsn, pg_sys::TEXTOID)] };
    Spi::get_one_with_args::<bool>(
        "SELECT shiba._begin_route_transaction($1::pg_lsn)",
        &checkpoint,
    )
    .expect("Shiba could not checkpoint a routed transaction")
    .expect("Shiba routing checkpoint returned NULL")
}

fn canonicalize_route_transaction(commit_lsn: &str) {
    let arguments = unsafe { [DatumWithOid::new(commit_lsn, pg_sys::TEXTOID)] };
    Spi::run_with_args(
        "SELECT shiba._canonicalize_change_log_commit($1::pg_lsn)",
        &arguments,
    )
    .expect("Shiba could not canonicalize a routed source transaction");
}

fn route_delta(transaction: &mut Option<RouteTransaction>, relid: u32, delta: i32, row: Value) {
    let transaction = transaction
        .as_mut()
        .expect("Shiba received a logical row outside a transaction");
    let sequence = transaction.next_sequence;
    transaction.next_sequence = sequence
        .checked_add(1)
        .expect("Shiba transaction has too many row deltas");
    if !transaction.is_new {
        return;
    }
    let row = row.to_string();
    let arguments = unsafe {
        [
            DatumWithOid::new(pg_sys::Oid::from(relid), pg_sys::OIDOID),
            DatumWithOid::new(row.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(delta, pg_sys::INT4OID),
            DatumWithOid::new(transaction.commit_lsn.as_str(), pg_sys::TEXTOID),
            DatumWithOid::new(sequence, pg_sys::INT4OID),
        ]
    };
    Spi::run_with_args(
        "SELECT shiba._route_change_log_delta($1, $2::jsonb, $3, $4, $5)",
        &arguments,
    )
    .expect("Shiba could not route a logical WAL delta");
}

fn advance_slot_through(commit_lsn: &str) {
    let arguments = unsafe { [DatumWithOid::new(commit_lsn, pg_sys::TEXTOID)] };
    Spi::connect_mut(|client| {
        client
            .update(
                "SELECT 1
                 FROM pg_logical_slot_get_binary_changes(
                     shiba_internal.slot_name(), $1::pg_lsn, NULL,
                     'proto_version', '1',
                     'publication_names', 'shiba_publication'
                 )",
                None,
                &arguments,
            )
            .expect("Shiba could not advance its durable logical replication checkpoint")
            .for_each(drop);
    });
}

/// Deterministic crash injection used only by pgrx and recovery-test builds.
#[cfg(any(test, feature = "pg_test"))]
mod test_failpoints {
    use super::*;

    pub(super) fn claim(
        kind: &str,
        result_oid: Option<pg_sys::Oid>,
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
        let commit_lsn = commit_lsn.unwrap_or("0/0");
        let arguments = unsafe {
            [
                DatumWithOid::new(kind, pg_sys::TEXTOID),
                DatumWithOid::new(result_oid, pg_sys::OIDOID),
                DatumWithOid::new(commit_lsn, pg_sys::TEXTOID),
            ]
        };
        let pause_ms = Spi::get_one_with_args::<i32>(
            "SELECT max(pause_ms)
             FROM public.shiba_runtime_failpoints
             WHERE kind = $1
               AND NOT fired
               AND (runtime_pid IS NULL OR runtime_pid = pg_backend_pid())
               AND (result_oid IS NULL OR result_oid = $2::oid)
               AND (commit_lsn IS NULL OR commit_lsn = $3::pg_lsn)",
            &arguments,
        )
        .expect("Shiba could not inspect its test worker failpoint");
        if pause_ms.is_some() {
            Spi::run_with_args(
                "UPDATE public.shiba_runtime_failpoints
                 SET runtime_pid = pg_backend_pid(),
                     commit_lsn = COALESCE(commit_lsn, $3::pg_lsn),
                     fired = true
                 WHERE kind = $1
                   AND NOT fired
                   AND (runtime_pid IS NULL OR runtime_pid = pg_backend_pid())
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
    fn ready_dags_rotate_after_previous_cursor() {
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
    fn ready_dag_rotation_handles_missing_cursor() {
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
        assert_eq!(cache.peek(&30), Some(&"thirty"));
    }

    #[test]
    fn runtime_cache_replacement_refreshes_recency() {
        let mut cache = DeterministicLru::new(2);
        assert!(cache.insert(10_u32, "old").is_empty());
        assert!(cache.insert(20_u32, "twenty").is_empty());
        assert!(cache.insert(10_u32, "new").is_empty());

        assert_eq!(cache.insert(30, "thirty"), vec![(20, "twenty")]);
        assert_eq!(cache.peek(&10), Some(&"new"));
    }
}
