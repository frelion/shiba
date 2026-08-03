use std::time::{Duration, Instant};

use postgres::{Client, NoTls};
use shiba_ingress::{
    BOOTSTRAP_CONNECTIONS_PER_GRAPH, BootstrapCatchupProgress, BootstrapOptions, PreparedRebuild,
    SnapshotProgress,
};
use shiba_runtime::MAX_BOOTSTRAP_BATCH_ROWS;

#[path = "m12_rebuild_admission/support.rs"]
#[allow(dead_code, unused_imports)]
mod admission;
#[path = "m12_rebuild_performance/support.rs"]
mod performance_support;

use admission::{RebuildFixture, TARGET_SLOT};
use performance_support::{
    assert_building, assert_differential, required, retained_wal_bytes, rss_kib,
};

const SNAPSHOT_ROWS: usize = 1_000_000;
const SNAPSHOT_BATCH_ROWS: usize = 10_000;
const CONCURRENT_WAL_CHANGES: usize = 10_000;

// Frozen before the first M12 measurement. Do not tune these from observed results.
const PREPARE_TIME_LIMIT: Duration = Duration::from_secs(10);
const HANDOFF_TIME_LIMIT: Duration = Duration::from_secs(10);
const SNAPSHOT_TIME_LIMIT: Duration = Duration::from_secs(12);
const MIN_SNAPSHOT_ROWS_PER_SECOND: f64 = 10_000.0;
const CATCHUP_TIME_LIMIT: Duration = Duration::from_secs(8);
const ACTIVATION_TIME_LIMIT: Duration = Duration::from_secs(2);
const REBUILD_TIME_LIMIT: Duration = Duration::from_secs(25);
const MAX_RSS_GROWTH_KIB: u64 = 128 * 1024;
const MAX_RETAINED_WAL_BYTES: i64 = 256 * 1024 * 1024;

#[test]
#[ignore = "requires the M12 rebuild performance integration gate"]
#[allow(clippy::too_many_lines, reason = "one frozen million-row M12 gate")]
fn million_row_active_source_rebuild_is_bounded_and_catches_up_exactly() {
    assert_eq!(MAX_BOOTSTRAP_BATCH_ROWS, SNAPSHOT_BATCH_ROWS);
    assert_eq!(BOOTSTRAP_CONNECTIONS_PER_GRAPH, 3);
    let database_url = required("SHIBA_M12_PERFORMANCE_DATABASE_URL");
    let replication_url = required("SHIBA_M12_PERFORMANCE_REPLICATION_URL");
    let (mut admin, active) =
        admission::establish_active_scalar_source(&database_url, &replication_url);
    let fixture = RebuildFixture::install(&mut admin, active.publication_oid);
    admin
        .batch_execute(&format!(
            "TRUNCATE target.events;
             INSERT INTO target.events
             SELECT id,
                    CASE WHEN id % 10 = 0 THEN NULL ELSE (id % 97)::bigint END
             FROM generate_series(1::bigint, {SNAPSHOT_ROWS}::bigint) AS rows(id);"
        ))
        .expect("install million-row rebuild target");

    let old_state = admin
        .query(
            "SELECT state_payload FROM shiba_internal.graph_node_state
             WHERE graph_id = 1 AND node_id IN (1, 3) ORDER BY node_id",
            &[],
        )
        .expect("read non-pristine operator state");
    assert_eq!(old_state.len(), 2);
    assert!(old_state.iter().all(|row| {
        let payload: Vec<u8> = row.get(0);
        i64::from_be_bytes(payload.try_into().expect("int8 node state")) > 0
    }));
    let old_continuations: i64 = admin
        .query_one(
            "SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id = 1",
            &[],
        )
        .expect("read pre-rebuild continuation")
        .get(0);
    assert!(old_continuations > 0);

    let options = BootstrapOptions::new(SNAPSHOT_BATCH_ROWS, Duration::from_secs(10))
        .expect("maximum bounded snapshot batch");
    let rss_baseline = rss_kib();
    let mut rss_peak = rss_baseline;
    let rebuild_started = Instant::now();
    let prepare_started = Instant::now();
    let prepared =
        PreparedRebuild::prepare(&database_url, &replication_url, &fixture.spec(), options)
            .expect("prepare million-row active rebuild");
    let prepare_elapsed = prepare_started.elapsed();
    assert!(
        prepare_elapsed <= PREPARE_TIME_LIMIT,
        "rebuild prepare took {prepare_elapsed:?}, exceeding frozen {PREPARE_TIME_LIMIT:?}"
    );
    assert_building(&mut admin);

    let handoff_started = Instant::now();
    let mut bootstrap = prepared
        .into_bootstrap()
        .expect("export exact rebuild snapshot");
    let handoff_elapsed = handoff_started.elapsed();
    assert!(
        handoff_elapsed <= HANDOFF_TIME_LIMIT,
        "slot/snapshot handoff took {handoff_elapsed:?}, exceeding frozen {HANDOFF_TIME_LIMIT:?}"
    );

    let writer_url = database_url.clone();
    let writer = std::thread::spawn(move || {
        let mut client = Client::connect(&writer_url, NoTls).expect("connect concurrent writer");
        client
            .batch_execute(
                "BEGIN;
                 UPDATE target.events SET payload = 7 WHERE id BETWEEN 1 AND 3334;
                 DELETE FROM target.events WHERE id BETWEEN 3335 AND 6667;
                 INSERT INTO target.events
                 SELECT id, 11
                 FROM generate_series(1000001::bigint, 1003333::bigint) AS rows(id);
                 COMMIT;",
            )
            .expect("commit exactly 10,000 rebuild-period WAL changes");
    });

    let scan_started = Instant::now();
    let mut scanned_rows = 0usize;
    let mut batches = 0usize;
    while let SnapshotProgress::BatchApplied { rows, .. } =
        bootstrap.scan_next().expect("scan bounded rebuild batch")
    {
        assert_eq!(rows, SNAPSHOT_BATCH_ROWS);
        assert!(rows <= MAX_BOOTSTRAP_BATCH_ROWS);
        scanned_rows += rows;
        batches += 1;
        if let Some(current) = rss_kib() {
            rss_peak = Some(rss_peak.unwrap_or(current).max(current));
        }
        if batches.is_multiple_of(10) {
            assert_building(&mut admin);
        }
    }
    let scan_elapsed = scan_started.elapsed();
    writer.join().expect("concurrent writer did not panic");
    assert_eq!(scanned_rows, SNAPSHOT_ROWS);
    assert_eq!(batches, SNAPSHOT_ROWS / SNAPSHOT_BATCH_ROWS);
    let scan_rate = f64::from(u32::try_from(scanned_rows).expect("row count fits u32"))
        / scan_elapsed.as_secs_f64();
    assert!(
        scan_elapsed <= SNAPSHOT_TIME_LIMIT,
        "million-row rebuild scan took {scan_elapsed:?}, exceeding frozen {SNAPSHOT_TIME_LIMIT:?}"
    );
    assert!(
        scan_rate >= MIN_SNAPSHOT_ROWS_PER_SECOND,
        "rebuild scan rate {scan_rate:.2} rows/s is below frozen {MIN_SNAPSHOT_ROWS_PER_SECOND:.2}"
    );
    let retained_after_scan = retained_wal_bytes(&mut admin, TARGET_SLOT);
    assert!(
        (0..=MAX_RETAINED_WAL_BYTES).contains(&retained_after_scan),
        "target slot retained {retained_after_scan} bytes after scan, exceeding frozen {MAX_RETAINED_WAL_BYTES}"
    );

    let mut catchup = bootstrap.into_catchup().expect("enter rebuild catch-up");
    let catchup_started = Instant::now();
    assert_eq!(
        catchup
            .catch_up_next()
            .expect("apply 10,000-change rebuild WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    let catchup_elapsed = catchup_started.elapsed();
    assert!(
        catchup_elapsed <= CATCHUP_TIME_LIMIT,
        "10,000-change rebuild catch-up took {catchup_elapsed:?}, exceeding frozen {CATCHUP_TIME_LIMIT:?}"
    );
    assert_building(&mut admin);
    let retained_after_catchup = retained_wal_bytes(&mut admin, TARGET_SLOT);

    let activation_started = Instant::now();
    assert_eq!(
        catchup
            .catch_up_next()
            .expect("activate exact rebuild fence"),
        BootstrapCatchupProgress::Active
    );
    let activation_elapsed = activation_started.elapsed();
    assert!(
        activation_elapsed <= ACTIVATION_TIME_LIMIT,
        "rebuild activation took {activation_elapsed:?}, exceeding frozen {ACTIVATION_TIME_LIMIT:?}"
    );
    let rebuild_elapsed = rebuild_started.elapsed();
    assert!(
        rebuild_elapsed <= REBUILD_TIME_LIMIT,
        "complete rebuild took {rebuild_elapsed:?}, exceeding frozen {REBUILD_TIME_LIMIT:?}"
    );
    if let Some(current) = rss_kib() {
        rss_peak = Some(rss_peak.unwrap_or(current).max(current));
    }
    let rss_delta = rss_baseline
        .zip(rss_peak)
        .map(|(baseline, peak)| peak.saturating_sub(baseline));
    if let Some(delta) = rss_delta {
        assert!(
            delta <= MAX_RSS_GROWTH_KIB,
            "Rust RSS grew {delta} KiB, exceeding frozen {MAX_RSS_GROWTH_KIB} KiB"
        );
    }
    let oracle = assert_differential(&mut admin);
    assert_eq!(oracle.0, i64::try_from(SNAPSHOT_ROWS).unwrap());
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation
                 WHERE graph_id = 1 AND slot_generation = 3",
                &[],
            )
            .expect("query new-generation WAL continuation")
            .get::<_, i64>(0),
        1
    );

    let retained_after_activation = retained_wal_bytes(&mut admin, TARGET_SLOT);
    let peak_retained_wal = retained_after_scan
        .max(retained_after_catchup)
        .max(retained_after_activation);
    assert!(
        (0..=MAX_RETAINED_WAL_BYTES).contains(&peak_retained_wal),
        "target slot retained peak {peak_retained_wal} bytes, exceeding frozen {MAX_RETAINED_WAL_BYTES}"
    );
    catchup
        .into_live()
        .expect("handoff rebuilt performance source")
        .detach()
        .expect("detach rebuilt performance session");

    eprintln!(
        "M12 performance measured snapshot_rows={scanned_rows} batches={batches} \
         batch_rows={SNAPSHOT_BATCH_ROWS} concurrent_wal_changes={CONCURRENT_WAL_CHANGES} \
         prepare={prepare_elapsed:?} handoff={handoff_elapsed:?} scan={scan_elapsed:?} \
         scan_rate={scan_rate:.2}rows/s catchup={catchup_elapsed:?} \
         activation={activation_elapsed:?} total={rebuild_elapsed:?} \
         rss_baseline_kib={rss_baseline:?} rss_peak_kib={rss_peak:?} rss_delta_kib={rss_delta:?} \
         retained_wal_after_scan={retained_after_scan} retained_wal_after_catchup={retained_after_catchup} \
         retained_wal_after_activation={retained_after_activation}; frozen limits \
         prepare={PREPARE_TIME_LIMIT:?} handoff={HANDOFF_TIME_LIMIT:?} snapshot={SNAPSHOT_TIME_LIMIT:?} \
         min_rate={MIN_SNAPSHOT_ROWS_PER_SECOND:.2}rows/s catchup={CATCHUP_TIME_LIMIT:?} \
         activation={ACTIVATION_TIME_LIMIT:?} total={REBUILD_TIME_LIMIT:?} \
         rss_growth_kib={MAX_RSS_GROWTH_KIB} retained_wal_bytes={MAX_RETAINED_WAL_BYTES}; \
         one synchronous batch and direct catch-up imply no test-owned queue or per-row API"
    );
}
