use std::{
    process::Command,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_ingress::{
    BOOTSTRAP_CONNECTIONS_PER_GRAPH, BootstrapCatchupProgress, BootstrapOptions, BootstrapSession,
    BootstrapSpec, SnapshotProgress,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};
use shiba_runtime::{MAX_BOOTSTRAP_BATCH_ROWS, ProcessOutcome, compile_and_register};

const SLOT: &str = "shiba_m11_bootstrap_performance_slot";
const PUBLICATION: &str = "shiba_m11_bootstrap_performance_pub";
const SNAPSHOT_ROWS: usize = 1_000_000;
const SNAPSHOT_BATCH_ROWS: usize = 10_000;
const CONCURRENT_WAL_CHANGES: usize = 10_000;
// Frozen before the first measurement. Do not tune these from observed results.
const SNAPSHOT_TIME_LIMIT: Duration = Duration::from_mins(2);
const MIN_SNAPSHOT_ROWS_PER_SECOND: f64 = 10_000.0;
const CATCHUP_TIME_LIMIT: Duration = Duration::from_secs(15);
const MAX_RSS_GROWTH_KIB: u64 = 256 * 1024;

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m11-bootstrap-performance.sh must set {name}"))
}

#[path = "support/mod.rs"]
#[allow(dead_code)]
mod test_support;
use test_support::count_sum_spec;

fn rss_kib() -> Option<u64> {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then_some(output.stdout)?
        .split(u8::is_ascii_whitespace)
        .find(|field| !field.is_empty())
        .and_then(|field| std::str::from_utf8(field).ok())
        .and_then(|field| field.parse().ok())
}

fn public_results(client: &mut Client) -> Vec<(i64, String, Option<i64>)> {
    client
        .query(
            "SELECT result_id, result_status, value_bigint
             FROM shiba.graph_result WHERE graph_id = 1 ORDER BY result_id",
            &[],
        )
        .expect("query public results")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

fn sql_oracle(client: &mut Client) -> (i64, i64) {
    let row = client
        .query_one(
            "SELECT count(*)::bigint, COALESCE(sum(payload), 0)::bigint
             FROM source.events",
            &[],
        )
        .expect("query SQL differential oracle");
    (row.get(0), row.get(1))
}

fn assert_differential(client: &mut Client) -> (i64, i64) {
    let oracle = sql_oracle(client);
    assert_eq!(
        public_results(client),
        vec![
            (3, "active".to_owned(), Some(oracle.0)),
            (4, "active".to_owned(), Some(oracle.1)),
        ]
    );
    let row_state = client
        .query_one(
            "SELECT count(*)::bigint, COALESCE(sum(payload_int8), 0)::bigint
             FROM shiba_internal.source_row_state WHERE source_id = 1",
            &[],
        )
        .expect("query current source-row authority");
    assert_eq!((row_state.get(0), row_state.get(1)), oracle);
    oracle
}

#[test]
#[ignore = "requires scripts/test-m11-bootstrap-performance.sh"]
#[allow(clippy::too_many_lines, reason = "one frozen million-row M11 gate")]
fn million_row_snapshot_is_bounded_and_catches_up_exactly() {
    assert_eq!(MAX_BOOTSTRAP_BATCH_ROWS, SNAPSHOT_BATCH_ROWS);
    assert_eq!(BOOTSTRAP_CONNECTIONS_PER_GRAPH, 3);
    let database_url = required("SHIBA_M11_PERFORMANCE_DATABASE_URL");
    let replication_url = required("SHIBA_M11_PERFORMANCE_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect admin database");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events
                 WITH (publish = 'insert, update, delete');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);
             INSERT INTO source.events
             SELECT id,
                    CASE WHEN id % 10 = 0 THEN NULL ELSE (id % 97)::bigint END
             FROM generate_series(1::bigint, {SNAPSHOT_ROWS}::bigint) AS rows(id);"
        ))
        .expect("install million-row snapshot fixture");
    compile_and_register(&mut admin, &count_sum_spec(1)).expect("register graph");

    let publication_oid: u32 = admin
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("read publication OID")
        .get(0);
    let options = BootstrapOptions::new(SNAPSHOT_BATCH_ROWS, Duration::from_secs(10))
        .expect("maximum bounded snapshot batch");
    let mut bootstrap = BootstrapSession::begin(
        &database_url,
        &replication_url,
        BootstrapSpec {
            graph_id: GraphId::new(1).expect("graph ID"),
            bootstrap_id: BootstrapId::new(1).expect("bootstrap ID"),
            publication_oid,
            slot_name: SLOT.to_owned(),
            slot_generation: SlotGeneration::new(1).expect("slot generation"),
        },
        options,
    )
    .expect("begin consistent bootstrap");
    assert_eq!(
        public_results(&mut admin),
        vec![
            (3, "building".to_owned(), None),
            (4, "building".to_owned(), None),
        ]
    );

    let writer_url = database_url.clone();
    let writer = std::thread::spawn(move || {
        let mut client = Client::connect(&writer_url, NoTls).expect("connect concurrent writer");
        client
            .batch_execute(
                "BEGIN;
                 UPDATE source.events SET payload = 7 WHERE id BETWEEN 1 AND 3334;
                 DELETE FROM source.events WHERE id BETWEEN 3335 AND 6667;
                 INSERT INTO source.events
                 SELECT id, 11 FROM generate_series(1000001::bigint, 1003333::bigint) AS rows(id);
                 COMMIT;",
            )
            .expect("commit exactly 10,000 snapshot-period changes");
    });

    let rss_baseline = rss_kib();
    let mut rss_peak = rss_baseline;
    let scan_started = Instant::now();
    let mut scanned_rows = 0usize;
    let mut batches = 0usize;
    while let SnapshotProgress::BatchApplied { rows, .. } =
        bootstrap.scan_next().expect("scan bounded snapshot batch")
    {
        assert_eq!(rows, SNAPSHOT_BATCH_ROWS);
        assert!(rows <= MAX_BOOTSTRAP_BATCH_ROWS);
        scanned_rows += rows;
        batches += 1;
        if let Some(current) = rss_kib() {
            rss_peak = Some(rss_peak.unwrap_or(current).max(current));
        }
        if batches.is_multiple_of(10) {
            assert_eq!(
                public_results(&mut admin),
                vec![
                    (3, "building".to_owned(), None),
                    (4, "building".to_owned(), None),
                ]
            );
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
        "million-row snapshot took {scan_elapsed:?}, exceeding frozen {SNAPSHOT_TIME_LIMIT:?}"
    );
    assert!(
        scan_rate >= MIN_SNAPSHOT_ROWS_PER_SECOND,
        "snapshot rate {scan_rate:.2} rows/s is below frozen {MIN_SNAPSHOT_ROWS_PER_SECOND:.2} rows/s"
    );
    let rss_delta = rss_baseline
        .zip(rss_peak)
        .map(|(baseline, peak)| peak.saturating_sub(baseline));
    if let Some(delta) = rss_delta {
        assert!(
            delta <= MAX_RSS_GROWTH_KIB,
            "Rust RSS grew {delta} KiB, exceeding frozen {MAX_RSS_GROWTH_KIB} KiB"
        );
    } else {
        eprintln!(
            "M11 RSS unavailable; strict modeled bound: one synchronous batch of at most \
             {MAX_BOOTSTRAP_BATCH_ROWS} rows, no channel or test-owned queue"
        );
    }

    let catchup_started = Instant::now();
    let mut catchup = bootstrap
        .into_catchup()
        .expect("enter bounded WAL catch-up");
    assert_eq!(
        catchup.catch_up_next().expect("apply 10,000-change WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        catchup
            .catch_up_next()
            .expect("activate exact catch-up fence"),
        BootstrapCatchupProgress::Active
    );
    let catchup_elapsed = catchup_started.elapsed();
    assert!(
        catchup_elapsed <= CATCHUP_TIME_LIMIT,
        "10,000-change catch-up took {catchup_elapsed:?}, exceeding frozen {CATCHUP_TIME_LIMIT:?}"
    );
    let caught_up_oracle = assert_differential(&mut admin);
    assert_eq!(caught_up_oracle.0, i64::try_from(SNAPSHOT_ROWS).unwrap());
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation",
                &[],
            )
            .expect("query WAL-only continuation")
            .get::<_, i64>(0),
        1
    );

    let mut live = catchup
        .into_live()
        .expect("handoff to ordinary M10 ingress");
    admin
        .batch_execute("INSERT INTO source.events VALUES (2000000, 13)")
        .expect("commit post-handoff live transaction");
    let applied = live
        .receive_and_apply_one()
        .expect("apply post-handoff live transaction");
    assert_eq!(applied.outcome(), ProcessOutcome::Applied);
    live.acknowledge(&applied).expect("ack durable live Apply");
    let live_oracle = assert_differential(&mut admin);
    assert_eq!(
        live_oracle,
        (caught_up_oracle.0 + 1, caught_up_oracle.1 + 13)
    );
    live.detach().expect("detach live session");

    eprintln!(
        "M11 performance measured snapshot_rows={scanned_rows} batches={batches} \
         batch_rows={SNAPSHOT_BATCH_ROWS} scan={scan_elapsed:?} \
         scan_rate={scan_rate:.2}rows/s catchup_changes={CONCURRENT_WAL_CHANGES} \
         catchup={catchup_elapsed:?} rss_baseline_kib={rss_baseline:?} \
         rss_peak_kib={rss_peak:?} rss_delta_kib={rss_delta:?}; frozen limits \
         snapshot={SNAPSHOT_TIME_LIMIT:?} min_rate={MIN_SNAPSHOT_ROWS_PER_SECOND:.2}rows/s \
         catchup={CATCHUP_TIME_LIMIT:?} rss_growth_kib={MAX_RSS_GROWTH_KIB}; \
         synchronous batch API and direct catch-up provide no queue"
    );
}
