use std::{
    num::NonZeroU64,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_ingress::{
    AttachOptions, CONNECTIONS_PER_SOURCE, GovernedSourceSession, IngressError, ReplicationMode,
};
use shiba_operator::OperatorId;
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{ProcessOutcome, compile_and_register};

#[allow(dead_code)]
mod support;

use support::{slot_lsn, wait_for_slot_lsn};

const SLOT: &str = "shiba_m10_performance_slot";
const PUBLICATION: &str = "shiba_m10_performance_pub";
const REPLICATION_APPLICATION: &str = "shiba_m10_performance_receiver";
const E2E_LIMIT: Duration = Duration::from_secs(15);
const REPLAY_LIMIT: Duration = Duration::from_secs(2);
const SUSTAINED_TRANSACTIONS: usize = 100;
const ROWS_PER_TRANSACTION: i64 = 10;
const MIN_TRANSACTIONS_PER_SECOND: f64 = 20.0;
const P50_LIMIT: Duration = Duration::from_millis(250);
const P95_LIMIT: Duration = Duration::from_millis(500);
const P99_LIMIT: Duration = Duration::from_secs(1);
const SLOW_APPLY_FLOOR: Duration = Duration::from_millis(300);
const BACKPRESSURE_LIMIT: Duration = Duration::from_millis(250);
const MAX_RUST_ASSEMBLY_BYTES: usize = 16 * 1024 * 1024;
const MAX_DECODED_CHANGES: usize = 10_000;

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m10-performance-ingress.sh must set {name}"))
}

fn attach(database_url: &str, replication_url: &str) -> GovernedSourceSession {
    GovernedSourceSession::attach(
        database_url,
        replication_url,
        SourceId::new(1).expect("source ID"),
        SlotGeneration::new(1).expect("slot generation"),
        AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
            .expect("attach options"),
    )
    .expect("attach governed committed session")
}

fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1)]
}

fn assert_within(label: &str, measured: Duration, limit: Duration) {
    assert!(
        measured <= limit,
        "{label} took {measured:?}, exceeding frozen limit {limit:?}"
    );
}

fn install_fixture(client: &mut Client) {
    client
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events (id)
                 WITH (publish = 'insert, update, delete, truncate');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);"
        ))
        .expect("install performance source");
    compile_and_register(
        client,
        &OperatorSpecV1 {
            version: OPERATOR_SPEC_VERSION,
            operator_id: OperatorId::new(NonZeroU64::new(1).expect("operator ID")),
            source_id: SourceId::new(1).expect("source ID"),
            operation: OperatorOperationV1::CountRows,
        },
    )
    .expect("register CountRows");
    client
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .expect("create performance slot");
    let publication_oid: u32 = client
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("read publication OID")
        .get(0);
    client
        .execute(
            "SELECT shiba_internal.configure_source_ingress(1, $1, $2, 1)",
            &[&publication_oid, &SLOT],
        )
        .expect("configure governed ingress");
}

fn count_rows(client: &mut Client) -> i64 {
    client
        .query_one(
            "SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1",
            &[],
        )
        .expect("read CountRows result")
        .get(0)
}

#[test]
#[ignore = "requires scripts/test-m10-performance-ingress.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one frozen production performance gate"
)]
fn governed_committed_ingress_has_bounded_latency_throughput_and_backpressure() {
    let database_url = required("SHIBA_M10_PERFORMANCE_DATABASE_URL");
    let replication_url = required("SHIBA_M10_PERFORMANCE_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect admin database");
    install_fixture(&mut admin);

    assert_eq!(CONNECTIONS_PER_SOURCE, 2);
    let initial_lsn = slot_lsn(&mut admin, SLOT);
    let mut session = attach(&database_url, &replication_url);
    let apply_connections: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_stat_activity
             WHERE application_name = 'shiba-governed-apply'",
            &[],
        )
        .expect("count Apply connections")
        .get(0);
    let replication_connections: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_stat_replication WHERE application_name = $1",
            &[&REPLICATION_APPLICATION],
        )
        .expect("count replication connections")
        .get(0);
    assert_eq!((apply_connections, replication_connections), (1, 1));

    let e2e_started = Instant::now();
    admin
        .batch_execute(
            "INSERT INTO source.events
             SELECT generate_series(1::bigint, 10000::bigint)",
        )
        .expect("commit 10,000-change source transaction");
    let applied = session
        .receive_and_apply_one()
        .expect("receive and Apply 10,000-change transaction");
    let e2e_elapsed = e2e_started.elapsed();
    assert_eq!(applied.outcome(), ProcessOutcome::Applied);
    assert_eq!(count_rows(&mut admin), 10_000);
    assert_eq!(slot_lsn(&mut admin, SLOT), initial_lsn);
    assert_within("10,000-change governed E2E", e2e_elapsed, E2E_LIMIT);
    drop(session);

    let replay_started = Instant::now();
    let mut session = attach(&database_url, &replication_url);
    let replay = session
        .receive_and_apply_one()
        .expect("receive and Apply exact replay");
    assert_eq!(replay.outcome(), ProcessOutcome::AlreadyApplied);
    session.acknowledge(&replay).expect("ack exact replay");
    wait_for_slot_lsn(&mut admin, SLOT, replay.end_lsn());
    let replay_elapsed = replay_started.elapsed();
    assert_eq!(count_rows(&mut admin), 10_000);
    assert_within("production exact replay", replay_elapsed, REPLAY_LIMIT);

    for transaction in 0..SUSTAINED_TRANSACTIONS {
        let first = 10_001 + i64::try_from(transaction).expect("transaction index") * 10;
        admin
            .execute(
                "INSERT INTO source.events SELECT generate_series($1::bigint, $2::bigint)",
                &[&first, &(first + ROWS_PER_TRANSACTION - 1)],
            )
            .expect("commit sustained source transaction");
    }
    let sustained_started = Instant::now();
    let mut latencies = Vec::with_capacity(SUSTAINED_TRANSACTIONS);
    for _ in 0..SUSTAINED_TRANSACTIONS {
        let started = Instant::now();
        let applied = session
            .receive_and_apply_one()
            .expect("receive and Apply sustained transaction");
        assert_eq!(applied.outcome(), ProcessOutcome::Applied);
        session.acknowledge(&applied).expect("ack sustained Apply");
        latencies.push(started.elapsed());
    }
    let sustained_elapsed = sustained_started.elapsed();
    latencies.sort_unstable();
    let p50 = percentile(&latencies, 50, 100);
    let p95 = percentile(&latencies, 95, 100);
    let p99 = percentile(&latencies, 99, 100);
    let throughput =
        f64::from(u32::try_from(SUSTAINED_TRANSACTIONS).expect("transaction count fits u32"))
            / sustained_elapsed.as_secs_f64();
    assert!(
        throughput >= MIN_TRANSACTIONS_PER_SECOND,
        "sustained throughput {throughput:.2} tx/s is below frozen floor {MIN_TRANSACTIONS_PER_SECOND:.2} tx/s"
    );
    assert_within("backlog receiver-service p50", p50, P50_LIMIT);
    assert_within("backlog receiver-service p95", p95, P95_LIMIT);
    assert_within("backlog receiver-service p99", p99, P99_LIMIT);
    assert_eq!(count_rows(&mut admin), 11_000);

    admin
        .batch_execute(
            "CREATE SCHEMA m10_performance_test;
             CREATE FUNCTION m10_performance_test.slow_apply()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_sleep(0.35);
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m10_slow_apply BEFORE UPDATE
             ON shiba_internal.operator_state FOR EACH ROW
             EXECUTE FUNCTION m10_performance_test.slow_apply();",
        )
        .expect("install slow Apply fixture");
    admin
        .execute("INSERT INTO source.events VALUES (11001)", &[])
        .expect("commit first backpressure transaction");
    admin
        .execute("INSERT INTO source.events VALUES (11002)", &[])
        .expect("commit second backpressure transaction");
    let first = session
        .receive_one()
        .expect("receive first slow transaction");
    assert_eq!(first.transaction().changes.len(), 1);
    let rejected_started = Instant::now();
    assert!(matches!(
        session.receive_one(),
        Err(IngressError::FeedbackPending)
    ));
    let rejected_elapsed = rejected_started.elapsed();
    assert_within(
        "outstanding receive rejection",
        rejected_elapsed,
        BACKPRESSURE_LIMIT,
    );
    let slow_started = Instant::now();
    let first = session.apply_received(&first).expect("perform slow Apply");
    let slow_elapsed = slow_started.elapsed();
    assert!(
        slow_elapsed >= SLOW_APPLY_FLOOR,
        "slow Apply completed in {slow_elapsed:?}, below fixture floor {SLOW_APPLY_FLOOR:?}"
    );
    session.acknowledge(&first).expect("ack first slow Apply");
    assert_eq!(count_rows(&mut admin), 11_001);
    admin
        .batch_execute("DROP SCHEMA m10_performance_test CASCADE")
        .expect("remove slow Apply fixture");
    let second = session
        .receive_and_apply_one()
        .expect("receive second transaction after backpressure release");
    assert_eq!(second.outcome(), ProcessOutcome::Applied);
    session
        .acknowledge(&second)
        .expect("ack second transaction");
    assert_eq!(count_rows(&mut admin), 11_002);

    eprintln!(
        "M10 performance measured source_commit_to_apply_e2e_10000={e2e_elapsed:?} replay={replay_elapsed:?} precommitted_backlog_service={sustained_elapsed:?} throughput={throughput:.2}tx/s service_p50={p50:?} service_p95={p95:?} service_p99={p99:?} backpressure_reject={rejected_elapsed:?} slow_apply={slow_elapsed:?}; frozen Rust bounds assembly_bytes={MAX_RUST_ASSEMBLY_BYTES} decoded_changes={MAX_DECODED_CHANGES} connections_per_source={CONNECTIONS_PER_SOURCE}"
    );
}
