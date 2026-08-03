use std::time::{Duration, Instant};

use postgres::{Client, NoTls};
use shiba_protocol::{
    GraphId, GraphTransactionId, IngressTransactionId, InputSequence, PostgresLsn, SlotGeneration,
    SourceId,
};
use shiba_runtime::{
    GraphSourceChange, GraphTransaction, M2Error, PgoutputSource, ProcessOutcome, SourceChange,
    SourceInsert, decode_committed_changes, process,
};

mod support;

use support::{PgoutputCapture, register_source};

const DECODE_LIMIT: Duration = Duration::from_secs(2);
const APPLY_LIMIT: Duration = Duration::from_secs(10);
const REPLAY_LIMIT: Duration = Duration::from_secs(2);
const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m8-performance.sh",
    env_prefix: "SHIBA_M8_PERFORMANCE",
    slot: "shiba_m8_performance_slot",
    publication: "shiba_m8_performance_pub",
};

fn source(client: &mut Client) -> PgoutputSource {
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m8_performance.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source OID")
            .get::<_, i64>(0),
    )
    .expect("source OID fits u32");
    PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    )
}

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.graph_result WHERE graph_id = 1 AND result_id = 1001),
                (SELECT state_payload FROM shiba_internal.graph_node_state
                 WHERE graph_id = 1 AND node_id = 1 AND namespace = 0),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.graph_continuation)",
            &[],
        )
        .expect("query durable state");
    (
        row.get(0),
        support::decode_optional_scalar_state(row.get::<_, Option<Vec<u8>>>(1).as_deref()),
        row.get(2),
        row.get(3),
    )
}

fn assert_within(operation: &str, measured: Duration, limit: Duration) {
    assert!(
        measured <= limit,
        "{operation} took {measured:?}, exceeding {limit:?}"
    );
}

fn test_identity() -> GraphTransactionId {
    GraphTransactionId::new(
        GraphId::new(1).expect("non-zero graph"),
        SlotGeneration::new(1).expect("non-zero generation"),
        PostgresLsn::from_u64(1),
        IngressTransactionId::new(1).expect("non-zero ingress transaction"),
    )
    .expect("non-zero source transaction identity")
}

fn oversized_inserts() -> Vec<SourceInsert> {
    (1_u64..=10_001)
        .map(|value| {
            SourceInsert::new(
                InputSequence::new(value).expect("non-zero input sequence"),
                i64::try_from(value).expect("test row id fits bigint"),
            )
        })
        .collect()
}

fn tagged(inserts: Vec<SourceInsert>) -> Vec<GraphSourceChange> {
    let source_id = SourceId::new(1).expect("non-zero source");
    inserts
        .into_iter()
        .map(|insert| GraphSourceChange {
            source_id,
            change: SourceChange::Insert(insert),
        })
        .collect()
}

#[test]
fn constructors_reject_more_than_10000_changes() {
    let inserts = oversized_inserts();
    assert!(matches!(
        GraphTransaction::new(test_identity(), tagged(inserts.clone())),
        Err(M2Error::TransactionLimitExceeded)
    ));
    assert!(matches!(
        GraphTransaction::new(test_identity(), tagged(inserts)),
        Err(M2Error::TransactionLimitExceeded)
    ));
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m8-performance.sh"]
fn m8_real_pgoutput_10000_change_latency_is_bounded() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m8_performance;
             CREATE TABLE source_m8_performance.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m8_performance_pub
                 FOR TABLE source_m8_performance.events;",
        )
        .expect("install performance source objects");
    let source = source(&mut client);
    register_source(&mut client, "source_m8_performance.events");
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);
    client
        .batch_execute(
            "INSERT INTO source_m8_performance.events
             SELECT generate_series(1, 10000)",
        )
        .expect("commit 10,000-row source transaction");
    let wire = CAPTURE.capture(&mut client, "performance.pgoutput");

    let started = Instant::now();
    let transaction = decode_committed_changes(&wire, &support::singleton_graph(1, source))
        .expect("decode 10,000-row transaction");
    let decode_elapsed = started.elapsed();
    assert_eq!(transaction.changes.len(), 10_000);

    let started = Instant::now();
    assert_eq!(
        process(&mut client, &transaction).expect("apply 10,000-row transaction"),
        ProcessOutcome::Applied
    );
    let apply_elapsed = started.elapsed();
    assert_eq!(durable_state(&mut client), (10_000, 10_000, 10_000, 1));

    let started = Instant::now();
    assert_eq!(
        process(&mut client, &transaction).expect("exact replay"),
        ProcessOutcome::AlreadyApplied
    );
    let replay_elapsed = started.elapsed();
    assert_eq!(durable_state(&mut client), (10_000, 10_000, 10_000, 1));

    let mut forged_changes = transaction.changes.clone();
    forged_changes.push(transaction.changes[0].clone());
    let forged = GraphTransaction {
        identity: transaction.identity,
        changes: forged_changes,
    };
    assert!(matches!(
        process(&mut client, &forged),
        Err(M2Error::TransactionLimitExceeded)
    ));
    assert_eq!(durable_state(&mut client), (10_000, 10_000, 10_000, 1));

    eprintln!(
        "M8.4 measured decode={decode_elapsed:?} apply={apply_elapsed:?} replay={replay_elapsed:?}"
    );
    assert_within("decode", decode_elapsed, DECODE_LIMIT);
    assert_within("first Apply", apply_elapsed, APPLY_LIMIT);
    assert_within("exact replay", replay_elapsed, REPLAY_LIMIT);
}
