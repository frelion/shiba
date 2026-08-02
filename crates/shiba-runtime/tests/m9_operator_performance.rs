use std::{
    num::NonZeroU64,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_operator::OperatorId;
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    M2Error, PgoutputSource, ProcessOutcome, compile_and_register, decode_committed_changes,
    process,
};

mod support;

use support::{PgoutputCapture, register_source};

const DECODE_LIMIT: Duration = Duration::from_secs(2);
const APPLY_LIMIT: Duration = Duration::from_secs(10);
const REPLAY_LIMIT: Duration = Duration::from_secs(2);
const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m9-operator-performance.sh",
    env_prefix: "SHIBA_M9_OPERATOR_PERFORMANCE",
    slot: "shiba_m9_operator_performance_slot",
    publication: "shiba_m9_operator_performance_pub",
};

fn source(client: &mut Client) -> PgoutputSource {
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m9_performance.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source OID")
            .get::<_, i64>(0),
    )
    .expect("source OID fits u32");
    PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    )
}

fn register_sum(client: &mut Client) {
    let spec = OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(2).expect("non-zero operator")),
        source_id: SourceId::new(1).expect("non-zero source"),
        operation: OperatorOperationV1::SumInt8 {
            input_column: "payload".to_owned(),
        },
    };
    compile_and_register(client, &spec).expect("compile and register SumInt8");
}

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 2),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 2),
                (SELECT count(*) FROM shiba_internal.applied_insert),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query count and sum durable state");
    (
        row.get(0),
        row.get(1),
        row.get(2),
        row.get(3),
        row.get(4),
        row.get(5),
    )
}

fn assert_within(operation: &str, measured: Duration, limit: Duration) {
    assert!(
        measured <= limit,
        "count+sum {operation} took {measured:?}, exceeding {limit:?}"
    );
}

fn install_ordered_failure(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m9_operator_performance_test;
             CREATE TABLE m9_operator_performance_test.update_order (
                 ordinal bigint GENERATED ALWAYS AS IDENTITY,
                 operator_id bigint NOT NULL
             );
             CREATE FUNCTION m9_operator_performance_test.fail_second()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 INSERT INTO m9_operator_performance_test.update_order (operator_id)
                 VALUES (NEW.operator_id);
                 IF NEW.operator_id = 2 THEN
                     IF NOT EXISTS (
                         SELECT 1 FROM m9_operator_performance_test.update_order
                         WHERE operator_id = 1
                     ) THEN
                         RAISE EXCEPTION 'operator order violation';
                     END IF;
                     RAISE EXCEPTION 'injected second operator failure';
                 END IF;
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m9_ordered_failure
             BEFORE UPDATE ON shiba_internal.operator_state
             FOR EACH ROW EXECUTE FUNCTION
                 m9_operator_performance_test.fail_second();",
        )
        .expect("install ordered second-operator failure");
}

fn prove_ordered_atomic_failure(
    client: &mut Client,
    source: PgoutputSource,
    expected: (i64, i64, i64, i64, i64, i64),
) {
    client
        .batch_execute("INSERT INTO source_m9_performance.events VALUES (10001, 2)")
        .expect("commit failure-case source transaction");
    let wire = CAPTURE.capture(client, "ordered-failure.pgoutput");
    let transaction =
        decode_committed_changes(&wire, source).expect("decode failure-case transaction");
    install_ordered_failure(client);
    let error = process(client, &transaction).expect_err("second operator must fail");
    let M2Error::Postgres(error) = error else {
        panic!("expected injected PostgreSQL failure, got {error}");
    };
    assert_eq!(
        error.as_db_error().expect("server error detail").message(),
        "injected second operator failure"
    );
    assert_eq!(durable_state(client), expected);
    let audit_rows: i64 = client
        .query_one(
            "SELECT count(*) FROM m9_operator_performance_test.update_order",
            &[],
        )
        .expect("query rolled-back order audit")
        .get(0);
    assert_eq!(audit_rows, 0);
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m9-operator-performance.sh"]
fn m9_count_and_sum_10000_change_latency_and_atomicity_are_bounded() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m9_performance;
             CREATE TABLE source_m9_performance.events (
                 id bigint PRIMARY KEY, payload bigint
             );
             CREATE PUBLICATION shiba_m9_operator_performance_pub
                 FOR TABLE source_m9_performance.events;",
        )
        .expect("install performance source objects");
    let source = source(&mut client);
    register_source(&mut client, "source_m9_performance.events");
    register_sum(&mut client);
    CAPTURE.create_slot();
    client
        .batch_execute(
            "INSERT INTO source_m9_performance.events
             SELECT id, CASE WHEN id % 4 = 0 THEN NULL ELSE 2 END
             FROM generate_series(1, 10000) AS id",
        )
        .expect("commit fixed 10,000-row nullable-int8 transaction");
    let wire = CAPTURE.capture(&mut client, "count-sum-performance.pgoutput");

    let started = Instant::now();
    let transaction =
        decode_committed_changes(&wire, source).expect("decode fixed count+sum transaction");
    let decode_elapsed = started.elapsed();
    assert_eq!(transaction.changes.len(), 10_000);

    let started = Instant::now();
    assert_eq!(
        process(&mut client, &transaction).expect("apply count+sum transaction"),
        ProcessOutcome::Applied
    );
    let apply_elapsed = started.elapsed();
    let expected = (10_000, 15_000, 10_000, 15_000, 10_000, 1);
    assert_eq!(durable_state(&mut client), expected);

    let started = Instant::now();
    assert_eq!(
        process(&mut client, &transaction).expect("exact count+sum replay"),
        ProcessOutcome::AlreadyApplied
    );
    let replay_elapsed = started.elapsed();
    assert_eq!(durable_state(&mut client), expected);
    prove_ordered_atomic_failure(&mut client, source, expected);

    eprintln!(
        "M9.2 count+sum measured decode={decode_elapsed:?} apply={apply_elapsed:?} replay={replay_elapsed:?}"
    );
    assert_within("decode", decode_elapsed, DECODE_LIMIT);
    assert_within("first Apply", apply_elapsed, APPLY_LIMIT);
    assert_within("exact replay", replay_elapsed, REPLAY_LIMIT);
}
