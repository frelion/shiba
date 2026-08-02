use std::{
    num::NonZeroU64,
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_operator::OperatorId;
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    PgoutputSource, ProcessOutcome, SourceTransaction, compile_and_register,
    decode_committed_changes, process,
};

mod support;

use support::{PgoutputCapture, register_count_operator};

const ADVISORY_KEY: i64 = 90_002;
const CAPTURE1: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m9-operator-concurrency.sh",
    env_prefix: "SHIBA_M9_OPERATOR_CONCURRENCY",
    slot: "shiba_m9_operator_concurrency_one_slot",
    publication: "shiba_m9_operator_concurrency_one_pub",
};
const CAPTURE2: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m9-operator-concurrency.sh",
    env_prefix: "SHIBA_M9_OPERATOR_CONCURRENCY",
    slot: "shiba_m9_operator_concurrency_two_slot",
    publication: "shiba_m9_operator_concurrency_two_pub",
};

type ProcessReceiver = Receiver<Result<ProcessOutcome, String>>;

fn spawn_process(
    connection: &str,
    name: &str,
    input: SourceTransaction,
) -> (ProcessReceiver, thread::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel();
    let connection = format!("{connection} application_name={name}");
    let handle = thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let result = Client::connect(&connection, NoTls)
                .map_err(|error| error.to_string())
                .and_then(|mut client| {
                    process(&mut client, &input).map_err(|error| error.to_string())
                });
            sender.send(result).expect("send process result");
        })
        .expect("spawn process thread");
    (receiver, handle)
}

fn wait_for_lock(client: &mut Client, name: &str, event: Option<&str>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let waiting: bool = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE application_name = $1 AND wait_event_type = 'Lock'
                      AND ($2::text IS NULL OR wait_event = $2))",
                &[&name, &event],
            )
            .expect("poll lock wait")
            .get(0);
        if waiting {
            return;
        }
        assert!(Instant::now() < deadline, "{name} did not wait for a lock");
        thread::sleep(Duration::from_millis(10));
    }
}

fn register_sum(client: &mut Client, source_id: u64, operator_id: u64) {
    compile_and_register(
        client,
        &OperatorSpecV1 {
            version: OPERATOR_SPEC_VERSION,
            operator_id: OperatorId::new(NonZeroU64::new(operator_id).unwrap()),
            source_id: SourceId::new(source_id).unwrap(),
            operation: OperatorOperationV1::SumInt8 {
                input_column: "payload".into(),
            },
        },
    )
    .expect("register SumInt8");
}

fn install(client: &mut Client) -> (PgoutputSource, PgoutputSource) {
    client
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m9_concurrency_one;
             CREATE SCHEMA source_m9_concurrency_two;
             CREATE TABLE source_m9_concurrency_one.events (
                 id bigint PRIMARY KEY, payload bigint NULL);
             CREATE TABLE source_m9_concurrency_two.events (
                 id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION shiba_m9_operator_concurrency_one_pub
                 FOR TABLE source_m9_concurrency_one.events;
             CREATE PUBLICATION shiba_m9_operator_concurrency_two_pub
                 FOR TABLE source_m9_concurrency_two.events;
             CREATE SCHEMA m9_concurrency_test;
             CREATE FUNCTION m9_concurrency_test.pause_source_one()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.source_id = 1 THEN
                     PERFORM pg_advisory_xact_lock({ADVISORY_KEY});
                 END IF;
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m9_pause_source_one
             BEFORE INSERT ON shiba_internal.source_row_state
             FOR EACH ROW EXECUTE FUNCTION m9_concurrency_test.pause_source_one();"
        ))
        .expect("install source relations and pause trigger");
    let oid = |client: &mut Client, relation: &str| {
        u32::try_from(
            client
                .query_one(&format!("SELECT '{relation}'::regclass::oid::bigint"), &[])
                .unwrap()
                .get::<_, i64>(0),
        )
        .unwrap()
    };
    client
        .query_one(
            "SELECT shiba_internal.register_source(
                1, 'source_m9_concurrency_one.events'::regclass)",
            &[],
        )
        .expect("register source one");
    register_count_operator(client, 1, 1);
    register_sum(client, 1, 2);
    client
        .query_one(
            "SELECT shiba_internal.register_source(
                2, 'source_m9_concurrency_two.events'::regclass)",
            &[],
        )
        .expect("register source two");
    register_count_operator(client, 2, 3);
    register_sum(client, 2, 4);
    CAPTURE1.create_slot();
    CAPTURE2.create_slot();
    (
        PgoutputSource::with_nullable_int8_payload(
            SourceId::new(1).unwrap(),
            SlotGeneration::new(1).unwrap(),
            oid(client, "source_m9_concurrency_one.events"),
        ),
        PgoutputSource::with_nullable_int8_payload(
            SourceId::new(2).unwrap(),
            SlotGeneration::new(1).unwrap(),
            oid(client, "source_m9_concurrency_two.events"),
        ),
    )
}

fn capture_inputs(
    client: &mut Client,
    source1: PgoutputSource,
    source2: PgoutputSource,
) -> (SourceTransaction, SourceTransaction, SourceTransaction) {
    client
        .batch_execute("INSERT INTO source_m9_concurrency_one.events VALUES (1, 10)")
        .unwrap();
    let first = decode_committed_changes(&CAPTURE1.capture(client, "one-first.pgoutput"), source1)
        .expect("decode source one first transaction");
    client
        .batch_execute("INSERT INTO source_m9_concurrency_one.events VALUES (2, 7)")
        .unwrap();
    let second =
        decode_committed_changes(&CAPTURE1.capture(client, "one-second.pgoutput"), source1)
            .expect("decode source one second transaction");
    client
        .batch_execute("INSERT INTO source_m9_concurrency_two.events VALUES (1, 5)")
        .unwrap();
    let independent = decode_committed_changes(&CAPTURE2.capture(client, "two.pgoutput"), source2)
        .expect("decode source two transaction");
    (first, second, independent)
}

fn results(client: &mut Client) -> Vec<(i64, i64, i64)> {
    client
        .query(
            "SELECT result.operator_id, result.value_bigint, state.value_bigint
             FROM shiba.operator_result AS result
             JOIN shiba_internal.operator_state AS state USING (operator_id)
             ORDER BY result.operator_id",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

fn continuations(client: &mut Client) -> Vec<(i64, i64)> {
    client
        .query(
            "SELECT source_id, ingress_transaction_id
             FROM shiba_internal.source_continuation ORDER BY source_id, commit_lsn",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[test]
#[ignore = "requires scripts/test-m9-operator-concurrency.sh"]
fn m9_operator_lock_order_serializes_one_source_without_blocking_another() {
    let connection = CAPTURE1.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect temporary PostgreSQL");
    let (source1, source2) = install(&mut client);
    let (first, second, independent) = capture_inputs(&mut client, source1, source2);
    client
        .query_one("SELECT pg_advisory_lock($1)", &[&ADVISORY_KEY])
        .expect("hold deterministic source-one pause");

    let (first_rx, first_task) = spawn_process(&connection, "m9_source_one_first", first.clone());
    wait_for_lock(&mut client, "m9_source_one_first", Some("advisory"));
    let (second_rx, second_task) =
        spawn_process(&connection, "m9_source_one_second", second.clone());
    wait_for_lock(&mut client, "m9_source_one_second", None);
    let (independent_rx, independent_task) =
        spawn_process(&connection, "m9_source_two", independent.clone());
    assert_eq!(
        independent_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap(),
        ProcessOutcome::Applied
    );
    independent_task.join().unwrap();
    assert_eq!(
        results(&mut client),
        vec![(1, 0, 0), (2, 0, 0), (3, 1, 1), (4, 5, 5)]
    );
    assert_eq!(
        continuations(&mut client),
        vec![(
            2,
            i64::try_from(independent.identity.ingress_transaction_id.get()).unwrap()
        )]
    );

    let released: bool = client
        .query_one("SELECT pg_advisory_unlock($1)", &[&ADVISORY_KEY])
        .unwrap()
        .get(0);
    assert!(released);
    assert_eq!(
        first_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(
        second_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap(),
        ProcessOutcome::Applied
    );
    first_task.join().unwrap();
    second_task.join().unwrap();
    assert_eq!(
        results(&mut client),
        vec![(1, 2, 2), (2, 17, 17), (3, 1, 1), (4, 5, 5)]
    );
    assert_eq!(continuations(&mut client).len(), 3);
    assert_eq!(
        process(&mut client, &first).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(
        process(&mut client, &second).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(
        process(&mut client, &independent).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(
        results(&mut client),
        vec![(1, 2, 2), (2, 17, 17), (3, 1, 1), (4, 5, 5)]
    );
}
