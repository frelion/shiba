use std::{
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_operator::{ResultSchemaV1, TypedResultRowV1, TypedValue};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    GraphTransaction, PgoutputSource, ProcessOutcome, decode_committed_changes, process,
};

mod support;

use support::PgoutputCapture;

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
    input: GraphTransaction,
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
    support::register_count_sum_graph(client, 1);
    client
        .query_one(
            "SELECT shiba_internal.register_source(
                2, 'source_m9_concurrency_two.events'::regclass)",
            &[],
        )
        .expect("register source two");
    support::register_count_sum_graph(client, 2);
    CAPTURE1.create_slot();
    CAPTURE2.create_slot();
    support::configure_graph_ingress(client, 1, CAPTURE1.publication, CAPTURE1.slot);
    support::configure_graph_ingress(client, 2, CAPTURE2.publication, CAPTURE2.slot);
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
) -> (GraphTransaction, GraphTransaction, GraphTransaction) {
    client
        .batch_execute("INSERT INTO source_m9_concurrency_one.events VALUES (1, 10)")
        .unwrap();
    let first = decode_committed_changes(
        &CAPTURE1.capture(client, "one-first.pgoutput"),
        &support::singleton_graph(1, source1),
    )
    .expect("decode source one first transaction");
    client
        .batch_execute("INSERT INTO source_m9_concurrency_one.events VALUES (2, 7)")
        .unwrap();
    let second = decode_committed_changes(
        &CAPTURE1.capture(client, "one-second.pgoutput"),
        &support::singleton_graph(1, source1),
    )
    .expect("decode source one second transaction");
    client
        .batch_execute("INSERT INTO source_m9_concurrency_two.events VALUES (1, 5)")
        .unwrap();
    let independent = decode_committed_changes(
        &CAPTURE2.capture(client, "two.pgoutput"),
        &support::singleton_graph(2, source2),
    )
    .expect("decode source two transaction");
    (first, second, independent)
}

fn results(client: &mut Client) -> Vec<(i64, Option<i64>, i64)> {
    let scalar_partition = TypedValue::Bool(true)
        .to_canonical_json()
        .expect("canonical scalar state partition");
    client
        .query(
            "SELECT result.graph_id, result.result_id, result.schema_payload,
                    result.schema_digest, result.row_payload,
                    COALESCE(state.state_payload, decode('0000000000000000','hex'))
             FROM shiba.graph_result_rows AS result
             LEFT JOIN shiba_internal.graph_node_state AS state
              ON state.graph_id = result.graph_id
              AND state.node_id = result.result_id - 2
              AND state.namespace = 0
              AND state.partition_key_payload = $1
              AND state.item_key_payload = $2
             ORDER BY result.graph_id, result.result_id",
            &[&scalar_partition, &b"null".as_slice()],
        )
        .unwrap()
        .into_iter()
        .map(|row| {
            let graph_id: i64 = row.get(0);
            let result_id: i64 = row.get(1);
            let schema_payload: Vec<u8> = row.get(2);
            let schema_digest: Vec<u8> = row.get(3);
            let row_payload: Vec<u8> = row.get(4);
            let schema = ResultSchemaV1::from_canonical_payload(
                &schema_payload,
                schema_digest
                    .try_into()
                    .expect("exact schema digest length"),
            )
            .expect("decode exact result schema");
            let result = TypedResultRowV1::from_canonical_payload(&schema, &row_payload)
                .expect("decode exact result row");
            let value = match result.values[0] {
                TypedValue::Int8(value) => Some(value),
                TypedValue::Null(_) => None,
                _ => panic!("concurrency scalar result must be nullable int8"),
            };
            (
                (graph_id - 1) * 2 + result_id - 2,
                value,
                support::decode_optional_scalar_state(row.get::<_, Option<Vec<u8>>>(5).as_deref()),
            )
        })
        .collect()
}

fn continuations(client: &mut Client) -> Vec<(i64, i64)> {
    client
        .query(
            "SELECT graph_id, ingress_transaction_id
             FROM shiba_internal.graph_continuation ORDER BY graph_id, commit_lsn",
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
        vec![
            (1, Some(0), 0),
            (2, None, 0),
            (3, Some(1), 1),
            (4, Some(5), 5)
        ]
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
        vec![
            (1, Some(2), 2),
            (2, Some(17), 17),
            (3, Some(1), 1),
            (4, Some(5), 5)
        ]
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
        vec![
            (1, Some(2), 2),
            (2, Some(17), 17),
            (3, Some(1), 1),
            (4, Some(5), 5)
        ]
    );
}
