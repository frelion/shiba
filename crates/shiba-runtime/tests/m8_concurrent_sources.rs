use std::{
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    PgoutputSource, ProcessOutcome, SourceTransaction, decode_committed_changes, process,
};

mod support;

use support::{PgoutputCapture, register_source};

const ADVISORY_KEY: i64 = 80_002;
const SOURCE1_CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m8-concurrent-sources.sh",
    env_prefix: "SHIBA_M8_CONCURRENT_SOURCES",
    slot: "shiba_m8_concurrent_source1_slot",
    publication: "shiba_m8_concurrent_source1_pub",
};
const SOURCE2_CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m8-concurrent-sources.sh",
    env_prefix: "SHIBA_M8_CONCURRENT_SOURCES",
    slot: "shiba_m8_concurrent_source2_slot",
    publication: "shiba_m8_concurrent_source2_pub",
};

type ApplyReceiver = Receiver<Result<ProcessOutcome, String>>;

fn spawn_process(
    connection: &str,
    application_name: &str,
    input: SourceTransaction,
) -> (ApplyReceiver, thread::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel();
    let connection = format!("{connection} application_name={application_name}");
    let handle = thread::Builder::new()
        .name(application_name.to_owned())
        .spawn(move || {
            let result = Client::connect(&connection, NoTls)
                .map_err(|error| error.to_string())
                .and_then(|mut client| {
                    process(&mut client, &input).map_err(|error| error.to_string())
                });
            sender.send(result).expect("send process outcome");
        })
        .expect("spawn named process thread");
    (receiver, handle)
}

fn wait_until_lock_waiting(client: &mut Client, application_name: &str, event: Option<&str>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let waiting: bool = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE application_name = $1
                      AND wait_event_type = 'Lock'
                      AND ($2::text IS NULL OR wait_event = $2)
                )",
                &[&application_name, &event],
            )
            .expect("poll process lock wait")
            .get(0);
        if waiting {
            return;
        }
        assert!(Instant::now() < deadline, "process did not enter lock wait");
        thread::sleep(Duration::from_millis(10));
    }
}

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT sum(value_bigint)::bigint FROM shiba.operator_result),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable state");
    (
        row.get(0),
        support::scalar_state_sum(client),
        row.get(1),
        row.get(2),
    )
}

fn continuations(client: &mut Client) -> Vec<(i64, i64)> {
    client
        .query(
            "SELECT source_id, count(*)
             FROM shiba_internal.source_continuation
             GROUP BY source_id ORDER BY source_id",
            &[],
        )
        .expect("query per-source continuations")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn operator_results(client: &mut Client) -> Vec<(i64, i64)> {
    client
        .query(
            "SELECT operator_id, value_bigint
             FROM shiba.operator_result ORDER BY operator_id",
            &[],
        )
        .expect("query per-source operator results")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn hold_blocker(client: &mut Client) {
    client
        .query_one("SELECT pg_advisory_lock($1)", &[&ADVISORY_KEY])
        .expect("hold source1 blocker");
}

fn release_blocker(client: &mut Client) {
    let released: bool = client
        .query_one("SELECT pg_advisory_unlock($1)", &[&ADVISORY_KEY])
        .expect("release source1 blocker")
        .get(0);
    assert!(released);
}

fn install_sources(client: &mut Client) -> (PgoutputSource, PgoutputSource) {
    client
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m8_concurrent_one;
             CREATE SCHEMA source_m8_concurrent_two;
             CREATE TABLE source_m8_concurrent_one.events (id bigint PRIMARY KEY);
             CREATE TABLE source_m8_concurrent_two.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m8_concurrent_source1_pub
                 FOR TABLE source_m8_concurrent_one.events;
             CREATE PUBLICATION shiba_m8_concurrent_source2_pub
                 FOR TABLE source_m8_concurrent_two.events;
             CREATE SCHEMA m8_concurrent_test;
             CREATE FUNCTION m8_concurrent_test.block_source1()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.source_id = 1 THEN
                     PERFORM pg_advisory_xact_lock({ADVISORY_KEY});
                 END IF;
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m8_block_source1
             BEFORE INSERT ON shiba_internal.source_row_state
             FOR EACH ROW EXECUTE FUNCTION m8_concurrent_test.block_source1();"
        ))
        .expect("install sources and source1 blocker");
    let oid = |client: &mut Client, table: &str| {
        u32::try_from(
            client
                .query_one(&format!("SELECT '{table}'::regclass::oid::bigint"), &[])
                .expect("read source OID")
                .get::<_, i64>(0),
        )
        .expect("source OID fits u32")
    };
    let source1 = PgoutputSource::new(
        SourceId::new(1).expect("source1 id"),
        SlotGeneration::new(1).expect("source1 generation"),
        oid(client, "source_m8_concurrent_one.events"),
    );
    let source2 = PgoutputSource::new(
        SourceId::new(2).expect("source2 id"),
        SlotGeneration::new(1).expect("source2 generation"),
        oid(client, "source_m8_concurrent_two.events"),
    );
    register_source(client, "source_m8_concurrent_one.events");
    client
        .query_one(
            "SELECT shiba_internal.register_source(
                2, 'source_m8_concurrent_two.events'::regclass)",
            &[],
        )
        .expect("register source2");
    support::register_count_operator(client, 2, 2);
    SOURCE1_CAPTURE.create_slot();
    SOURCE2_CAPTURE.create_slot();
    (source1, source2)
}

fn capture_inputs(
    client: &mut Client,
    source1: PgoutputSource,
    source2: PgoutputSource,
) -> (SourceTransaction, SourceTransaction, SourceTransaction) {
    client
        .batch_execute("INSERT INTO source_m8_concurrent_one.events VALUES (1801)")
        .expect("commit source1 duplicate input");
    let source1_first = decode_committed_changes(
        &SOURCE1_CAPTURE.capture(client, "source1-first.pgoutput"),
        source1,
    )
    .expect("decode source1 duplicate input");
    client
        .batch_execute("INSERT INTO source_m8_concurrent_one.events VALUES (1802)")
        .expect("commit source1 next input");
    let source1_next = decode_committed_changes(
        &SOURCE1_CAPTURE.capture(client, "source1-next.pgoutput"),
        source1,
    )
    .expect("decode source1 next input");
    client
        .batch_execute("INSERT INTO source_m8_concurrent_two.events VALUES (2801)")
        .expect("commit source2 input");
    let source2_input = decode_committed_changes(
        &SOURCE2_CAPTURE.capture(client, "source2.pgoutput"),
        source2,
    )
    .expect("decode source2 input");
    (source1_first, source1_next, source2_input)
}

fn prove_duplicate_serialization(client: &mut Client, connection: &str, input: &SourceTransaction) {
    hold_blocker(client);
    let (first_rx, first) = spawn_process(connection, "m8_source1_first", input.clone());
    wait_until_lock_waiting(client, "m8_source1_first", Some("advisory"));
    let (duplicate_rx, duplicate) =
        spawn_process(connection, "m8_source1_duplicate", input.clone());
    wait_until_lock_waiting(client, "m8_source1_duplicate", None);
    assert_eq!(durable_state(client), (0, 0, 0, 0));
    release_blocker(client);
    let first_outcome = first_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("first source1 completion")
        .expect("first source1 success");
    let duplicate_outcome = duplicate_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("duplicate source1 completion")
        .expect("duplicate source1 success");
    first.join().expect("join first source1 thread");
    duplicate.join().expect("join duplicate source1 thread");
    assert_eq!(first_outcome, ProcessOutcome::Applied);
    assert_eq!(duplicate_outcome, ProcessOutcome::AlreadyApplied);
    assert_eq!(durable_state(client), (1, 1, 1, 1));
    assert_eq!(operator_results(client), vec![(1, 1), (2, 0)]);
    assert_eq!(continuations(client), vec![(1, 1)]);
}

fn prove_independent_progress(
    client: &mut Client,
    connection: &str,
    source1: &SourceTransaction,
    source2: &SourceTransaction,
) {
    hold_blocker(client);
    let (source1_rx, source1_task) =
        spawn_process(connection, "m8_source1_blocked", source1.clone());
    wait_until_lock_waiting(client, "m8_source1_blocked", Some("advisory"));
    let (source2_rx, source2_task) =
        spawn_process(connection, "m8_source2_independent", source2.clone());
    assert_eq!(
        source2_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("source2 completes while source1 is blocked")
            .expect("source2 succeeds"),
        ProcessOutcome::Applied
    );
    source2_task.join().expect("join source2 thread");
    wait_until_lock_waiting(client, "m8_source1_blocked", Some("advisory"));
    assert_eq!(durable_state(client), (2, 2, 2, 2));
    assert_eq!(operator_results(client), vec![(1, 1), (2, 1)]);
    assert_eq!(continuations(client), vec![(1, 1), (2, 1)]);
    release_blocker(client);
    assert_eq!(
        source1_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("blocked source1 completion")
            .expect("blocked source1 succeeds"),
        ProcessOutcome::Applied
    );
    source1_task.join().expect("join blocked source1 thread");
    assert_eq!(durable_state(client), (3, 3, 3, 3));
    assert_eq!(operator_results(client), vec![(1, 2), (2, 1)]);
    assert_eq!(continuations(client), vec![(1, 2), (2, 1)]);
    assert_eq!(
        process(client, source1).expect("replay source1"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(
        process(client, source2).expect("replay source2"),
        ProcessOutcome::AlreadyApplied
    );
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m8-concurrent-sources.sh"]
fn m8_same_source_serializes_while_independent_source_progresses() {
    let connection = SOURCE1_CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    let (source1, source2) = install_sources(&mut client);
    let (duplicate, source1_next, source2_input) = capture_inputs(&mut client, source1, source2);
    prove_duplicate_serialization(&mut client, &connection, &duplicate);
    prove_independent_progress(&mut client, &connection, &source1_next, &source2_input);
}
