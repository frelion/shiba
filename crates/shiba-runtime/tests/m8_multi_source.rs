use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{M2Error, PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, register_source};

const SOURCE1_CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m8-multi-source.sh",
    env_prefix: "SHIBA_M8_MULTI_SOURCE",
    slot: "shiba_m8_source1_slot",
    publication: "shiba_m8_source1_pub",
};
const SOURCE2_CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m8-multi-source.sh",
    env_prefix: "SHIBA_M8_MULTI_SOURCE",
    slot: "shiba_m8_source2_slot",
    publication: "shiba_m8_source2_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT sum(value_bigint)::bigint FROM shiba.graph_result),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.graph_continuation)",
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

fn applied_rows(client: &mut Client) -> Vec<(i64, i64)> {
    client
        .query(
            "SELECT source_id, source_row_id
             FROM shiba_internal.source_row_state
             ORDER BY source_id, source_row_id",
            &[],
        )
        .expect("query per-source rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn continuations(client: &mut Client) -> Vec<(i64, i64, i64)> {
    client
        .query(
            "SELECT graph_id, slot_generation, count(*)
             FROM shiba_internal.graph_continuation
             GROUP BY graph_id, slot_generation
             ORDER BY graph_id",
            &[],
        )
        .expect("query independent singleton-graph continuations")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

fn graph_results(client: &mut Client) -> Vec<(i64, i64)> {
    client
        .query(
            "SELECT graph_id, value_bigint
             FROM shiba.graph_result ORDER BY graph_id",
            &[],
        )
        .expect("query singleton-graph results")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m8_multi_source_test;
             CREATE FUNCTION m8_multi_source_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m8_source2_crash
             AFTER INSERT ON shiba_internal.graph_continuation
             FOR EACH ROW EXECUTE FUNCTION
                 m8_multi_source_test.crash_after_continuation();",
        )
        .expect("install source2 continuation crash point");
}

fn install_sources(client: &mut Client) -> (PgoutputSource, PgoutputSource, u32) {
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m8_one;
             CREATE SCHEMA source_m8_two;
             CREATE TABLE source_m8_one.events (id bigint PRIMARY KEY);
             CREATE TABLE source_m8_two.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m8_source1_pub FOR TABLE source_m8_one.events;
             CREATE PUBLICATION shiba_m8_source2_pub FOR TABLE source_m8_two.events;",
        )
        .expect("install two source objects");
    let relation_oid = |client: &mut Client, table: &str| {
        u32::try_from(
            client
                .query_one(&format!("SELECT '{table}'::regclass::oid::bigint"), &[])
                .expect("read source OID")
                .get::<_, i64>(0),
        )
        .expect("source OID fits u32")
    };
    let source1_oid = relation_oid(client, "source_m8_one.events");
    let source2_oid = relation_oid(client, "source_m8_two.events");
    let source1 = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source1"),
        SlotGeneration::new(1).expect("non-zero generation"),
        source1_oid,
    );
    let source2 = PgoutputSource::new(
        SourceId::new(2).expect("non-zero source2"),
        SlotGeneration::new(1).expect("non-zero generation"),
        source2_oid,
    );
    register_source(client, "source_m8_one.events");
    client
        .query_one(
            "SELECT shiba_internal.register_source(
                2, 'source_m8_two.events'::regclass)",
            &[],
        )
        .expect("register source2");
    support::register_count_operator(client, 2, 2);
    SOURCE1_CAPTURE.create_slot();
    SOURCE2_CAPTURE.create_slot();
    (source1, source2, source2_oid)
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m8-multi-source.sh"]
fn m8_two_singleton_graphs_have_independent_continuation_and_recovery() {
    let connection = SOURCE1_CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    let (source1, source2, source2_oid) = install_sources(&mut client);

    client
        .batch_execute("INSERT INTO source_m8_one.events VALUES (1601)")
        .expect("commit source1 transaction");
    let source1_wire = SOURCE1_CAPTURE.capture(&mut client, "source1.pgoutput");
    let transaction1 =
        decode_committed_changes(&source1_wire, &support::singleton_graph(1, source1))
            .expect("decode source1 transaction");
    client
        .batch_execute("INSERT INTO source_m8_two.events VALUES (2601)")
        .expect("commit source2 transaction");
    let source2_wire = SOURCE2_CAPTURE.capture(&mut client, "source2.pgoutput");
    let transaction2 =
        decode_committed_changes(&source2_wire, &support::singleton_graph(2, source2))
            .expect("decode source2 transaction");
    assert_eq!(
        process(&mut client, &transaction1).expect("apply source1"),
        ProcessOutcome::Applied
    );
    assert_eq!(
        process(&mut client, &transaction2).expect("apply source2"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 2));
    assert_eq!(graph_results(&mut client), vec![(1, 1), (2, 1)]);
    assert_eq!(applied_rows(&mut client), vec![(1, 1601), (2, 2601)]);
    assert_eq!(continuations(&mut client), vec![(1, 1, 1), (2, 1, 1)]);
    assert_eq!(
        process(&mut client, &transaction1).expect("replay source1"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(
        process(&mut client, &transaction2).expect("replay source2"),
        ProcessOutcome::AlreadyApplied
    );

    client
        .batch_execute("INSERT INTO source_m8_two.events VALUES (2602)")
        .expect("commit source2 recovery transaction");
    let recovery_wire = SOURCE2_CAPTURE.capture(&mut client, "source2-recovery.pgoutput");
    let recovery = decode_committed_changes(&recovery_wire, &support::singleton_graph(2, source2))
        .expect("decode source2 recovery");
    install_crash_trigger(&mut client);
    assert!(process(&mut client, &recovery).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after source2 crash");
    assert_eq!(durable_state(&mut client), (2, 2, 2, 2));
    assert_eq!(graph_results(&mut client), vec![(1, 1), (2, 1)]);
    assert_eq!(applied_rows(&mut client), vec![(1, 1601), (2, 2601)]);
    assert_eq!(continuations(&mut client), vec![(1, 1, 1), (2, 1, 1)]);
    client
        .batch_execute("DROP SCHEMA m8_multi_source_test CASCADE")
        .expect("remove crash point");
    assert_eq!(
        process(&mut client, &recovery).expect("retry source2 recovery"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (3, 3, 3, 3));
    assert_eq!(graph_results(&mut client), vec![(1, 1), (2, 2)]);
    assert_eq!(continuations(&mut client), vec![(1, 1, 1), (2, 1, 2)]);
    assert_eq!(
        process(&mut client, &recovery).expect("replay source2 recovery"),
        ProcessOutcome::AlreadyApplied
    );

    let generation2 = PgoutputSource::new(
        SourceId::new(2).expect("non-zero source2"),
        SlotGeneration::new(2).expect("non-zero generation2"),
        source2_oid,
    );
    let drift = decode_committed_changes(&recovery_wire, &support::singleton_graph(2, generation2))
        .expect("decode generation drift");
    assert!(matches!(
        process(&mut client, &drift),
        Err(M2Error::SlotGenerationMismatch)
    ));
    assert_eq!(durable_state(&mut client), (3, 3, 3, 3));
    assert_eq!(graph_results(&mut client), vec![(1, 1), (2, 2)]);
    assert_eq!(continuations(&mut client), vec![(1, 1, 1), (2, 1, 2)]);
}
