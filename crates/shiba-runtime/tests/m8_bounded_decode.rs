use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    PgoutputError, PgoutputSource, ProcessOutcome, decode_committed_changes,
    decode_streamed_changes, process,
};

mod support;

use support::{PgoutputCapture, register_source};

const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m8-bounded-decode.sh",
    env_prefix: "SHIBA_M8_BOUNDED_DECODE",
    slot: "shiba_m8_bounded_decode_slot",
    publication: "shiba_m8_bounded_decode_pub",
};

fn source(client: &mut Client) -> PgoutputSource {
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m8_bounded.events'::regclass::oid::bigint",
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
                (SELECT value_bigint FROM shiba.graph_result WHERE graph_id = 1 AND result_id = 2),
                (SELECT state_payload FROM shiba_internal.graph_node_state WHERE graph_id = 1 AND node_id = 1 AND namespace = 0),
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

#[test]
fn exact_input_limit_is_parsed_and_one_more_byte_is_rejected() {
    let mut input = vec![0; MAX_INPUT_BYTES];
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        1,
    );
    assert!(matches!(
        decode_committed_changes(&input, &support::singleton_graph(1, source)),
        Err(PgoutputError::MessageOrder)
    ));
    assert!(matches!(
        decode_streamed_changes(&input, &support::singleton_graph(1, source)),
        Err(PgoutputError::MessageOrder)
    ));

    input.push(0);
    assert!(matches!(
        decode_committed_changes(&input, &support::singleton_graph(1, source)),
        Err(PgoutputError::LimitExceeded)
    ));
    assert!(matches!(
        decode_streamed_changes(&input, &support::singleton_graph(1, source)),
        Err(PgoutputError::LimitExceeded)
    ));
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m8-bounded-decode.sh"]
fn m8_committed_decode_admits_limit_and_rejects_next_change() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m8_bounded;
             CREATE TABLE source_m8_bounded.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m8_bounded_decode_pub
                 FOR TABLE source_m8_bounded.events;",
        )
        .expect("install bounded-decode source objects");
    let source = source(&mut client);
    register_source(&mut client, "source_m8_bounded.events");
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    client
        .batch_execute(
            "INSERT INTO source_m8_bounded.events
             SELECT generate_series(1, 10000)",
        )
        .expect("commit transaction at change limit");
    let admitted_wire = CAPTURE.capture(&mut client, "admitted.pgoutput");
    let admitted = decode_committed_changes(&admitted_wire, &support::singleton_graph(1, source))
        .expect("decode 10,000 changes");
    assert_eq!(admitted.changes.len(), 10_000);
    assert_eq!(
        process(&mut client, &admitted).expect("apply admitted transaction"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (10_000, 10_000, 10_000, 1));
    assert_eq!(
        process(&mut client, &admitted).expect("replay admitted transaction"),
        ProcessOutcome::AlreadyApplied
    );

    client
        .batch_execute(
            "INSERT INTO source_m8_bounded.events
             SELECT generate_series(20001, 30001)",
        )
        .expect("commit transaction above change limit");
    let rejected_wire = CAPTURE.capture(&mut client, "rejected.pgoutput");
    assert!(matches!(
        decode_committed_changes(&rejected_wire, &support::singleton_graph(1, source)),
        Err(PgoutputError::LimitExceeded)
    ));
    assert_eq!(durable_state(&mut client), (10_000, 10_000, 10_000, 1));
}
