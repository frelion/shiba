use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_streamed_changes, process};

mod support;

use support::{PgoutputCapture, read_u32, register_source, stream_message_end};

const ROW_COUNT: i64 = 10_000;
const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m6-stream-commit.sh",
    env_prefix: "SHIBA_M6_STREAM_COMMIT",
    slot: "shiba_m6_stream_commit_slot",
    publication: "shiba_m6_stream_commit_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 2),
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

fn streamed_messages(wire: &[u8]) -> Vec<(usize, u8)> {
    let mut messages = Vec::new();
    let mut start = 0;
    let mut in_segment = false;
    while start < wire.len() {
        let tag = wire[start];
        messages.push((start, tag));
        let end = stream_message_end(wire, start, in_segment);
        in_segment = match tag {
            b'S' => true,
            b'E' => false,
            _ => in_segment,
        };
        start = end;
    }
    messages
}

fn assert_segment_shape(wire: &[u8], messages: &[(usize, u8)]) -> usize {
    let starts: Vec<_> = messages
        .iter()
        .filter_map(|(at, tag)| (*tag == b'S').then_some(*at))
        .collect();
    let stops = messages.iter().filter(|(_, tag)| *tag == b'E').count();
    assert!(
        starts.len() >= 2,
        "transaction must span multiple stream segments"
    );
    assert_eq!(stops, starts.len());
    let xid = read_u32(wire, starts[0] + 1);
    assert_ne!(xid, 0);
    assert_eq!(wire[starts[0] + 5], 1);
    for start in &starts[1..] {
        assert_eq!(read_u32(wire, *start + 1), xid);
        assert_eq!(wire[*start + 5], 0);
    }
    let (commit, tag) = messages.last().copied().expect("stream messages");
    assert_eq!(tag, b'c');
    assert_eq!(read_u32(wire, commit + 1), xid);
    commit
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m6_stream_test;
             CREATE FUNCTION m6_stream_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m6_stream_crash
             AFTER INSERT ON shiba_internal.graph_continuation
             FOR EACH ROW EXECUTE FUNCTION m6_stream_test.crash_after_continuation();",
        )
        .expect("install continuation crash point");
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m6-stream-commit.sh"]
fn m6_real_stream_commit_crash_retry_and_replay() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m6_stream;
             CREATE TABLE source_m6_stream.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m6_stream_commit_pub
                 FOR TABLE source_m6_stream.events;",
        )
        .expect("install streamed source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m6_stream.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m6_stream.events");
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    client
        .execute(
            "INSERT INTO source_m6_stream.events
             SELECT generate_series(1::bigint, $1::bigint)",
            &[&ROW_COUNT],
        )
        .expect("commit large source transaction");
    let wire = CAPTURE.capture_streamed(&mut client, "streamed-insert.pgoutput");
    let messages = streamed_messages(&wire);
    let commit = assert_segment_shape(&wire, &messages);

    assert!(
        decode_streamed_changes(&wire[..commit], &support::singleton_graph(1, source)).is_err()
    );
    assert_eq!(durable_state(&mut client), (0, 0, 0, 0));
    assert!(
        decode_streamed_changes(
            &wire[..wire.len() - 1],
            &support::singleton_graph(1, source)
        )
        .is_err()
    );
    let mut corrupt = wire.clone();
    corrupt[commit + 5] = 1;
    assert!(decode_streamed_changes(&corrupt, &support::singleton_graph(1, source)).is_err());
    let mut aborted = wire[..commit + 9].to_vec();
    aborted[commit] = b'A';
    assert!(decode_streamed_changes(&aborted, &support::singleton_graph(1, source)).is_err());
    assert_eq!(durable_state(&mut client), (0, 0, 0, 0));

    let transaction = decode_streamed_changes(&wire, &support::singleton_graph(1, source))
        .expect("decode committed stream");
    install_crash_trigger(&mut client);
    assert!(process(&mut client, &transaction).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(durable_state(&mut client), (0, 0, 0, 0));
    client
        .batch_execute("DROP SCHEMA m6_stream_test CASCADE")
        .expect("remove crash point");

    assert_eq!(
        process(&mut client, &transaction).expect("retry committed stream"),
        ProcessOutcome::Applied
    );
    assert_eq!(
        durable_state(&mut client),
        (ROW_COUNT, ROW_COUNT, ROW_COUNT, 1)
    );
    assert_eq!(
        process(&mut client, &transaction).expect("replay committed stream"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(
        durable_state(&mut client),
        (ROW_COUNT, ROW_COUNT, ROW_COUNT, 1)
    );
}
