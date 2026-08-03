use std::{
    fs,
    path::PathBuf,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_streamed_changes, process};

mod support;

use support::{
    PgoutputCapture, read_u32, register_source, stream_message_end, streamed_framed_terminal,
    strip_streamed_delimiters,
};

const ROW_COUNT: i64 = 10_000;
const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m6-stream-abort.sh",
    env_prefix: "SHIBA_M6_STREAM_ABORT",
    slot: "shiba_m6_stream_abort_slot",
    publication: "shiba_m6_stream_abort_pub",
};

struct RunningReceiver(Option<Child>);

impl RunningReceiver {
    fn stop(mut self) {
        let mut child = self.0.take().expect("running receiver");
        let status = Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .expect("interrupt streamed receiver");
        assert!(status.success(), "interrupt streamed receiver");
        child.wait().expect("reap streamed receiver");
    }
}

impl Drop for RunningReceiver {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
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

fn start_receiver(client: &mut Client, output: &PathBuf) -> RunningReceiver {
    let child = CAPTURE
        .command("pg_recvlogical")
        .args(["-S", CAPTURE.slot, "--start", "-f"])
        .arg(output)
        .args([
            "-n",
            "-F",
            "1",
            "-s",
            "1",
            "-o",
            "proto_version=2",
            "-o",
            "streaming=on",
            "-o",
            "publication_names=shiba_m6_stream_abort_pub",
        ])
        .spawn()
        .expect("start streamed receiver");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let active: bool = client
            .query_one(
                "SELECT active FROM pg_replication_slots
                 WHERE slot_name = 'shiba_m6_stream_abort_slot'",
                &[],
            )
            .expect("query active slot")
            .get(0);
        if active {
            break;
        }
        assert!(Instant::now() < deadline, "receiver did not claim slot");
        thread::sleep(Duration::from_millis(10));
    }
    RunningReceiver(Some(child))
}

fn wait_for_terminal(output: &PathBuf, expected: u8, deadline: Instant) -> Vec<u8> {
    loop {
        let capture = fs::read(output).unwrap_or_default();
        if streamed_framed_terminal(&capture) == Some(expected) {
            return capture;
        }
        assert!(Instant::now() < deadline, "missing streamed terminal");
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_abort_shape(wire: &[u8]) {
    let mut start = 0;
    let mut in_segment = false;
    let mut starts = 0;
    let mut stops = 0;
    let mut top_xid = None;
    while start < wire.len() {
        let tag = wire[start];
        let end = stream_message_end(wire, start, in_segment);
        match tag {
            b'S' => {
                starts += 1;
                top_xid.get_or_insert_with(|| read_u32(wire, start + 1));
                in_segment = true;
            }
            b'E' => {
                stops += 1;
                in_segment = false;
            }
            b'A' => {
                let xid = top_xid.expect("stream start xid");
                assert_eq!(read_u32(wire, start + 1), xid);
                assert_eq!(read_u32(wire, start + 5), xid);
                assert_eq!(end, wire.len());
            }
            _ => {}
        }
        start = end;
    }
    assert!(starts >= 1);
    assert_eq!(starts, stops);
    assert_eq!(wire[wire.len() - 9], b'A');
}

fn assert_commit_shape(wire: &[u8]) {
    let mut start = 0;
    let mut in_segment = false;
    let mut starts = 0;
    let mut stops = 0;
    let mut terminal = None;
    while start < wire.len() {
        let tag = wire[start];
        let end = stream_message_end(wire, start, in_segment);
        match tag {
            b'S' => {
                starts += 1;
                in_segment = true;
            }
            b'E' => {
                stops += 1;
                in_segment = false;
            }
            _ => {}
        }
        terminal = Some(tag);
        start = end;
    }
    assert!(starts >= 1);
    assert_eq!(starts, stops);
    assert_eq!(terminal, Some(b'c'));
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m6-stream-abort.sh"]
fn m6_real_stream_abort_never_applies_then_slot_commits() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m6_abort;
             CREATE TABLE source_m6_abort.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m6_stream_abort_pub
                 FOR TABLE source_m6_abort.events;",
        )
        .expect("install abort source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m6_abort.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m6_abort.events");
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    let output = CAPTURE.capture_path("streamed-abort.pgoutput");
    let receiver = start_receiver(&mut client, &output);
    let mut transaction = client.transaction().expect("begin abort transaction");
    transaction
        .execute(
            "INSERT INTO source_m6_abort.events
             SELECT generate_series(1::bigint, $1::bigint)",
            &[&ROW_COUNT],
        )
        .expect("write abort transaction");
    let deadline = Instant::now() + Duration::from_secs(15);
    let _ = wait_for_terminal(&output, b'E', deadline);
    transaction
        .rollback()
        .expect("rollback streamed transaction");
    let abort_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .expect("read abort WAL position")
        .get(0);
    let raw_abort = wait_for_terminal(&output, b'A', deadline);
    loop {
        let acknowledged: bool = client
            .query_one(
                "SELECT confirmed_flush_lsn >= $1::text::pg_lsn
                 FROM pg_replication_slots
                 WHERE slot_name = 'shiba_m6_stream_abort_slot'",
                &[&abort_lsn],
            )
            .expect("query abort feedback")
            .get(0);
        if acknowledged {
            break;
        }
        assert!(Instant::now() < deadline, "abort was not acknowledged");
        thread::sleep(Duration::from_millis(10));
    }
    receiver.stop();
    let abort_wire = strip_streamed_delimiters(&raw_abort);
    assert_abort_shape(&abort_wire);
    assert!(decode_streamed_changes(&abort_wire, &support::singleton_graph(1, source)).is_err());
    assert_eq!(durable_state(&mut client), (0, 0, 0, 0));

    client
        .execute(
            "INSERT INTO source_m6_abort.events
             SELECT generate_series(20001::bigint, 30000::bigint)",
            &[],
        )
        .expect("commit replacement streamed transaction");
    let commit_wire = CAPTURE.capture_streamed(&mut client, "streamed-commit.pgoutput");
    assert_commit_shape(&commit_wire);
    let committed = decode_streamed_changes(&commit_wire, &support::singleton_graph(1, source))
        .expect("decode committed stream");
    assert_eq!(
        process(&mut client, &committed).expect("apply committed stream"),
        ProcessOutcome::Applied
    );
    assert_eq!(
        durable_state(&mut client),
        (ROW_COUNT, ROW_COUNT, ROW_COUNT, 1)
    );
    assert_eq!(
        process(&mut client, &committed).expect("replay committed stream"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(
        durable_state(&mut client),
        (ROW_COUNT, ROW_COUNT, ROW_COUNT, 1)
    );
}
