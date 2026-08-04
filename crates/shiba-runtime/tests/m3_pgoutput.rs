use std::{
    fs,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{
    PgoutputCapture, framed_message_count, register_source, strip_recvlogical_delimiters,
};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m3.sh",
    env_prefix: "SHIBA_M3",
    slot: "shiba_m3_slot",
    publication: "shiba_m3_pub",
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

fn public_result(client: &mut Client) -> i64 {
    client
        .query_one(
            "SELECT (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 2",
            &[],
        )
        .expect("query SQL result")
        .get(0)
}

struct StoppedCapture(Option<Child>);

impl StoppedCapture {
    fn crash(mut self) {
        let mut child = self.0.take().expect("capture process");
        child.kill().expect("kill capture before feedback");
        child.wait().expect("reap killed capture");
    }
}

impl Drop for StoppedCapture {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn capture_committed_row_without_ack(
    client: &mut Client,
    baseline: &str,
    name: &str,
) -> (Vec<u8>, StoppedCapture) {
    let output = CAPTURE.capture_path(name);
    let child = CAPTURE
        .command("pg_recvlogical")
        .args(["-S", "shiba_m3_slot", "--start", "-f"])
        .arg(&output)
        .args([
            "-n",
            "-F",
            "3600",
            "-s",
            "0",
            "-o",
            "proto_version=1",
            "-o",
            "publication_names=shiba_m3_pub",
        ])
        .spawn()
        .expect("start non-acking capture");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let row = client
            .query_one(
                "SELECT active FROM pg_replication_slots WHERE slot_name = 'shiba_m3_slot'",
                &[],
            )
            .expect("read active slot");
        if row.get::<_, bool>(0) {
            break;
        }
        assert!(Instant::now() < deadline, "capture did not claim slot");
        thread::sleep(Duration::from_millis(10));
    }
    client
        .execute("INSERT INTO source_m3.events VALUES (103)", &[])
        .expect("commit row in unacknowledged window");
    let capture = loop {
        let bytes = fs::read(&output).unwrap_or_default();
        if framed_message_count(&bytes) == Some(4) {
            break bytes;
        }
        assert!(
            Instant::now() < deadline,
            "capture did not receive one complete transaction: {} bytes",
            bytes.len(),
        );
        thread::sleep(Duration::from_millis(10));
    };
    let stop = Command::new("kill")
        .args(["-STOP", &child.id().to_string()])
        .status()
        .expect("stop capture at crash point");
    assert!(stop.success(), "stop capture at crash point");
    let row = client
        .query_one(
            "SELECT confirmed_flush_lsn::text, active
             FROM pg_replication_slots WHERE slot_name = 'shiba_m3_slot'",
            &[],
        )
        .expect("read unacknowledged slot position");
    assert_eq!(row.get::<_, &str>(0), baseline);
    assert!(row.get::<_, bool>(1));
    (
        strip_recvlogical_delimiters(&capture),
        StoppedCapture(Some(child)),
    )
}

fn install_source(client: &mut Client) -> PgoutputSource {
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m3;
             CREATE TABLE source_m3.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m3_pub FOR TABLE source_m3.events;",
        )
        .expect("install M3 database objects");
    let relation_id = u32::try_from(
        client
            .query_one("SELECT 'source_m3.events'::regclass::oid::bigint", &[])
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(client, "source_m3.events");
    CAPTURE.create_slot();
    support::configure_graph_ingress(client, 1, CAPTURE.publication, CAPTURE.slot);
    source
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m3.sh"]
fn m3_real_pgoutput_replay_decode_failure_and_capture_restart() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    let source = install_source(&mut client);

    client
        .batch_execute("INSERT INTO source_m3.events VALUES (101), (102)")
        .expect("commit first real source transaction");
    let first_wire = CAPTURE.capture(&mut client, "first.pgoutput");
    let first = decode_committed_changes(&first_wire, &support::singleton_graph(1, source))
        .expect("decode first transaction");
    assert_eq!(
        process(&mut client, &first).expect("apply first"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    assert!(
        decode_committed_changes(
            &first_wire[..first_wire.len() - 1],
            &support::singleton_graph(1, source)
        )
        .is_err()
    );
    let mut corrupt = first_wire.clone();
    corrupt[0] = b'X';
    assert!(decode_committed_changes(&corrupt, &support::singleton_graph(1, source)).is_err());
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));
    assert_eq!(
        process(&mut client, &first).expect("exact replay"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    let slot_before: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'shiba_m3_slot'",
            &[],
        )
        .expect("read slot position before crash window")
        .get(0);
    let (second_wire, stopped_capture) =
        capture_committed_row_without_ack(&mut client, &slot_before, "unacked.pgoutput");
    let second = decode_committed_changes(&second_wire, &support::singleton_graph(1, source))
        .expect("decode after restart");
    assert_ne!(first.identity, second.identity);
    assert_eq!(
        process(&mut client, &second).expect("apply second"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (3, 3, 3, 2));
    stopped_capture.crash();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let row = client
            .query_one(
                "SELECT confirmed_flush_lsn::text, active FROM pg_replication_slots WHERE slot_name = 'shiba_m3_slot'",
                &[],
            )
            .expect("read slot position after crash");
        assert_eq!(row.get::<_, &str>(0), slot_before);
        if !row.get::<_, bool>(1) {
            break;
        }
        assert!(Instant::now() < deadline, "crashed capture remains active");
        thread::sleep(Duration::from_millis(10));
    }
    let replay_wire = CAPTURE.capture(&mut client, "replayed.pgoutput");
    let replay = decode_committed_changes(&replay_wire, &support::singleton_graph(1, source))
        .expect("decode slot replay");
    assert_eq!(second, replay);
    assert_eq!(
        process(&mut client, &replay).expect("apply slot replay"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (3, 3, 3, 2));
    let acknowledged: bool = client
        .query_one(
            "SELECT confirmed_flush_lsn > $1::text::pg_lsn
             FROM pg_replication_slots WHERE slot_name = 'shiba_m3_slot'",
            &[&slot_before],
        )
        .expect("read acknowledged replay position")
        .get(0);
    assert!(acknowledged, "clean replay capture must acknowledge WAL");
    assert_eq!(public_result(&mut client), 3);
}
