use std::{
    fs,
    path::PathBuf,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("scripts/test-m3.sh must provide {name}"))
}

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT row_count FROM shiba.count_result WHERE singleton = 1),
                (SELECT row_count FROM shiba_internal.count_state WHERE singleton = 1),
                (SELECT count(*) FROM shiba_internal.applied_insert),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable state");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

fn command(name: &str) -> Command {
    let mut command = Command::new(PathBuf::from(required("SHIBA_M3_PG_BINDIR")).join(name));
    command.args([
        "-h",
        &required("SHIBA_M3_HOST"),
        "-p",
        &required("SHIBA_M3_PORT"),
        "-U",
        &required("SHIBA_M3_USER"),
        "-d",
        "postgres",
    ]);
    command
}

fn create_slot() {
    let status = command("pg_recvlogical")
        .args(["-S", "shiba_m3_slot", "-P", "pgoutput", "--create-slot"])
        .status()
        .expect("run pg_recvlogical --create-slot");
    assert!(status.success(), "create logical slot");
}

fn capture(client: &mut Client, name: &str) -> Vec<u8> {
    let end_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .expect("read capture end LSN")
        .get(0);
    let output = PathBuf::from(required("SHIBA_M3_CAPTURE_DIR")).join(name);
    let status = command("pg_recvlogical")
        .args(["-S", "shiba_m3_slot", "--start", "-f"])
        .arg(&output)
        .args([
            "-n",
            "-E",
            &end_lsn,
            "-o",
            "proto_version=1",
            "-o",
            "publication_names=shiba_m3_pub",
        ])
        .status()
        .expect("capture pgoutput");
    assert!(status.success(), "capture through end LSN {end_lsn}");
    strip_recvlogical_delimiters(&fs::read(output).expect("read captured pgoutput"))
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
    let output = PathBuf::from(required("SHIBA_M3_CAPTURE_DIR")).join(name);
    let child = command("pg_recvlogical")
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
        if delimiter_count(&bytes) == 4 {
            break bytes;
        }
        assert!(
            Instant::now() < deadline,
            "capture did not receive transaction"
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

fn delimiter_count(bytes: &[u8]) -> usize {
    bytes.split(|byte| *byte == b'\n').count().saturating_sub(1)
}

// pg_recvlogical appends one newline after every XLogData payload. Parse the
// narrow B/R/I/C protocol-v1 records so only that client framing is removed.
fn strip_recvlogical_delimiters(capture: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    let mut start = 0;
    while start < capture.len() {
        let end = message_end(capture, start);
        assert_eq!(capture.get(end), Some(&b'\n'), "missing client delimiter");
        wire.extend_from_slice(&capture[start..end]);
        start = end + 1;
    }
    wire
}

fn message_end(bytes: &[u8], start: usize) -> usize {
    let mut at = start + 1;
    match bytes[start] {
        b'B' => at + 20,
        b'C' => at + 25,
        b'R' => {
            at += 4;
            at = cstring_end(bytes, at);
            at = cstring_end(bytes, at);
            at += 1;
            let columns = read_u16(bytes, at);
            at += 2;
            for _ in 0..columns {
                at += 1;
                at = cstring_end(bytes, at);
                at += 8;
            }
            at
        }
        b'I' => {
            at += 5;
            let columns = read_u16(bytes, at);
            at += 2;
            for _ in 0..columns {
                let kind = bytes[at];
                at += 1;
                if matches!(kind, b't' | b'b') {
                    let length = usize::try_from(read_u32(bytes, at)).expect("tuple length");
                    at += 4 + length;
                } else {
                    assert!(matches!(kind, b'n' | b'u'), "unknown tuple kind");
                }
            }
            at
        }
        tag => panic!("unexpected pgoutput tag {tag:#x}"),
    }
}

fn cstring_end(bytes: &[u8], start: usize) -> usize {
    start
        + bytes[start..]
            .iter()
            .position(|byte| *byte == 0)
            .expect("terminated pgoutput string")
        + 1
}

fn read_u16(bytes: &[u8], at: usize) -> u16 {
    u16::from_be_bytes(bytes[at..at + 2].try_into().expect("u16 field"))
}

fn read_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(bytes[at..at + 4].try_into().expect("u32 field"))
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m3.sh"]
fn m3_real_pgoutput_replay_decode_failure_and_capture_restart() {
    let connection = required("SHIBA_M3_DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
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
    create_slot();

    client
        .batch_execute("INSERT INTO source_m3.events VALUES (101), (102)")
        .expect("commit first real source transaction");
    let first_wire = capture(&mut client, "first.pgoutput");
    let first = decode_committed_changes(&first_wire, source).expect("decode first transaction");
    assert_eq!(
        process(&mut client, &first).expect("apply first"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    assert!(decode_committed_changes(&first_wire[..first_wire.len() - 1], source).is_err());
    let mut corrupt = first_wire.clone();
    corrupt[0] = b'X';
    assert!(decode_committed_changes(&corrupt, source).is_err());
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
    let second = decode_committed_changes(&second_wire, source).expect("decode after restart");
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
    let replay_wire = capture(&mut client, "replayed.pgoutput");
    let replay = decode_committed_changes(&replay_wire, source).expect("decode slot replay");
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
    let result: i64 = client
        .query_one("SELECT row_count FROM shiba.count_result", &[])
        .expect("query SQL result")
        .get(0);
    assert_eq!(result, 3);
}
