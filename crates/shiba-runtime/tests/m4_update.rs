use std::{fs, path::PathBuf, process::Command};

use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("scripts/test-m4-update.sh must provide {name}"))
}

fn command(name: &str) -> Command {
    let mut command = Command::new(PathBuf::from(required("SHIBA_M4_UPDATE_PG_BINDIR")).join(name));
    command.args([
        "-h",
        &required("SHIBA_M4_UPDATE_HOST"),
        "-p",
        &required("SHIBA_M4_UPDATE_PORT"),
        "-U",
        &required("SHIBA_M4_UPDATE_USER"),
        "-d",
        "postgres",
    ]);
    command
}

fn create_slot() {
    let status = command("pg_recvlogical")
        .args([
            "-S",
            "shiba_m4_update_slot",
            "-P",
            "pgoutput",
            "--create-slot",
        ])
        .status()
        .expect("run pg_recvlogical --create-slot");
    assert!(status.success(), "create logical slot");
}

fn capture(client: &mut Client, name: &str) -> Vec<u8> {
    let end_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .expect("read capture end LSN")
        .get(0);
    let output = PathBuf::from(required("SHIBA_M4_UPDATE_CAPTURE_DIR")).join(name);
    let status = command("pg_recvlogical")
        .args(["-S", "shiba_m4_update_slot", "--start", "-f"])
        .arg(&output)
        .args([
            "-n",
            "-E",
            &end_lsn,
            "-o",
            "proto_version=1",
            "-o",
            "publication_names=shiba_m4_update_pub",
        ])
        .status()
        .expect("capture pgoutput");
    assert!(status.success(), "capture through end LSN {end_lsn}");
    strip_recvlogical_delimiters(&fs::read(output).expect("read captured pgoutput"))
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

fn payload(client: &mut Client) -> (bool, Option<i64>) {
    let row = client
        .query_one(
            "SELECT payload_present, payload_int8
             FROM shiba_internal.applied_insert WHERE source_row_id = 301",
            &[],
        )
        .expect("query applied payload");
    (row.get(0), row.get(1))
}

// pg_recvlogical adds one newline per XLogData. Structural lengths identify
// only those delimiters; no tuple byte is globally rewritten.
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
        b'I' | b'U' => {
            at += 5;
            let columns = read_u16(bytes, at);
            at += 2;
            for _ in 0..columns {
                let kind = bytes[at];
                at += 1;
                if matches!(kind, b't' | b'b') {
                    at += 4 + usize::try_from(read_u32(bytes, at)).expect("tuple length");
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

fn update_key_tag(wire: &[u8]) -> usize {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let update = message_end(wire, relation);
    assert_eq!(wire[update], b'U');
    assert_eq!(wire[update + 5], b'N', "UPDATE must have only a new tuple");
    assert_eq!(read_u16(wire, update + 6), 2);
    update + 8
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m4_update_test;
             CREATE FUNCTION m4_update_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m4_update_crash
             AFTER INSERT ON shiba_internal.source_continuation
             FOR EACH ROW EXECUTE FUNCTION m4_update_test.crash_after_continuation();",
        )
        .expect("install continuation crash point");
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m4-update.sh"]
fn m4_real_pgoutput_update_replay_decode_failure_and_crash() {
    let connection = required("SHIBA_M4_UPDATE_DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m4_update;
             CREATE TABLE source_m4_update.events (
                 id bigint PRIMARY KEY,
                 payload bigint NULL
             );
             CREATE PUBLICATION shiba_m4_update_pub
                 FOR TABLE source_m4_update.events;",
        )
        .expect("install update source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m4_update.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    create_slot();

    client
        .batch_execute("INSERT INTO source_m4_update.events VALUES (301, 10)")
        .expect("commit source insert");
    let insert_wire = capture(&mut client, "insert.pgoutput");
    let insert = decode_committed_changes(&insert_wire, source).expect("decode insert");
    assert_eq!(
        process(&mut client, &insert).expect("apply insert"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(payload(&mut client), (true, Some(10)));

    client
        .batch_execute("UPDATE source_m4_update.events SET payload = NULL WHERE id = 301")
        .expect("commit source update");
    let update_wire = capture(&mut client, "update.pgoutput");
    let mut bad_update = update_wire.clone();
    let key_tag = update_key_tag(&bad_update);
    assert_eq!(bad_update[key_tag], b't');
    bad_update[key_tag] = b'n';
    assert!(decode_committed_changes(&bad_update, source).is_err());
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(payload(&mut client), (true, Some(10)));

    let update = decode_committed_changes(&update_wire, source).expect("decode update");
    install_crash_trigger(&mut client);
    assert!(process(&mut client, &update).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(payload(&mut client), (true, Some(10)));
    client
        .batch_execute("DROP SCHEMA m4_update_test CASCADE")
        .expect("remove crash point");

    assert_eq!(
        process(&mut client, &update).expect("apply update"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_eq!(payload(&mut client), (true, None));
    assert_eq!(
        process(&mut client, &update).expect("exact update replay"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_eq!(payload(&mut client), (true, None));
}
