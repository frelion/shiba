use std::{fs, path::PathBuf, process::Command};

use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("scripts/test-m4-empty.sh must provide {name}"))
}

fn command(name: &str) -> Command {
    let mut command = Command::new(PathBuf::from(required("SHIBA_M4_EMPTY_PG_BINDIR")).join(name));
    command.args([
        "-h",
        &required("SHIBA_M4_EMPTY_HOST"),
        "-p",
        &required("SHIBA_M4_EMPTY_PORT"),
        "-U",
        &required("SHIBA_M4_EMPTY_USER"),
        "-d",
        "postgres",
    ]);
    command
}

fn create_slot() {
    let status = command("pg_recvlogical")
        .args([
            "-S",
            "shiba_m4_empty_slot",
            "-P",
            "pgoutput",
            "--create-slot",
        ])
        .status()
        .expect("run pg_recvlogical --create-slot");
    assert!(status.success(), "create logical slot");
}

fn capture(client: &mut Client) -> Vec<u8> {
    let end_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .expect("read capture end LSN")
        .get(0);
    let output = PathBuf::from(required("SHIBA_M4_EMPTY_CAPTURE_DIR")).join("empty.pgoutput");
    let status = command("pg_recvlogical")
        .args(["-S", "shiba_m4_empty_slot", "--start", "-f"])
        .arg(&output)
        .args([
            "-n",
            "-E",
            &end_lsn,
            "-o",
            "proto_version=1",
            "-o",
            "publication_names=shiba_m4_empty_pub",
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

// pg_recvlogical adds one newline per XLogData. Structural message lengths
// identify only those delimiters, so no payload byte is globally rewritten.
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

fn first_insert_column_count(wire: &[u8]) -> usize {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let insert = message_end(wire, relation);
    assert_eq!(read_u16(wire, insert - 2), 0);
    assert_eq!(wire[insert], b'I');
    assert_eq!(wire[insert + 5], b'N');
    insert + 6
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m4-empty.sh"]
fn m4_real_pgoutput_empty_tuples_and_bad_column_count() {
    let connection = required("SHIBA_M4_EMPTY_DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m4_empty;
             CREATE TABLE source_m4_empty.events ();
             CREATE PUBLICATION shiba_m4_empty_pub FOR TABLE source_m4_empty.events;",
        )
        .expect("install empty source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m4_empty.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::empty(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    create_slot();
    client
        .batch_execute(
            "BEGIN;
             INSERT INTO source_m4_empty.events DEFAULT VALUES;
             INSERT INTO source_m4_empty.events DEFAULT VALUES;
             COMMIT;",
        )
        .expect("commit two empty tuples in one transaction");
    let wire = capture(&mut client);

    let mut bad_columns = wire.clone();
    let count = first_insert_column_count(&bad_columns);
    assert_eq!(&bad_columns[count..count + 2], &[0, 0]);
    bad_columns[count + 1] = 1;
    assert!(decode_committed_changes(&bad_columns, source).is_err());
    assert_eq!(durable_state(&mut client), (0, 0, 0, 0));

    let transaction = decode_committed_changes(&wire, source).expect("decode empty transaction");
    assert_eq!(
        process(&mut client, &transaction).expect("apply empty transaction"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));
    assert_eq!(
        process(&mut client, &transaction).expect("exact replay"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    let rows = client
        .query(
            "SELECT source_row_id IS NULL, payload_present, payload_int8
             FROM shiba_internal.applied_insert ORDER BY input_sequence",
            &[],
        )
        .expect("query applied empty facts");
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert!(row.get::<_, bool>(0));
        assert!(!row.get::<_, bool>(1));
        assert_eq!(row.get::<_, Option<i64>>(2), None);
    }
    let result: i64 = client
        .query_one("SELECT row_count FROM shiba.count_result", &[])
        .expect("query SQL result")
        .get(0);
    assert_eq!(result, 2);
}
