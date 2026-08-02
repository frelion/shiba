use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, message_end, read_u16, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m4-empty.sh",
    env_prefix: "SHIBA_M4_EMPTY",
    slot: "shiba_m4_empty_slot",
    publication: "shiba_m4_empty_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 1),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable state");
    (row.get(0), row.get(1), row.get(2), row.get(3))
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
    let connection = CAPTURE.required("DATABASE_URL");
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
    register_source(&mut client, "source_m4_empty.events");
    CAPTURE.create_slot();
    client
        .batch_execute(
            "BEGIN;
             INSERT INTO source_m4_empty.events DEFAULT VALUES;
             INSERT INTO source_m4_empty.events DEFAULT VALUES;
             COMMIT;",
        )
        .expect("commit two empty tuples in one transaction");
    let wire = CAPTURE.capture(&mut client, "empty.pgoutput");

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
             FROM shiba_internal.source_row_state ORDER BY row_state_id",
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
        .query_one(
            "SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1",
            &[],
        )
        .expect("query SQL result")
        .get(0);
    assert_eq!(result, 2);
}
