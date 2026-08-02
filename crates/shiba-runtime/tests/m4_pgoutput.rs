use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{M2Error, PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, message_end, read_u16, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m4.sh",
    env_prefix: "SHIBA_M4",
    slot: "shiba_m4_slot",
    publication: "shiba_m4_pub",
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

fn first_key_tuple_tag(wire: &[u8]) -> usize {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let insert = message_end(wire, relation);
    assert_eq!(wire[insert], b'I');
    assert_eq!(wire[insert + 5], b'N');
    assert_eq!(read_u16(wire, insert + 6), 2);
    insert + 8
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m4.sh"]
fn m4_real_pgoutput_nullable_payload_and_bad_key_tag() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m4;
             CREATE TABLE source_m4.events (
                 id bigint PRIMARY KEY,
                 payload bigint NULL
             );
             CREATE PUBLICATION shiba_m4_pub FOR TABLE source_m4.events;",
        )
        .expect("install M4 database objects");
    let relation_id = u32::try_from(
        client
            .query_one("SELECT 'source_m4.events'::regclass::oid::bigint", &[])
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m4.events");
    CAPTURE.create_slot();
    client
        .batch_execute("INSERT INTO source_m4.events VALUES (201, NULL), (202, 42)")
        .expect("commit real nullable-payload transaction");
    let wire = CAPTURE.capture(&mut client, "m4.pgoutput");

    let mut bad_key = wire.clone();
    let key_tag = first_key_tuple_tag(&bad_key);
    assert_eq!(bad_key[key_tag], b't');
    bad_key[key_tag] = b'n';
    assert!(decode_committed_changes(&bad_key, source).is_err());
    assert_eq!(durable_state(&mut client), (0, 0, 0, 0));

    let transaction = decode_committed_changes(&wire, source).expect("decode payload transaction");
    client
        .batch_execute(
            "CREATE SCHEMA m4_test;
             CREATE FUNCTION m4_test.fail_operator() RETURNS trigger
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'M4 failure'; END $$;
             CREATE TRIGGER m4_fail_operator BEFORE UPDATE
             ON shiba_internal.operator_state FOR EACH ROW
             EXECUTE FUNCTION m4_test.fail_operator();",
        )
        .expect("install payload rollback failure point");
    assert!(matches!(
        process(&mut client, &transaction),
        Err(M2Error::Postgres(_))
    ));
    assert_eq!(durable_state(&mut client), (0, 0, 0, 0));
    client
        .batch_execute("DROP SCHEMA m4_test CASCADE")
        .expect("remove payload rollback failure point");
    assert_eq!(
        process(&mut client, &transaction).expect("apply payload transaction"),
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
            "SELECT source_row_id, payload_present, payload_int8
             FROM shiba_internal.source_row_state ORDER BY row_state_id",
            &[],
        )
        .expect("query applied payload facts");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (
            rows[0].get::<_, i64>(0),
            rows[0].get::<_, bool>(1),
            rows[0].get::<_, Option<i64>>(2)
        ),
        (201, true, None)
    );
    assert_eq!(
        (
            rows[1].get::<_, i64>(0),
            rows[1].get::<_, bool>(1),
            rows[1].get::<_, Option<i64>>(2)
        ),
        (202, true, Some(42))
    );
    let result: i64 = client
        .query_one(
            "SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1",
            &[],
        )
        .expect("query SQL result")
        .get(0);
    assert_eq!(result, 2);
}
