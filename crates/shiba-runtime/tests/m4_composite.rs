use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, message_end, read_u16, read_u32, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m4-composite.sh",
    env_prefix: "SHIBA_M4_COMPOSITE",
    slot: "shiba_m4_composite_slot",
    publication: "shiba_m4_composite_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1),
                (SELECT state_payload FROM shiba_internal.operator_state WHERE operator_id = 1),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable state");
    (
        row.get(0),
        support::decode_scalar_state(&row.get::<_, Vec<u8>>(1)),
        row.get(2),
        row.get(3),
    )
}

fn first_insert_second_key_tag(wire: &[u8]) -> usize {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let insert = message_end(wire, relation);
    assert_eq!(wire[insert], b'I');
    assert_eq!(wire[insert + 5], b'N');
    assert_eq!(read_u16(wire, insert + 6), 2);
    let first_tag = insert + 8;
    assert_eq!(wire[first_tag], b't');
    first_tag + 5 + usize::try_from(read_u32(wire, first_tag + 1)).expect("first key length")
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m4-composite.sh"]
fn m4_real_pgoutput_composite_keys_and_bad_second_key_tag() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m4_composite;
             CREATE TABLE source_m4_composite.events (
                 tenant_id bigint,
                 id bigint,
                 PRIMARY KEY (tenant_id, id)
             );
             CREATE PUBLICATION shiba_m4_composite_pub
                 FOR TABLE source_m4_composite.events;",
        )
        .expect("install composite source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m4_composite.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::composite_int8(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m4_composite.events");
    CAPTURE.create_slot();
    client
        .batch_execute("INSERT INTO source_m4_composite.events VALUES (10, 201), (10, 202)")
        .expect("commit same-first-key composite rows");
    let wire = CAPTURE.capture(&mut client, "composite.pgoutput");

    let mut bad_second_key = wire.clone();
    let second_tag = first_insert_second_key_tag(&bad_second_key);
    assert_eq!(bad_second_key[second_tag], b't');
    bad_second_key[second_tag] = b'n';
    assert!(decode_committed_changes(&bad_second_key, source).is_err());
    assert_eq!(durable_state(&mut client), (0, 0, 0, 0));

    let transaction =
        decode_committed_changes(&wire, source).expect("decode composite transaction");
    assert_eq!(
        process(&mut client, &transaction).expect("apply composite transaction"),
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
            "SELECT source_row_id, source_row_sub_id
             FROM shiba_internal.source_row_state ORDER BY row_state_id",
            &[],
        )
        .expect("query applied composite facts");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (rows[0].get::<_, i64>(0), rows[0].get::<_, i64>(1)),
        (10, 201)
    );
    assert_eq!(
        (rows[1].get::<_, i64>(0), rows[1].get::<_, i64>(1)),
        (10, 202)
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
