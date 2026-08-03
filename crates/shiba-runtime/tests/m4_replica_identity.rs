use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, cstring_end, message_end, read_u16, read_u32, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m4-replica-identity.sh",
    env_prefix: "SHIBA_M4_REPLICA",
    slot: "shiba_m4_replica_slot",
    publication: "shiba_m4_replica_pub",
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

fn relation_metadata(wire: &[u8]) -> (u8, u16, u8, usize) {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let mut at = relation + 5;
    at = cstring_end(wire, at);
    at = cstring_end(wire, at);
    let identity = wire[at];
    let columns = read_u16(wire, at + 1);
    let first_column_flags = wire[at + 3];
    (
        identity,
        columns,
        first_column_flags,
        message_end(wire, relation),
    )
}

fn assert_full_delete_shape(wire: &[u8]) {
    let (identity, columns, _, delete) = relation_metadata(wire);
    assert_eq!(identity, b'f');
    assert_eq!(columns, 1);
    assert_eq!(wire[delete], b'D');
    assert_eq!(wire[delete + 5], b'O');
    assert_eq!(read_u16(wire, delete + 6), 1);
    assert_eq!(wire[delete + 8], b't');
    let length = usize::try_from(read_u32(wire, delete + 9)).expect("key length");
    assert_eq!(&wire[delete + 13..delete + 13 + length], b"501");
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m4-replica-identity.sh"]
fn m4_replica_identity_full_delete_stops_before_apply() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m4_replica;
             CREATE TABLE source_m4_replica.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m4_replica_pub
                 FOR TABLE source_m4_replica.events;",
        )
        .expect("install replica identity source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m4_replica.events'::regclass::oid::bigint",
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
    register_source(&mut client, "source_m4_replica.events");
    CAPTURE.create_slot();

    client
        .batch_execute("INSERT INTO source_m4_replica.events VALUES (501)")
        .expect("commit default-identity insert");
    let insert_wire = CAPTURE.capture(&mut client, "default-insert.pgoutput");
    let (identity, columns, key_flags, insert_at) = relation_metadata(&insert_wire);
    assert_eq!((identity, columns, key_flags), (b'd', 1, 1));
    assert_eq!(insert_wire[insert_at], b'I');
    let insert = decode_committed_changes(&insert_wire, source).expect("decode default insert");
    assert_eq!(
        process(&mut client, &insert).expect("apply default insert"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(
        process(&mut client, &insert).expect("replay default insert"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));

    client
        .batch_execute(
            "ALTER TABLE source_m4_replica.events REPLICA IDENTITY FULL;
             DELETE FROM source_m4_replica.events WHERE id = 501;",
        )
        .expect("commit full-identity delete");
    let delete_wire = CAPTURE.capture(&mut client, "full-delete.pgoutput");
    assert_full_delete_shape(&delete_wire);
    assert!(decode_committed_changes(&delete_wire, source).is_err());
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
}
