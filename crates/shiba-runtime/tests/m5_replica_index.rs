use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, cstring_end, message_end, read_u16, read_u32, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m5-replica-index.sh",
    env_prefix: "SHIBA_M5_REPLICA_INDEX",
    slot: "shiba_m5_replica_index_slot",
    publication: "shiba_m5_replica_index_pub",
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

fn relation_metadata(wire: &[u8]) -> (u32, u8, u16, u8, usize) {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let relation_id = read_u32(wire, relation + 1);
    let mut at = relation + 5;
    at = cstring_end(wire, at);
    at = cstring_end(wire, at);
    let identity = wire[at];
    let columns = read_u16(wire, at + 1);
    let key_flags = wire[at + 3];
    (
        relation_id,
        identity,
        columns,
        key_flags,
        message_end(wire, relation),
    )
}

fn assert_index_delete(wire: &[u8], expected_relation: u32) {
    let (relation, identity, columns, key_flags, delete) = relation_metadata(wire);
    assert_eq!((relation, identity), (expected_relation, b'i'));
    assert_eq!((columns, key_flags), (1, 1));
    assert_eq!(wire[delete], b'D');
    assert_eq!(wire[delete + 5], b'K');
    assert_eq!(read_u16(wire, delete + 6), 1);
    assert_eq!(wire[delete + 8], b't');
    let length = usize::try_from(read_u32(wire, delete + 9)).expect("key length");
    assert_eq!(&wire[delete + 13..delete + 13 + length], b"901");
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m5_replica_index_test;
             CREATE FUNCTION m5_replica_index_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m5_replica_index_crash
             AFTER INSERT ON shiba_internal.graph_continuation
             FOR EACH ROW EXECUTE FUNCTION
                 m5_replica_index_test.crash_after_continuation();",
        )
        .expect("install continuation crash point");
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m5-replica-index.sh"]
fn m5_real_replica_index_delete_and_identity_drift() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m5_replica_index;
             CREATE TABLE source_m5_replica_index.events (id bigint PRIMARY KEY);
             CREATE UNIQUE INDEX events_replica_key
                 ON source_m5_replica_index.events (id);
             ALTER TABLE source_m5_replica_index.events
                 REPLICA IDENTITY USING INDEX events_replica_key;
             CREATE PUBLICATION shiba_m5_replica_index_pub
                 FOR TABLE source_m5_replica_index.events;",
        )
        .expect("install replica-index source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m5_replica_index.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::with_replica_index(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m5_replica_index.events");
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    client
        .batch_execute("INSERT INTO source_m5_replica_index.events VALUES (901)")
        .expect("commit replica-index insert");
    let insert_wire = CAPTURE.capture(&mut client, "replica-index-insert.pgoutput");
    let (relation, identity, columns, key_flags, insert_at) = relation_metadata(&insert_wire);
    assert_eq!((relation, identity), (relation_id, b'i'));
    assert_eq!((columns, key_flags), (1, 1));
    assert_eq!(insert_wire[insert_at], b'I');
    let insert = decode_committed_changes(&insert_wire, &support::singleton_graph(1, source))
        .expect("decode index insert");
    assert_eq!(
        process(&mut client, &insert).expect("apply index insert"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(
        process(&mut client, &insert).expect("replay index insert"),
        ProcessOutcome::AlreadyApplied
    );

    client
        .batch_execute("DELETE FROM source_m5_replica_index.events WHERE id = 901")
        .expect("commit replica-index delete");
    let delete_wire = CAPTURE.capture(&mut client, "replica-index-delete.pgoutput");
    assert_index_delete(&delete_wire, relation_id);
    let delete = decode_committed_changes(&delete_wire, &support::singleton_graph(1, source))
        .expect("decode index delete");
    install_crash_trigger(&mut client);
    assert!(process(&mut client, &delete).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    client
        .batch_execute("DROP SCHEMA m5_replica_index_test CASCADE")
        .expect("remove crash point");

    assert_eq!(
        process(&mut client, &delete).expect("retry index delete"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (0, 0, 0, 2));
    assert_eq!(
        process(&mut client, &delete).expect("replay index delete"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (0, 0, 0, 2));

    client
        .batch_execute(
            "ALTER TABLE source_m5_replica_index.events REPLICA IDENTITY DEFAULT;
             INSERT INTO source_m5_replica_index.events VALUES (902);",
        )
        .expect("commit default-identity insert");
    let default_wire = CAPTURE.capture(&mut client, "default-identity-insert.pgoutput");
    let (relation, identity, columns, key_flags, insert_at) = relation_metadata(&default_wire);
    assert_eq!((relation, identity), (relation_id, b'd'));
    assert_eq!((columns, key_flags), (1, 1));
    assert_eq!(default_wire[insert_at], b'I');
    assert!(decode_committed_changes(&default_wire, &support::singleton_graph(1, source)).is_err());
    assert_eq!(durable_state(&mut client), (0, 0, 0, 2));
}
