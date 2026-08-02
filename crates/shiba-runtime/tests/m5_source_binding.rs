use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    PgoutputError, PgoutputSource, ProcessOutcome, decode_committed_changes, process,
};

mod support;

use support::{PgoutputCapture, message_end, read_u32};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m5-source-binding.sh",
    env_prefix: "SHIBA_M5_SOURCE_BINDING",
    slot: "shiba_m5_source_binding_slot",
    publication: "shiba_m5_source_binding_pub",
};

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

fn applied_ids(client: &mut Client) -> Vec<i64> {
    client
        .query(
            "SELECT source_row_id FROM shiba_internal.applied_insert
             ORDER BY source_row_id",
            &[],
        )
        .expect("query applied source rows")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

fn wire_relation_id(wire: &[u8]) -> u32 {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    read_u32(wire, relation + 1)
}

fn relation_id(client: &mut Client, name: &str) -> u32 {
    u32::try_from(
        client
            .query_one("SELECT $1::text::regclass::oid::bigint", &[&name])
            .expect("read relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32")
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m5_source_binding_test;
             CREATE FUNCTION m5_source_binding_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m5_source_binding_crash
             AFTER INSERT ON shiba_internal.source_continuation
             FOR EACH ROW EXECUTE FUNCTION
                 m5_source_binding_test.crash_after_continuation();",
        )
        .expect("install continuation crash point");
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m5-source-binding.sh"]
fn m5_relation_oid_survives_rename_and_rejects_same_name_recreation() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m5_binding;
             CREATE TABLE source_m5_binding.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m5_source_binding_pub
                 FOR TABLE source_m5_binding.events;",
        )
        .expect("install source-binding objects");
    let original_oid = relation_id(&mut client, "source_m5_binding.events");
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        original_oid,
    );
    CAPTURE.create_slot();

    client
        .batch_execute("INSERT INTO source_m5_binding.events VALUES (1001)")
        .expect("commit original-name insert");
    let first_wire = CAPTURE.capture(&mut client, "original-name.pgoutput");
    assert_eq!(wire_relation_id(&first_wire), original_oid);
    let first = decode_committed_changes(&first_wire, source).expect("decode original insert");
    assert_eq!(
        process(&mut client, &first).expect("apply original insert"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(
        process(&mut client, &first).expect("replay original insert"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));

    client
        .batch_execute(
            "ALTER SCHEMA source_m5_binding RENAME TO renamed_schema;
             ALTER TABLE renamed_schema.events RENAME TO renamed_events;
             ALTER TABLE renamed_schema.renamed_events RENAME COLUMN id TO event_id;
             INSERT INTO renamed_schema.renamed_events VALUES (1002);",
        )
        .expect("rename source and commit renamed insert");
    assert_eq!(
        relation_id(&mut client, "renamed_schema.renamed_events"),
        original_oid
    );
    let renamed_wire = CAPTURE.capture(&mut client, "renamed.pgoutput");
    assert_eq!(wire_relation_id(&renamed_wire), original_oid);
    let renamed = decode_committed_changes(&renamed_wire, source).expect("decode renamed insert");
    install_crash_trigger(&mut client);
    assert!(process(&mut client, &renamed).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(applied_ids(&mut client), vec![1001]);
    client
        .batch_execute("DROP SCHEMA m5_source_binding_test CASCADE")
        .expect("remove crash point");
    assert_eq!(
        process(&mut client, &renamed).expect("retry renamed insert"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 2));
    assert_eq!(
        process(&mut client, &renamed).expect("replay renamed insert"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 2));

    client
        .batch_execute(
            "DROP TABLE renamed_schema.renamed_events;
             ALTER SCHEMA renamed_schema RENAME TO source_m5_binding;
             CREATE TABLE source_m5_binding.events (id bigint PRIMARY KEY);
             ALTER PUBLICATION shiba_m5_source_binding_pub
                 ADD TABLE source_m5_binding.events;
             INSERT INTO source_m5_binding.events VALUES (1003);",
        )
        .expect("recreate same-name source and commit insert");
    let recreated_oid = relation_id(&mut client, "source_m5_binding.events");
    assert_ne!(recreated_oid, original_oid);
    let recreated_wire = CAPTURE.capture(&mut client, "recreated.pgoutput");
    assert_eq!(wire_relation_id(&recreated_wire), recreated_oid);
    assert!(matches!(
        decode_committed_changes(&recreated_wire, source),
        Err(PgoutputError::RelationMismatch)
    ));
    assert_eq!(durable_state(&mut client), (2, 2, 2, 2));

    let recreated_source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        recreated_oid,
    );
    let _ = decode_committed_changes(&recreated_wire, recreated_source)
        .expect("same wire is valid for the recreated OID");
    assert_eq!(durable_state(&mut client), (2, 2, 2, 2));
}
