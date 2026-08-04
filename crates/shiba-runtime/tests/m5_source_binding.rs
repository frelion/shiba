use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    M2Error, PgoutputError, PgoutputSource, ProcessOutcome, decode_committed_changes, process,
};

mod support;

use support::{PgoutputCapture, message_end, read_u32, register_source};

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

fn applied_ids(client: &mut Client) -> Vec<i64> {
    client
        .query(
            "SELECT source_row_id FROM shiba_internal.source_row_state
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
    register_source(&mut client, "source_m5_binding.events");
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    client
        .batch_execute("INSERT INTO source_m5_binding.events VALUES (1001)")
        .expect("commit original-name insert");
    let first_wire = CAPTURE.capture(&mut client, "original-name.pgoutput");
    assert_eq!(wire_relation_id(&first_wire), original_oid);
    let first = decode_committed_changes(&first_wire, &support::singleton_graph(1, source))
        .expect("decode original insert");
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
    let renamed = decode_committed_changes(&renamed_wire, &support::singleton_graph(1, source))
        .expect("decode renamed insert");
    assert!(matches!(
        process(&mut client, &renamed),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(applied_ids(&mut client), vec![1001]);

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
        decode_committed_changes(&recreated_wire, &support::singleton_graph(1, source)),
        Err(PgoutputError::RelationMismatch)
    ));
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));

    let recreated_source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        recreated_oid,
    );
    let _ = decode_committed_changes(
        &recreated_wire,
        &support::singleton_graph(1, recreated_source),
    )
    .expect("same wire is valid for the recreated OID");
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
}
