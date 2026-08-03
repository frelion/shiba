use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{M2Error, PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m7-index-invalidation.sh",
    env_prefix: "SHIBA_M7_INDEX_INVALIDATION",
    slot: "shiba_m7_index_invalidation_slot",
    publication: "shiba_m7_index_invalidation_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.graph_result WHERE graph_id = 1 AND result_id = 2),
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

fn object_oid(client: &mut Client, name: &str) -> u32 {
    u32::try_from(
        client
            .query_one("SELECT $1::text::regclass::oid::bigint", &[&name])
            .expect("read object OID")
            .get::<_, i64>(0),
    )
    .expect("object OID fits u32")
}

fn invalidation_count(client: &mut Client) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM shiba_internal.source_invalidation",
            &[],
        )
        .expect("query source invalidations")
        .get(0)
}

fn assert_binding_set(client: &mut Client, relation_id: u32, index_id: u32) {
    let bindings: Vec<(String, i64, i32)> = client
        .query(
            "SELECT binding_kind, address_objid::bigint, address_objsubid
             FROM shiba_internal.source_binding
             WHERE source_id = 1
             ORDER BY binding_kind",
            &[],
        )
        .expect("query identity-index bindings")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(
        bindings,
        vec![
            ("column".to_owned(), i64::from(relation_id), 1),
            ("identity_index".to_owned(), i64::from(index_id), 0),
            ("relation".to_owned(), i64::from(relation_id), 0),
        ]
    );
}

fn prove_unrelated_index_isolation(client: &mut Client) {
    client
        .batch_execute(
            "ALTER INDEX source_m7_index.unrelated_key
                 RENAME TO still_unrelated_key",
        )
        .expect("rename unrelated index");
    assert_eq!(invalidation_count(client), 0);
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m7-index-invalidation.sh"]
fn m7_replica_identity_index_rollback_commit_and_isolation() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m7_index;
             CREATE TABLE source_m7_index.events (id bigint NOT NULL);
             CREATE UNIQUE INDEX events_identity_key ON source_m7_index.events (id);
             ALTER TABLE source_m7_index.events
                 REPLICA IDENTITY USING INDEX events_identity_key;
             CREATE TABLE source_m7_index.unrelated (id bigint NOT NULL);
             CREATE INDEX unrelated_key ON source_m7_index.unrelated (id);
             CREATE PUBLICATION shiba_m7_index_invalidation_pub
                 FOR TABLE source_m7_index.events;",
        )
        .expect("install identity-index source objects");
    let relation_id = object_oid(&mut client, "source_m7_index.events");
    let index_id = object_oid(&mut client, "source_m7_index.events_identity_key");
    let source = PgoutputSource::with_replica_index(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m7_index.events");
    assert_binding_set(&mut client, relation_id, index_id);
    prove_unrelated_index_isolation(&mut client);
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    client
        .batch_execute("INSERT INTO source_m7_index.events VALUES (1401)")
        .expect("commit first source transaction");
    let first_wire = CAPTURE.capture(&mut client, "first.pgoutput");
    let first = decode_committed_changes(&first_wire, &support::singleton_graph(1, source))
        .expect("decode first row");
    assert_eq!(
        process(&mut client, &first).expect("apply first row"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));

    client
        .batch_execute("INSERT INTO source_m7_index.events VALUES (1402)")
        .expect("commit rollback-pending transaction");
    let second_wire = CAPTURE.capture(&mut client, "before-rollback.pgoutput");
    let second = decode_committed_changes(&second_wire, &support::singleton_graph(1, source))
        .expect("decode second row");
    client
        .batch_execute(
            "BEGIN;
             ALTER INDEX source_m7_index.events_identity_key
                 RENAME TO rolled_back_identity_key;
             ROLLBACK;",
        )
        .expect("roll back identity-index rename");
    assert_eq!(invalidation_count(&mut client), 0);
    assert_eq!(
        object_oid(&mut client, "source_m7_index.events_identity_key"),
        index_id
    );
    assert_eq!(
        process(&mut client, &second).expect("apply after rolled-back index DDL"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 2));

    client
        .batch_execute("INSERT INTO source_m7_index.events VALUES (1403)")
        .expect("commit invalidation-pending transaction");
    let third_wire = CAPTURE.capture(&mut client, "before-commit.pgoutput");
    let third = decode_committed_changes(&third_wire, &support::singleton_graph(1, source))
        .expect("decode third row");
    client
        .batch_execute(
            "ALTER INDEX source_m7_index.events_identity_key
                 RENAME TO committed_identity_key",
        )
        .expect("commit identity-index rename");
    assert_eq!(
        object_oid(&mut client, "source_m7_index.committed_identity_key"),
        index_id
    );
    let invalidation = client
        .query_one(
            "SELECT address_classid = 'pg_class'::regclass,
                    address_objid::bigint, address_objsubid
             FROM shiba_internal.source_invalidation WHERE source_id = 1",
            &[],
        )
        .expect("query identity-index invalidation");
    assert!(invalidation.get::<_, bool>(0));
    assert_eq!(invalidation.get::<_, i64>(1), i64::from(index_id));
    assert_eq!(invalidation.get::<_, i32>(2), 0);
    assert!(matches!(
        process(&mut client, &third),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(durable_state(&mut client), (2, 2, 2, 2));
    assert_eq!(
        process(&mut client, &second).expect("replay before index invalidation"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 2));
}
