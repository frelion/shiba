use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{M2Error, PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m7-column-invalidation.sh",
    env_prefix: "SHIBA_M7_COLUMN_INVALIDATION",
    slot: "shiba_m7_column_invalidation_slot",
    publication: "shiba_m7_column_invalidation_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.graph_result WHERE graph_id = 1 AND result_id = 1001),
                (SELECT state_payload FROM shiba_internal.graph_node_state WHERE graph_id = 1 AND node_id = 1 AND namespace = 0),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.graph_continuation)",
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

fn relation_oid(client: &mut Client, name: &str) -> u32 {
    u32::try_from(
        client
            .query_one("SELECT $1::text::regclass::oid::bigint", &[&name])
            .expect("read relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32")
}

fn assert_exact_invalidation(
    client: &mut Client,
    source_id: i64,
    relation_id: u32,
    object_sub_id: i32,
) {
    let row = client
        .query_one(
            "SELECT address_classid = 'pg_class'::regclass,
                    address_objid::bigint, address_objsubid
             FROM shiba_internal.source_invalidation
             WHERE source_id = $1 AND address_objsubid = $2",
            &[&source_id, &object_sub_id],
        )
        .expect("query exact column invalidation");
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i64>(1), i64::from(relation_id));
    assert_eq!(row.get::<_, i32>(2), object_sub_id);
}

fn prove_column_rename_invalidation(client: &mut Client) {
    client
        .batch_execute(
            "CREATE TABLE source_m7_column.rename_source (
                 id bigint PRIMARY KEY,
                 payload bigint
             );",
        )
        .expect("install rename-only source object");
    let relation_id = relation_oid(client, "source_m7_column.rename_source");
    client
        .query_one(
            "SELECT shiba_internal.register_source(
                2, 'source_m7_column.rename_source'::regclass)",
            &[],
        )
        .expect("register rename-only source relation");
    let binding_sub_ids: Vec<i32> = client
        .query(
            "SELECT address_objsubid FROM shiba_internal.source_binding
             WHERE source_id = 2 ORDER BY address_objsubid",
            &[],
        )
        .expect("query relation and column bindings")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(binding_sub_ids, vec![0, 1, 2]);
    client
        .batch_execute(
            "ALTER TABLE source_m7_column.rename_source
                 RENAME COLUMN payload TO renamed_payload",
        )
        .expect("commit source column rename");
    assert_exact_invalidation(client, 2, relation_id, 2);
    let count: i64 = client
        .query_one(
            "SELECT count(*) FROM shiba_internal.source_invalidation
             WHERE source_id = 2",
            &[],
        )
        .expect("count rename-only invalidations")
        .get(0);
    assert_eq!(count, 1);
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m7-column-invalidation.sh"]
fn m7_column_type_rollback_commit_and_rename_invalidation() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m7_column;
             CREATE TABLE source_m7_column.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m7_column_invalidation_pub
                 FOR TABLE source_m7_column.events;",
        )
        .expect("install column-invalidation source objects");
    let relation_id = relation_oid(&mut client, "source_m7_column.events");
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m7_column.events");
    CAPTURE.create_slot();

    client
        .batch_execute("INSERT INTO source_m7_column.events VALUES (1301)")
        .expect("commit first pending source transaction");
    let first_wire = CAPTURE.capture(&mut client, "before-type-rollback.pgoutput");
    let first = decode_committed_changes(&first_wire, &support::singleton_graph(1, source))
        .expect("decode first pending row");
    client
        .batch_execute(
            "BEGIN;
             ALTER TABLE source_m7_column.events
                 ALTER COLUMN id TYPE integer USING id::integer;
             ROLLBACK;",
        )
        .expect("roll back source column type change");
    let invalidations: i64 = client
        .query_one(
            "SELECT count(*) FROM shiba_internal.source_invalidation",
            &[],
        )
        .expect("query rolled-back invalidation")
        .get(0);
    assert_eq!(invalidations, 0);
    assert_eq!(
        relation_oid(&mut client, "source_m7_column.events"),
        relation_id
    );
    assert_eq!(
        process(&mut client, &first).expect("apply after rolled-back type DDL"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));

    client
        .batch_execute("INSERT INTO source_m7_column.events VALUES (1302)")
        .expect("commit second pending source transaction");
    let second_wire = CAPTURE.capture(&mut client, "before-type-commit.pgoutput");
    let second = decode_committed_changes(&second_wire, &support::singleton_graph(1, source))
        .expect("decode second pending row");
    client
        .batch_execute(
            "ALTER TABLE source_m7_column.events
                 ALTER COLUMN id TYPE integer USING id::integer",
        )
        .expect("commit source column type change");
    assert_eq!(
        relation_oid(&mut client, "source_m7_column.events"),
        relation_id
    );
    assert_exact_invalidation(&mut client, 1, relation_id, 0);
    assert!(matches!(
        process(&mut client, &second),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(
        process(&mut client, &first).expect("replay before type change"),
        ProcessOutcome::AlreadyApplied
    );

    prove_column_rename_invalidation(&mut client);
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
}
