use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{M2Error, PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m7-drop-invalidation.sh",
    env_prefix: "SHIBA_M7_DROP_INVALIDATION",
    slot: "shiba_m7_drop_invalidation_slot",
    publication: "shiba_m7_drop_invalidation_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 1),
                (SELECT count(*) FROM shiba_internal.applied_insert),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable state");
    (row.get(0), row.get(1), row.get(2), row.get(3))
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

fn assert_exact_invalidation(client: &mut Client, source_id: i64, relation_id: u32) {
    let row = client
        .query_one(
            "SELECT address_classid = 'pg_class'::regclass,
                    address_objid::bigint, address_objsubid
             FROM shiba_internal.source_invalidation WHERE source_id = $1",
            &[&source_id],
        )
        .expect("query exact drop invalidation");
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i64>(1), i64::from(relation_id));
    assert_eq!(row.get::<_, i32>(2), 0);
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m7-drop-invalidation.sh"]
fn m7_drop_rollback_commit_recreate_and_schema_cascade() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m7_drop;
             CREATE TABLE source_m7_drop.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m7_drop_invalidation_pub
                 FOR TABLE source_m7_drop.events;",
        )
        .expect("install drop-invalidation source objects");
    let original_oid = relation_oid(&mut client, "source_m7_drop.events");
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        original_oid,
    );
    register_source(&mut client, "source_m7_drop.events");
    CAPTURE.create_slot();

    client
        .batch_execute("INSERT INTO source_m7_drop.events VALUES (1201)")
        .expect("commit first pending source transaction");
    let first_wire = CAPTURE.capture(&mut client, "before-rollback-drop.pgoutput");
    let first = decode_committed_changes(&first_wire, source).expect("decode first pending row");
    client
        .batch_execute(
            "BEGIN;
             DROP TABLE source_m7_drop.events;
             ROLLBACK;",
        )
        .expect("roll back source table drop");
    let invalidations: i64 = client
        .query_one(
            "SELECT count(*) FROM shiba_internal.source_invalidation",
            &[],
        )
        .expect("query rolled-back invalidation")
        .get(0);
    assert_eq!(invalidations, 0);
    assert_eq!(
        relation_oid(&mut client, "source_m7_drop.events"),
        original_oid
    );
    assert_eq!(
        process(&mut client, &first).expect("apply after rolled-back drop"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));

    client
        .batch_execute("INSERT INTO source_m7_drop.events VALUES (1202)")
        .expect("commit second pending source transaction");
    let second_wire = CAPTURE.capture(&mut client, "before-committed-drop.pgoutput");
    let second = decode_committed_changes(&second_wire, source).expect("decode second pending row");
    client
        .batch_execute("DROP TABLE source_m7_drop.events")
        .expect("commit source table drop");
    assert_exact_invalidation(&mut client, 1, original_oid);
    assert!(matches!(
        process(&mut client, &second),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));

    client
        .batch_execute("CREATE TABLE source_m7_drop.events (id bigint PRIMARY KEY)")
        .expect("recreate same qualified source name");
    let recreated_oid = relation_oid(&mut client, "source_m7_drop.events");
    assert_ne!(recreated_oid, original_oid);
    assert!(matches!(
        process(&mut client, &second),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(
        process(&mut client, &first).expect("replay transaction before drop"),
        ProcessOutcome::AlreadyApplied
    );

    client
        .batch_execute(
            "CREATE SCHEMA source_m7_cascade;
             CREATE TABLE source_m7_cascade.events (id bigint PRIMARY KEY);",
        )
        .expect("install cascade source object");
    let cascade_oid = relation_oid(&mut client, "source_m7_cascade.events");
    client
        .query_one(
            "SELECT shiba_internal.register_source(
                2, 'source_m7_cascade.events'::regclass)",
            &[],
        )
        .expect("register cascade source relation");
    client
        .batch_execute("DROP SCHEMA source_m7_cascade CASCADE")
        .expect("commit source schema cascade");
    assert_exact_invalidation(&mut client, 2, cascade_oid);
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
}
