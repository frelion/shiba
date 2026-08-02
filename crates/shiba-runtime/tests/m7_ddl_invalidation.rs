use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{M2Error, PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m7-ddl-invalidation.sh",
    env_prefix: "SHIBA_M7_DDL_INVALIDATION",
    slot: "shiba_m7_ddl_invalidation_slot",
    publication: "shiba_m7_ddl_invalidation_pub",
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

fn invalidation_count(client: &mut Client) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM shiba_internal.source_invalidation",
            &[],
        )
        .expect("query source invalidations")
        .get(0)
}

fn prove_ordinary_role_denied(client: &mut Client) {
    client
        .batch_execute(
            "CREATE ROLE m7_ordinary;
             GRANT USAGE ON SCHEMA shiba_internal, source_m7 TO m7_ordinary;
             GRANT SELECT ON source_m7.events TO m7_ordinary;
             SET ROLE m7_ordinary;",
        )
        .expect("enter ordinary role");
    assert!(
        client
            .query_one(
                "SELECT shiba_internal.register_source(
                    2, 'source_m7.events'::regclass)",
                &[],
            )
            .is_err()
    );
    assert!(
        client
            .query("SELECT * FROM shiba_internal.source_binding", &[])
            .is_err()
    );
    assert!(
        client
            .query("SELECT * FROM shiba_internal.source_invalidation", &[])
            .is_err()
    );
    client
        .batch_execute("RESET ROLE")
        .expect("restore test owner");
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m7-ddl-invalidation.sh"]
fn m7_exact_object_address_invalidation_and_rollback() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m7;
             CREATE TABLE source_m7.events (id bigint PRIMARY KEY);
             CREATE TABLE source_m7.unrelated (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m7_ddl_invalidation_pub
                 FOR TABLE source_m7.events;",
        )
        .expect("install DDL-invalidation source objects");
    let relation_id = u32::try_from(
        client
            .query_one("SELECT 'source_m7.events'::regclass::oid::bigint", &[])
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m7.events");
    let binding = client
        .query_one(
            "SELECT address_classid::bigint, address_objid::bigint, address_objsubid
             FROM shiba_internal.source_binding WHERE source_id = 1",
            &[],
        )
        .expect("query source ObjectAddress");
    let pg_class_oid: i64 = client
        .query_one("SELECT 'pg_class'::regclass::oid::bigint", &[])
        .expect("read pg_class OID")
        .get(0);
    assert_eq!(binding.get::<_, i64>(0), pg_class_oid);
    assert_eq!(binding.get::<_, i64>(1), i64::from(relation_id));
    assert_eq!(binding.get::<_, i32>(2), 0);
    prove_ordinary_role_denied(&mut client);

    client
        .batch_execute("ALTER TABLE source_m7.unrelated RENAME TO still_unrelated")
        .expect("rename unrelated table");
    assert_eq!(invalidation_count(&mut client), 0);
    client
        .batch_execute(
            "BEGIN;
             ALTER TABLE source_m7.events RENAME TO rolled_back_name;
             ROLLBACK;",
        )
        .expect("roll back source rename");
    assert_eq!(invalidation_count(&mut client), 0);
    CAPTURE.create_slot();

    client
        .batch_execute("INSERT INTO source_m7.events VALUES (1101)")
        .expect("commit source row after rolled-back DDL");
    let first_wire = CAPTURE.capture(&mut client, "after-rollback.pgoutput");
    let first = decode_committed_changes(&first_wire, source).expect("decode first transaction");
    assert_eq!(
        process(&mut client, &first).expect("apply after rolled-back DDL"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));

    client
        .batch_execute(
            "ALTER TABLE source_m7.events RENAME TO invalidated_events;
             INSERT INTO source_m7.invalidated_events VALUES (1102);",
        )
        .expect("commit source rename and next row");
    let invalidation = client
        .query_one(
            "SELECT address_classid::bigint, address_objid::bigint, address_objsubid
             FROM shiba_internal.source_invalidation WHERE source_id = 1",
            &[],
        )
        .expect("query exact invalidation ObjectAddress");
    assert_eq!(invalidation.get::<_, i64>(0), pg_class_oid);
    assert_eq!(invalidation.get::<_, i64>(1), i64::from(relation_id));
    assert_eq!(invalidation.get::<_, i32>(2), 0);
    assert_eq!(invalidation_count(&mut client), 1);

    let renamed_wire = CAPTURE.capture(&mut client, "after-commit.pgoutput");
    let renamed = decode_committed_changes(&renamed_wire, source).expect("decode renamed source");
    assert!(matches!(
        process(&mut client, &renamed),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(
        process(&mut client, &first).expect("replay prior transaction"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
}
