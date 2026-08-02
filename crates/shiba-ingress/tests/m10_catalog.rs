use libpq::{Connection, Status};
use postgres::{Client, NoTls};

const ACTIVE_SLOT: &str = "shiba_m10_catalog_active";
const MAIN_SLOT: &str = "shiba_m10_catalog_main";
const OLD_SLOT: &str = "shiba_m10_catalog_old";
const NEW_SLOT: &str = "shiba_m10_catalog_new";
const THIRD_SLOT: &str = "shiba_m10_catalog_third";
const PHYSICAL_SLOT: &str = "shiba_m10_catalog_physical";
const OTHER_DATABASE_SLOT: &str = "shiba_m10_catalog_otherdb";

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m10-catalog-ingress.sh must set {name}"))
}

fn publication_oid(client: &mut Client, publication: &str) -> u32 {
    client
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .expect("read publication OID")
        .get(0)
}

fn create_logical_slot(client: &mut Client, slot: &str) {
    client
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .expect("create test-owned logical slot");
}

fn configure(
    client: &mut Client,
    source_id: i64,
    publication: u32,
    slot: &str,
    generation: i64,
) -> Result<u64, postgres::Error> {
    client.execute(
        "SELECT shiba_internal.configure_source_ingress($1, $2, $3, $4)",
        &[&source_id, &publication, &slot, &generation],
    )
}

fn rotate(
    client: &mut Client,
    source_id: i64,
    generation: i64,
    slot: &str,
) -> Result<u64, postgres::Error> {
    client.execute(
        "SELECT shiba_internal.rotate_source_ingress_slot($1, $2, $3)",
        &[&source_id, &generation, &slot],
    )
}

fn slot_names(client: &mut Client) -> Vec<String> {
    client
        .query(
            "SELECT slot_name FROM pg_replication_slots ORDER BY slot_name",
            &[],
        )
        .expect("list slots")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

fn activate_slot(conninfo: &str, slot: &str, publication: &str) -> Connection {
    let connection = Connection::new(conninfo).expect("connect test-owned replication client");
    let result = connection.exec(&format!(
        "START_REPLICATION SLOT \"{slot}\" LOGICAL 0/0
         (proto_version '1', publication_names '{publication}')"
    ));
    assert_eq!(result.status(), Status::CopyBoth);
    connection
}

#[test]
#[ignore = "requires scripts/test-m10-catalog-ingress.sh"]
#[allow(clippy::too_many_lines, reason = "one ordered catalog authority proof")]
fn catalog_configures_invalidates_and_rotates_without_managing_slots() {
    let database_url = required("SHIBA_M10_CATALOG_DATABASE_URL");
    let replication_url = required("SHIBA_M10_CATALOG_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect catalog database");
    admin
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.one (id bigint PRIMARY KEY);
             CREATE TABLE source.two (id bigint PRIMARY KEY);
             CREATE TABLE source.three (id bigint PRIMARY KEY);
             CREATE TABLE source.four (id bigint PRIMARY KEY);
             CREATE TABLE source.extra (id bigint PRIMARY KEY);
             CREATE PUBLICATION ingress_main FOR TABLE source.one
                 WITH (publish = 'insert, update, delete, truncate');
             CREATE PUBLICATION ingress_rotate FOR TABLE source.two
                 WITH (publish = 'insert, update, delete, truncate');
             CREATE PUBLICATION ingress_invalid_source FOR TABLE source.three
                 WITH (publish = 'insert, update, delete, truncate');
             CREATE PUBLICATION ingress_active FOR TABLE source.four
                 WITH (publish = 'insert, update, delete, truncate');
             CREATE PUBLICATION ingress_empty;
             CREATE PUBLICATION ingress_multi FOR TABLE source.four, source.extra
                 WITH (publish = 'insert, update, delete, truncate');
             CREATE PUBLICATION ingress_all FOR ALL TABLES;
             SELECT shiba_internal.register_source(1, 'source.one'::regclass);
             SELECT shiba_internal.register_source(2, 'source.two'::regclass);
             SELECT shiba_internal.register_source(3, 'source.three'::regclass);
             SELECT shiba_internal.register_source(4, 'source.four'::regclass);",
        )
        .expect("install catalog fixtures");

    for slot in [MAIN_SLOT, OLD_SLOT, NEW_SLOT, THIRD_SLOT, ACTIVE_SLOT] {
        create_logical_slot(&mut admin, slot);
    }
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_physical_replication_slot($1)",
            &[&PHYSICAL_SLOT],
        )
        .expect("create wrong-plugin physical slot");
    admin
        .batch_execute("CREATE DATABASE shiba_m10_catalog_other")
        .expect("create other slot database");
    let other_url = database_url.replace("dbname=postgres", "dbname=shiba_m10_catalog_other");
    let mut other = Client::connect(&other_url, NoTls).expect("connect other database");
    create_logical_slot(&mut other, OTHER_DATABASE_SLOT);
    drop(other);
    let owned_slots = slot_names(&mut admin);

    let main_pub = publication_oid(&mut admin, "ingress_main");
    configure(&mut admin, 1, main_pub, MAIN_SLOT, 1).expect("configure exact ingress");
    assert!(configure(&mut admin, 1, main_pub, NEW_SLOT, 2).is_err());
    let configured = admin
        .query_one(
            "SELECT publication_objid, slot_name::text, slot_generation
             FROM shiba_internal.source_ingress_config WHERE source_id = 1",
            &[],
        )
        .expect("read atomic configuration");
    assert_eq!(configured.get::<_, u32>(0), main_pub);
    assert_eq!(configured.get::<_, String>(1), MAIN_SLOT);
    assert_eq!(configured.get::<_, i64>(2), 1);

    let active_pub = publication_oid(&mut admin, "ingress_active");
    assert!(configure(&mut admin, 4, active_pub, "missing_slot", 1).is_err());
    assert!(configure(&mut admin, 4, active_pub, PHYSICAL_SLOT, 1).is_err());
    assert!(configure(&mut admin, 4, active_pub, OTHER_DATABASE_SLOT, 1).is_err());
    assert!(configure(&mut admin, 4, active_pub, ACTIVE_SLOT, 0).is_err());
    for publication in ["ingress_empty", "ingress_multi", "ingress_all"] {
        let publication = publication_oid(&mut admin, publication);
        assert!(configure(&mut admin, 4, publication, ACTIVE_SLOT, 1).is_err());
    }
    let active_receiver = activate_slot(&replication_url, ACTIVE_SLOT, "ingress_active");
    assert!(configure(&mut admin, 4, active_pub, ACTIVE_SLOT, 1).is_err());
    drop(active_receiver);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_ingress_config
                 WHERE source_id = 4",
                &[],
            )
            .expect("failed configuration remains absent")
            .get::<_, i64>(0),
        0
    );

    admin
        .batch_execute("ALTER TABLE source.three ADD COLUMN payload bigint")
        .expect("invalidate bound source");
    let invalid_source_pub = publication_oid(&mut admin, "ingress_invalid_source");
    assert!(configure(&mut admin, 3, invalid_source_pub, ACTIVE_SLOT, 1).is_err());

    admin
        .batch_execute("BEGIN; ALTER PUBLICATION ingress_main DROP TABLE source.one; ROLLBACK")
        .expect("roll back publication mutation");
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_ingress_invalidation
                 WHERE source_id = 1",
                &[],
            )
            .expect("rollback leaves no ingress invalidation")
            .get::<_, i64>(0),
        0
    );
    admin
        .batch_execute("ALTER PUBLICATION ingress_main DROP TABLE source.one")
        .expect("commit publication membership removal");
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_ingress_invalidation
                 WHERE source_id = 1",
                &[],
            )
            .expect("committed invalidation persists")
            .get::<_, i64>(0),
        1
    );
    admin
        .batch_execute("ALTER PUBLICATION ingress_main ADD TABLE source.one")
        .expect("re-add source to publication");
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_ingress_invalidation
                 WHERE source_id = 1",
                &[],
            )
            .expect("re-add does not clear invalidation")
            .get::<_, i64>(0),
        1
    );
    admin
        .batch_execute(
            "DROP PUBLICATION ingress_main;
             CREATE PUBLICATION ingress_main FOR TABLE source.one
                 WITH (publish = 'insert, update, delete, truncate');",
        )
        .expect("recreate same-name publication");
    assert_ne!(publication_oid(&mut admin, "ingress_main"), main_pub);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_ingress_invalidation
                 WHERE source_id = 1",
                &[],
            )
            .expect("same-name recreation does not restore authority")
            .get::<_, i64>(0),
        1
    );

    let rotate_pub = publication_oid(&mut admin, "ingress_rotate");
    configure(&mut admin, 2, rotate_pub, OLD_SLOT, 1).expect("configure pristine source");
    rotate(&mut admin, 2, 1, NEW_SLOT).expect("rotate pristine source by CAS");
    assert!(rotate(&mut admin, 2, 1, THIRD_SLOT).is_err());
    let active_receiver = activate_slot(&replication_url, NEW_SLOT, "ingress_rotate");
    assert!(rotate(&mut admin, 2, 2, THIRD_SLOT).is_err());
    drop(active_receiver);
    assert!(rotate(&mut admin, 2, 2, PHYSICAL_SLOT).is_err());
    admin
        .execute(
            "INSERT INTO shiba_internal.source_continuation
             (source_id, slot_generation, commit_lsn, ingress_transaction_id)
             VALUES (2, 2, '0/1', 1)",
            &[],
        )
        .expect("make configured source non-pristine");
    assert!(rotate(&mut admin, 2, 2, THIRD_SLOT).is_err());
    let rotated = admin
        .query_one(
            "SELECT slot_name::text, slot_generation
             FROM shiba_internal.source_ingress_config WHERE source_id = 2",
            &[],
        )
        .expect("read retained rotation authority");
    assert_eq!(rotated.get::<_, String>(0), NEW_SLOT);
    assert_eq!(rotated.get::<_, i64>(1), 2);

    let dynamic_columns: i64 = admin
        .query_one(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema = 'shiba_internal'
               AND table_name IN ('source_ingress_config', 'source_ingress_invalidation')
               AND column_name IN ('confirmed_flush_lsn', 'restart_lsn', 'active_pid')",
            &[],
        )
        .expect("check absence of dynamic slot state")
        .get(0);
    assert_eq!(dynamic_columns, 0);
    admin
        .batch_execute(
            "CREATE ROLE shiba_m10_catalog_reader NOLOGIN; SET ROLE shiba_m10_catalog_reader",
        )
        .expect("assume ordinary role");
    assert!(
        admin
            .query("SELECT * FROM shiba_internal.source_ingress_config", &[])
            .is_err()
    );
    assert!(configure(&mut admin, 4, active_pub, ACTIVE_SLOT, 1).is_err());
    admin
        .batch_execute("RESET ROLE")
        .expect("restore admin role");

    assert_eq!(
        slot_names(&mut admin),
        owned_slots,
        "catalog never creates or drops slots"
    );
}
