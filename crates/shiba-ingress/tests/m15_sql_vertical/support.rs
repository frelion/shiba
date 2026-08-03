use std::time::{Duration, Instant};

use postgres::Client;
use shiba_ingress::{BootstrapSession, SnapshotProgress};
use shiba_operator::OperatorGraph;

#[path = "support/ddl_race.rs"]
mod ddl_race;
#[path = "support/rebuild.rs"]
mod rebuild;

pub(crate) use ddl_race::prove_ddl_first_race;
pub(crate) use rebuild::{Fixture, changed_object_rebuild};

pub(crate) const OLD_SLOT: &str = "shiba_m15_sql_vertical_1";
pub(crate) const NEW_SLOT: &str = "shiba_m15_sql_vertical_2";
const OLD_PUBLICATION: &str = "shiba_m15_sql_vertical_old_pub";
const NEW_PUBLICATION: &str = "shiba_m15_sql_vertical_new_pub";

pub(crate) fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m15-sql-vertical.sh must set {name}"))
}

pub(crate) fn install(client: &mut Client) -> Fixture {
    client
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA \"Source Schema\";
             CREATE TABLE \"Source Schema\".\"Event Rows\" (
                 \"Id\" bigint PRIMARY KEY, \"Payload\" bigint NULL
             );
             INSERT INTO \"Source Schema\".\"Event Rows\" VALUES (1,10),(2,NULL),(3,-5);
             CREATE PUBLICATION {OLD_PUBLICATION}
                 FOR TABLE \"Source Schema\".\"Event Rows\"
                 WITH (publish='insert, update, delete');
             SELECT shiba_internal.register_source(
                 1, '\"Source Schema\".\"Event Rows\"'::regclass
             );
             CREATE SCHEMA \"Target Schema\";
             CREATE TABLE \"Target Schema\".\"Event Rows\" (
                 \"Id\" bigint PRIMARY KEY, \"Payload\" bigint NULL
             );
             INSERT INTO \"Target Schema\".\"Event Rows\" VALUES (10,100),(11,NULL),(12,-1);
             CREATE PUBLICATION {NEW_PUBLICATION}
                 FOR TABLE \"Target Schema\".\"Event Rows\"
                 WITH (publish='insert, update, delete');"
        ))
        .expect("install quoted source and changed-ObjectAddress target");
    Fixture {
        old_relation: oid(client, "\"Source Schema\".\"Event Rows\""),
        old_identity: oid(client, "\"Source Schema\".\"Event Rows_pkey\""),
        old_publication: publication_oid(client, OLD_PUBLICATION),
        target_relation: oid(client, "\"Target Schema\".\"Event Rows\""),
        target_identity: oid(client, "\"Target Schema\".\"Event Rows_pkey\""),
        target_publication: publication_oid(client, NEW_PUBLICATION),
    }
}

pub(crate) fn install_registration_failure(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m15_registration_failure;
             CREATE FUNCTION m15_registration_failure.reject_result()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'injected SQL result registration failure'; END $$;
             CREATE TRIGGER m15_reject_result BEFORE INSERT ON shiba.graph_result
             FOR EACH ROW EXECUTE FUNCTION m15_registration_failure.reject_result();",
        )
        .expect("install deterministic registration failure");
}

pub(crate) fn remove_registration_failure(client: &mut Client) {
    client
        .batch_execute("DROP SCHEMA m15_registration_failure CASCADE")
        .expect("remove deterministic registration failure");
}

pub(crate) fn assert_no_registered_graph(client: &mut Client) {
    let row = client
        .query_one(
            "SELECT (SELECT count(*) FROM shiba_internal.graph_definition),
                    (SELECT count(*) FROM shiba_internal.graph_source_member),
                    (SELECT count(*) FROM shiba.graph_result)",
            &[],
        )
        .expect("read rolled-back authority");
    assert_eq!(
        (
            row.get::<_, i64>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2)
        ),
        (0, 0, 0)
    );
}

pub(crate) fn assert_registered(client: &mut Client, graph: &OperatorGraph) {
    let row = client
        .query_one(
            "SELECT spec_payload,graph_payload,graph_digest
             FROM shiba_internal.graph_definition WHERE graph_id=1",
            &[],
        )
        .expect("read canonical SQL graph authority");
    let spec: Vec<u8> = row.get(0);
    let text = std::str::from_utf8(&spec).expect("canonical QuerySpec JSON");
    assert!(!text.contains("SELECT"));
    assert!(!text.contains("Source Schema"));
    assert!(!text.contains("Event Rows"));
    assert_eq!(row.get::<_, Vec<u8>>(1), graph.canonical_payload);
    assert_eq!(row.get::<_, Vec<u8>>(2), graph.digest.to_vec());
}

pub(crate) fn scan_all(bootstrap: &mut BootstrapSession, client: &mut Client) {
    let mut rows = 0;
    while let SnapshotProgress::BatchApplied { rows: batch, .. } =
        bootstrap.scan_next().expect("scan bounded SQL snapshot")
    {
        assert!((1..=2).contains(&batch));
        rows += batch;
        assert_building(client);
    }
    assert!(rows >= 3);
}

pub(crate) fn assert_building(client: &mut Client) {
    let row = client
        .query_one(
            "SELECT result_status,value_payload,value_bigint
             FROM shiba.graph_result WHERE graph_id=1",
            &[],
        )
        .expect("read building result");
    assert_eq!(row.get::<_, &str>(0), "building");
    assert_eq!(row.get::<_, Option<Vec<u8>>>(1), None);
    assert_eq!(row.get::<_, Option<i64>>(2), None);
    assert!(
        client
            .query(
                "SELECT * FROM shiba.graph_result_rows WHERE graph_id=1",
                &[]
            )
            .expect("query hidden building rows")
            .is_empty()
    );
}

pub(crate) fn assert_oracle(client: &mut Client, schema: &str, relation: &str) {
    let expected = client
        .query(
            &format!(
                "SELECT \"Id\",\"Payload\"+1 FROM \"{schema}\".\"{relation}\"
                 WHERE \"Payload\">0 ORDER BY \"Id\""
            ),
            &[],
        )
        .expect("query SQL differential oracle")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
        .collect::<Vec<_>>();
    let actual = client
        .query(
            "SELECT result_key_bigint,result_value_bigint
             FROM shiba.graph_result_rows WHERE graph_id=1 ORDER BY result_key_bigint",
            &[],
        )
        .expect("query materialized SQL result")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let status: String = client
        .query_one(
            "SELECT result_status FROM shiba.graph_result WHERE graph_id=1",
            &[],
        )
        .expect("read active result")
        .get(0);
    assert_eq!(status, "active");
}

pub(crate) fn wait_for_slot_lsn(client: &mut Client, slot: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let value: String = client
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots
             WHERE slot_name=$1",
                &[&slot],
            )
            .expect("query feedback LSN")
            .get(0);
        if parse_lsn(&value) >= expected {
            return;
        }
        assert!(Instant::now() < deadline, "slot feedback did not advance");
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub(crate) fn assert_target_authority(client: &mut Client, fixture: &Fixture) {
    let row = client
        .query_one(
            "SELECT address_objid::bigint FROM shiba_internal.source_binding
         WHERE source_id=1 AND binding_kind='relation'",
            &[],
        )
        .expect("read rebuilt relation authority");
    assert_eq!(row.get::<_, i64>(0), i64::from(fixture.target_relation));
}

fn oid(client: &mut Client, name: &str) -> u32 {
    client
        .query_one("SELECT $1::text::regclass::oid", &[&name])
        .expect("resolve object OID")
        .get(0)
}

fn publication_oid(client: &mut Client, name: &str) -> u32 {
    client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname=$1",
            &[&name],
        )
        .expect("resolve publication OID")
        .get(0)
}

fn parse_lsn(value: &str) -> u64 {
    let (high, low) = value.split_once('/').expect("PostgreSQL LSN shape");
    (u64::from_str_radix(high, 16).expect("LSN high") << 32)
        | u64::from_str_radix(low, 16).expect("LSN low")
}
