use std::str;

use libpq::{Connection, Status};
use postgres::{Client, IsolationLevel, NoTls, Transaction};

const SLOT: &str = "shiba_m11_snapshot_contract_slot";

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m11-bootstrap-contract.sh must set {name}"))
}

fn text(result: &libpq::PQResult, column: usize) -> String {
    str::from_utf8(result.value(0, column).expect("non-null replication field"))
        .expect("replication field is UTF-8")
        .to_owned()
}

fn import_snapshot<'a>(client: &'a mut Client, snapshot: &str) -> Transaction<'a> {
    assert!(
        snapshot
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-'),
        "unexpected PostgreSQL snapshot identifier {snapshot:?}"
    );
    let mut transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .expect("start repeatable-read read-only scanner batch");
    transaction
        .batch_execute(&format!("SET TRANSACTION SNAPSHOT '{snapshot}'"))
        .expect("import exported slot snapshot before the first query");
    transaction
}

fn oracle(transaction: &mut Transaction<'_>) -> (i64, i64, i64) {
    let row = transaction
        .query_one(
            "SELECT count(*)::bigint,
                    COALESCE(sum(payload), 0)::bigint,
                    count(*) FILTER (WHERE payload IS NULL)::bigint
             FROM source.events",
            &[],
        )
        .expect("query snapshot oracle");
    (row.get(0), row.get(1), row.get(2))
}

#[test]
#[ignore = "requires scripts/test-m11-bootstrap-contract.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered exported-snapshot lifetime experiment"
)]
fn exported_slot_snapshot_is_exact_repeatable_and_ephemeral() {
    let database_url = required("SHIBA_M11_DATABASE_URL");
    let replication_url = required("SHIBA_M11_REPLICATION_URL");
    let mut writer = Client::connect(&database_url, NoTls).expect("connect writer");
    writer
        .batch_execute(
            "CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO source.events VALUES (1, 10), (2, NULL), (3, 30);
             CREATE PUBLICATION shiba_m11_snapshot_contract_pub
                 FOR TABLE source.events (id, payload);",
        )
        .expect("install snapshot contract fixture");

    let exporter = Connection::new(&replication_url).expect("connect logical replication exporter");
    let created = exporter.exec(&format!(
        "CREATE_REPLICATION_SLOT {SLOT} LOGICAL pgoutput (SNAPSHOT 'export')"
    ));
    assert_eq!(created.status(), Status::TuplesOk);
    assert_eq!((created.ntuples(), created.nfields()), (1, 4));
    assert_eq!(created.field_name(0).unwrap().as_deref(), Some("slot_name"));
    assert_eq!(
        created.field_name(1).unwrap().as_deref(),
        Some("consistent_point")
    );
    assert_eq!(
        created.field_name(2).unwrap().as_deref(),
        Some("snapshot_name")
    );
    assert_eq!(
        created.field_name(3).unwrap().as_deref(),
        Some("output_plugin")
    );
    let slot_name = text(&created, 0);
    let consistent_point = text(&created, 1);
    let snapshot_name = text(&created, 2);
    let output_plugin = text(&created, 3);
    assert_eq!(slot_name, SLOT);
    assert_ne!(consistent_point, "0/0");
    assert!(!snapshot_name.is_empty());
    assert_eq!(output_plugin, "pgoutput");

    let mut scanner_one = Client::connect(&database_url, NoTls).expect("connect first scanner");
    let mut batch_one = import_snapshot(&mut scanner_one, &snapshot_name);
    assert_eq!(oracle(&mut batch_one), (3, 40, 1));

    writer
        .batch_execute(
            "INSERT INTO source.events VALUES (4, 5);
             UPDATE source.events SET payload = 20 WHERE id = 1;
             DELETE FROM source.events WHERE id = 3;",
        )
        .expect("commit concurrent insert update and delete");

    let mut scanner_two = Client::connect(&database_url, NoTls).expect("connect second scanner");
    let mut batch_two = import_snapshot(&mut scanner_two, &snapshot_name);
    assert_eq!(oracle(&mut batch_two), (3, 40, 1));
    batch_two.commit().expect("commit second scanner batch");
    let current: (i64, i64, i64) = {
        let row = writer
            .query_one(
                "SELECT count(*)::bigint,
                        COALESCE(sum(payload), 0)::bigint,
                        count(*) FILTER (WHERE payload IS NULL)::bigint
                 FROM source.events",
                &[],
            )
            .expect("query current SQL oracle");
        (row.get(0), row.get(1), row.get(2))
    };
    assert_eq!(current, (3, 25, 1));

    let slot = writer
        .query_one(
            "SELECT plugin, confirmed_flush_lsn::text, database, active
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&SLOT],
        )
        .expect("query persistent slot boundary");
    assert_eq!(slot.get::<_, &str>(0), "pgoutput");
    assert_eq!(slot.get::<_, &str>(1), consistent_point);
    assert_eq!(slot.get::<_, &str>(2), "postgres");
    assert!(!slot.get::<_, bool>(3));
    let wal_accumulated: bool = writer
        .query_one(
            "SELECT pg_current_wal_flush_lsn() > $1::text::pg_lsn",
            &[&consistent_point],
        )
        .expect("compare concurrent WAL with snapshot boundary")
        .get(0);
    assert!(
        wal_accumulated,
        "concurrent writes must lie after the boundary"
    );

    let identified = exporter.exec("IDENTIFY_SYSTEM");
    assert_eq!(identified.status(), Status::TuplesOk);
    assert_eq!(oracle(&mut batch_one), (3, 40, 1));
    batch_one
        .commit()
        .expect("existing imported snapshot remains valid");

    let mut scanner_three = Client::connect(&database_url, NoTls).expect("connect third scanner");
    let mut unavailable = scanner_three
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .expect("start post-export scanner");
    assert!(
        unavailable
            .batch_execute(&format!("SET TRANSACTION SNAPSHOT '{snapshot_name}'"))
            .is_err(),
        "snapshot name must not survive the exporter's next command"
    );
    drop(unavailable);

    let shiba_state_exists: bool = writer
        .query_one(
            "SELECT to_regclass('shiba_internal.graph_continuation') IS NOT NULL",
            &[],
        )
        .expect("check absence of Shiba durable state")
        .get(0);
    assert!(!shiba_state_exists);
    drop(exporter);
    writer
        .execute("SELECT pg_drop_replication_slot($1)", &[&SLOT])
        .expect("drop test-only slot");
}
