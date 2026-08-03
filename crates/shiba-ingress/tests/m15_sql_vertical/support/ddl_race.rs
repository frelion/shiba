use std::thread;

use postgres::{Client, NoTls};
use shiba_protocol::GraphId;
use shiba_sql_registration::compile_sql_and_register;

const APPLICATION: &str = "shiba_m15_sql_ddl_race";
const SQL: &str = "SELECT r.\"Id\", r.\"Payload\" + 1 \
                   FROM \"Race Schema\".\"Race Rows\" AS r \
                   WHERE r.\"Payload\" > 0";

pub(crate) fn prove_ddl_first_race(database_url: &str, admin: &mut Client) {
    admin
        .batch_execute(
            "CREATE SCHEMA \"Race Schema\";
             CREATE TABLE \"Race Schema\".\"Race Rows\" (
                 \"Id\" bigint PRIMARY KEY, \"Payload\" bigint NULL
             );
             SELECT shiba_internal.register_source(
                 2, '\"Race Schema\".\"Race Rows\"'::regclass
             );",
        )
        .expect("install independent DDL-race source");

    let mut ddl = Client::connect(database_url, NoTls).expect("connect DDL session");
    let mut ddl_transaction = ddl.transaction().expect("open DDL transaction");
    ddl_transaction
        .batch_execute(
            "ALTER TABLE \"Race Schema\".\"Race Rows\" DROP COLUMN \"Payload\";
             ALTER TABLE \"Race Schema\".\"Race Rows\" ADD COLUMN \"Payload\" bigint NULL;",
        )
        .expect("replace payload ObjectAddress before registration");

    let registration_url = format!("{database_url} application_name={APPLICATION}");
    let registration = thread::spawn(move || {
        let mut client = Client::connect(&registration_url, NoTls)
            .expect("connect concurrent SQL registration session");
        compile_sql_and_register(&mut client, GraphId::new(2).expect("graph ID"), SQL)
            .map_or_else(|error| Some(error.code().to_string()), |_| None)
    });

    let mut observed_wait = false;
    for _ in 0..100_000 {
        observed_wait = admin
            .query_one(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_catalog.pg_stat_activity
                     WHERE application_name=$1 AND wait_event_type='Lock'
                       AND pg_catalog.cardinality(pg_catalog.pg_blocking_pids(pid)) > 0
                 )",
                &[&APPLICATION],
            )
            .expect("observe deterministic registration lock wait")
            .get(0);
        if observed_wait {
            break;
        }
        thread::yield_now();
    }
    ddl_transaction
        .commit()
        .expect("commit DDL-first ObjectAddress replacement");
    let outcome = registration.join().expect("join registration session");
    assert!(
        observed_wait,
        "registration never reached an observable lock wait"
    );
    assert_eq!(outcome.as_deref(), Some("ddl_drift"));

    let row = admin
        .query_one(
            "SELECT
                 (SELECT count(*) FROM shiba_internal.graph_definition WHERE graph_id=2),
                 (SELECT count(*) FROM shiba_internal.graph_source_member WHERE graph_id=2),
                 (SELECT count(*) FROM shiba.graph_result WHERE graph_id=2),
                 EXISTS (SELECT 1 FROM shiba_internal.source_invalidation WHERE source_id=2)",
            &[],
        )
        .expect("read DDL-race fail-closed authority");
    assert_eq!(
        (
            row.get::<_, i64>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2),
            row.get::<_, bool>(3),
        ),
        (0, 0, 0, true)
    );
}
