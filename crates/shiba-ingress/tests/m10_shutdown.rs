use std::{
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_ingress::{AttachOptions, GovernedGraphSession, IngressError, ReplicationMode};
use shiba_protocol::{GraphId, SlotGeneration};
use shiba_runtime::compile_and_register;

#[allow(dead_code)]
mod support;

use support::{count_spec, slot_lsn};

const SLOT: &str = "shiba_m10_shutdown_slot";
const PUBLICATION: &str = "shiba_m10_shutdown_pub";
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(1);

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m10-shutdown-ingress.sh must set {name}"))
}

fn attach(database_url: &str, replication_url: &str) -> GovernedGraphSession {
    GovernedGraphSession::attach(
        database_url,
        replication_url,
        GraphId::new(1).expect("graph ID"),
        SlotGeneration::new(1).expect("slot generation"),
        AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
            .expect("attach options"),
    )
    .expect("attach governed session")
}

fn durable_state(client: &mut Client) -> (i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.graph_result WHERE graph_id = 1 AND result_id = 2),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.graph_continuation)",
            &[],
        )
        .expect("query durable state");
    (row.get(0), row.get(1), row.get(2))
}

#[test]
#[ignore = "requires scripts/test-m10-shutdown-ingress.sh"]
fn idle_receive_shutdown_is_bounded_and_preserves_durable_state() {
    let database_url = required("SHIBA_M10_SHUTDOWN_DATABASE_URL");
    let replication_url = required("SHIBA_M10_SHUTDOWN_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect admin database");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events (id)
                 WITH (publish = 'insert, update, delete, truncate');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);"
        ))
        .expect("install shutdown fixture");
    compile_and_register(&mut admin, &count_spec(1)).expect("register CountRows");
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .expect("create shutdown slot");
    let publication_oid: u32 = admin
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("read publication OID")
        .get(0);
    admin
        .execute(
            "SELECT shiba_internal.configure_graph_ingress(1, $1, $2, 1)",
            &[&publication_oid, &SLOT],
        )
        .expect("configure shutdown ingress");

    let initial_lsn = slot_lsn(&mut admin, SLOT);
    let initial_state = durable_state(&mut admin);
    assert_eq!(initial_state, (0, 0, 0));
    let mut session = attach(&database_url, &replication_url);
    let shutdown = session.shutdown_handle();
    let (result_tx, result_rx) = mpsc::channel();
    let receive_thread = thread::spawn(move || {
        let result = session.receive_one();
        result_tx
            .send((session, result))
            .expect("return interrupted session");
    });
    assert!(matches!(
        result_rx.recv_timeout(Duration::from_millis(200)),
        Err(RecvTimeoutError::Timeout)
    ));

    let shutdown_started = Instant::now();
    shutdown.request();
    let (session, result) = result_rx
        .recv_timeout(SHUTDOWN_LIMIT)
        .expect("blocking receive observes shutdown within bound");
    let shutdown_elapsed = shutdown_started.elapsed();
    receive_thread.join().expect("join interrupted receive");
    assert!(matches!(result, Err(IngressError::ShutdownRequested)));
    assert!(
        shutdown_elapsed <= SHUTDOWN_LIMIT,
        "idle shutdown took {shutdown_elapsed:?}, exceeding {SHUTDOWN_LIMIT:?}"
    );
    assert_eq!(slot_lsn(&mut admin, SLOT), initial_lsn);
    assert_eq!(durable_state(&mut admin), initial_state);
    session.detach().expect("detach shutdown session");

    attach(&database_url, &replication_url)
        .detach()
        .expect("fresh attach/detach after shutdown");
    assert_eq!(slot_lsn(&mut admin, SLOT), initial_lsn);
    assert_eq!(durable_state(&mut admin), initial_state);
    eprintln!("M10 idle shutdown measured {shutdown_elapsed:?} (limit {SHUTDOWN_LIMIT:?})");
}
