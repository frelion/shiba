use std::{num::NonZeroU64, sync::mpsc, thread, time::Duration};

use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_ingress::{AttachOptions, GovernedSourceSession, ReplicationMode};
use shiba_operator::OperatorId;
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{ProcessOutcome, compile_and_register};

mod support;

use support::{slot_lsn, wait_for_keepalive_reply, wait_for_slot_lsn};

const SLOT: &str = "shiba_m10_committed_slot";
const PUBLICATION: &str = "shiba_m10_committed_pub";
const APPLICATION: &str = "shiba_m10_receiver";

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m10-committed-ingress.sh must set {name}"))
}

fn spec(operator_id: u64, operation: OperatorOperationV1) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(operator_id).expect("non-zero operator")),
        source_id: SourceId::new(1).expect("non-zero source"),
        operation,
    }
}

fn attach(database_url: &str, replication_url: &str) -> GovernedSourceSession {
    GovernedSourceSession::attach(
        database_url,
        replication_url,
        SourceId::new(1).expect("source ID"),
        SlotGeneration::new(1).expect("slot generation"),
        AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
            .expect("attach options"),
    )
    .expect("attach governed committed session")
}

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 2),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 2),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable business state");
    (
        row.get(0),
        row.get(1),
        row.get(2),
        row.get(3),
        row.get(4),
        row.get(5),
    )
}

#[test]
#[ignore = "requires scripts/test-m10-committed-ingress.sh"]
#[allow(clippy::too_many_lines, reason = "one ordered governed crash proof")]
fn governed_committed_apply_ack_replay_and_rollback() {
    let database_url = required("SHIBA_M10_DATABASE_URL");
    let replication_url = required("SHIBA_M10_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect admin database");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events
                 WITH (publish = 'insert, update, delete, truncate');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);"
        ))
        .expect("install source and binding");
    compile_and_register(&mut admin, &spec(1, OperatorOperationV1::CountRows))
        .expect("register CountRows");
    compile_and_register(
        &mut admin,
        &spec(
            2,
            OperatorOperationV1::SumInt8 {
                input_column: "payload".to_owned(),
            },
        ),
    )
    .expect("register SumInt8");
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .expect("create test-owned slot");
    let publication_oid: u32 = admin
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("read publication OID")
        .get(0);
    admin
        .execute(
            "SELECT shiba_internal.configure_source_ingress(1, $1, $2, 1)",
            &[&publication_oid, &SLOT],
        )
        .expect("configure governed ingress");

    let initial_lsn = slot_lsn(&mut admin, SLOT);
    let mut session = attach(&database_url, &replication_url);
    assert_eq!(session.source_id().get(), 1);
    assert_eq!(session.slot_generation().get(), 1);
    let (received_tx, received_rx) = mpsc::channel();
    let receiver_thread = thread::spawn(move || {
        let input = session.receive_one();
        received_tx
            .send((session, input))
            .expect("return governed volatile input");
    });
    wait_for_keepalive_reply(&mut admin, APPLICATION, initial_lsn);
    admin
        .batch_execute("INSERT INTO source.events VALUES (1, 10), (2, NULL)")
        .expect("commit first source transaction");
    let (preapply_session, volatile_input) = received_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("receive committed input");
    receiver_thread.join().expect("join receive thread");
    let received_end = volatile_input.expect("decode committed input").end_lsn();
    assert!(received_end > initial_lsn);
    assert_eq!(durable_state(&mut admin), (0, 0, 0, 0, 0, 0));
    assert_eq!(slot_lsn(&mut admin, SLOT), initial_lsn);
    drop(preapply_session);

    let mut session = attach(&database_url, &replication_url);
    let first = session
        .receive_and_apply_one()
        .expect("apply replay after receive-before-Apply crash");
    assert_eq!(first.outcome(), ProcessOutcome::Applied);
    assert_eq!(first.end_lsn(), received_end);
    assert_eq!(durable_state(&mut admin), (2, 10, 2, 10, 2, 1));
    assert_eq!(slot_lsn(&mut admin, SLOT), initial_lsn);
    drop(session);

    let mut session = attach(&database_url, &replication_url);
    let replay = session
        .receive_and_apply_one()
        .expect("replay applied transaction");
    assert_eq!(replay.outcome(), ProcessOutcome::AlreadyApplied);
    assert_eq!(replay.end_lsn(), received_end);
    session.acknowledge(&replay).expect("ack exact replay");
    wait_for_slot_lsn(&mut admin, SLOT, received_end);
    session.detach().expect("detach replay session");

    admin
        .batch_execute(
            "CREATE SCHEMA m10_test;
             CREATE FUNCTION m10_test.fail_operator() RETURNS trigger
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'M10 operator failure'; END $$;
             CREATE TRIGGER m10_fail_operator BEFORE UPDATE
             ON shiba_internal.operator_state FOR EACH ROW
             EXECUTE FUNCTION m10_test.fail_operator();
             INSERT INTO source.events VALUES (3, 11);",
        )
        .expect("install failure point and commit source transaction");
    let mut session = attach(&database_url, &replication_url);
    let input = session
        .receive_one()
        .expect("receive operator-failure input");
    assert!(session.apply_received(&input).is_err());
    assert_eq!(durable_state(&mut admin), (2, 10, 2, 10, 2, 1));
    assert_eq!(slot_lsn(&mut admin, SLOT), received_end);
    drop(session);
    admin
        .batch_execute("DROP SCHEMA m10_test CASCADE")
        .expect("remove failure point");

    let mut session = attach(&database_url, &replication_url);
    let retry = session
        .receive_and_apply_one()
        .expect("retry rolled-back transaction");
    assert_eq!(retry.outcome(), ProcessOutcome::Applied);
    session.acknowledge(&retry).expect("ack operator retry");
    wait_for_slot_lsn(&mut admin, SLOT, retry.end_lsn());
    assert_eq!(durable_state(&mut admin), (3, 21, 3, 21, 3, 2));
    let durable_lsn = retry.end_lsn();
    session.detach().expect("detach governed session");

    admin
        .batch_execute("TRUNCATE source.events")
        .expect("commit unsupported published TRUNCATE");
    let mut session = attach(&database_url, &replication_url);
    assert!(session.receive_one().is_err());
    assert_eq!(slot_lsn(&mut admin, SLOT), durable_lsn);
    assert_eq!(durable_state(&mut admin), (3, 21, 3, 21, 3, 2));
}
