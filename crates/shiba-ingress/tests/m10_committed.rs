use std::{num::NonZeroU64, sync::mpsc, thread, time::Duration};

use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_ingress::SourceReceiver;
use shiba_operator::OperatorId;
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, compile_and_register};

const SLOT: &str = "shiba_m10_committed_slot";
const PUBLICATION: &str = "shiba_m10_committed_pub";
const APPLICATION: &str = "shiba_m10_receiver";

mod support;

use support::{slot_lsn, wait_for_keepalive_reply, wait_for_slot_lsn};

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

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 2),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 2),
                (SELECT count(*) FROM shiba_internal.applied_insert),
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
#[allow(
    clippy::too_many_lines,
    reason = "one ordered slot-feedback and crash-window proof"
)]
fn production_copy_both_acknowledges_only_durable_apply() {
    let database_url = required("SHIBA_M10_DATABASE_URL");
    let replication_url = required("SHIBA_M10_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect admin/apply database");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events;
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
    let relation_oid = admin
        .query_one("SELECT 'source.events'::regclass::oid::bigint", &[])
        .expect("read relation OID")
        .get::<_, i64>(0);
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .expect("create test-owned slot");

    let source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("source ID"),
        SlotGeneration::new(1).expect("slot generation"),
        u32::try_from(relation_oid).expect("OID fits u32"),
    );
    let initial_slot_lsn = slot_lsn(&mut admin, SLOT);
    let mut idle_receiver = SourceReceiver::connect(
        &replication_url,
        SLOT,
        PUBLICATION,
        initial_slot_lsn,
        initial_slot_lsn,
    )
    .expect("connect production replication receiver");
    let (received_tx, received_rx) = mpsc::channel();
    let receiver_thread = thread::spawn(move || {
        let result = idle_receiver.receive_one(source);
        received_tx
            .send((idle_receiver, result))
            .expect("return receiver and volatile input");
    });
    wait_for_keepalive_reply(&mut admin, APPLICATION, initial_slot_lsn);
    admin
        .batch_execute("INSERT INTO source.events VALUES (1, 10), (2, NULL)")
        .expect("commit first source transaction");
    let (preapply_receiver, volatile_input) = received_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("receive after requested keepalive");
    receiver_thread.join().expect("receiver thread exits");
    let volatile_input = volatile_input.expect("receive first committed transaction");
    let received_end_lsn = volatile_input.end_lsn();
    assert!(received_end_lsn > initial_slot_lsn);
    assert_eq!(preapply_receiver.outstanding_lsn(), Some(received_end_lsn));
    assert_eq!(preapply_receiver.pending_feedback_lsn(), None);
    assert_eq!(slot_lsn(&mut admin, SLOT), initial_slot_lsn);
    assert_eq!(durable_state(&mut admin), (0, 0, 0, 0, 0, 0));
    drop(volatile_input);
    drop(preapply_receiver);

    let mut apply = Client::connect(&database_url, NoTls).expect("connect Apply client");
    let mut receiver = SourceReceiver::connect(
        &replication_url,
        SLOT,
        PUBLICATION,
        initial_slot_lsn,
        initial_slot_lsn,
    )
    .expect("restart after receive-before-Apply crash window");
    let first = receiver
        .receive_and_apply_one(&mut apply, source)
        .expect("apply replay after receive-before-Apply crash");
    assert_eq!(first.outcome(), ProcessOutcome::Applied);
    assert_eq!(first.end_lsn(), received_end_lsn);
    assert_eq!(receiver.pending_feedback_lsn(), Some(first.end_lsn()));
    assert_eq!(receiver.last_acknowledged_lsn(), initial_slot_lsn);
    assert_eq!(slot_lsn(&mut admin, SLOT), initial_slot_lsn);
    assert_eq!(durable_state(&mut admin), (2, 10, 2, 10, 2, 1));

    receiver
        .acknowledge(&first)
        .expect("ack durable first apply");
    assert_eq!(receiver.last_acknowledged_lsn(), first.end_lsn());
    assert_eq!(receiver.pending_feedback_lsn(), None);
    wait_for_slot_lsn(&mut admin, SLOT, first.end_lsn());
    drop(receiver);

    admin
        .batch_execute("INSERT INTO source.events VALUES (3, 5)")
        .expect("commit restart-window transaction");
    let mut receiver = SourceReceiver::connect(
        &replication_url,
        SLOT,
        PUBLICATION,
        first.end_lsn(),
        first.end_lsn(),
    )
    .expect("connect receiver before unacknowledged restart");
    let unacknowledged = receiver
        .receive_and_apply_one(&mut apply, source)
        .expect("durably apply without feedback");
    assert_eq!(unacknowledged.outcome(), ProcessOutcome::Applied);
    assert_eq!(slot_lsn(&mut admin, SLOT), first.end_lsn());
    assert_eq!(durable_state(&mut admin), (3, 15, 3, 15, 3, 2));
    let replay_end_lsn = unacknowledged.end_lsn();
    drop(receiver);

    let mut receiver = SourceReceiver::connect(
        &replication_url,
        SLOT,
        PUBLICATION,
        first.end_lsn(),
        first.end_lsn(),
    )
    .expect("restart receiver from last acknowledged LSN");
    let replay = receiver
        .receive_and_apply_one(&mut apply, source)
        .expect("replay durable transaction");
    assert_eq!(replay.outcome(), ProcessOutcome::AlreadyApplied);
    assert_eq!(replay.end_lsn(), replay_end_lsn);
    assert_eq!(durable_state(&mut admin), (3, 15, 3, 15, 3, 2));
    receiver
        .acknowledge(&replay)
        .expect("acknowledge idempotent replay");
    wait_for_slot_lsn(&mut admin, SLOT, replay_end_lsn);
    drop(receiver);

    admin
        .batch_execute("INSERT INTO source.events VALUES (4, 7)")
        .expect("commit decoder-failure transaction");
    let wrong_source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("source ID"),
        SlotGeneration::new(1).expect("slot generation"),
        u32::try_from(relation_oid).expect("OID fits u32") + 1,
    );
    let mut receiver = SourceReceiver::connect(
        &replication_url,
        SLOT,
        PUBLICATION,
        replay_end_lsn,
        replay_end_lsn,
    )
    .expect("connect decoder-failure receiver");
    assert!(receiver.receive_one(wrong_source).is_err());
    assert_eq!(receiver.pending_feedback_lsn(), None);
    assert_eq!(slot_lsn(&mut admin, SLOT), replay_end_lsn);
    assert_eq!(durable_state(&mut admin), (3, 15, 3, 15, 3, 2));
    drop(receiver);

    let mut receiver = SourceReceiver::connect(
        &replication_url,
        SLOT,
        PUBLICATION,
        replay_end_lsn,
        replay_end_lsn,
    )
    .expect("restart after decoder failure");
    let decoded_retry = receiver
        .receive_and_apply_one(&mut apply, source)
        .expect("retry decoder-failure transaction");
    assert_eq!(decoded_retry.outcome(), ProcessOutcome::Applied);
    receiver
        .acknowledge(&decoded_retry)
        .expect("acknowledge decoder retry");
    wait_for_slot_lsn(&mut admin, SLOT, decoded_retry.end_lsn());
    assert_eq!(durable_state(&mut admin), (4, 22, 4, 22, 4, 3));
    let decoder_retry_lsn = decoded_retry.end_lsn();
    drop(receiver);

    admin
        .batch_execute(
            "CREATE SCHEMA m10_test;
             CREATE FUNCTION m10_test.fail_operator() RETURNS trigger
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'M10 operator failure'; END $$;
             CREATE TRIGGER m10_fail_operator BEFORE UPDATE
             ON shiba_internal.operator_state FOR EACH ROW
             EXECUTE FUNCTION m10_test.fail_operator();
             INSERT INTO source.events VALUES (5, 11);",
        )
        .expect("install failure point and commit operator-failure transaction");
    let mut receiver = SourceReceiver::connect(
        &replication_url,
        SLOT,
        PUBLICATION,
        decoder_retry_lsn,
        decoder_retry_lsn,
    )
    .expect("connect operator-failure receiver");
    assert!(receiver.receive_and_apply_one(&mut apply, source).is_err());
    assert_eq!(receiver.pending_feedback_lsn(), None);
    assert_eq!(slot_lsn(&mut admin, SLOT), decoder_retry_lsn);
    assert_eq!(durable_state(&mut admin), (4, 22, 4, 22, 4, 3));
    drop(receiver);

    admin
        .batch_execute("DROP SCHEMA m10_test CASCADE")
        .expect("remove test-only operator failure point");
    let mut receiver = SourceReceiver::connect(
        &replication_url,
        SLOT,
        PUBLICATION,
        decoder_retry_lsn,
        decoder_retry_lsn,
    )
    .expect("restart after operator failure");
    let operator_retry = receiver
        .receive_and_apply_one(&mut apply, source)
        .expect("retry rolled-back operator transaction");
    assert_eq!(operator_retry.outcome(), ProcessOutcome::Applied);
    receiver
        .acknowledge(&operator_retry)
        .expect("acknowledge operator retry");
    wait_for_slot_lsn(&mut admin, SLOT, operator_retry.end_lsn());
    assert_eq!(durable_state(&mut admin), (5, 33, 5, 33, 5, 4));
}
