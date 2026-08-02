use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, message_end, read_u16, read_u32, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m5-toast.sh",
    env_prefix: "SHIBA_M5_TOAST",
    slot: "shiba_m5_toast_slot",
    publication: "shiba_m5_toast_pub",
};

fn deterministic_payload() -> String {
    let mut payload = String::with_capacity(64 * 1024);
    for value in 0..8192_u32 {
        write!(&mut payload, "{value:08x}").expect("write deterministic payload");
    }
    payload
}

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 1),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable state");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

fn applied_payload(client: &mut Client) -> String {
    client
        .query_one(
            "SELECT payload_text FROM shiba_internal.source_row_state
             WHERE source_row_id = 601",
            &[],
        )
        .expect("query applied text payload")
        .get(0)
}

fn update_payload_tag(wire: &[u8]) -> usize {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let update = message_end(wire, relation);
    assert_eq!(wire[update], b'U');
    assert_eq!(wire[update + 5], b'N');
    assert_eq!(read_u16(wire, update + 6), 2);
    let key_tag = update + 8;
    assert_eq!(wire[key_tag], b't');
    let key_length = usize::try_from(read_u32(wire, key_tag + 1)).expect("key length");
    assert_eq!(&wire[key_tag + 5..key_tag + 5 + key_length], b"601");
    key_tag + 5 + key_length
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m5_toast_test;
             CREATE FUNCTION m5_toast_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m5_toast_crash
             AFTER INSERT ON shiba_internal.source_continuation
             FOR EACH ROW EXECUTE FUNCTION m5_toast_test.crash_after_continuation();",
        )
        .expect("install continuation crash point");
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m5-toast.sh"]
fn m5_real_unchanged_toast_crash_retry_and_replay() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m5_toast;
             CREATE TABLE source_m5_toast.events (
                 id bigint PRIMARY KEY,
                 payload text NOT NULL
             );
             ALTER TABLE source_m5_toast.events
                 ALTER COLUMN payload SET STORAGE EXTERNAL;
             CREATE PUBLICATION shiba_m5_toast_pub FOR TABLE source_m5_toast.events;",
        )
        .expect("install TOAST source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m5_toast.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::with_text_payload(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m5_toast.events");
    CAPTURE.create_slot();

    let payload = deterministic_payload();
    client
        .execute(
            "INSERT INTO source_m5_toast.events VALUES (601, $1)",
            &[&payload],
        )
        .expect("commit external text payload");
    let has_toast_storage: bool = client
        .query_one(
            "SELECT pg_relation_size(reltoastrelid) > 0
             FROM pg_class WHERE oid = 'source_m5_toast.events'::regclass",
            &[],
        )
        .expect("verify TOAST relation storage")
        .get(0);
    assert!(
        has_toast_storage,
        "payload must have out-of-line TOAST chunks"
    );

    let insert_wire = CAPTURE.capture(&mut client, "toast-insert.pgoutput");
    let insert = decode_committed_changes(&insert_wire, source).expect("decode TOAST insert");
    assert_eq!(
        process(&mut client, &insert).expect("apply TOAST insert"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(applied_payload(&mut client), payload);
    assert_eq!(
        process(&mut client, &insert).expect("replay TOAST insert"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));

    client
        .batch_execute("UPDATE source_m5_toast.events SET id = id WHERE id = 601")
        .expect("commit update retaining external payload");
    let update_wire = CAPTURE.capture(&mut client, "unchanged-toast.pgoutput");
    let payload_tag = update_payload_tag(&update_wire);
    assert_eq!(update_wire[payload_tag], b'u');
    let mut corrupt = update_wire.clone();
    corrupt[payload_tag] = b't';
    assert!(decode_committed_changes(&corrupt, source).is_err());
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(applied_payload(&mut client), payload);

    let update = decode_committed_changes(&update_wire, source).expect("decode unchanged TOAST");
    install_crash_trigger(&mut client);
    assert!(process(&mut client, &update).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(applied_payload(&mut client), payload);
    client
        .batch_execute("DROP SCHEMA m5_toast_test CASCADE")
        .expect("remove crash point");

    assert_eq!(
        process(&mut client, &update).expect("retry unchanged TOAST"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_eq!(applied_payload(&mut client), payload);
    assert_eq!(
        process(&mut client, &update).expect("replay unchanged TOAST"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_eq!(applied_payload(&mut client), payload);
}
use std::fmt::Write as _;
