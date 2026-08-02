use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, message_end, read_u16, read_u32, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m5-incompressible-toast.sh",
    env_prefix: "SHIBA_M5_INCOMPRESSIBLE",
    slot: "shiba_m5_incompressible_slot",
    publication: "shiba_m5_incompressible_pub",
};

fn high_entropy_payload(mut state: u64) -> String {
    let mut payload = String::with_capacity(64 * 1024);
    for _ in 0..64 * 1024 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let ascii = b'!' + u8::try_from(state % 94).expect("ASCII range");
        payload.push(char::from(ascii));
    }
    payload
}

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

fn applied_payload(client: &mut Client) -> String {
    client
        .query_one(
            "SELECT payload_text FROM shiba_internal.applied_insert
             WHERE source_row_id = 701",
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
    assert_eq!(&wire[key_tag + 5..key_tag + 5 + key_length], b"701");
    key_tag + 5 + key_length
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m5_incompressible_test;
             CREATE FUNCTION m5_incompressible_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m5_incompressible_crash
             AFTER INSERT ON shiba_internal.source_continuation
             FOR EACH ROW EXECUTE FUNCTION
                 m5_incompressible_test.crash_after_continuation();",
        )
        .expect("install continuation crash point");
}

fn assert_external_uncompressed(client: &mut Client, payload: &str) {
    let toast = client
        .query_one(
            "SELECT pg_relation_size(c.reltoastrelid) > 0,
                    pg_column_compression(e.payload) IS NULL,
                    octet_length(e.payload), e.payload = $1
             FROM pg_class c CROSS JOIN source_m5_incompressible.events e
             WHERE c.oid = 'source_m5_incompressible.events'::regclass",
            &[&payload],
        )
        .expect("verify external uncompressed payload");
    assert!(
        toast.get::<_, bool>(0),
        "TOAST relation must contain storage"
    );
    assert!(toast.get::<_, bool>(1), "payload must not be compressed");
    assert_eq!(toast.get::<_, i32>(2), 64 * 1024);
    assert!(toast.get::<_, bool>(3), "source payload must remain exact");
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m5-incompressible-toast.sh"]
fn m5_real_incompressible_toast_update_crash_retry_and_replay() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m5_incompressible;
             CREATE TABLE source_m5_incompressible.events (
                 id bigint PRIMARY KEY,
                 payload text NOT NULL
             );
             CREATE PUBLICATION shiba_m5_incompressible_pub
                 FOR TABLE source_m5_incompressible.events;",
        )
        .expect("install incompressible TOAST source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m5_incompressible.events'::regclass::oid::bigint",
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
    register_source(&mut client, "source_m5_incompressible.events");
    CAPTURE.create_slot();

    let first_payload = high_entropy_payload(0x4d59_5df4_d0f3_3173);
    let second_payload = high_entropy_payload(0x8b8b_8b8b_1357_9bdf);
    assert_ne!(first_payload, second_payload);
    client
        .execute(
            "INSERT INTO source_m5_incompressible.events VALUES (701, $1)",
            &[&first_payload],
        )
        .expect("commit incompressible payload");
    assert_external_uncompressed(&mut client, &first_payload);

    let insert_wire = CAPTURE.capture(&mut client, "incompressible-insert.pgoutput");
    let insert = decode_committed_changes(&insert_wire, source).expect("decode text insert");
    assert_eq!(
        process(&mut client, &insert).expect("apply text insert"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(applied_payload(&mut client), first_payload);
    assert_eq!(
        process(&mut client, &insert).expect("replay text insert"),
        ProcessOutcome::AlreadyApplied
    );

    client
        .execute(
            "UPDATE source_m5_incompressible.events SET payload = $1 WHERE id = 701",
            &[&second_payload],
        )
        .expect("commit replacement payload");
    assert_external_uncompressed(&mut client, &second_payload);
    let update_wire = CAPTURE.capture(&mut client, "incompressible-update.pgoutput");
    let payload_tag = update_payload_tag(&update_wire);
    assert_eq!(update_wire[payload_tag], b't');
    let payload_length =
        usize::try_from(read_u32(&update_wire, payload_tag + 1)).expect("payload length");
    assert_eq!(payload_length, second_payload.len());
    assert_eq!(
        &update_wire[payload_tag + 5..payload_tag + 5 + payload_length],
        second_payload.as_bytes()
    );
    let mut corrupt = update_wire.clone();
    corrupt[payload_tag] = b'b';
    assert!(decode_committed_changes(&corrupt, source).is_err());
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(applied_payload(&mut client), first_payload);

    let update = decode_committed_changes(&update_wire, source).expect("decode replacement update");
    install_crash_trigger(&mut client);
    assert!(process(&mut client, &update).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(applied_payload(&mut client), first_payload);
    client
        .batch_execute("DROP SCHEMA m5_incompressible_test CASCADE")
        .expect("remove crash point");

    assert_eq!(
        process(&mut client, &update).expect("retry replacement update"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_eq!(applied_payload(&mut client), second_payload);
    assert_eq!(
        process(&mut client, &update).expect("replay replacement update"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_eq!(applied_payload(&mut client), second_payload);
}
