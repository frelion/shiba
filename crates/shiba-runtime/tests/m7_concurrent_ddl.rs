use std::{
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    M2Error, PgoutputSource, ProcessOutcome, SourceTransaction, decode_committed_changes, process,
};

mod support;

use support::{PgoutputCapture, register_source};

const ADVISORY_KEY: i64 = 75_001;
const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m7-concurrent-ddl.sh",
    env_prefix: "SHIBA_M7_CONCURRENT_DDL",
    slot: "shiba_m7_concurrent_ddl_slot",
    publication: "shiba_m7_concurrent_ddl_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1),
                (SELECT state_payload FROM shiba_internal.operator_state WHERE operator_id = 1),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable state");
    (
        row.get(0),
        support::decode_scalar_state(&row.get::<_, Vec<u8>>(1)),
        row.get(2),
        row.get(3),
    )
}

fn invalidation_count(client: &mut Client) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM shiba_internal.source_invalidation",
            &[],
        )
        .expect("query invalidation state")
        .get(0)
}

fn wait_for_lock(
    client: &mut Client,
    application_name: &str,
    relation_id: i64,
    mode: &str,
    granted: bool,
    wait_event: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observed: bool = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity AS activity
                    JOIN pg_locks AS lock ON lock.pid = activity.pid
                    WHERE activity.application_name = $1
                      AND lock.relation = $2::bigint::oid
                      AND lock.mode = $3
                      AND lock.granted = $4
                      AND activity.wait_event = $5
                )",
                &[
                    &application_name,
                    &relation_id,
                    &mode,
                    &granted,
                    &wait_event,
                ],
            )
            .expect("poll deterministic lock state")
            .get(0);
        if observed {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected lock state was not observed"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn install_apply_blocker(client: &mut Client) {
    client
        .batch_execute(&format!(
            "CREATE SCHEMA m7_concurrent_test;
             CREATE FUNCTION m7_concurrent_test.block_apply()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_advisory_xact_lock({ADVISORY_KEY});
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m7_concurrent_apply_block
             BEFORE INSERT ON shiba_internal.source_row_state
             FOR EACH ROW EXECUTE FUNCTION m7_concurrent_test.block_apply();"
        ))
        .expect("install Apply advisory-lock blocker");
}

fn spawn_apply(
    connection: String,
    input: SourceTransaction,
) -> thread::JoinHandle<Result<(), String>> {
    thread::Builder::new()
        .name("m7-apply".to_owned())
        .spawn(move || {
            let mut client =
                Client::connect(&connection, NoTls).map_err(|error| error.to_string())?;
            let outcome = process(&mut client, &input).map_err(|error| error.to_string())?;
            (outcome == ProcessOutcome::Applied)
                .then_some(())
                .ok_or_else(|| format!("unexpected Apply outcome {outcome:?}"))
        })
        .expect("spawn named Apply thread")
}

fn spawn_ddl(connection: String) -> thread::JoinHandle<Result<(), String>> {
    thread::Builder::new()
        .name("m7-ddl".to_owned())
        .spawn(move || {
            let mut client =
                Client::connect(&connection, NoTls).map_err(|error| error.to_string())?;
            client
                .batch_execute(
                    "ALTER TABLE source_m7_concurrent.events
                     RENAME TO invalidated_events",
                )
                .map_err(|error| error.to_string())
        })
        .expect("spawn named DDL thread")
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m7-concurrent-ddl.sh"]
fn m7_concurrent_apply_then_ddl_has_one_lock_order() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m7_concurrent;
             CREATE TABLE source_m7_concurrent.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m7_concurrent_ddl_pub
                 FOR TABLE source_m7_concurrent.events;",
        )
        .expect("install concurrent-DDL source objects");
    let relation_id: i64 = client
        .query_one(
            "SELECT 'source_m7_concurrent.events'::regclass::oid::bigint",
            &[],
        )
        .expect("read source relation OID")
        .get(0);
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        u32::try_from(relation_id).expect("relation OID fits u32"),
    );
    register_source(&mut client, "source_m7_concurrent.events");
    CAPTURE.create_slot();

    client
        .batch_execute("INSERT INTO source_m7_concurrent.events VALUES (1501)")
        .expect("commit first pending source transaction");
    let first_wire = CAPTURE.capture(&mut client, "first-pending.pgoutput");
    let first = decode_committed_changes(&first_wire, source).expect("decode first pending row");
    client
        .batch_execute("INSERT INTO source_m7_concurrent.events VALUES (1502)")
        .expect("commit second pending source transaction");
    let second_wire = CAPTURE.capture(&mut client, "second-pending.pgoutput");
    let second = decode_committed_changes(&second_wire, source).expect("decode second pending row");

    install_apply_blocker(&mut client);
    client
        .query_one("SELECT pg_advisory_lock($1)", &[&ADVISORY_KEY])
        .expect("hold Apply blocker");
    let apply = spawn_apply(
        format!("{connection} application_name=m7_apply"),
        first.clone(),
    );
    wait_for_lock(
        &mut client,
        "m7_apply",
        relation_id,
        "AccessShareLock",
        true,
        "advisory",
    );

    let ddl = spawn_ddl(format!("{connection} application_name=m7_ddl"));
    wait_for_lock(
        &mut client,
        "m7_ddl",
        relation_id,
        "AccessExclusiveLock",
        false,
        "relation",
    );
    assert_eq!(durable_state(&mut client), (0, 0, 0, 0));
    assert_eq!(invalidation_count(&mut client), 0);

    let unlocked: bool = client
        .query_one("SELECT pg_advisory_unlock($1)", &[&ADVISORY_KEY])
        .expect("release Apply blocker")
        .get(0);
    assert!(unlocked);
    apply
        .join()
        .expect("join Apply thread")
        .expect("Apply commits");
    ddl.join().expect("join DDL thread").expect("DDL commits");
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    let invalidation = client
        .query_one(
            "SELECT address_classid = 'pg_class'::regclass,
                    address_objid::bigint, address_objsubid
             FROM shiba_internal.source_invalidation WHERE source_id = 1",
            &[],
        )
        .expect("query committed DDL invalidation");
    assert!(invalidation.get::<_, bool>(0));
    assert_eq!(invalidation.get::<_, i64>(1), relation_id);
    assert_eq!(invalidation.get::<_, i32>(2), 0);

    assert!(matches!(
        process(&mut client, &second),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(
        process(&mut client, &first).expect("replay committed Apply"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
}
