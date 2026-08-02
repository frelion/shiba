use std::{num::NonZeroU64, process::Command, time::Duration};

use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_ingress::{
    BootstrapCatchupProgress, BootstrapCatchupSession, BootstrapOptions, BootstrapSession,
    BootstrapSpec, SnapshotProgress,
};
use shiba_operator::OperatorId;
use shiba_protocol::{BootstrapBatchId, BootstrapId, SlotGeneration, SourceId};
use shiba_runtime::{
    BootstrapBatch, BootstrapProcessOutcome, SnapshotRow, compile_and_register,
    process_bootstrap_batch,
};

#[allow(dead_code)]
mod support;

use support::slot_lsn;

const SLOT: &str = "shiba_m11_recovery_slot";
const OLD_SLOT: &str = "shiba_m11_abandoned_slot";
const FAILED_SLOT: &str = "shiba_m11_failed_create_slot";
const FOREIGN_SLOT: &str = "shiba_m11_foreign_slot";
const PUBLICATION: &str = "shiba_m11_recovery_pub";
const APPLICATION: &str = "shiba_m11_recovery_receiver";

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("scripts/test-m11-recovery.sh must set {name}"))
}

fn restart_postgres(mode: &str) {
    let pg_ctl = required("SHIBA_TEST_PG_CTL");
    let data = required("SHIBA_TEST_PG_DATA");
    let socket = required("SHIBA_TEST_PG_SOCKET");
    let port = required("SHIBA_TEST_PG_PORT");
    let stopped = Command::new(&pg_ctl)
        .args(["-D", &data, "-m", mode, "-w", "stop"])
        .status()
        .expect("execute pg_ctl stop");
    assert!(stopped.success(), "pg_ctl immediate stop failed");
    let started = Command::new(pg_ctl)
        .args(["-D", &data, "-o"])
        .arg(format!("-k {socket} -p {port}"))
        .args(["-w", "start"])
        .status()
        .expect("execute pg_ctl start");
    assert!(started.success(), "pg_ctl restart failed");
}

fn operator_spec(operator_id: u64, operation: OperatorOperationV1) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(operator_id).expect("operator ID")),
        source_id: SourceId::new(1).expect("source ID"),
        operation,
    }
}

fn states(client: &mut Client) -> Vec<(i64, i64)> {
    client
        .query(
            "SELECT operator_id, value_bigint
             FROM shiba_internal.operator_state ORDER BY operator_id",
            &[],
        )
        .expect("query operator states")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn rows(client: &mut Client) -> Vec<(i64, Option<i64>)> {
    client
        .query(
            "SELECT source_row_id, payload_int8
             FROM shiba_internal.source_row_state ORDER BY source_row_id",
            &[],
        )
        .expect("query source state")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn checkpoint(client: &mut Client) -> (String, i64, Option<i64>) {
    let row = client
        .query_one(
            "SELECT phase, last_batch_ordinal, last_source_row_id
             FROM shiba_internal.source_bootstrap WHERE source_id = 1",
            &[],
        )
        .expect("query bootstrap checkpoint");
    (row.get(0), row.get(1), row.get(2))
}

fn install_receiver_kill_trigger(client: &mut Client, target: &str, event: &str) {
    client
        .batch_execute(&format!(
            "CREATE FUNCTION public.kill_m11_receiver() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
               PERFORM pg_catalog.pg_terminate_backend(pid)
               FROM pg_catalog.pg_stat_replication
               WHERE application_name = '{APPLICATION}';
               RETURN NEW;
             END
             $body$;
             CREATE TRIGGER kill_m11_receiver {event} ON {target}
             FOR EACH ROW EXECUTE FUNCTION public.kill_m11_receiver();"
        ))
        .expect("install receiver failure injection");
}

fn remove_receiver_kill_trigger(client: &mut Client, target: &str) {
    client
        .batch_execute(&format!(
            "DROP TRIGGER kill_m11_receiver ON {target};
             DROP FUNCTION public.kill_m11_receiver();"
        ))
        .expect("remove receiver failure injection");
}

#[test]
#[ignore = "requires scripts/test-m11-recovery.sh"]
#[allow(clippy::too_many_lines, reason = "one ordered crash/recovery proof")]
fn bootstrap_batches_workers_restart_feedback_and_cutover_recover() {
    let database_url = required("SHIBA_M11_RECOVERY_DATABASE_URL");
    let replication_url = required("SHIBA_M11_RECOVERY_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect test database");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events
               WITH (publish = 'insert, update, delete');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);
             INSERT INTO source.events VALUES (1, 10), (2, 20), (3, 10);"
        ))
        .expect("install recovery source");
    compile_and_register(
        &mut admin,
        &operator_spec(1, OperatorOperationV1::CountRows),
    )
    .expect("register CountRows");
    compile_and_register(
        &mut admin,
        &operator_spec(
            2,
            OperatorOperationV1::SumInt8 {
                input_column: "payload".to_owned(),
            },
        ),
    )
    .expect("register SumInt8");

    let publication_oid = admin
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("query publication OID")
        .get::<_, u32>(0);
    let source_id = SourceId::new(1).expect("source ID");
    let options = BootstrapOptions::new(2, Duration::from_secs(5)).expect("bootstrap options");

    let failed_spec = BootstrapSpec {
        source_id,
        bootstrap_id: BootstrapId::new(10).expect("failed bootstrap ID"),
        publication_oid,
        slot_name: FAILED_SLOT.to_owned(),
        slot_generation: SlotGeneration::new(1).expect("failed generation"),
    };
    admin
        .query_one(
            "SELECT shiba_internal.reserve_source_bootstrap(
                 $1, $2, $3::bigint::oid, $4::text::name, $5
             )",
            &[
                &i64::try_from(failed_spec.bootstrap_id.get()).expect("bootstrap bigint"),
                &i64::try_from(source_id.get()).expect("source bigint"),
                &i64::from(publication_oid),
                &FAILED_SLOT,
                &i64::try_from(failed_spec.slot_generation.get()).expect("generation bigint"),
            ],
        )
        .expect("persist exact crash-after-reservation window");
    assert_eq!(checkpoint(&mut admin), ("creating".to_owned(), 0, None));
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&FAILED_SLOT],
            )
            .expect("reservation must not imply physical slot")
            .get::<_, i64>(0),
        0
    );

    let abandoned_spec = BootstrapSpec {
        source_id,
        bootstrap_id: BootstrapId::new(11).expect("abandoned bootstrap ID"),
        publication_oid,
        slot_name: OLD_SLOT.to_owned(),
        slot_generation: SlotGeneration::new(2).expect("abandoned generation"),
    };
    let mut bootstrap = BootstrapSession::restart_abandoned(
        &database_url,
        &replication_url,
        &failed_spec,
        abandoned_spec.clone(),
        options,
    )
    .expect("replace cleanup-pending attempt");
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 0, None));
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&FAILED_SLOT],
            )
            .expect("verify failed slot removal")
            .get::<_, i64>(0),
        0
    );

    let Err(competing) = BootstrapSession::begin(
        &database_url,
        &replication_url,
        abandoned_spec.clone(),
        options,
    ) else {
        panic!("a second worker must not own the same source");
    };
    assert!(
        competing
            .to_string()
            .contains("source already has an active session"),
        "unexpected competing-worker failure: {competing}"
    );

    assert_eq!(
        bootstrap.scan_next().expect("apply first batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 1,
            rows: 2
        }
    );
    let exact = BootstrapBatch::new(
        source_id,
        BootstrapBatchId::new(abandoned_spec.bootstrap_id, 1).expect("batch ID"),
        vec![
            SnapshotRow {
                source_row_id: 1,
                payload: Some(10),
            },
            SnapshotRow {
                source_row_id: 2,
                payload: Some(20),
            },
        ],
    )
    .expect("exact replay batch");
    assert_eq!(
        process_bootstrap_batch(&mut admin, &exact).expect("replay exact batch"),
        BootstrapProcessOutcome::AlreadyApplied
    );
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 1, Some(2)));
    assert_eq!(states(&mut admin), vec![(1, 2), (2, 30)]);

    drop(bootstrap);
    let bootstrap_id = BootstrapId::new(12).expect("replacement bootstrap ID");
    let generation = SlotGeneration::new(3).expect("replacement generation");
    let stale_replacement = BootstrapSpec {
        source_id,
        bootstrap_id,
        publication_oid,
        slot_name: SLOT.to_owned(),
        slot_generation: abandoned_spec.slot_generation,
    };
    assert!(
        BootstrapSession::restart_abandoned(
            &database_url,
            &replication_url,
            &abandoned_spec,
            stale_replacement,
            options,
        )
        .is_err(),
        "replacement generation must strictly advance"
    );
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&FOREIGN_SLOT],
        )
        .expect("create foreign replacement slot");
    let foreign_replacement = BootstrapSpec {
        source_id,
        bootstrap_id,
        publication_oid,
        slot_name: FOREIGN_SLOT.to_owned(),
        slot_generation: generation,
    };
    assert!(
        BootstrapSession::restart_abandoned(
            &database_url,
            &replication_url,
            &abandoned_spec,
            foreign_replacement,
            options,
        )
        .is_err(),
        "an existing replacement slot must never be adopted"
    );
    admin
        .query_one("SELECT pg_drop_replication_slot($1)", &[&FOREIGN_SLOT])
        .expect("drop test-owned foreign slot");
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 1, Some(2)));
    assert_eq!(states(&mut admin), vec![(1, 2), (2, 30)]);
    assert_eq!(rows(&mut admin), vec![(1, Some(10)), (2, Some(20))]);

    let spec = BootstrapSpec {
        source_id,
        bootstrap_id,
        publication_oid,
        slot_name: SLOT.to_owned(),
        slot_generation: generation,
    };
    let mut bootstrap = BootstrapSession::restart_abandoned(
        &database_url,
        &replication_url,
        &abandoned_spec,
        spec,
        options,
    )
    .expect("restart abandoned partial scan");
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 0, None));
    assert_eq!(states(&mut admin), vec![(1, 0), (2, 0)]);
    assert!(rows(&mut admin).is_empty());
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba.operator_result
                 WHERE result_status = 'building' AND value_bigint IS NULL",
                &[],
            )
            .expect("replacement results remain unavailable")
            .get::<_, i64>(0),
        2
    );
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&OLD_SLOT],
            )
            .expect("verify abandoned slot removal")
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        bootstrap
            .scan_next()
            .expect("apply replacement first batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 1,
            rows: 2
        }
    );

    admin
        .execute(
            "UPDATE shiba_internal.operator_state SET value_bigint = $1
             WHERE operator_id = 2",
            &[&(i64::MAX - 5)],
        )
        .expect("inject bounded operator overflow");
    let overflowing = BootstrapBatch::new(
        source_id,
        BootstrapBatchId::new(bootstrap_id, 2).expect("batch ID"),
        vec![SnapshotRow {
            source_row_id: 3,
            payload: Some(10),
        }],
    )
    .expect("overflow batch");
    assert!(process_bootstrap_batch(&mut admin, &overflowing).is_err());
    assert_eq!(rows(&mut admin), vec![(1, Some(10)), (2, Some(20))]);
    assert_eq!(checkpoint(&mut admin), ("scanning".to_owned(), 1, Some(2)));
    assert_eq!(states(&mut admin), vec![(1, 2), (2, i64::MAX - 5)]);
    admin
        .execute(
            "UPDATE shiba_internal.operator_state SET value_bigint = 30
             WHERE operator_id = 2",
            &[],
        )
        .expect("restore operator state after failure injection");
    assert_eq!(
        bootstrap.scan_next().expect("retry second batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 2,
            rows: 1
        }
    );
    assert_eq!(
        bootstrap.scan_next().expect("finish scan"),
        SnapshotProgress::ScanComplete
    );

    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO source.events VALUES (4, 5);
             UPDATE source.events SET payload = 15 WHERE id = 1;
             COMMIT;",
        )
        .expect("commit catch-up transaction");
    drop(bootstrap);
    drop(admin);
    restart_postgres("immediate");

    let mut admin = Client::connect(&database_url, NoTls).expect("reconnect after restart");
    assert_eq!(checkpoint(&mut admin).0, "scan_complete");
    let mut catchup = BootstrapCatchupSession::resume(
        &database_url,
        &replication_url,
        source_id,
        bootstrap_id,
        generation,
        options,
    )
    .expect("resume scan-complete attempt after PostgreSQL restart");
    assert_eq!(checkpoint(&mut admin).0, "catching_up");
    install_receiver_kill_trigger(
        &mut admin,
        "shiba_internal.source_continuation",
        "AFTER INSERT",
    );
    assert!(
        catchup.catch_up_next().is_err(),
        "feedback transport must fail after durable Apply"
    );
    assert_eq!(states(&mut admin), vec![(1, 4), (2, 50)]);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_continuation",
                &[]
            )
            .expect("query continuation after failed feedback")
            .get::<_, i64>(0),
        1
    );
    assert_eq!(checkpoint(&mut admin).0, "catching_up");
    drop(catchup);
    remove_receiver_kill_trigger(&mut admin, "shiba_internal.source_continuation");

    let mut catchup = BootstrapCatchupSession::resume(
        &database_url,
        &replication_url,
        source_id,
        bootstrap_id,
        generation,
        options,
    )
    .expect("resume after durable Apply before feedback");
    assert_eq!(
        catchup.catch_up_next().expect("ack exact Apply replay"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(states(&mut admin), vec![(1, 4), (2, 50)]);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_continuation",
                &[]
            )
            .expect("query replay continuation")
            .get::<_, i64>(0),
        1
    );

    install_receiver_kill_trigger(
        &mut admin,
        "shiba_internal.source_bootstrap",
        "AFTER UPDATE OF phase",
    );
    assert!(
        catchup.catch_up_next().is_err(),
        "feedback transport must fail after durable activation"
    );
    let activation_end: String = admin
        .query_one(
            "SELECT activation_end_lsn::text FROM shiba_internal.source_bootstrap
             WHERE source_id = 1 AND phase = 'active'",
            &[],
        )
        .expect("activation committed before feedback failure")
        .get(0);
    assert_eq!(
        admin
            .query(
                "SELECT operator_id, result_status, value_bigint
                 FROM shiba.operator_result ORDER BY operator_id",
                &[],
            )
            .expect("query active results")
            .into_iter()
            .map(|row| (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, i64>(2)
            ))
            .collect::<Vec<_>>(),
        vec![(1, "active".to_owned(), 4), (2, "active".to_owned(), 50)]
    );
    drop(catchup);
    remove_receiver_kill_trigger(&mut admin, "shiba_internal.source_bootstrap");

    let mut resumed = BootstrapCatchupSession::resume(
        &database_url,
        &replication_url,
        source_id,
        bootstrap_id,
        generation,
        options,
    )
    .expect("resume active cutover before feedback");
    assert_eq!(
        resumed
            .catch_up_next()
            .expect("replay and acknowledge fence"),
        BootstrapCatchupProgress::Active
    );
    drop(resumed);
    drop(admin);
    restart_postgres("fast");
    let mut admin = Client::connect(&database_url, NoTls).expect("reconnect after active restart");
    let mut resumed = BootstrapCatchupSession::resume(
        &database_url,
        &replication_url,
        source_id,
        bootstrap_id,
        generation,
        options,
    )
    .expect("resume after feedback covered active cutover");
    assert_eq!(
        resumed.catch_up_next().expect("active restart is a no-op"),
        BootstrapCatchupProgress::Active
    );
    let activation_lsn = {
        let (high, low) = activation_end
            .split_once('/')
            .expect("activation LSN shape");
        (u64::from_str_radix(high, 16).expect("activation high") << 32)
            | u64::from_str_radix(low, 16).expect("activation low")
    };
    assert!(slot_lsn(&mut admin, SLOT) >= activation_lsn);
    assert_eq!(
        rows(&mut admin),
        vec![(1, Some(15)), (2, Some(20)), (3, Some(10)), (4, Some(5))]
    );
    let oracle = admin
        .query_one(
            "SELECT count(*), COALESCE(sum(payload), 0)::bigint FROM source.events",
            &[],
        )
        .expect("query final SQL oracle");
    assert_eq!((oracle.get::<_, i64>(0), oracle.get::<_, i64>(1)), (4, 50));
}
