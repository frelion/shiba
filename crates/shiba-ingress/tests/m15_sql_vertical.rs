use std::time::Duration;

use postgres::{Client, NoTls};
use shiba_ingress::{
    AttachOptions, BootstrapCatchupProgress, BootstrapOptions, BootstrapSession, BootstrapSpec,
    GovernedGraphSession, PreparedRebuild, ReplicationMode,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};
use shiba_runtime::ProcessOutcome;
use shiba_sql_registration::compile_sql_and_register;

#[path = "m15_sql_vertical/support.rs"]
mod support;

const SQL: &str = "SELECT e.\"Id\", e.\"Payload\" + 1 \
                   FROM \"Source Schema\".\"Event Rows\" AS e \
                   WHERE e.\"Payload\" > 0";

fn bootstrap_options() -> BootstrapOptions {
    BootstrapOptions::new(2, Duration::from_secs(5)).expect("bounded bootstrap options")
}

fn attach_options() -> AttachOptions {
    AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
        .expect("bounded attach options")
}

#[test]
#[ignore = "requires scripts/test-m15-sql-vertical.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered SQL registration/bootstrap/live/rebuild proof"
)]
fn sql_projection_uses_production_bootstrap_receiver_ack_and_rebuild() {
    let database_url = support::required("SHIBA_M15_SQL_VERTICAL_DATABASE_URL");
    let replication_url = support::required("SHIBA_M15_SQL_VERTICAL_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect test database");
    let fixture = support::install(&mut admin);
    support::prove_ddl_first_race(&database_url, &mut admin);

    support::install_registration_failure(&mut admin);
    assert!(compile_sql_and_register(&mut admin, GraphId::new(1).expect("graph ID"), SQL).is_err());
    support::assert_no_registered_graph(&mut admin);
    support::remove_registration_failure(&mut admin);
    let graph = compile_sql_and_register(&mut admin, GraphId::new(1).expect("graph ID"), SQL)
        .expect("atomically bind and register SQL graph");
    support::assert_registered(&mut admin, &graph);

    let mut bootstrap = BootstrapSession::begin(
        &database_url,
        &replication_url,
        BootstrapSpec {
            graph_id: GraphId::new(1).expect("graph ID"),
            bootstrap_id: BootstrapId::new(1).expect("bootstrap ID"),
            publication_oid: fixture.old_publication,
            slot_name: support::OLD_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(1).expect("generation"),
        },
        bootstrap_options(),
    )
    .expect("export SQL graph snapshot");
    support::assert_building(&mut admin);
    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO \"Source Schema\".\"Event Rows\" VALUES (4,7);
             UPDATE \"Source Schema\".\"Event Rows\" SET \"Payload\"=0 WHERE \"Id\"=1;
             DELETE FROM \"Source Schema\".\"Event Rows\" WHERE \"Id\"=3;
             COMMIT;",
        )
        .expect("commit snapshot-era I/U/D");
    support::scan_all(&mut bootstrap, &mut admin);
    support::assert_building(&mut admin);
    let mut catchup = bootstrap.into_catchup().expect("enter catch-up");
    assert_eq!(
        catchup.catch_up_next().expect("apply snapshot-era WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        catchup.catch_up_next().expect("activate SQL graph"),
        BootstrapCatchupProgress::Active
    );
    support::assert_oracle(&mut admin, "Source Schema", "Event Rows");
    let mut live = catchup.into_live().expect("enter production live receiver");

    admin
        .batch_execute(
            "BEGIN;
             UPDATE \"Source Schema\".\"Event Rows\" SET \"Payload\"=2 WHERE \"Id\"=2;
             UPDATE \"Source Schema\".\"Event Rows\" SET \"Id\"=5,\"Payload\"=8 WHERE \"Id\"=4;
             INSERT INTO \"Source Schema\".\"Event Rows\" VALUES (6,NULL);
             DELETE FROM \"Source Schema\".\"Event Rows\" WHERE \"Id\"=1;
             COMMIT;",
        )
        .expect("commit predicate, NULL, key-change and delete transaction");
    let applied = live
        .receive_and_apply_one()
        .expect("apply live SQL graph WAL");
    assert_eq!(applied.outcome(), ProcessOutcome::Applied);
    support::assert_oracle(&mut admin, "Source Schema", "Event Rows");
    live.acknowledge(&applied).expect("ACK durable live Apply");
    support::wait_for_slot_lsn(&mut admin, support::OLD_SLOT, applied.end_lsn());

    admin
        .batch_execute(
            "BEGIN;
             UPDATE \"Source Schema\".\"Event Rows\" SET \"Payload\"=-1 WHERE \"Id\"=2;
             UPDATE \"Source Schema\".\"Event Rows\" SET \"Payload\"=3 WHERE \"Id\"=6;
             DELETE FROM \"Source Schema\".\"Event Rows\" WHERE \"Id\"=5;
             COMMIT;",
        )
        .expect("commit ACK crash-window transaction");
    let pending = live
        .receive_and_apply_one()
        .expect("durably Apply before ACK");
    assert_eq!(pending.outcome(), ProcessOutcome::Applied);
    let pending_end = pending.end_lsn();
    drop(live);
    let mut replay = GovernedGraphSession::attach(
        &database_url,
        &replication_url,
        GraphId::new(1).expect("graph ID"),
        SlotGeneration::new(1).expect("generation"),
        attach_options(),
    )
    .expect("restart production receiver");
    let replayed = replay
        .receive_and_apply_one()
        .expect("receive exact replay");
    assert_eq!(replayed.outcome(), ProcessOutcome::AlreadyApplied);
    assert_eq!(replayed.end_lsn(), pending_end);
    replay.acknowledge(&replayed).expect("ACK exact replay");
    support::wait_for_slot_lsn(&mut admin, support::OLD_SLOT, pending_end);
    support::assert_oracle(&mut admin, "Source Schema", "Event Rows");
    replay.detach().expect("detach old generation");

    let rebuild_spec = support::changed_object_rebuild(&mut admin, &fixture);
    let prepared = PreparedRebuild::prepare(
        &database_url,
        &replication_url,
        &rebuild_spec,
        bootstrap_options(),
    )
    .expect("prepare changed-ObjectAddress SQL rebuild");
    support::assert_building(&mut admin);
    let mut target_bootstrap = prepared.into_bootstrap().expect("export target snapshot");
    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO \"Target Schema\".\"Event Rows\" VALUES (13,5);
             UPDATE \"Target Schema\".\"Event Rows\" SET \"Payload\"=0 WHERE \"Id\"=10;
             DELETE FROM \"Target Schema\".\"Event Rows\" WHERE \"Id\"=12;
             COMMIT;",
        )
        .expect("commit rebuild catch-up I/U/D");
    support::scan_all(&mut target_bootstrap, &mut admin);
    support::assert_building(&mut admin);
    let mut rebuilt_catchup = target_bootstrap
        .into_catchup()
        .expect("enter rebuild catch-up");
    assert_eq!(
        rebuilt_catchup.catch_up_next().expect("apply target WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        rebuilt_catchup
            .catch_up_next()
            .expect("activate target graph"),
        BootstrapCatchupProgress::Active
    );
    support::assert_oracle(&mut admin, "Target Schema", "Event Rows");
    support::assert_target_authority(&mut admin, &fixture);

    let mut target_live = rebuilt_catchup
        .into_live()
        .expect("enter rebuilt live receiver");
    admin
        .batch_execute(
            "BEGIN;
             UPDATE \"Target Schema\".\"Event Rows\" SET \"Payload\"=2 WHERE \"Id\"=11;
             UPDATE \"Target Schema\".\"Event Rows\" SET \"Id\"=14 WHERE \"Id\"=13;
             COMMIT;",
        )
        .expect("commit post-rebuild live SQL transaction");
    let final_token = target_live
        .receive_and_apply_one()
        .expect("apply rebuilt live WAL");
    assert_eq!(final_token.outcome(), ProcessOutcome::Applied);
    support::assert_oracle(&mut admin, "Target Schema", "Event Rows");
    target_live
        .acknowledge(&final_token)
        .expect("ACK rebuilt live Apply");
    support::wait_for_slot_lsn(&mut admin, support::NEW_SLOT, final_token.end_lsn());
    target_live.detach().expect("detach rebuilt SQL graph");
}
