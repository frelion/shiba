use std::time::Duration;

use postgres::{Client, NoTls};
use shiba_ingress::{
    AttachOptions, BootstrapCatchupProgress, BootstrapOptions, BootstrapSession, BootstrapSpec,
    GovernedGraphSession, PreparedRebuild, ReplicationMode,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};
use shiba_runtime::ProcessOutcome;
use shiba_sql_registration::compile_sql_and_register;

#[path = "m15_sql_join/support.rs"]
mod support;

const GRAPH_ID: u64 = 1;
const SQL: &str = "SELECT l.id, r.payload FROM left_source.events AS l \
     INNER JOIN right_source.events AS r ON l.right_key = r.id";

fn bootstrap_options() -> BootstrapOptions {
    BootstrapOptions::new(2, Duration::from_secs(5)).expect("bounded bootstrap options")
}

fn attach_options() -> AttachOptions {
    AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
        .expect("bounded attach options")
}

#[test]
#[ignore = "requires scripts/test-m15-sql-join.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered SQL registration and production lifecycle proof"
)]
fn sql_join_uses_one_graph_snapshot_receiver_and_changed_object_rebuild() {
    let database_url = support::required("SHIBA_M15_SQL_JOIN_DATABASE_URL");
    let replication_url = support::required("SHIBA_M15_SQL_JOIN_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect test database");
    let fixture = support::install(&mut admin);
    support::assert_role_shape(&mut admin);
    let control_url = support::as_role(&database_url, support::CONTROL_ROLE);
    let receiver_url = support::as_role(&replication_url, support::RECEIVER_ROLE);
    let reader_url = support::as_role(&database_url, support::READER_ROLE);
    let mut control = Client::connect(&control_url, NoTls).expect("connect registration control");
    assert!(
        compile_sql_and_register(&mut control, GraphId::new(GRAPH_ID).expect("graph ID"), SQL,)
            .is_err(),
        "missing graph INSERT privileges must fail registration closed"
    );
    support::assert_no_registered_graph(&mut admin);
    support::grant_registration_control(&mut admin);
    let graph =
        compile_sql_and_register(&mut control, GraphId::new(GRAPH_ID).expect("graph ID"), SQL)
            .expect("least-privilege compile and atomic SQL join registration");
    support::assert_sql_registration(&mut admin, &fixture.old, &graph);
    support::prove_missing_bootstrap_grant(&control_url, &receiver_url, &mut admin, &fixture);
    support::grant_bootstrap_control(&mut admin);
    let mut reader =
        Client::connect(&reader_url, NoTls).expect("connect SELECT-only result reader");

    let mut bootstrap = BootstrapSession::begin(
        &control_url,
        &receiver_url,
        BootstrapSpec {
            graph_id: GraphId::new(GRAPH_ID).expect("graph ID"),
            bootstrap_id: BootstrapId::new(1).expect("bootstrap ID"),
            publication_oid: fixture.old.publication_oid,
            slot_name: support::OLD_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(1).expect("generation"),
        },
        bootstrap_options(),
    )
    .expect("export one snapshot for both SQL join sources");
    support::assert_building(&mut admin);
    support::assert_reader_building(&mut reader);
    admin
        .batch_execute(
            "BEGIN;
             UPDATE right_source.events SET payload=110 WHERE id=10;
             INSERT INTO left_source.events VALUES (4,20);
             DELETE FROM left_source.events WHERE id=2;
             COMMIT;",
        )
        .expect("commit both-source snapshot-era WAL");
    support::scan_all(&mut bootstrap, &mut admin);
    support::assert_one_snapshot_two_sources(&mut admin);
    let mut catchup = bootstrap.into_catchup().expect("enter SQL join catch-up");
    assert_eq!(
        catchup.catch_up_next().expect("apply snapshot-era WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        catchup.catch_up_next().expect("activate SQL join graph"),
        BootstrapCatchupProgress::Active
    );
    support::assert_continuations(&mut admin, 1, 1);
    support::assert_old_oracle(&mut admin);
    support::assert_reader_matches(
        &mut reader,
        &mut admin,
        "left_source.events",
        "right_source.events",
    );
    let mut live = catchup.into_live().expect("enter SQL join live receiver");

    admin
        .batch_execute(
            "BEGIN;
             UPDATE right_source.events SET payload=200 WHERE id=20;
             INSERT INTO left_source.events VALUES (5,20);
             COMMIT;",
        )
        .expect("commit both-source live transaction");
    let applied = live
        .receive_and_apply_one()
        .expect("durably apply both-source live transaction");
    assert_eq!(applied.outcome(), ProcessOutcome::Applied);
    support::assert_old_oracle(&mut admin);
    support::assert_reader_matches(
        &mut reader,
        &mut admin,
        "left_source.events",
        "right_source.events",
    );
    live.acknowledge(&applied).expect("ACK durable SQL join");
    support::assert_feedback(&mut admin, support::OLD_SLOT, applied.end_lsn());

    admin
        .batch_execute(
            "BEGIN;
             UPDATE left_source.events SET right_key=10 WHERE id=4;
             UPDATE right_source.events SET payload=210 WHERE id=10;
             COMMIT;",
        )
        .expect("commit SQL join ACK crash-window transaction");
    let pending = live
        .receive_and_apply_one()
        .expect("Apply SQL join transaction before ACK");
    assert_eq!(pending.outcome(), ProcessOutcome::Applied);
    let pending_end = pending.end_lsn();
    let feedback_before = support::slot_lsn(&mut admin, support::OLD_SLOT);
    assert!(feedback_before < pending_end);
    support::assert_old_oracle(&mut admin);
    drop(live);

    let mut replay = GovernedGraphSession::attach(
        &control_url,
        &receiver_url,
        GraphId::new(GRAPH_ID).expect("graph ID"),
        SlotGeneration::new(1).expect("generation"),
        attach_options(),
    )
    .expect("restart SQL join receiver");
    let replayed = replay
        .receive_and_apply_one()
        .expect("receive exact SQL join replay");
    assert_eq!(replayed.outcome(), ProcessOutcome::AlreadyApplied);
    assert_eq!(replayed.end_lsn(), pending_end);
    replay.acknowledge(&replayed).expect("ACK exact replay");
    support::assert_feedback(&mut admin, support::OLD_SLOT, pending_end);

    support::invalidate_identity_and_assert_fail_closed(&mut admin, &mut replay);
    drop(replay);
    let spec = support::changed_object_rebuild(&mut admin, &fixture);
    let prepared =
        PreparedRebuild::prepare(&control_url, &receiver_url, &spec, bootstrap_options())
            .expect("prepare changed-ObjectAddress SQL join rebuild");
    support::assert_building(&mut admin);
    support::assert_reader_building(&mut reader);
    let mut rebuilt = prepared
        .into_bootstrap()
        .expect("export one target graph snapshot");
    admin
        .batch_execute(
            "BEGIN;
             UPDATE join_target_right.events SET payload=310 WHERE id=100;
             INSERT INTO join_target_left.events VALUES (13,200);
             DELETE FROM join_target_left.events WHERE id=11;
             COMMIT;",
        )
        .expect("commit both-target-source rebuild WAL");
    support::scan_all(&mut rebuilt, &mut admin);
    let mut rebuilt_catchup = rebuilt.into_catchup().expect("enter rebuilt catch-up");
    assert_eq!(
        rebuilt_catchup
            .catch_up_next()
            .expect("apply rebuilt SQL join WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        rebuilt_catchup
            .catch_up_next()
            .expect("activate rebuilt SQL join"),
        BootstrapCatchupProgress::Active
    );
    support::assert_target_oracle(&mut admin);
    support::assert_reader_matches(
        &mut reader,
        &mut admin,
        "join_target_left.events",
        "join_target_right.events",
    );
    support::assert_target_authority(&mut admin, &fixture, spec.target.graph_digest);
    support::assert_generation(&mut admin, 2, support::NEW_SLOT);

    let mut rebuilt_live = rebuilt_catchup
        .into_live()
        .expect("enter rebuilt SQL join live receiver");
    admin
        .batch_execute(
            "BEGIN;
             UPDATE join_target_right.events SET payload=320 WHERE id=200;
             UPDATE join_target_left.events SET right_key=100 WHERE id=13;
             COMMIT;",
        )
        .expect("commit post-rebuild both-source transaction");
    let final_token = rebuilt_live
        .receive_and_apply_one()
        .expect("apply post-rebuild SQL join transaction");
    assert_eq!(final_token.outcome(), ProcessOutcome::Applied);
    support::assert_target_oracle(&mut admin);
    support::assert_reader_matches(
        &mut reader,
        &mut admin,
        "join_target_left.events",
        "join_target_right.events",
    );
    rebuilt_live
        .acknowledge(&final_token)
        .expect("ACK rebuilt SQL join transaction");
    support::assert_feedback(&mut admin, support::NEW_SLOT, final_token.end_lsn());
    rebuilt_live.detach().expect("detach rebuilt SQL join");
}
