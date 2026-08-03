use std::time::Duration;

use postgres::{Client, NoTls};
use shiba_ingress::{
    AttachOptions, BootstrapCatchupProgress, BootstrapOptions, BootstrapSession, BootstrapSpec,
    GovernedGraphSession, PreparedRebuild, ReplicationMode,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};
use shiba_runtime::ProcessOutcome;

#[path = "m14_join_lifecycle/support.rs"]
mod support;

const GRAPH_ID: u64 = 1;
const OLD_SLOT: &str = "shiba_m14_join_lifecycle_1";
const NEW_SLOT: &str = "shiba_m14_join_lifecycle_2";
const PUBLICATION: &str = "shiba_m14_join_lifecycle_pub";

fn bootstrap_options() -> BootstrapOptions {
    BootstrapOptions::new(2, Duration::from_secs(5)).expect("bounded bootstrap options")
}

fn attach_options() -> AttachOptions {
    AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
        .expect("bounded attach options")
}

#[test]
#[ignore = "requires scripts/test-m14-join-lifecycle.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered bootstrap/live/crash/rebuild lifecycle proof"
)]
fn two_source_join_uses_one_snapshot_continuation_and_rebuild_lifecycle() {
    let database_url = support::required("SHIBA_M14_JOIN_LIFECYCLE_DATABASE_URL");
    let replication_url = support::required("SHIBA_M14_JOIN_LIFECYCLE_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect test database");
    let fixture = support::install(&mut admin, PUBLICATION);
    support::register_join_graph(&mut admin, &fixture);
    support::assert_registered(&mut admin, &fixture);

    let mut bootstrap = BootstrapSession::begin(
        &database_url,
        &replication_url,
        BootstrapSpec {
            graph_id: GraphId::new(GRAPH_ID).expect("graph ID"),
            bootstrap_id: BootstrapId::new(1).expect("bootstrap ID"),
            publication_oid: fixture.publication_oid,
            slot_name: OLD_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(1).expect("generation"),
        },
        bootstrap_options(),
    )
    .expect("export one graph snapshot");
    support::assert_building(&mut admin);

    admin
        .batch_execute(
            "BEGIN;
             UPDATE right_source.events SET payload=110 WHERE id=10;
             INSERT INTO left_source.events VALUES (4,20);
             DELETE FROM left_source.events WHERE id=2;
             COMMIT;",
        )
        .expect("commit both-source-era WAL during snapshot");
    support::scan_all(&mut bootstrap, &mut admin);
    support::assert_snapshot_rows(&mut admin);
    support::assert_building(&mut admin);

    let mut catchup = bootstrap.into_catchup().expect("enter initial catch-up");
    assert_eq!(
        catchup.catch_up_next().expect("apply concurrent WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        catchup.catch_up_next().expect("activate initial graph"),
        BootstrapCatchupProgress::Active
    );
    support::assert_continuations(&mut admin, 1, 1);
    support::assert_oracle(&mut admin);
    let mut live = catchup.into_live().expect("enter governed live graph");

    admin
        .batch_execute(
            "BEGIN;
             UPDATE right_source.events SET payload=200 WHERE id=20;
             INSERT INTO left_source.events VALUES (5,20);
             COMMIT;",
        )
        .expect("commit first live graph transaction");
    let first = live
        .receive_and_apply_one()
        .expect("receive and durably apply first live transaction");
    assert_eq!(first.outcome(), ProcessOutcome::Applied);
    support::assert_continuations(&mut admin, 1, 2);
    support::assert_oracle(&mut admin);
    live.acknowledge(&first)
        .expect("explicitly ACK durable Apply");
    support::assert_feedback(&mut admin, OLD_SLOT, first.end_lsn());

    admin
        .batch_execute(
            "BEGIN;
             DELETE FROM left_source.events WHERE id=1;
             UPDATE right_source.events SET payload=210 WHERE id=20;
             COMMIT;",
        )
        .expect("commit crash-window transaction");
    let pending = live
        .receive_and_apply_one()
        .expect("durably Apply without feedback");
    assert_eq!(pending.outcome(), ProcessOutcome::Applied);
    support::assert_continuations(&mut admin, 1, 3);
    let pending_end = pending.end_lsn();
    let feedback_before_crash = support::slot_lsn(&mut admin, OLD_SLOT);
    assert!(feedback_before_crash < pending_end);
    support::assert_oracle(&mut admin);
    drop(live); // deterministic process crash after commit and before ACK

    let mut replay = GovernedGraphSession::attach(
        &database_url,
        &replication_url,
        GraphId::new(GRAPH_ID).expect("graph ID"),
        SlotGeneration::new(1).expect("generation"),
        attach_options(),
    )
    .expect("restart governed graph receiver");
    let replayed = replay
        .receive_and_apply_one()
        .expect("receive exact durable replay");
    assert_eq!(replayed.outcome(), ProcessOutcome::AlreadyApplied);
    assert_eq!(replayed.end_lsn(), pending_end);
    support::assert_continuations(&mut admin, 1, 3);
    replay.acknowledge(&replayed).expect("ACK exact replay");
    support::assert_feedback(&mut admin, OLD_SLOT, pending_end);
    replay
        .detach()
        .expect("detach old generation before rebuild");

    let spec = support::same_binding_rebuild(&mut admin, &fixture, OLD_SLOT, NEW_SLOT);
    let prepared =
        PreparedRebuild::prepare(&database_url, &replication_url, &spec, bootstrap_options())
            .expect("prepare non-pristine two-source rebuild");
    support::assert_building(&mut admin);
    let mut rebuilt = prepared
        .into_bootstrap()
        .expect("export replacement graph snapshot");
    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO right_source.events VALUES (30,300);
             UPDATE left_source.events SET right_key=30 WHERE id=4;
             DELETE FROM left_source.events WHERE id=5;
             COMMIT;",
        )
        .expect("commit rebuild catch-up WAL");
    support::scan_all(&mut rebuilt, &mut admin);
    support::assert_building(&mut admin);
    let mut rebuilt_catchup = rebuilt.into_catchup().expect("enter rebuild catch-up");
    assert_eq!(
        rebuilt_catchup
            .catch_up_next()
            .expect("apply rebuild concurrent WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        rebuilt_catchup
            .catch_up_next()
            .expect("activate rebuilt graph"),
        BootstrapCatchupProgress::Active
    );
    support::assert_oracle(&mut admin);
    support::assert_generation(&mut admin, 2, NEW_SLOT);
    support::assert_continuations(&mut admin, 2, 1);

    let mut rebuilt_live = rebuilt_catchup
        .into_live()
        .expect("enter rebuilt governed live graph");
    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO left_source.events VALUES (6,30);
             UPDATE right_source.events SET payload=310 WHERE id=30;
             COMMIT;",
        )
        .expect("commit post-rebuild two-source live transaction");
    let final_token = rebuilt_live
        .receive_and_apply_one()
        .expect("apply post-rebuild live transaction");
    assert_eq!(final_token.outcome(), ProcessOutcome::Applied);
    support::assert_continuations(&mut admin, 2, 2);
    support::assert_oracle(&mut admin);
    rebuilt_live
        .acknowledge(&final_token)
        .expect("ACK post-rebuild durable transaction");
    support::assert_feedback(&mut admin, NEW_SLOT, final_token.end_lsn());
    support::assert_generation(&mut admin, 2, NEW_SLOT);
    rebuilt_live.detach().expect("detach rebuilt graph");
}
