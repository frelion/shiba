use std::time::Duration;

use shiba_ingress::{
    AttachOptions, BootstrapCatchupProgress, GovernedGraphSession, PreparedRebuild, ReplicationMode,
};
use shiba_protocol::{
    GraphId, GraphTransactionId, IngressTransactionId, InputSequence, PostgresLsn, SlotGeneration,
    SourceId,
};
use shiba_runtime::{
    GraphSourceChange, GraphTransaction, M2Error, ProcessOutcome, SourceChange, SourceInsert,
    SourcePayload, process,
};

#[path = "m12_rebuild_snapshot_live/support.rs"]
mod support;

use support::{RebuildFixture, TARGET_SLOT};

fn attach_options() -> AttachOptions {
    AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
        .expect("bounded attach options")
}

fn stale_generation_input() -> GraphTransaction {
    let identity = GraphTransactionId::new(
        GraphId::new(1).expect("graph ID"),
        SlotGeneration::new(2).expect("retired generation"),
        PostgresLsn::from_u64(0x50_0000),
        IngressTransactionId::new(502).expect("ingress transaction ID"),
    )
    .expect("nonzero stale transaction identity");
    GraphTransaction::new(
        identity,
        vec![GraphSourceChange {
            source_id: SourceId::new(1).expect("source ID"),
            change: SourceChange::Insert(SourceInsert::with_payload(
                InputSequence::new(1).expect("input sequence"),
                502,
                SourcePayload::Null,
            )),
        }],
    )
    .expect("valid stale nullable-int8 INSERT")
}

fn premature_target_input() -> GraphTransaction {
    let identity = GraphTransactionId::new(
        GraphId::new(1).expect("graph ID"),
        SlotGeneration::new(3).expect("target generation"),
        PostgresLsn::from_u64(0x50_0100),
        IngressTransactionId::new(503).expect("ingress transaction ID"),
    )
    .expect("nonzero target transaction identity");
    GraphTransaction::new(
        identity,
        vec![GraphSourceChange {
            source_id: SourceId::new(1).expect("source ID"),
            change: SourceChange::Insert(SourceInsert::with_payload(
                InputSequence::new(1).expect("input sequence"),
                503,
                SourcePayload::Null,
            )),
        }],
    )
    .expect("valid premature target nullable-int8 INSERT")
}

#[test]
#[ignore = "requires scripts/test-m12-rebuild-snapshot-live.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered rebuild snapshot-to-live proof"
)]
fn active_source_rebuilds_through_one_target_authority() {
    let database_url = support::required("SHIBA_M12_REBUILD_DATABASE_URL");
    let replication_url = support::required("SHIBA_M12_REBUILD_REPLICATION_URL");
    let (mut admin, active) = support::establish_active_source(&database_url, &replication_url);
    let fixture = RebuildFixture::install(&mut admin, active.publication_oid);
    admin
        .batch_execute(
            "INSERT INTO target.events VALUES (12, 30);
             UPDATE source.events SET payload = 8 WHERE id = 5;",
        )
        .expect("make old and target authorities non-pristine");

    let mut old_live = GovernedGraphSession::attach(
        &database_url,
        &replication_url,
        GraphId::new(1).expect("graph ID"),
        SlotGeneration::new(2).expect("old generation"),
        attach_options(),
    )
    .expect("attach old generation");
    let stale_durable = old_live
        .receive_and_apply_one()
        .expect("durably Apply old-generation transaction without feedback");
    assert_eq!(stale_durable.outcome(), ProcessOutcome::Applied);
    assert_eq!(
        support::continuation_generations(&mut admin).last(),
        Some(&2)
    );
    old_live
        .detach()
        .expect("leave old slot inactive with stale token");

    let prepared = PreparedRebuild::prepare(
        &database_url,
        &replication_url,
        &fixture.spec(),
        support::rebuild_options(),
    )
    .expect("atomically install sole target building authority");
    support::assert_building(&mut admin);
    assert!(support::continuation_generations(&mut admin).is_empty());
    support::assert_slots(&mut admin, true, false);
    let prepared_identity = support::catalog_identity(&mut admin);
    let stale_input = stale_generation_input();
    let prepared_before_stale = support::rebuild_state_snapshot(&mut admin);
    assert!(matches!(
        process(&mut admin, &stale_input),
        Err(M2Error::SlotGenerationMismatch)
    ));
    assert_eq!(
        support::rebuild_state_snapshot(&mut admin),
        prepared_before_stale,
        "retired generation cannot mutate target building authority"
    );
    support::assert_building(&mut admin);
    assert!(support::continuation_generations(&mut admin).is_empty());
    let premature_input = premature_target_input();
    assert!(matches!(
        process(&mut admin, &premature_input),
        Err(M2Error::InvalidBootstrapPhase)
    ));
    assert_eq!(
        support::rebuild_state_snapshot(&mut admin),
        prepared_before_stale,
        "target generation cannot execute before snapshot bootstrap begins"
    );
    support::assert_building(&mut admin);
    assert!(support::continuation_generations(&mut admin).is_empty());

    assert!(
        GovernedGraphSession::attach(
            &database_url,
            &replication_url,
            GraphId::new(1).expect("graph ID"),
            SlotGeneration::new(2).expect("retired generation"),
            attach_options(),
        )
        .is_err(),
        "old generation cannot reattach after destructive prepare"
    );

    let mut bootstrap = prepared
        .into_bootstrap()
        .expect("retire exact old slot and export target snapshot");
    support::assert_slots(&mut admin, false, true);
    assert_eq!(support::catalog_identity(&mut admin), prepared_identity);
    support::assert_building(&mut admin);

    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO target.events VALUES (13, 7);
             UPDATE target.events SET payload = 110 WHERE id = 10;
             DELETE FROM target.events WHERE id = 12;
             COMMIT;",
        )
        .expect("commit target I/U/D during exported snapshot scan");

    assert_eq!(support::scan_all(&mut bootstrap, &mut admin), 3);
    assert_eq!(
        support::source_rows(&mut admin),
        vec![(10, Some(100)), (11, None), (12, Some(30))]
    );
    assert!(support::continuation_generations(&mut admin).is_empty());
    assert_eq!(support::catalog_identity(&mut admin), prepared_identity);

    let mut catchup = bootstrap
        .into_catchup()
        .expect("enter exact target catch-up");
    assert_eq!(
        catchup
            .catch_up_next()
            .expect("Apply target WAL exactly once"),
        BootstrapCatchupProgress::TransactionApplied
    );
    support::assert_building(&mut admin);
    assert_eq!(
        support::source_rows(&mut admin),
        vec![(10, Some(110)), (11, None), (13, Some(7))]
    );
    assert_eq!(support::continuation_generations(&mut admin), vec![3]);
    assert_eq!(support::catalog_identity(&mut admin), prepared_identity);

    assert_eq!(
        catchup
            .catch_up_next()
            .expect("activate exact rebuild fence"),
        BootstrapCatchupProgress::Active
    );
    support::assert_active(&mut admin, 3, 117);
    support::assert_oracle(&mut admin, 3, 117);
    assert_eq!(support::catalog_identity(&mut admin), prepared_identity);
    let lifecycle = admin
        .query_one(
            "SELECT phase, bootstrap_id, slot_generation
             FROM shiba_internal.graph_bootstrap WHERE graph_id = 1",
            &[],
        )
        .expect("query activated target lifecycle");
    assert_eq!(lifecycle.get::<_, &str>(0), "active");
    assert_eq!(lifecycle.get::<_, i64>(1), 2);
    assert_eq!(lifecycle.get::<_, i64>(2), 3);
    let active_before_stale = support::rebuild_state_snapshot(&mut admin);
    assert!(matches!(
        process(&mut admin, &stale_input),
        Err(M2Error::SlotGenerationMismatch)
    ));
    assert_eq!(
        support::rebuild_state_snapshot(&mut admin),
        active_before_stale,
        "retired generation remains fenced after activation"
    );
    support::assert_active(&mut admin, 3, 117);

    let mut live = catchup
        .into_live()
        .expect("handoff to ordinary M10 live ingress");
    assert!(
        live.acknowledge(&stale_durable).is_err(),
        "foreign pre-rebuild durable token cannot ACK the new receiver"
    );
    admin
        .batch_execute("INSERT INTO target.events VALUES (14, -2)")
        .expect("commit ordinary target live DML after cutover");
    let durable = live
        .receive_and_apply_one()
        .expect("Apply post-cutover live transaction");
    assert_eq!(durable.outcome(), ProcessOutcome::Applied);
    live.acknowledge(&durable)
        .expect("ACK exact new-generation durable token");
    support::wait_for_slot_lsn(&mut admin, TARGET_SLOT, durable.end_lsn());
    support::assert_active(&mut admin, 4, 115);
    support::assert_oracle(&mut admin, 4, 115);
    assert_eq!(support::continuation_generations(&mut admin), vec![3, 3]);
    assert_eq!(support::catalog_identity(&mut admin), prepared_identity);
    support::assert_slots(&mut admin, false, true);
    live.detach().expect("detach rebuilt live session");
}
