use std::{
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use shiba_ingress::{AttachOptions, GovernedGraphSession, PreparedRebuild, ReplicationMode};
use shiba_protocol::{GraphId, SlotGeneration};

#[path = "m12_rebuild_admission/support.rs"]
mod support;

use support::{
    RebuildFixture, TARGET_SLOT, full_authority_snapshot, grant_prepare, options, required,
};

#[test]
#[ignore = "requires scripts/test-m12-rebuild-admission.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered destructive-boundary proof"
)]
fn active_source_rebuild_admission_is_atomic_and_single_winner() {
    let database_url = required("SHIBA_M12_REBUILD_DATABASE_URL");
    let replication_url = required("SHIBA_M12_REBUILD_REPLICATION_URL");
    let (mut admin, active) = support::establish_active_source(&database_url, &replication_url);
    let fixture = RebuildFixture::install(&mut admin, active.publication_oid);

    let active_before = full_authority_snapshot(&mut admin);
    let active_result = admin
        .query(
            "SELECT result_id, result_status, value_bigint
             FROM shiba.graph_result WHERE graph_id = 1 ORDER BY result_id",
            &[],
        )
        .expect("read active public result");
    assert_eq!(active_result[0].get::<_, &str>(1), "active");
    assert_eq!(active_result[0].get::<_, Option<i64>>(2), Some(4));
    assert_eq!(active_result[1].get::<_, Option<i64>>(2), Some(32));

    assert!(
        PreparedRebuild::prepare(
            &database_url,
            &replication_url,
            &fixture.spec_with(fixture.old, fixture.bad_target, 1, 2, 3),
            options(),
        )
        .is_err(),
        "non-int8 target shape must fail before destructive prepare"
    );
    assert_eq!(full_authority_snapshot(&mut admin), active_before);

    admin
        .batch_execute(
            "CREATE ROLE shiba_m12_denied LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;",
        )
        .expect("create target-permission negative role");
    grant_prepare(&mut admin, "shiba_m12_denied");
    let denied_url = format!("{database_url} user=shiba_m12_denied");
    assert!(
        PreparedRebuild::prepare(&denied_url, &replication_url, &fixture.spec(), options(),)
            .is_err(),
        "session user without target SELECT must fail before mutation"
    );
    assert_eq!(full_authority_snapshot(&mut admin), active_before);

    assert!(
        PreparedRebuild::prepare(
            &database_url,
            &replication_url,
            &fixture.spec_with(fixture.old, fixture.target, 99, 2, 3),
            options(),
        )
        .is_err(),
        "stale bootstrap identity must fail exact CAS"
    );
    assert_eq!(full_authority_snapshot(&mut admin), active_before);
    assert!(
        PreparedRebuild::prepare(
            &database_url,
            &replication_url,
            &fixture.spec_with(fixture.old, fixture.target, 1, 2, 4),
            options(),
        )
        .is_err(),
        "generation must advance exactly once"
    );
    assert_eq!(full_authority_snapshot(&mut admin), active_before);

    let active_receiver = GovernedGraphSession::attach(
        &database_url,
        &replication_url,
        GraphId::new(1).expect("graph ID"),
        SlotGeneration::new(2).expect("active old generation"),
        AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
            .expect("attach options"),
    )
    .expect("hold old slot active");
    let while_active = full_authority_snapshot(&mut admin);
    assert!(
        PreparedRebuild::prepare(&database_url, &replication_url, &fixture.spec(), options(),)
            .is_err(),
        "rebuild must not race an active old receiver"
    );
    assert_eq!(full_authority_snapshot(&mut admin), while_active);
    active_receiver.detach().expect("detach old receiver");
    assert_eq!(full_authority_snapshot(&mut admin), active_before);

    admin
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&TARGET_SLOT],
        )
        .expect("preoccupy target slot");
    let preoccupied = full_authority_snapshot(&mut admin);
    assert!(
        PreparedRebuild::prepare(&database_url, &replication_url, &fixture.spec(), options(),)
            .is_err(),
        "preoccupied target slot must not be adopted"
    );
    assert_eq!(full_authority_snapshot(&mut admin), preoccupied);
    admin
        .execute(
            "SELECT pg_catalog.pg_drop_replication_slot($1)",
            &[&TARGET_SLOT],
        )
        .expect("remove test-owned target preoccupation");
    assert_eq!(full_authority_snapshot(&mut admin), active_before);

    let mut foreign_old = fixture.old;
    foreign_old.relation = fixture.target.relation;
    assert!(
        PreparedRebuild::prepare(
            &database_url,
            &replication_url,
            &fixture.spec_with(foreign_old, fixture.target, 1, 2, 3),
            options(),
        )
        .is_err(),
        "foreign expected binding must fail exact old identity CAS"
    );
    assert_eq!(full_authority_snapshot(&mut admin), active_before);
    let spec_payload: Vec<u8> = admin
        .query_one(
            "SELECT spec_payload FROM shiba_internal.graph_definition WHERE graph_id = 1",
            &[],
        )
        .expect("read exact ProjectRows plan digest")
        .get(0);
    admin
        .execute(
            "UPDATE shiba_internal.graph_definition SET spec_payload = decode('00', 'hex')
             WHERE graph_id = 1",
            &[],
        )
        .expect("inject compiled plan drift");
    assert!(
        PreparedRebuild::prepare(&database_url, &replication_url, &fixture.spec(), options())
            .is_err(),
        "corrupt generic plan must fail before destructive prepare"
    );
    admin
        .execute(
            "UPDATE shiba_internal.graph_definition SET spec_payload = $1 WHERE graph_id = 1",
            &[&spec_payload],
        )
        .expect("restore exact ProjectRows plan digest");
    assert_eq!(full_authority_snapshot(&mut admin), active_before);

    admin
        .batch_execute("ALTER TABLE source.events RENAME TO retired_events")
        .expect("create old-generation invalidation before destructive prepare");
    let invalidations = admin
        .query_one(
            "SELECT
                (SELECT count(*) FROM shiba_internal.source_invalidation WHERE source_id = 1),
                (SELECT count(*) FROM shiba_internal.graph_ingress_invalidation WHERE graph_id = 1)",
            &[],
        )
        .expect("read old-generation invalidation");
    assert!(invalidations.get::<_, i64>(0) > 0);
    assert_eq!(invalidations.get::<_, i64>(1), 0);

    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let barrier = Arc::clone(&barrier);
        let apply = database_url.clone();
        let replication = replication_url.clone();
        let spec = fixture.spec();
        workers.push(thread::spawn(move || {
            barrier.wait();
            let result = PreparedRebuild::prepare(&apply, &replication, &spec, options());
            match result {
                Ok(prepared) => {
                    assert_eq!(prepared.graph_id(), GraphId::new(1).expect("graph ID"));
                    assert_eq!(prepared.target_generation().get(), 3);
                    assert_eq!(prepared.target_bootstrap_id().get(), 2);
                    prepared.detach().expect("detach prepared admission");
                    true
                }
                Err(error) => {
                    eprintln!("concurrent rebuild admission rejected: {error:?}");
                    false
                }
            }
        }));
    }
    barrier.wait();
    let winners = workers
        .into_iter()
        .map(|worker| worker.join().expect("prepare worker did not panic"))
        .filter(|won| *won)
        .count();
    assert_eq!(winners, 1, "exactly one concurrent rebuild request may win");

    let authority = admin
        .query_one(
            "SELECT
                    (SELECT address_objid::bigint
                     FROM shiba_internal.source_binding
                     WHERE source_id = 1 AND binding_kind = 'relation'),
                    (SELECT indexrelid::bigint
                     FROM pg_catalog.pg_index
                     WHERE indrelid = $1::bigint::oid AND indisprimary),
                    config.publication_objid::bigint, config.slot_name::text,
                    config.slot_generation,
                    bootstrap.bootstrap_id, bootstrap.phase,
                    bootstrap.retired_bootstrap_id,
                    bootstrap.retired_slot_name::text,
                    bootstrap.retired_slot_generation
             FROM shiba_internal.graph_ingress_config AS config
             JOIN shiba_internal.graph_bootstrap AS bootstrap USING (graph_id)
             WHERE config.graph_id = 1",
            &[&i64::from(fixture.target.relation)],
        )
        .expect("read sole target building authority");
    assert_eq!(
        authority.get::<_, i64>(0),
        i64::from(fixture.target.relation)
    );
    assert_eq!(
        authority.get::<_, i64>(1),
        i64::from(fixture.target.identity_index)
    );
    assert_eq!(
        authority.get::<_, i64>(2),
        i64::from(fixture.target.publication)
    );
    assert_eq!(authority.get::<_, &str>(3), TARGET_SLOT);
    assert_eq!(authority.get::<_, i64>(4), 3);
    assert_eq!(authority.get::<_, i64>(5), 2);
    assert_eq!(authority.get::<_, &str>(6), "rebuild_prepared");
    assert_eq!(authority.get::<_, Option<i64>>(7), Some(1));
    assert_eq!(authority.get::<_, Option<&str>>(8), Some(support::OLD_SLOT));
    assert_eq!(authority.get::<_, Option<i64>>(9), Some(2));
    let binding_shape = admin
        .query_one(
            "SELECT count(*), count(*) FILTER (WHERE binding_kind = 'identity_index')
             FROM shiba_internal.source_binding WHERE source_id = 1",
            &[],
        )
        .expect("read sole M10/M11-compatible target binding set");
    assert_eq!(binding_shape.get::<_, i64>(0), 4);
    assert_eq!(binding_shape.get::<_, i64>(1), 1);
    let prepared_identity: i64 = admin
        .query_one(
            "SELECT address_objid::bigint
             FROM shiba_internal.source_binding
             WHERE source_id = 1 AND binding_kind = 'identity_index'",
            &[],
        )
        .expect("read exact prepared replica identity authority")
        .get(0);
    assert_eq!(prepared_identity, i64::from(fixture.target.identity_index));

    let retired = admin
        .query_one(
            "SELECT
                (SELECT count(*) FROM shiba_internal.source_row_state WHERE source_id = 1),
                (SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id = 1),
                (SELECT count(*) FROM shiba_internal.source_invalidation WHERE source_id = 1),
                (SELECT count(*) FROM shiba_internal.graph_ingress_invalidation WHERE graph_id = 1)",
            &[],
        )
        .expect("prove old generation retirement");
    for column in 0..4 {
        assert_eq!(retired.get::<_, i64>(column), 0);
    }
    assert_eq!(
        admin
            .query(
                "SELECT encode(state_payload, 'hex')
                 FROM shiba_internal.graph_node_state WHERE graph_id = 1 ORDER BY node_id",
                &[],
            )
            .expect("read reset private state")
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>(),
        Vec::<String>::new()
    );
    let results = admin
        .query(
            "SELECT result_status, value_bigint FROM shiba.graph_result
             WHERE graph_id = 1 ORDER BY result_id",
            &[],
        )
        .expect("read hidden public results");
    assert!(results.iter().all(|row| {
        row.get::<_, &str>(0) == "building" && row.get::<_, Option<i64>>(1).is_none()
    }));
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&TARGET_SLOT],
            )
            .expect("prepare must not create target slot")
            .get::<_, i64>(0),
        0
    );
    let old_slot = admin
        .query_one(
            "SELECT active FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&support::OLD_SLOT],
        )
        .expect("prepare leaves inactive old slot for recoverable cleanup");
    assert!(!old_slot.get::<_, bool>(0));

    assert!(
        GovernedGraphSession::attach(
            &database_url,
            &replication_url,
            GraphId::new(1).expect("graph ID"),
            SlotGeneration::new(2).expect("retired generation"),
            AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
                .expect("attach options"),
        )
        .is_err(),
        "retired worker generation cannot receive, Apply, or ACK"
    );
}
