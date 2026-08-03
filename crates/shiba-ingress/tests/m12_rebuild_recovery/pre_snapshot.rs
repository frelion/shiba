use std::{
    sync::{Arc, Barrier},
    thread,
};

use postgres::Client;
use shiba_ingress::{BootstrapSession, PreparedRebuild};

use crate::support::{self, Attempt, RebuildFixture};

pub(crate) fn prove_windows(
    database_url: &str,
    replication_url: &str,
    admin: &mut Client,
    fixture: &RebuildFixture,
) -> PreparedRebuild {
    let before = support::evidence(admin);
    let mut invalid = fixture.spec();
    invalid.target.slot_generation = shiba_protocol::SlotGeneration::new(5).unwrap();
    assert!(
        PreparedRebuild::prepare(database_url, replication_url, invalid, support::options())
            .is_err()
    );
    assert_eq!(
        support::evidence(admin),
        before,
        "pre-intent failure changed authority"
    );

    admin
        .batch_execute("GRANT SELECT ON target.events TO shiba_m12_recovery_replication")
        .expect("grant replication role target read capability");
    PreparedRebuild::prepare(
        database_url,
        replication_url,
        fixture.spec(),
        support::options(),
    )
    .expect("commit rebuild intent")
    .detach()
    .expect("release intent worker");
    support::assert_building(admin);

    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let apply = database_url.to_owned();
        let replication = replication_url.to_owned();
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            PreparedRebuild::resume_prepared(
                &apply,
                &replication,
                shiba_protocol::SourceId::new(1).unwrap(),
                shiba_protocol::BootstrapId::new(2).unwrap(),
                shiba_protocol::SlotGeneration::new(3).unwrap(),
                support::options(),
            )
        }));
    }
    barrier.wait();
    let mut winner = None;
    for prepared in workers
        .into_iter()
        .filter_map(|worker| worker.join().expect("worker").ok())
    {
        assert!(winner.is_none(), "two rebuild recovery workers won");
        winner = Some(prepared);
    }
    winner.expect("one rebuild recovery worker must win")
}

#[allow(clippy::too_many_lines, reason = "ordered physical slot crash windows")]
pub(crate) fn prove_slot_windows(
    database_url: &str,
    replication_url: &str,
    admin: &mut Client,
    prepared: PreparedRebuild,
) -> shiba_ingress::BootstrapSession {
    prepared.detach().expect("release competition winner");
    let stable = support::evidence(admin);
    admin
        .query_one(
            "SELECT slot_name FROM pg_catalog.pg_create_physical_replication_slot($1)",
            &[&support::TARGET_SLOT],
        )
        .expect("preoccupy target with foreign physical slot");
    let foreign_target = PreparedRebuild::resume_prepared(
        database_url,
        replication_url,
        shiba_protocol::SourceId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(2).unwrap(),
        shiba_protocol::SlotGeneration::new(3).unwrap(),
        support::options(),
    );
    assert!(foreign_target.is_err());
    admin
        .execute(
            "SELECT pg_drop_replication_slot($1)",
            &[&support::TARGET_SLOT],
        )
        .expect("drop foreign target slot");
    assert_eq!(support::evidence(admin), stable);

    admin
        .execute("SELECT pg_drop_replication_slot($1)", &[&support::OLD_SLOT])
        .expect("replace old slot shape for drift proof");
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_physical_replication_slot($1)",
            &[&support::OLD_SLOT],
        )
        .expect("create foreign old physical slot");
    let foreign_old = PreparedRebuild::resume_prepared(
        database_url,
        replication_url,
        shiba_protocol::SourceId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(2).unwrap(),
        shiba_protocol::SlotGeneration::new(3).unwrap(),
        support::options(),
    )
    .expect("resume before foreign old check");
    assert!(foreign_old.into_bootstrap().is_err());
    admin
        .execute("SELECT pg_drop_replication_slot($1)", &[&support::OLD_SLOT])
        .expect("drop foreign old slot");
    admin
        .query_one(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&support::OLD_SLOT],
        )
        .expect("restore test-owned old pgoutput shape");

    admin
        .batch_execute(
            "CREATE FUNCTION public.stop_after_old_drop() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'm12 stop after old drop'; END $$;
         CREATE TRIGGER stop_after_old_drop BEFORE UPDATE OF phase
         ON shiba_internal.source_bootstrap FOR EACH ROW
         EXECUTE FUNCTION public.stop_after_old_drop();",
        )
        .expect("install old-drop barrier");
    let old_drop = PreparedRebuild::resume_prepared(
        database_url,
        replication_url,
        shiba_protocol::SourceId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(2).unwrap(),
        shiba_protocol::SlotGeneration::new(3).unwrap(),
        support::options(),
    )
    .expect("resume old-drop window");
    assert!(old_drop.into_bootstrap().is_err());
    admin
        .batch_execute(
            "DROP TRIGGER stop_after_old_drop ON shiba_internal.source_bootstrap;
         DROP FUNCTION public.stop_after_old_drop();",
        )
        .expect("remove old-drop barrier");
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&support::OLD_SLOT],
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    support::assert_building(admin);

    admin.batch_execute(
        "CREATE FUNCTION public.stop_before_target_slot() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN EXECUTE 'ALTER ROLE shiba_m12_recovery_replication NOREPLICATION'; RETURN NEW; END $$;
         CREATE TRIGGER stop_before_target_slot AFTER UPDATE OF phase
         ON shiba_internal.source_bootstrap FOR EACH ROW
         WHEN (NEW.phase = 'creating') EXECUTE FUNCTION public.stop_before_target_slot();"
    ).expect("install creating-before-slot barrier");
    let creating = PreparedRebuild::resume_prepared(
        database_url,
        replication_url,
        shiba_protocol::SourceId::new(1).unwrap(),
        shiba_protocol::BootstrapId::new(2).unwrap(),
        shiba_protocol::SlotGeneration::new(3).unwrap(),
        support::options(),
    )
    .expect("resume creating window");
    assert!(creating.into_bootstrap().is_err());
    admin
        .batch_execute(
            "ALTER ROLE shiba_m12_recovery_replication REPLICATION;
         DROP TRIGGER stop_before_target_slot ON shiba_internal.source_bootstrap;
         DROP FUNCTION public.stop_before_target_slot();",
        )
        .expect("restore replication capability");
    let phase: String = admin
        .query_one(
            "SELECT phase FROM shiba_internal.source_bootstrap WHERE source_id=1",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(phase, "creating");
    support::assert_building(admin);

    let publication_value: i64 = admin
        .query_one(
            "SELECT publication_objid::bigint
             FROM shiba_internal.source_ingress_config WHERE source_id=1",
            &[],
        )
        .unwrap()
        .get(0);
    let failed = Attempt {
        bootstrap: 2,
        generation: 3,
        slot: support::TARGET_SLOT,
        publication: u32::try_from(publication_value).expect("publication OID"),
    };
    let next = Attempt {
        bootstrap: 3,
        generation: 4,
        slot: support::RECOVERY_SLOTS[0],
        publication: failed.publication,
    };
    BootstrapSession::restart_abandoned(
        database_url,
        replication_url,
        &failed.spec(),
        next.spec(),
        support::options(),
    )
    .expect("recover creating target-absent attempt with exact successor")
}
