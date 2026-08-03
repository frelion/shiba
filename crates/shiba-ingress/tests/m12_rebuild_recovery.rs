#![allow(clippy::duplicate_mod)]

use postgres::{Client, NoTls};

#[path = "m12_rebuild_recovery/catchup.rs"]
mod catchup;
#[allow(dead_code)]
#[path = "support/mod.rs"]
mod pg_support;
#[path = "m12_rebuild_recovery/pre_snapshot.rs"]
mod pre_snapshot;
#[path = "m12_rebuild_recovery/scan.rs"]
mod scan;
#[path = "m12_rebuild_recovery/support.rs"]
mod support;

#[test]
#[ignore = "requires scripts/test-m12-rebuild-recovery.sh"]
fn active_rebuild_recovers_along_one_forward_path() {
    let database_url = support::required("SHIBA_M12_RECOVERY_DATABASE_URL");
    let replication_url = support::required("SHIBA_M12_RECOVERY_REPLICATION_URL");
    let control_replication_url = support::required("SHIBA_M12_RECOVERY_CONTROL_REPLICATION_URL");
    let (mut admin, active) = support::establish_active_source(&database_url, &replication_url);
    let fixture = support::RebuildFixture::install(&mut admin, active.publication_oid);
    support::extend_target(&mut admin);
    let prepared = pre_snapshot::prove_windows(
        &database_url,
        &control_replication_url,
        &mut admin,
        &fixture,
    );
    let bootstrap = pre_snapshot::prove_slot_windows(
        &database_url,
        &control_replication_url,
        &mut admin,
        prepared,
    );
    let attempt = scan::prove_snapshot_restarts(
        &database_url,
        &control_replication_url,
        &mut admin,
        bootstrap,
        fixture.target.publication,
    );
    drop(admin);
    support::restart_postgres("immediate");
    let mut admin = Client::connect(&database_url, NoTls).expect("reconnect after scan restart");
    catchup::prove_catchup_activation_feedback(
        &database_url,
        &control_replication_url,
        &mut admin,
        attempt,
    );
}
