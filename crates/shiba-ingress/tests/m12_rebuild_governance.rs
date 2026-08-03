#![allow(clippy::duplicate_mod)]

use postgres::{Client, NoTls};

#[path = "m12_rebuild_governance/competition.rs"]
mod competition;
#[path = "m12_rebuild_governance/ddl.rs"]
mod ddl;
#[path = "m12_rebuild_governance/roles.rs"]
mod roles;
#[path = "m12_rebuild_governance/support.rs"]
mod support;

/// M12.5 proves that rebuild admission remains identity-based, source-scoped,
/// and usable by split production roles.  Each ordered scenario tears down its
/// own test-owned catalog and slots before the next one starts.
#[test]
#[ignore = "requires scripts/test-m12-rebuild-governance.sh"]
fn rebuild_governance_is_exact_scoped_and_least_privilege() {
    let database_url = support::required("SHIBA_M12_GOVERNANCE_DATABASE_URL");
    let replication_url = support::required("SHIBA_M12_GOVERNANCE_REPLICATION_URL");

    ddl::prove_relation_replacement_is_not_adopted(&database_url, &replication_url);
    let mut cleanup = Client::connect(&database_url, NoTls).expect("connect DDL cleanup");
    support::teardown(&mut cleanup);

    ddl::prove_publication_drift_requires_explicit_new_admission(&database_url, &replication_url);
    let mut cleanup = Client::connect(&database_url, NoTls).expect("connect publication cleanup");
    support::teardown(&mut cleanup);

    ddl::prove_identity_shape_and_operator_plan(&database_url, &replication_url);
    let mut cleanup = Client::connect(&database_url, NoTls).expect("connect identity cleanup");
    support::teardown(&mut cleanup);

    competition::prove_same_source_exclusion_and_other_source_progress(
        &database_url,
        &replication_url,
    );
    let mut cleanup = Client::connect(&database_url, NoTls).expect("connect competition cleanup");
    support::teardown(&mut cleanup);

    roles::prove_rebuild_roles_and_fail_closed_permission_loss(&database_url, &replication_url);
    let mut cleanup = Client::connect(&database_url, NoTls).expect("connect role cleanup");
    support::teardown(&mut cleanup);
}
