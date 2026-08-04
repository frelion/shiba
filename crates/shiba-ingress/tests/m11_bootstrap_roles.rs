use std::time::Duration;

use postgres::{Client, NoTls};
use shiba_ingress::{
    BootstrapCatchupProgress, BootstrapOptions, BootstrapSession, SnapshotProgress,
};

#[path = "m11_bootstrap_roles/support.rs"]
mod roles_support;

use roles_support::{
    CONTROL_ROLE, PUBLICATION, READER_ROLE, RECEIVER_ROLE, SLOT, SWAPPED_PUBLICATION, SWAPPED_SLOT,
    as_role, assert_no_apply, assert_results, bootstrap_spec, install_fixture,
    load_publication_oid, required,
};

#[test]
#[ignore = "requires scripts/test-m11-bootstrap-roles.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered split-role privilege proof"
)]
fn bootstrap_runs_without_superuser_and_permission_loss_fails_closed() {
    let database_url = required("SHIBA_M11_ROLES_DATABASE_URL");
    let replication_url = required("SHIBA_M11_ROLES_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect admin");
    install_fixture(&mut admin);
    let role_shape = admin
        .query(
            "SELECT rolname, rolsuper, rolreplication, rolcreatedb, rolcreaterole,
                    EXISTS (SELECT 1 FROM pg_auth_members WHERE member = role.oid)
             FROM pg_roles AS role
             WHERE rolname IN ($1, $2, $3) ORDER BY rolname",
            &[&CONTROL_ROLE, &RECEIVER_ROLE, &READER_ROLE],
        )
        .expect("audit role attributes and inheritance");
    assert_eq!(role_shape.len(), 3);
    for role in role_shape {
        let name: &str = role.get(0);
        assert!(!role.get::<_, bool>(1), "{name} must not be superuser");
        assert_eq!(
            role.get::<_, bool>(2),
            name == RECEIVER_ROLE,
            "only the transport role may have REPLICATION"
        );
        assert!(!role.get::<_, bool>(3), "{name} must not create databases");
        assert!(!role.get::<_, bool>(4), "{name} must not create roles");
        assert!(
            !role.get::<_, bool>(5),
            "{name} must not inherit another role"
        );
    }
    let publication_oid = load_publication_oid(&mut admin, PUBLICATION);
    let swapped_publication_oid = load_publication_oid(&mut admin, SWAPPED_PUBLICATION);
    let options = BootstrapOptions::new(2, Duration::from_secs(5)).expect("options");
    let spec = bootstrap_spec(1, 1, publication_oid, SLOT);
    let control_url = as_role(&database_url, CONTROL_ROLE);
    let receiver_url = as_role(&replication_url, RECEIVER_ROLE);
    let reader_url = as_role(&database_url, READER_ROLE);

    assert!(
        BootstrapSession::begin(
            &as_role(&database_url, RECEIVER_ROLE),
            &receiver_url,
            spec.clone(),
            options
        )
        .is_err(),
        "replication role cannot become bootstrap control/apply/scanner"
    );
    assert_no_apply(&mut admin, 1, None);
    assert!(
        BootstrapSession::begin(&control_url, &receiver_url, spec.clone(), options).is_err(),
        "control role without revoked function EXECUTE must fail"
    );
    assert_no_apply(&mut admin, 1, None);
    assert_results(&mut admin, 1, "active", [Some(0), None]);

    admin
        .batch_execute(&format!(
            "GRANT EXECUTE ON FUNCTION shiba_internal.reserve_graph_bootstrap(bigint, bigint, oid, name, bigint) TO {CONTROL_ROLE};
             GRANT EXECUTE ON FUNCTION shiba_internal.replace_pristine_graph_bootstrap(bigint, bigint, name, bigint, bigint, oid, name, bigint) TO {CONTROL_ROLE};"
        ))
        .expect("grant the two revoked bootstrap control functions");
    let swapped = bootstrap_spec(2, 2, swapped_publication_oid, SWAPPED_SLOT);
    assert!(
        BootstrapSession::begin(
            &control_url,
            &as_role(&replication_url, CONTROL_ROLE),
            swapped,
            options
        )
        .is_err(),
        "NOREPLICATION control role cannot own replication transport"
    );
    assert_no_apply(&mut admin, 2, Some("creating"));
    assert_results(&mut admin, 2, "building", [None, None]);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&SWAPPED_SLOT]
            )
            .expect("check unauthorized slot")
            .get::<_, i64>(0),
        0
    );

    let mut reader = Client::connect(&reader_url, NoTls).expect("connect result reader");
    assert!(
        reader
            .query("SELECT * FROM shiba_internal.graph_node_state", &[])
            .is_err()
    );
    assert!(reader.query("SELECT * FROM source.events", &[]).is_err());
    assert!(
        reader
            .execute(
                "UPDATE shiba.graph_result SET schema_payload = schema_payload
                 WHERE graph_id = 1 AND result_id = 3",
                &[],
            )
            .is_err(),
        "result reader must not mutate the public projection"
    );
    let mut bootstrap = BootstrapSession::begin(&control_url, &receiver_url, spec, options)
        .expect("begin least-privilege bootstrap");
    assert_results(&mut reader, 1, "building", [None, None]);

    admin
        .batch_execute(&format!(
            "REVOKE UPDATE ON shiba_internal.graph_bootstrap,
                 shiba_internal.graph_bootstrap_checkpoint FROM {CONTROL_ROLE}"
        ))
        .expect("remove checkpoint UPDATE");
    assert!(
        bootstrap.scan_next().is_err(),
        "missing checkpoint UPDATE must roll back batch"
    );
    assert_no_apply(&mut admin, 1, Some("scanning"));
    assert_results(&mut reader, 1, "building", [None, None]);
    admin
        .batch_execute(&format!(
            "GRANT UPDATE ON shiba_internal.graph_bootstrap,
                 shiba_internal.graph_bootstrap_checkpoint TO {CONTROL_ROLE};
             REVOKE SELECT ON source.events FROM {CONTROL_ROLE}"
        ))
        .expect("exchange UPDATE for missing scanner SELECT");
    assert!(
        bootstrap.scan_next().is_err(),
        "missing source SELECT must fail before Apply"
    );
    assert_no_apply(&mut admin, 1, Some("scanning"));
    assert!(
        admin
            .query_one(
                "SELECT slot.confirmed_flush_lsn = bootstrap.consistent_point
                 FROM pg_replication_slots AS slot
                 JOIN shiba_internal.graph_bootstrap AS bootstrap
                   ON bootstrap.slot_name = slot.slot_name
                 WHERE bootstrap.graph_id = 1",
                &[],
            )
            .expect("verify no unauthorized feedback")
            .get::<_, bool>(0),
        "permission failures must not advance the physical slot"
    );
    admin
        .batch_execute(&format!("GRANT SELECT ON source.events TO {CONTROL_ROLE}"))
        .expect("restore scanner SELECT");

    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO source.events VALUES (4, 5);
             UPDATE source.events SET payload = 20 WHERE id = 1;
             DELETE FROM source.events WHERE id = 3;
             COMMIT;",
        )
        .expect("commit concurrent WAL");
    assert_eq!(
        bootstrap.scan_next().expect("first batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 1,
            rows: 2
        }
    );
    assert_eq!(
        bootstrap.scan_next().expect("second batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 2,
            rows: 1
        }
    );
    assert_eq!(
        bootstrap.scan_next().expect("complete scan"),
        SnapshotProgress::ScanComplete
    );
    let mut catchup = bootstrap
        .into_catchup()
        .expect("enter least-privilege catch-up");
    assert_eq!(
        catchup.catch_up_next().expect("apply concurrent WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        catchup.catch_up_next().expect("activate exact fence"),
        BootstrapCatchupProgress::Active
    );
    assert_results(&mut reader, 1, "active", [Some(3), Some(25)]);
    let oracle = admin
        .query_one(
            "SELECT count(*), COALESCE(sum(payload), 0)::bigint FROM source.events",
            &[],
        )
        .expect("SQL oracle");
    assert_eq!((oracle.get::<_, i64>(0), oracle.get::<_, i64>(1)), (3, 25));
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id = 1",
                &[]
            )
            .expect("WAL continuation")
            .get::<_, i64>(0),
        1
    );
    assert_eq!(admin.query_one("SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1 AND confirmed_flush_lsn >= (SELECT activation_end_lsn FROM shiba_internal.graph_bootstrap WHERE graph_id = 1)", &[&SLOT]).expect("authorized feedback").get::<_, i64>(0), 1);
    catchup
        .into_live()
        .expect("least-privilege live handoff")
        .detach()
        .expect("detach live session");
}
