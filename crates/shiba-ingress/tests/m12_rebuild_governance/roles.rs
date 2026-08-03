use postgres::{Client, NoTls};
use shiba_ingress::{
    BootstrapCatchupProgress, BootstrapCatchupSession, PreparedRebuild, SnapshotProgress,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};

use crate::support::{
    CONTROL_ROLE, READER_ROLE, RECEIVER_ROLE, RebuildFixture, as_role, assert_building,
    authority_snapshot, establish_active_source, grant_rebuild_control, options,
};

#[allow(
    clippy::too_many_lines,
    reason = "one ordered split-role rebuild and recovery proof"
)]
pub(crate) fn prove_rebuild_roles_and_fail_closed_permission_loss(
    database_url: &str,
    replication_url: &str,
) {
    let (mut admin, active) = establish_active_source(database_url, replication_url);
    let fixture = RebuildFixture::install(&mut admin, active.publication_oid);
    grant_rebuild_control(&mut admin);
    let control_url = as_role(database_url, CONTROL_ROLE);
    let receiver_url = as_role(replication_url, RECEIVER_ROLE);
    let reader_url = as_role(database_url, READER_ROLE);
    assert_role_shape(&mut admin);

    let active_before = authority_snapshot(&mut admin);
    assert!(
        PreparedRebuild::prepare(&reader_url, &receiver_url, &fixture.spec(), options()).is_err(),
        "read-only result role cannot become rebuild control"
    );
    assert_eq!(authority_snapshot(&mut admin), active_before);
    admin
        .batch_execute(&format!(
            "REVOKE EXECUTE ON FUNCTION shiba_internal.prepare_graph_rebuild(
                 bigint, bytea, bigint, oid[], oid[], oid, name, bigint,
                 bigint, bigint[], oid[], oid[], oid, name, bigint,
                 bytea, bytea, bytea, bigint[], text[], boolean[], boolean[]
             ) FROM {CONTROL_ROLE};"
        ))
        .expect("remove rebuild admission EXECUTE");
    assert!(
        PreparedRebuild::prepare(&control_url, &receiver_url, &fixture.spec(), options()).is_err(),
        "missing rebuild writer EXECUTE must fail before destructive prepare"
    );
    assert_eq!(authority_snapshot(&mut admin), active_before);
    admin
        .batch_execute(&format!(
            "GRANT EXECUTE ON FUNCTION shiba_internal.prepare_graph_rebuild(
                 bigint, bytea, bigint, oid[], oid[], oid, name, bigint,
                 bigint, bigint[], oid[], oid[], oid, name, bigint,
                 bytea, bytea, bytea, bigint[], text[], boolean[], boolean[]
             ) TO {CONTROL_ROLE};
             REVOKE SELECT ON target.events FROM {CONTROL_ROLE};"
        ))
        .expect("exchange admission privilege for missing target SELECT");
    assert!(
        PreparedRebuild::prepare(&control_url, &receiver_url, &fixture.spec(), options()).is_err(),
        "missing target SELECT must fail before destructive prepare"
    );
    assert_eq!(authority_snapshot(&mut admin), active_before);
    admin
        .batch_execute(&format!(
            "GRANT SELECT ON target.events TO {CONTROL_ROLE};
             REVOKE SELECT ON target.events FROM {RECEIVER_ROLE};"
        ))
        .expect("exchange control read for missing receiver read privilege");
    assert!(
        PreparedRebuild::prepare(&control_url, &receiver_url, &fixture.spec(), options()).is_err(),
        "receiver without target SELECT must fail before destructive prepare"
    );
    assert_eq!(authority_snapshot(&mut admin), active_before);
    admin
        .batch_execute(&format!("GRANT SELECT ON target.events TO {RECEIVER_ROLE}"))
        .expect("restore exact receiver read privilege");
    let active_value: Option<i64> = admin
        .query_one(
            "SELECT value_bigint FROM shiba.graph_result WHERE graph_id = 1 AND result_id = 4",
            &[],
        )
        .expect("read unchanged old result")
        .get(0);
    assert_eq!(active_value, Some(4));
    assert!(
        PreparedRebuild::prepare(
            &control_url,
            &as_role(replication_url, CONTROL_ROLE),
            &fixture.spec(),
            options()
        )
        .is_err(),
        "NOREPLICATION control credential cannot own transport"
    );

    let prepared =
        PreparedRebuild::prepare(&control_url, &receiver_url, &fixture.spec(), options())
            .expect("non-superuser control accepts rebuild through exact SQL authority");
    assert_building(&mut admin);
    let mut reader = Client::connect(&reader_url, NoTls).expect("connect public result reader");
    assert!(
        reader
            .query("SELECT * FROM shiba_internal.graph_bootstrap", &[])
            .is_err()
    );
    assert!(
        reader
            .execute("UPDATE shiba.graph_result SET value_bigint = 1", &[])
            .is_err()
    );
    let mut bootstrap = prepared
        .into_bootstrap()
        .expect("REPLICATION role owns slot lifecycle");
    admin
        .batch_execute("DELETE FROM target.events WHERE id = 10")
        .expect("commit target DELETE while exported snapshot is open");
    admin
        .batch_execute(&format!(
            "REVOKE UPDATE ON shiba_internal.graph_bootstrap_checkpoint FROM {CONTROL_ROLE}"
        ))
        .expect("remove checkpoint privilege");
    assert!(
        bootstrap.scan_next().is_err(),
        "checkpoint permission loss must roll back the snapshot batch"
    );
    assert_building(&mut reader);
    admin
        .batch_execute(&format!(
            "GRANT UPDATE ON shiba_internal.graph_bootstrap_checkpoint TO {CONTROL_ROLE}"
        ))
        .expect("restore checkpoint privilege");
    while bootstrap.scan_next().expect("retry exact snapshot batch")
        != SnapshotProgress::ScanComplete
    {}
    let mut catchup = bootstrap.into_catchup().expect("enter split-role catch-up");
    let before_delete = catchup_snapshot(&mut admin);
    admin
        .batch_execute(&format!(
            "REVOKE DELETE ON shiba_internal.source_row_state FROM {CONTROL_ROLE}"
        ))
        .expect("remove source-row DELETE privilege");
    assert!(
        catchup.catch_up_next().is_err(),
        "missing source-row DELETE must roll back the target WAL change"
    );
    assert_eq!(catchup_snapshot(&mut admin), before_delete);
    assert_building(&mut reader);
    admin
        .batch_execute(&format!(
            "GRANT DELETE ON shiba_internal.source_row_state TO {CONTROL_ROLE}"
        ))
        .expect("restore source-row DELETE privilege");
    drop(catchup);
    let mut catchup = BootstrapCatchupSession::resume(
        &control_url,
        &receiver_url,
        GraphId::new(1).expect("graph ID"),
        BootstrapId::new(2).expect("rebuild bootstrap ID"),
        SlotGeneration::new(3).expect("rebuild generation"),
        options(),
    )
    .expect("restart after unacknowledged permission failure");
    assert_eq!(
        catchup
            .catch_up_next()
            .expect("Apply retried target DELETE"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_eq!(
        catchup.catch_up_next().expect("activate rebuild"),
        BootstrapCatchupProgress::Active
    );
    let rows = reader
        .query(
            "SELECT result_status, value_bigint FROM shiba.graph_result WHERE graph_id = 1 ORDER BY result_id",
            &[],
        )
        .expect("read atomically activated public result");
    assert_eq!(
        rows.into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, Option<i64>>(1)))
            .collect::<Vec<_>>(),
        vec![
            ("active".to_owned(), Some(1)),
            ("active".to_owned(), Some(0)),
            ("active".to_owned(), None),
        ]
    );
    let projected = reader
        .query(
            "SELECT result_key_bigint, result_value_bigint
             FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 6 ORDER BY 1",
            &[],
        )
        .expect("read split-role ProjectRows result");
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].get::<_, i64>(0), 11);
    assert_eq!(projected[0].get::<_, Option<i64>>(1), None);
    let live = catchup.into_live().expect("handoff to normal ingress");
    live.detach().expect("detach split-role live receiver");
    drop(reader);
    drop(admin);
}

fn assert_role_shape(client: &mut Client) {
    let rows = client
        .query(
            "SELECT rolname, rolsuper, rolreplication, rolcreatedb, rolcreaterole,
                    EXISTS (SELECT 1 FROM pg_auth_members WHERE member = role.oid)
             FROM pg_catalog.pg_roles AS role
             WHERE rolname IN ($1, $2, $3) ORDER BY rolname",
            &[&CONTROL_ROLE, &RECEIVER_ROLE, &READER_ROLE],
        )
        .expect("audit split rebuild roles");
    assert_eq!(rows.len(), 3);
    for row in rows {
        let name: &str = row.get(0);
        assert!(!row.get::<_, bool>(1), "{name} must not be superuser");
        assert_eq!(row.get::<_, bool>(2), name == RECEIVER_ROLE);
        assert!(!row.get::<_, bool>(3), "{name} must not create databases");
        assert!(!row.get::<_, bool>(4), "{name} must not create roles");
        assert!(
            !row.get::<_, bool>(5),
            "{name} must not inherit role authority"
        );
    }
}

fn catchup_snapshot(client: &mut Client) -> Vec<Vec<String>> {
    [
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_row_state ORDER BY source_row_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_node_state ORDER BY graph_id, node_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba.graph_result ORDER BY graph_id, result_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_continuation ORDER BY commit_lsn) x",
        "SELECT row_to_json(x)::text FROM (SELECT phase, catchup_fence_lsn::text FROM shiba_internal.graph_bootstrap) x",
        "SELECT row_to_json(x)::text FROM (SELECT slot_name, confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots ORDER BY slot_name) x",
    ]
    .into_iter()
    .map(|query| {
        client
            .query(query, &[])
            .expect("snapshot failed catch-up authority")
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    })
    .collect()
}
