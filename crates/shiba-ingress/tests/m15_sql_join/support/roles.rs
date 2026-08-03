use std::time::Duration;

use postgres::Client;
use shiba_ingress::{BootstrapOptions, BootstrapSession, BootstrapSpec};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};

use super::{Fixture, OLD_SLOT, oracle};

pub(crate) const CONTROL_ROLE: &str = "shiba_m15_sql_join_control";
pub(crate) const RECEIVER_ROLE: &str = "shiba_m15_sql_join_receiver";
pub(crate) const READER_ROLE: &str = "shiba_m15_sql_join_reader";

pub(crate) fn install(client: &mut Client) {
    client
        .batch_execute(&format!(
            "CREATE ROLE {CONTROL_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
             CREATE ROLE {RECEIVER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION;
             CREATE ROLE {READER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
             GRANT USAGE ON SCHEMA shiba_internal,shiba,left_source,right_source,
                 join_target_left,join_target_right TO {CONTROL_ROLE};
             GRANT SELECT,UPDATE ON shiba_internal.source_binding TO {CONTROL_ROLE};
             GRANT SELECT,DELETE ON shiba_internal.source_invalidation TO {CONTROL_ROLE};
             GRANT SELECT,UPDATE ON shiba_internal.graph_ingress_config,
                 shiba_internal.graph_definition,shiba_internal.graph_source_member,
                 shiba_internal.graph_ingress_source TO {CONTROL_ROLE};
             GRANT SELECT ON shiba_internal.graph_ingress_invalidation TO {CONTROL_ROLE};
             GRANT SELECT,UPDATE ON shiba_internal.graph_bootstrap,
                 shiba_internal.graph_bootstrap_checkpoint TO {CONTROL_ROLE};
             GRANT SELECT,INSERT,UPDATE ON shiba_internal.graph_continuation TO {CONTROL_ROLE};
             GRANT SELECT,INSERT,UPDATE,DELETE ON shiba_internal.source_row_state,
                 shiba_internal.graph_node_state,
                 shiba_internal.graph_result_row TO {CONTROL_ROLE};
             GRANT USAGE ON SEQUENCE shiba_internal.source_row_state_row_state_id_seq
                 TO {CONTROL_ROLE};
             GRANT SELECT,UPDATE ON shiba.graph_result TO {CONTROL_ROLE};
             GRANT SELECT ON left_source.events,right_source.events,
                 join_target_left.events,join_target_right.events TO {CONTROL_ROLE};
             GRANT EXECUTE ON FUNCTION shiba_internal.prepare_graph_rebuild(
                 bigint,bytea,bigint,oid[],oid[],oid,name,bigint,
                 bigint,bigint[],oid[],oid[],oid,name,bigint,
                 bytea,bytea,bytea,bigint[],text[],boolean[],boolean[]
             ) TO {CONTROL_ROLE};
             GRANT USAGE ON SCHEMA left_source,right_source,
                 join_target_left,join_target_right TO {RECEIVER_ROLE};
             GRANT SELECT ON left_source.events,right_source.events,
                 join_target_left.events,join_target_right.events TO {RECEIVER_ROLE};
             GRANT USAGE ON SCHEMA shiba TO {READER_ROLE};
             GRANT SELECT ON shiba.graph_result,shiba.graph_result_rows TO {READER_ROLE};"
        ))
        .expect("install least-privilege SQL join roles");
}

pub(crate) fn as_role(conninfo: &str, role: &str) -> String {
    format!("{conninfo} user={role}")
}

pub(crate) fn assert_role_shape(client: &mut Client) {
    let rows = client
        .query(
            "SELECT rolname,rolsuper,rolreplication,rolcreatedb,rolcreaterole,
                    EXISTS (SELECT 1 FROM pg_auth_members WHERE member=role.oid)
             FROM pg_roles AS role WHERE rolname IN ($1,$2,$3) ORDER BY rolname",
            &[&CONTROL_ROLE, &RECEIVER_ROLE, &READER_ROLE],
        )
        .expect("audit split SQL join roles");
    assert_eq!(rows.len(), 3);
    for row in rows {
        let name: &str = row.get(0);
        assert!(!row.get::<_, bool>(1));
        assert_eq!(row.get::<_, bool>(2), name == RECEIVER_ROLE);
        assert!(!row.get::<_, bool>(3));
        assert!(!row.get::<_, bool>(4));
        assert!(!row.get::<_, bool>(5));
    }
}

pub(crate) fn prove_missing_bootstrap_grant(
    control_url: &str,
    receiver_url: &str,
    admin: &mut Client,
    fixture: &Fixture,
) {
    let spec = BootstrapSpec {
        graph_id: GraphId::new(1).expect("graph ID"),
        bootstrap_id: BootstrapId::new(1).expect("bootstrap ID"),
        publication_oid: fixture.old.publication_oid,
        slot_name: OLD_SLOT.to_owned(),
        slot_generation: SlotGeneration::new(1).expect("generation"),
    };
    assert!(
        BootstrapSession::begin(
            control_url,
            receiver_url,
            spec,
            BootstrapOptions::new(2, Duration::from_secs(5)).expect("options")
        )
        .is_err(),
        "missing bootstrap writer EXECUTE must fail closed"
    );
    let state = admin
        .query_one(
            "SELECT (SELECT count(*) FROM shiba_internal.graph_bootstrap WHERE graph_id=1),
                    (SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1),
                    (SELECT count(*) FROM shiba_internal.source_row_state
                     WHERE source_id IN (1,2))",
            &[&OLD_SLOT],
        )
        .expect("read failed bootstrap authority");
    assert_eq!(
        (
            state.get::<_, i64>(0),
            state.get::<_, i64>(1),
            state.get::<_, i64>(2)
        ),
        (0, 0, 0)
    );
}

pub(crate) fn grant_bootstrap_control(client: &mut Client) {
    client
        .batch_execute(&format!(
            "GRANT EXECUTE ON FUNCTION shiba_internal.reserve_graph_bootstrap(
                 bigint,bigint,oid,name,bigint
             ) TO {CONTROL_ROLE};
             GRANT EXECUTE ON FUNCTION shiba_internal.replace_pristine_graph_bootstrap(
                 bigint,bigint,name,bigint,bigint,oid,name,bigint
             ) TO {CONTROL_ROLE};"
        ))
        .expect("grant exact bootstrap writers");
}

pub(crate) fn grant_registration_control(client: &mut Client) {
    client
        .batch_execute(&format!(
            "GRANT INSERT ON shiba_internal.graph_definition,
                 shiba_internal.graph_source_member TO {CONTROL_ROLE};
             GRANT INSERT ON shiba.graph_result TO {CONTROL_ROLE};"
        ))
        .expect("grant exact graph registration writes");
}

pub(crate) fn assert_no_registered_graph(client: &mut Client) {
    let row = client
        .query_one(
            "SELECT (SELECT count(*) FROM shiba_internal.graph_definition),
                    (SELECT count(*) FROM shiba_internal.graph_source_member),
                    (SELECT count(*) FROM shiba.graph_result)",
            &[],
        )
        .expect("read failed SQL join registration authority");
    assert_eq!(
        (
            row.get::<_, i64>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2)
        ),
        (0, 0, 0)
    );
}

pub(crate) fn assert_reader_building(reader: &mut Client) {
    let row = reader
        .query_one(
            "SELECT result_status FROM shiba.graph_result WHERE graph_id=1",
            &[],
        )
        .expect("reader observes building result");
    assert_eq!(row.get::<_, &str>(0), "building");
    assert!(
        reader
            .query(
                "SELECT * FROM shiba.graph_result_rows WHERE graph_id=1",
                &[]
            )
            .expect("reader queries hidden result rows")
            .is_empty()
    );
    assert!(
        reader
            .query("SELECT * FROM shiba_internal.graph_node_state", &[])
            .is_err()
    );
    assert!(
        reader
            .execute("UPDATE shiba.graph_result SET value_bigint=1", &[])
            .is_err()
    );
}

pub(crate) fn assert_reader_matches(
    reader: &mut Client,
    admin: &mut Client,
    left: &str,
    right: &str,
) {
    let expected = oracle(admin, left, right);
    let actual = reader
        .query(
            "SELECT result_key_bigint,result_value_bigint,result_value_is_null
             FROM shiba.graph_result_rows WHERE graph_id=1 ORDER BY result_key_bigint",
            &[],
        )
        .expect("SELECT-only reader queries complete SQL join result")
        .into_iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, Option<i64>>(1),
                row.get::<_, bool>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
