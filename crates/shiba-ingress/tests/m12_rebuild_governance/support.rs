use std::time::Duration;

use postgres::Client;
use shiba_ingress::{
    BootstrapCatchupProgress, BootstrapOptions, BootstrapSession, BootstrapSpec, SnapshotProgress,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};
use shiba_runtime::{RebuildSourceTarget, compile_and_register, compile_rebuild_graph};

#[path = "../m12_rebuild_admission/support.rs"]
#[allow(dead_code, unused_imports)]
mod admission;

pub(crate) use admission::{
    IdentityCoordinates, RebuildFixture, authority_snapshot, establish_active_source, options,
};

#[path = "../support/mod.rs"]
#[allow(dead_code)]
mod test_support;

pub(crate) const CONTROL_ROLE: &str = "shiba_m12_rebuild_control";
pub(crate) const RECEIVER_ROLE: &str = "shiba_m12_rebuild_receiver";
pub(crate) const READER_ROLE: &str = "shiba_m12_rebuild_reader";
pub(crate) const SECOND_SLOT: &str = "shiba_m12_governance_source_two";
pub(crate) const SECOND_PUBLICATION: &str = "shiba_m12_governance_source_two_pub";

pub(crate) fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m12-rebuild-governance.sh must set {name}"))
}

pub(crate) fn as_role(conninfo: &str, role: &str) -> String {
    format!("{conninfo} user={role}")
}

pub(crate) fn assert_building(client: &mut Client) {
    let rows = client
        .query(
            "SELECT result_status, value_bigint FROM shiba.graph_result ORDER BY graph_id, result_id",
            &[],
        )
        .expect("query public rebuild visibility");
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| {
        row.get::<_, &str>(0) == "building" && row.get::<_, Option<i64>>(1).is_none()
    }));
}

pub(crate) fn refresh_target_digest(client: &mut Client, fixture: &mut RebuildFixture) {
    let mut transaction = client
        .transaction()
        .expect("compile refreshed target graph");
    let artifact = compile_rebuild_graph(
        &mut transaction,
        GraphId::new(1).expect("graph ID"),
        &[RebuildSourceTarget {
            source_id: shiba_protocol::SourceId::new(1).expect("source ID"),
            relation_id: fixture.target.relation,
            identity_index_id: fixture.target.identity_index,
        }],
    )
    .expect("compile refreshed target graph");
    transaction
        .rollback()
        .expect("rollback read-only compilation");
    fixture.target.graph_digest = artifact.graph_digest;
}

pub(crate) fn install_second_active_source(
    client: &mut Client,
    database_url: &str,
    replication_url: &str,
) -> shiba_ingress::GovernedGraphSession {
    client
        .batch_execute(&format!(
            "CREATE SCHEMA source_two;
             CREATE TABLE source_two.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {SECOND_PUBLICATION} FOR TABLE source_two.events
                WITH (publish = 'insert, update, delete');
             SELECT shiba_internal.register_source(2, 'source_two.events'::regclass);
             INSERT INTO source_two.events VALUES (201, 9);"
        ))
        .expect("install independent source");
    let mut graph = test_support::count_sum_spec(2);
    graph.graph_id = GraphId::new(2).expect("graph ID");
    compile_and_register(client, &graph).expect("register independent graph");
    let publication_oid: u32 = client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&SECOND_PUBLICATION],
        )
        .expect("read source-two publication identity")
        .get(0);
    let mut bootstrap = BootstrapSession::begin(
        database_url,
        replication_url,
        BootstrapSpec {
            graph_id: GraphId::new(2).expect("graph ID"),
            bootstrap_id: BootstrapId::new(1).expect("bootstrap ID"),
            publication_oid,
            slot_name: SECOND_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(1).expect("generation"),
        },
        BootstrapOptions::new(8, Duration::from_secs(5)).expect("bounded bootstrap options"),
    )
    .expect("bootstrap independent source");
    while bootstrap.scan_next().expect("scan independent source") != SnapshotProgress::ScanComplete
    {
    }
    let mut catchup = bootstrap.into_catchup().expect("catch up source two");
    assert_eq!(
        catchup.catch_up_next().expect("activate source two"),
        BootstrapCatchupProgress::Active
    );
    catchup.into_live().expect("enter source-two live ingress")
}

pub(crate) fn grant_rebuild_control(client: &mut Client) {
    client
        .batch_execute(&format!(
            "CREATE ROLE {CONTROL_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
             CREATE ROLE {RECEIVER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION;
             CREATE ROLE {READER_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
             GRANT USAGE ON SCHEMA shiba_internal, shiba, source, target TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.source_binding TO {CONTROL_ROLE};
             GRANT SELECT, DELETE ON shiba_internal.source_invalidation TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.graph_ingress_config TO {CONTROL_ROLE};
             GRANT SELECT ON
                 shiba_internal.graph_ingress_invalidation TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON
                 shiba_internal.graph_definition,
                 shiba_internal.graph_source_member,
                 shiba_internal.graph_ingress_source TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba_internal.graph_bootstrap,
                 shiba_internal.graph_bootstrap_checkpoint TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE ON shiba_internal.graph_continuation TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE, DELETE ON shiba_internal.source_row_state TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE, DELETE ON shiba_internal.graph_node_state TO {CONTROL_ROLE};
             GRANT SELECT, INSERT, UPDATE, DELETE
                 ON shiba_internal.graph_result_row TO {CONTROL_ROLE};
             GRANT SELECT, UPDATE ON shiba.graph_result TO {CONTROL_ROLE};
             GRANT SELECT ON source.events, target.events TO {CONTROL_ROLE};
             GRANT EXECUTE ON FUNCTION shiba_internal.prepare_graph_rebuild(
                 bigint, bytea, bigint, oid[], oid[], oid, name, bigint,
                 bigint, bigint[], oid[], oid[], oid, name, bigint,
                 bytea, bytea, bytea, bigint[], text[], boolean[], boolean[]
             ) TO {CONTROL_ROLE};
             GRANT USAGE ON SCHEMA target TO {RECEIVER_ROLE};
             GRANT SELECT ON target.events TO {RECEIVER_ROLE};
             GRANT USAGE ON SCHEMA shiba TO {READER_ROLE};
             GRANT SELECT ON shiba.graph_result, shiba.graph_result_rows TO {READER_ROLE};"
        ))
        .expect("grant only rebuild execution capabilities");
}

pub(crate) fn teardown(client: &mut Client) {
    let slots = client
        .query(
            "SELECT slot_name FROM pg_catalog.pg_replication_slots
             WHERE slot_name LIKE 'shiba_m12_%'",
            &[],
        )
        .expect("find test-owned slots");
    for slot in slots {
        let name: String = slot.get(0);
        client
            .execute("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&name])
            .expect("drop test-owned inactive slot");
    }
    let publications = client
        .query(
            "SELECT pubname FROM pg_catalog.pg_publication
             WHERE pubname LIKE 'shiba_m12_%'",
            &[],
        )
        .expect("find test-owned publications");
    for publication in publications {
        let name: String = publication.get(0);
        client
            .batch_execute(&format!(
                "DROP PUBLICATION \"{}\"",
                name.replace('"', "\"\"")
            ))
            .expect("drop test-owned publication");
    }
    client
        .batch_execute(&format!(
            "DROP EXTENSION IF EXISTS shiba_catalog CASCADE;
             DROP SCHEMA IF EXISTS target CASCADE;
             DROP SCHEMA IF EXISTS source CASCADE;
             DROP SCHEMA IF EXISTS source_two CASCADE;
             DROP ROLE IF EXISTS {READER_ROLE};
             DROP ROLE IF EXISTS {RECEIVER_ROLE};
             DROP ROLE IF EXISTS {CONTROL_ROLE};"
        ))
        .expect("remove test-owned governance fixture");
}
