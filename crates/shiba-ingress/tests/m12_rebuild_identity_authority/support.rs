use std::time::Duration;

use postgres::Client;
use shiba_ingress::{BootstrapOptions, PreparedRebuild, RebuildIdentity, RebuildSpec};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};
use shiba_runtime::{RebuildSourceTarget, compile_rebuild_graph};

#[path = "../m12_rebuild_admission/support.rs"]
#[allow(dead_code, unused_imports)]
mod admission;

pub(crate) use admission::{RebuildFixture, TARGET_SLOT, establish_active_source};

pub(crate) const SECOND_SLOT: &str = "shiba_m12_identity_next";
pub(crate) const SECOND_PUBLICATION: &str = "shiba_m12_identity_next_pub";

pub(crate) fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("scripts/test-m12-rebuild-identity-authority.sh must set {name}")
    })
}

pub(crate) fn options() -> BootstrapOptions {
    BootstrapOptions::new(2, Duration::from_secs(5)).expect("bounded rebuild options")
}

pub(crate) fn resume(
    database_url: &str,
    replication_url: &str,
    bootstrap: u64,
    generation: u64,
) -> Result<PreparedRebuild, shiba_ingress::IngressError> {
    PreparedRebuild::resume_prepared(
        database_url,
        replication_url,
        GraphId::new(1).expect("graph ID"),
        BootstrapId::new(bootstrap).expect("bootstrap ID"),
        SlotGeneration::new(generation).expect("slot generation"),
        options(),
    )
}

pub(crate) fn assert_exact_identity(client: &mut Client, relation_oid: u32, index_oid: u32) {
    let rows = client
        .query(
            "SELECT binding_kind, address_classid::bigint, address_objid::bigint,
                    address_objsubid
             FROM shiba_internal.source_binding
             WHERE source_id = 1
             ORDER BY binding_kind, address_objsubid",
            &[],
        )
        .expect("read exact durable source identity");
    let pg_class: i64 = client
        .query_one("SELECT 'pg_class'::regclass::oid::bigint", &[])
        .expect("read pg_class address")
        .get(0);
    assert_eq!(
        rows.into_iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    row.get::<_, i64>(1),
                    row.get::<_, i64>(2),
                    row.get::<_, i32>(3),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            ("column".to_owned(), pg_class, i64::from(relation_oid), 1),
            ("column".to_owned(), pg_class, i64::from(relation_oid), 2),
            (
                "identity_index".to_owned(),
                pg_class,
                i64::from(index_oid),
                0
            ),
            ("relation".to_owned(), pg_class, i64::from(relation_oid), 0),
        ]
    );
    let approved: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_index
                WHERE indexrelid = $1::bigint::oid
                  AND indrelid = $2::bigint::oid
                  AND indisprimary AND indisunique AND indisvalid AND indisready
                  AND indnkeyatts = 1 AND indnatts = 1
                  AND (indkey::smallint[])[0] = 1
                  AND indexprs IS NULL AND indpred IS NULL
             )",
            &[&i64::from(index_oid), &i64::from(relation_oid)],
        )
        .expect("validate approved identity index")
        .get(0);
    assert!(
        approved,
        "durable identity binding must name the approved live index"
    );
}

pub(crate) fn assert_prepared_closed(client: &mut Client, target_slot: &str) {
    let row = client
        .query_one(
            "SELECT bootstrap.phase,
                    checkpoint.last_batch_ordinal = 0
                    AND checkpoint.last_source_row_id IS NULL
                    AND checkpoint.last_batch_digest IS NULL
                    AND bootstrap.consistent_point IS NULL
                    AND catchup_fence_lsn IS NULL
                    AND activation_end_lsn IS NULL
             FROM shiba_internal.graph_bootstrap AS bootstrap
             JOIN shiba_internal.graph_bootstrap_checkpoint AS checkpoint USING (graph_id)
             WHERE bootstrap.graph_id = 1 AND checkpoint.source_id = 1",
            &[],
        )
        .expect("read prepared lifecycle");
    assert_eq!(row.get::<_, &str>(0), "rebuild_prepared");
    assert!(
        row.get::<_, bool>(1),
        "prepared checkpoint must remain empty"
    );
    let durable = client
        .query_one(
            "SELECT
                (SELECT count(*) FROM shiba_internal.source_row_state WHERE source_id = 1),
                (SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id = 1),
                (SELECT count(*) FROM shiba_internal.graph_node_state WHERE graph_id = 1),
                (SELECT count(*) FROM shiba.graph_result
                 WHERE result_status <> 'building' OR value_bigint IS NOT NULL),
                (SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
            &[&target_slot],
        )
        .expect("prove no scan, Apply, publication, or target slot entry");
    for column in 0..5 {
        assert_eq!(durable.get::<_, i64>(column), 0);
    }
}

pub(crate) fn prepared_snapshot(client: &mut Client, target_slot: &str) -> Vec<String> {
    let mut snapshot = Vec::new();
    for query in [
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_binding ORDER BY binding_kind, address_objsubid) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_bootstrap WHERE graph_id = 1) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_ingress_config WHERE graph_id = 1) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_node_state ORDER BY graph_id, node_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba.graph_result ORDER BY graph_id, result_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_continuation ORDER BY commit_lsn) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_invalidation ORDER BY source_id) x",
    ] {
        snapshot.extend(
            client
                .query(query, &[])
                .expect("snapshot prepared authority")
                .into_iter()
                .map(|row| row.get(0)),
        );
    }
    snapshot.push(
        client
            .query_one(
                "SELECT count(*)::text FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&target_slot],
            )
            .expect("snapshot target physical slot")
            .get(0),
    );
    snapshot
}

pub(crate) fn activate_prepared_fixture(client: &mut Client, prepared: PreparedRebuild) {
    let mut bootstrap = prepared
        .into_bootstrap()
        .expect("enter exact target bootstrap");
    while bootstrap.scan_next().expect("scan target")
        != shiba_ingress::SnapshotProgress::ScanComplete
    {}
    let mut catchup = bootstrap.into_catchup().expect("enter target catch-up");
    assert_eq!(
        catchup.catch_up_next().expect("activate target"),
        shiba_ingress::BootstrapCatchupProgress::Active
    );
    drop(catchup);
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM shiba_internal.source_row_state WHERE source_id = 1",
                &[]
            )
            .expect("target rows")
            .get::<_, i64>(0),
        2
    );
}

pub(crate) fn install_second_target(client: &mut Client, fixture: &RebuildFixture) -> RebuildSpec {
    client
        .batch_execute(&format!(
            "CREATE SCHEMA target_next;
             CREATE TABLE target_next.events (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO target_next.events VALUES (20, 4), (21, NULL);
             CREATE PUBLICATION {SECOND_PUBLICATION} FOR TABLE target_next.events
               WITH (publish = 'insert, update, delete');"
        ))
        .expect("install second explicit rebuild target");
    let relation = oid(client, "target_next.events");
    let index = oid(client, "target_next.events_pkey");
    let publication: u32 = client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&SECOND_PUBLICATION],
        )
        .expect("read second publication OID")
        .get(0);
    let target_digest = {
        let mut transaction = client.transaction().expect("compile second target");
        let artifact = compile_rebuild_graph(
            &mut transaction,
            GraphId::new(1).expect("graph ID"),
            &[RebuildSourceTarget {
                source_id: SourceId::new(1).expect("source ID"),
                relation_id: relation,
                identity_index_id: index,
            }],
        )
        .expect("compile second target graph");
        transaction
            .rollback()
            .expect("rollback read-only compilation");
        artifact.graph_digest
    };
    RebuildSpec {
        graph_id: GraphId::new(1).expect("graph ID"),
        expected: RebuildIdentity {
            bootstrap_id: BootstrapId::new(2).expect("old bootstrap ID"),
            graph_digest: fixture.target.graph_digest,
            members: vec![shiba_ingress::RebuildMemberIdentity {
                source_id: SourceId::new(1).expect("source ID"),
                relation_oid: fixture.target.relation,
                identity_index_oid: fixture.target.identity_index,
            }],
            publication_oid: fixture.target.publication,
            slot_name: TARGET_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(3).expect("old generation"),
        },
        target: RebuildIdentity {
            bootstrap_id: BootstrapId::new(3).expect("new bootstrap ID"),
            graph_digest: target_digest,
            members: vec![shiba_ingress::RebuildMemberIdentity {
                source_id: SourceId::new(1).expect("source ID"),
                relation_oid: relation,
                identity_index_oid: index,
            }],
            publication_oid: publication,
            slot_name: SECOND_SLOT.to_owned(),
            slot_generation: SlotGeneration::new(4).expect("new generation"),
        },
    }
}

pub(crate) fn oid(client: &mut Client, object: &str) -> u32 {
    client
        .query_one("SELECT $1::text::regclass::oid", &[&object])
        .expect("resolve object OID")
        .get(0)
}
