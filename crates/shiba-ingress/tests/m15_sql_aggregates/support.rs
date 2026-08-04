use std::time::Duration;

use postgres::Client;
use shiba_ingress::{
    AttachOptions, BootstrapCatchupProgress, BootstrapOptions, BootstrapSession, BootstrapSpec,
    GovernedGraphSession, ReplicationMode, SnapshotProgress,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};

#[allow(dead_code)]
#[path = "../support/mod.rs"]
mod integration_support;

#[path = "support/grouped.rs"]
pub(crate) mod grouped;
#[path = "support/rebuild.rs"]
pub(crate) mod rebuild;
#[path = "support/scalar.rs"]
pub(crate) mod scalar;

#[derive(Clone, Debug)]
pub(crate) struct GraphFixture {
    pub(crate) graph: u64,
    pub(crate) source: u64,
    pub(crate) schema: &'static str,
    pub(crate) slot: &'static str,
    pub(crate) publication_oid: u32,
}

pub(crate) struct Fixtures {
    pub(crate) graphs: Vec<GraphFixture>,
    pub(crate) target_relation: u32,
    pub(crate) target_identity: u32,
    pub(crate) target_publication: u32,
    pub(crate) multi_target_relation: u32,
    pub(crate) multi_target_identity: u32,
    pub(crate) multi_target_publication: u32,
}

pub(crate) fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m15-sql-aggregates.sh must set {name}"))
}

pub(crate) fn options() -> BootstrapOptions {
    BootstrapOptions::new(2, Duration::from_secs(5)).expect("bounded bootstrap options")
}

pub(crate) fn attach(
    database_url: &str,
    replication_url: &str,
    fixture: &GraphFixture,
    generation: u64,
) -> GovernedGraphSession {
    GovernedGraphSession::attach(
        database_url,
        replication_url,
        GraphId::new(fixture.graph).expect("graph ID"),
        SlotGeneration::new(generation).expect("generation"),
        AttachOptions::new(ReplicationMode::Committed, Duration::from_secs(5))
            .expect("bounded attach options"),
    )
    .expect("attach production aggregate receiver")
}

pub(crate) fn install(client: &mut Client) -> Fixtures {
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA agg_count;
             CREATE TABLE agg_count.rows (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO agg_count.rows VALUES (1,10),(2,NULL);
             CREATE PUBLICATION m15_agg_count_pub FOR TABLE agg_count.rows
                 WITH (publish='insert,update,delete');
             SELECT shiba_internal.register_source(1, 'agg_count.rows'::regclass);

             CREATE SCHEMA agg_sum;
             CREATE TABLE agg_sum.rows (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION m15_agg_sum_pub FOR TABLE agg_sum.rows
                 WITH (publish='insert,update,delete');
             SELECT shiba_internal.register_source(2, 'agg_sum.rows'::regclass);

             CREATE SCHEMA agg_group_count;
             CREATE TABLE agg_group_count.rows (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO agg_group_count.rows VALUES (1,10),(2,NULL),(3,-1);
             CREATE PUBLICATION m15_agg_group_count_pub FOR TABLE agg_group_count.rows
                 WITH (publish='insert,update,delete');
             SELECT shiba_internal.register_source(3, 'agg_group_count.rows'::regclass);

             CREATE SCHEMA agg_group_sum;
             CREATE TABLE agg_group_sum.rows (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO agg_group_sum.rows VALUES (1,10),(2,10),(3,NULL);
             CREATE PUBLICATION m15_agg_group_sum_pub FOR TABLE agg_group_sum.rows
                 WITH (publish='insert,update,delete');
             SELECT shiba_internal.register_source(4, 'agg_group_sum.rows'::regclass);

             CREATE SCHEMA agg_group_sum_target;
             CREATE TABLE agg_group_sum_target.rows (
                 id bigint PRIMARY KEY, payload bigint NULL
             );
             INSERT INTO agg_group_sum_target.rows VALUES (10,100),(11,NULL),(12,100);
             CREATE PUBLICATION m15_agg_group_sum_target_pub
                 FOR TABLE agg_group_sum_target.rows
                 WITH (publish='insert,update,delete');

             CREATE SCHEMA agg_multi_target;
             CREATE TABLE agg_multi_target.rows (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO agg_multi_target.rows VALUES (10,100),(11,NULL),(12,100);
             CREATE PUBLICATION m15_agg_multi_target_pub
                 FOR TABLE agg_multi_target.rows
                 WITH (publish='insert,update,delete');

             CREATE SCHEMA agg_multi;
             CREATE TABLE agg_multi.rows (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO agg_multi.rows VALUES (1,10),(2,NULL),(3,10);
             CREATE PUBLICATION m15_agg_multi_pub FOR TABLE agg_multi.rows
                 WITH (publish='insert,update,delete');
             SELECT shiba_internal.register_source(5, 'agg_multi.rows'::regclass);

             CREATE SCHEMA agg_extrema;
             CREATE TABLE agg_extrema.rows (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO agg_extrema.rows VALUES (1,10),(2,10),(3,5),(4,NULL);
             CREATE PUBLICATION m15_agg_extrema_pub FOR TABLE agg_extrema.rows
                 WITH (publish='insert,update,delete');
             SELECT shiba_internal.register_source(6, 'agg_extrema.rows'::regclass);",
        )
        .expect("install aggregate sources and target");
    let mut fixture = |graph, schema, publication, slot| GraphFixture {
        graph,
        source: graph,
        schema,
        slot,
        publication_oid: publication_oid(client, publication),
    };
    Fixtures {
        graphs: vec![
            fixture(1, "agg_count", "m15_agg_count_pub", "m15_agg_count_1"),
            fixture(2, "agg_sum", "m15_agg_sum_pub", "m15_agg_sum_1"),
            fixture(
                3,
                "agg_group_count",
                "m15_agg_group_count_pub",
                "m15_agg_group_count_1",
            ),
            fixture(
                4,
                "agg_group_sum",
                "m15_agg_group_sum_pub",
                "m15_agg_group_sum_1",
            ),
            fixture(5, "agg_multi", "m15_agg_multi_pub", "m15_agg_multi_1"),
            fixture(6, "agg_extrema", "m15_agg_extrema_pub", "m15_agg_extrema_1"),
        ],
        target_relation: oid(client, "agg_group_sum_target.rows"),
        target_identity: oid(client, "agg_group_sum_target.rows_pkey"),
        target_publication: publication_oid(client, "m15_agg_group_sum_target_pub"),
        multi_target_relation: oid(client, "agg_multi_target.rows"),
        multi_target_identity: oid(client, "agg_multi_target.rows_pkey"),
        multi_target_publication: publication_oid(client, "m15_agg_multi_target_pub"),
    }
}

pub(crate) fn bootstrap_and_detach(
    database_url: &str,
    replication_url: &str,
    fixture: &GraphFixture,
    client: &mut Client,
) {
    let mut bootstrap = BootstrapSession::begin(
        database_url,
        replication_url,
        BootstrapSpec {
            graph_id: GraphId::new(fixture.graph).expect("graph ID"),
            bootstrap_id: BootstrapId::new(fixture.graph).expect("bootstrap ID"),
            publication_oid: fixture.publication_oid,
            slot_name: fixture.slot.to_owned(),
            slot_generation: SlotGeneration::new(1).expect("generation"),
        },
        options(),
    )
    .expect("export aggregate snapshot");
    assert_building(client, fixture.graph);
    while let SnapshotProgress::BatchApplied { rows, .. } =
        bootstrap.scan_next().expect("scan aggregate snapshot")
    {
        assert!((1..=2).contains(&rows));
        assert_building(client, fixture.graph);
    }
    let mut catchup = bootstrap.into_catchup().expect("enter aggregate catch-up");
    while let BootstrapCatchupProgress::TransactionApplied =
        catchup.catch_up_next().expect("advance aggregate catch-up")
    {}
    catchup
        .into_live()
        .expect("enter aggregate live")
        .detach()
        .expect("detach aggregate live");
}

pub(crate) fn assert_registration_contracts(client: &mut Client) {
    let rows = client
        .query(
            "SELECT graph_id,source_count,compiler_version,
                    pg_catalog.convert_from(spec_payload,'UTF8') LIKE '%SELECT%'
             FROM shiba_internal.graph_definition ORDER BY graph_id",
            &[],
        )
        .expect("query registered aggregate contracts");
    assert_eq!(rows.len(), 6);
    for (ordinal, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i64>(0), i64::try_from(ordinal + 1).unwrap());
        assert_eq!(row.get::<_, i16>(1), 1);
        assert_eq!(row.get::<_, i32>(2), 4);
        assert!(!row.get::<_, bool>(3));
    }
}

pub(crate) fn assert_building(client: &mut Client, graph: u64) {
    let graph = i64::try_from(graph).expect("graph ID fits");
    let rows = client
        .query(
            "SELECT result_status
             FROM shiba.graph_result WHERE graph_id=$1",
            &[&graph],
        )
        .expect("query building aggregate result");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "building");
    assert!(
        client
            .query(
                "SELECT 1 FROM shiba.graph_result_rows WHERE graph_id=$1",
                &[&graph]
            )
            .expect("query hidden aggregate rows")
            .is_empty()
    );
}

pub(crate) fn assert_oracle(client: &mut Client, fixture: &GraphFixture) {
    match fixture.graph {
        1 => scalar::assert_count(client, fixture),
        2 => scalar::assert_sum(client, fixture),
        3 => grouped::assert_count(client, fixture),
        4 => grouped::assert_sum(client, fixture),
        5 => scalar::assert_multi_call(client, fixture),
        6 => scalar::assert_min_max(client, fixture),
        _ => panic!("unknown aggregate graph"),
    }
}

pub(crate) fn wait_for_slot_lsn(client: &mut Client, slot: &str, lsn: u64) {
    integration_support::wait_for_slot_lsn(client, slot, lsn);
}

pub(crate) fn publication_oid(client: &mut Client, publication: &str) -> u32 {
    client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname=$1",
            &[&publication],
        )
        .expect("resolve publication OID")
        .get(0)
}

pub(crate) fn oid(client: &mut Client, object: &str) -> u32 {
    client
        .query_one("SELECT $1::text::regclass::oid", &[&object])
        .expect("resolve object OID")
        .get(0)
}
