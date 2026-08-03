use std::num::NonZeroU32;

use postgres::Client;
use shiba_compiler::{GRAPH_SPEC_VERSION, GraphOutputSpecV1, GraphSpecV1};
use shiba_ingress::{RebuildIdentity, RebuildMemberIdentity, RebuildSpec, SnapshotProgress};
use shiba_operator::{NodeId, ObjectAddress};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};
use shiba_runtime::{RebuildSourceTarget, compile_and_register, compile_rebuild_graph};

pub(crate) struct Fixture {
    pub(crate) publication_oid: u32,
    pub(crate) left_relation: u32,
    pub(crate) right_relation: u32,
    pub(crate) left_identity: u32,
    pub(crate) right_identity: u32,
}

pub(crate) fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m14-join-lifecycle.sh must set {name}"))
}

pub(crate) fn install(client: &mut Client, publication: &str) -> Fixture {
    client
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA left_source;
             CREATE SCHEMA right_source;
             CREATE TABLE left_source.events (
                 id bigint PRIMARY KEY, right_key bigint NULL
             );
             CREATE TABLE right_source.events (
                 id bigint PRIMARY KEY, payload bigint NULL
             );
             INSERT INTO left_source.events VALUES (1,10),(2,20),(3,NULL);
             INSERT INTO right_source.events VALUES (10,100),(20,NULL);
             CREATE PUBLICATION {publication}
                 FOR TABLE left_source.events, right_source.events
                 WITH (publish='insert, update, delete');
             SELECT shiba_internal.register_source(1, 'left_source.events'::regclass);
             SELECT shiba_internal.register_source(2, 'right_source.events'::regclass);"
        ))
        .expect("install cross-schema join sources");
    Fixture {
        publication_oid: publication_oid(client, publication),
        left_relation: oid(client, "left_source.events"),
        right_relation: oid(client, "right_source.events"),
        left_identity: oid(client, "left_source.events_pkey"),
        right_identity: oid(client, "right_source.events_pkey"),
    }
}

pub(crate) fn register_join_graph(client: &mut Client, fixture: &Fixture) {
    let left = SourceId::new(1).expect("left source ID");
    let right = SourceId::new(2).expect("right source ID");
    let spec = GraphSpecV1 {
        version: GRAPH_SPEC_VERSION,
        graph_id: GraphId::new(1).expect("graph ID"),
        sources: vec![left, right],
        outputs: vec![GraphOutputSpecV1::InnerJoin {
            left_source_id: left,
            right_source_id: right,
            left_id_column: "id".to_owned(),
            left_right_key_column: "right_key".to_owned(),
            right_id_column: "id".to_owned(),
            right_payload_column: "payload".to_owned(),
            right_identity_index: ObjectAddress {
                class_id: oid(client, "pg_class"),
                object_id: fixture.right_identity,
                sub_id: 0,
            },
            join_node_id: node(1),
            result_node_id: node(2),
        }],
    };
    compile_and_register(client, &spec).expect("register exact two-source graph");
}

pub(crate) fn assert_registered(client: &mut Client, fixture: &Fixture) {
    let row = client
        .query_one(
            "SELECT definition.source_count,
                    (SELECT count(*) FROM shiba_internal.graph_source_member
                     WHERE graph_id=definition.graph_id),
                    result.output_shape, result.result_status
             FROM shiba_internal.graph_definition AS definition
             JOIN shiba.graph_result AS result USING (graph_id)
             WHERE definition.graph_id=1 AND result.result_id=2",
            &[],
        )
        .expect("read registered graph authority");
    assert_eq!(row.get::<_, i16>(0), 2);
    assert_eq!(row.get::<_, i64>(1), 2);
    assert_eq!(row.get::<_, &str>(2), "keyed");
    assert_eq!(row.get::<_, &str>(3), "active");
    let relations = client
        .query(
            "SELECT source_id,address_objid::bigint FROM shiba_internal.source_binding
             WHERE binding_kind='relation' AND source_id IN (1,2) ORDER BY source_id",
            &[],
        )
        .expect("read member bindings")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, i64>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        relations,
        vec![
            (1, i64::from(fixture.left_relation)),
            (2, i64::from(fixture.right_relation))
        ]
    );
}

pub(crate) fn scan_all(bootstrap: &mut shiba_ingress::BootstrapSession, client: &mut Client) {
    let mut batches = 0;
    while let SnapshotProgress::BatchApplied { rows, .. } =
        bootstrap.scan_next().expect("scan one bounded graph batch")
    {
        assert!((1..=2).contains(&rows));
        batches += 1;
        assert_building(client);
    }
    assert!(
        batches >= 2,
        "both source members must produce snapshot batches"
    );
    let checkpoints: Vec<(i64, i64)> = client
        .query(
            "SELECT source_id,last_batch_ordinal
             FROM shiba_internal.graph_bootstrap_checkpoint
             WHERE graph_id=1 ORDER BY source_id",
            &[],
        )
        .expect("read both member checkpoints")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(checkpoints.len(), 2);
    assert!(checkpoints.iter().all(|(_, ordinal)| *ordinal > 0));
}

pub(crate) fn assert_snapshot_rows(client: &mut Client) {
    let rows = source_rows(client);
    assert_eq!(
        rows,
        vec![
            (1, 1, Some(10)),
            (1, 2, Some(20)),
            (1, 3, None),
            (2, 10, Some(100)),
            (2, 20, None),
        ]
    );
}

pub(crate) fn assert_building(client: &mut Client) {
    let row = client
        .query_one(
            "SELECT result_status,value_bigint FROM shiba.graph_result
             WHERE graph_id=1 AND result_id=2",
            &[],
        )
        .expect("read public building result");
    assert_eq!(row.get::<_, &str>(0), "building");
    assert_eq!(row.get::<_, Option<i64>>(1), None);
    assert!(
        client
            .query(
                "SELECT * FROM shiba.graph_result_rows WHERE graph_id=1 AND result_id=2",
                &[],
            )
            .expect("query hidden public keyed rows")
            .is_empty()
    );
}

pub(crate) fn assert_oracle(client: &mut Client) {
    let expected = client
        .query(
            "SELECT left_row.id,right_row.payload
             FROM left_source.events AS left_row
             JOIN right_source.events AS right_row ON right_row.id=left_row.right_key
             ORDER BY left_row.id",
            &[],
        )
        .expect("query SQL join oracle")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
        .collect::<Vec<_>>();
    let actual = client
        .query(
            "SELECT result_key_bigint,result_value_bigint
             FROM shiba.graph_result_rows
             WHERE graph_id=1 AND result_id=2 ORDER BY result_key_bigint",
            &[],
        )
        .expect("query materialized join result")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let status: String = client
        .query_one(
            "SELECT result_status FROM shiba.graph_result WHERE graph_id=1 AND result_id=2",
            &[],
        )
        .expect("read active result status")
        .get(0);
    assert_eq!(status, "active");
}

pub(crate) fn same_binding_rebuild(
    client: &mut Client,
    fixture: &Fixture,
    old_slot: &str,
    new_slot: &str,
) -> RebuildSpec {
    let digest: Vec<u8> = client
        .query_one(
            "SELECT graph_digest FROM shiba_internal.graph_definition WHERE graph_id=1",
            &[],
        )
        .expect("read active graph digest")
        .get(0);
    let digest: [u8; 32] = digest.try_into().expect("32-byte graph digest");
    let targets = [
        RebuildSourceTarget {
            source_id: SourceId::new(1).expect("left source ID"),
            relation_id: fixture.left_relation,
            identity_index_id: fixture.left_identity,
        },
        RebuildSourceTarget {
            source_id: SourceId::new(2).expect("right source ID"),
            relation_id: fixture.right_relation,
            identity_index_id: fixture.right_identity,
        },
    ];
    let mut transaction = client.transaction().expect("open compilation transaction");
    let artifact = compile_rebuild_graph(
        &mut transaction,
        GraphId::new(1).expect("graph ID"),
        &targets,
    )
    .expect("compile same-binding rebuild graph");
    transaction
        .rollback()
        .expect("rollback read-only compilation");
    assert_eq!(artifact.graph_digest, digest);
    let members = vec![
        RebuildMemberIdentity {
            source_id: SourceId::new(1).expect("left source ID"),
            relation_oid: fixture.left_relation,
            identity_index_oid: fixture.left_identity,
        },
        RebuildMemberIdentity {
            source_id: SourceId::new(2).expect("right source ID"),
            relation_oid: fixture.right_relation,
            identity_index_oid: fixture.right_identity,
        },
    ];
    RebuildSpec {
        graph_id: GraphId::new(1).expect("graph ID"),
        expected: RebuildIdentity {
            bootstrap_id: BootstrapId::new(1).expect("old bootstrap ID"),
            graph_digest: digest,
            members: members.clone(),
            publication_oid: fixture.publication_oid,
            slot_name: old_slot.to_owned(),
            slot_generation: SlotGeneration::new(1).expect("old generation"),
        },
        target: RebuildIdentity {
            bootstrap_id: BootstrapId::new(2).expect("target bootstrap ID"),
            graph_digest: digest,
            members,
            publication_oid: fixture.publication_oid,
            slot_name: new_slot.to_owned(),
            slot_generation: SlotGeneration::new(2).expect("target generation"),
        },
    }
}

pub(crate) fn assert_generation(client: &mut Client, generation: i64, slot: &str) {
    let row = client
        .query_one(
            "SELECT bootstrap.slot_generation,bootstrap.slot_name::text,
                    bool_and(continuation.slot_generation=$2)
             FROM shiba_internal.graph_bootstrap AS bootstrap
             JOIN shiba_internal.graph_continuation AS continuation USING (graph_id)
             WHERE bootstrap.graph_id=1 AND bootstrap.slot_name=$1::text::name
             GROUP BY bootstrap.slot_generation,bootstrap.slot_name",
            &[&slot, &generation],
        )
        .expect("read sole graph generation and continuation");
    assert_eq!(row.get::<_, i64>(0), generation);
    assert_eq!(row.get::<_, &str>(1), slot);
    assert!(row.get::<_, bool>(2));
}

pub(crate) fn assert_continuations(client: &mut Client, generation: i64, count: i64) {
    let row = client
        .query_one(
            "SELECT count(*), COALESCE(bool_and(slot_generation=$1), true)
             FROM shiba_internal.graph_continuation WHERE graph_id=1",
            &[&generation],
        )
        .expect("read graph continuation cardinality");
    assert_eq!(row.get::<_, i64>(0), count);
    assert!(row.get::<_, bool>(1));
}

pub(crate) fn assert_feedback(client: &mut Client, slot: &str, expected: u64) {
    for _ in 0..10_000 {
        if slot_lsn(client, slot) >= expected {
            return;
        }
    }
    panic!("slot {slot} did not confirm exact durable end LSN {expected:#x}");
}

pub(crate) fn slot_lsn(client: &mut Client, slot: &str) -> u64 {
    let value: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots
             WHERE slot_name=$1",
            &[&slot],
        )
        .expect("read slot confirmed flush LSN")
        .get(0);
    parse_lsn(&value)
}

fn source_rows(client: &mut Client) -> Vec<(i64, i64, Option<i64>)> {
    client
        .query(
            "SELECT source_id,source_row_id,payload_int8
             FROM shiba_internal.source_row_state ORDER BY source_id,source_row_id",
            &[],
        )
        .expect("read graph source row state")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

fn node(value: u32) -> NodeId {
    NodeId::new(NonZeroU32::new(value).expect("node ID"))
}

fn oid(client: &mut Client, name: &str) -> u32 {
    client
        .query_one("SELECT $1::text::regclass::oid", &[&name])
        .expect("resolve object address")
        .get(0)
}

fn publication_oid(client: &mut Client, name: &str) -> u32 {
    client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_publication WHERE pubname=$1",
            &[&name],
        )
        .expect("resolve publication identity")
        .get(0)
}

fn parse_lsn(value: &str) -> u64 {
    let (high, low) = value.split_once('/').expect("PostgreSQL LSN shape");
    (u64::from_str_radix(high, 16).expect("LSN high") << 32)
        | u64::from_str_radix(low, 16).expect("LSN low")
}
