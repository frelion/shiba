use postgres::Client;
use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryFieldV1, QueryInputV1, QueryNodeV1, QueryOperationV1,
    QueryResultFieldV1, QueryResultV1, QuerySelectorV1, QuerySpecV1,
};
use shiba_ingress::SnapshotProgress;
use shiba_protocol::{GraphId, SourceId};
use shiba_runtime::compile_and_register;

#[path = "support/rebuild.rs"]
mod rebuild;

pub(crate) use rebuild::{
    assert_continuations, assert_feedback, assert_generation, same_binding_rebuild, slot_lsn,
};

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

pub(crate) fn register_join_graph(client: &mut Client, _fixture: &Fixture) {
    let left = SourceId::new(1).expect("left source ID");
    let right = SourceId::new(2).expect("right source ID");
    let spec = QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(1).expect("graph ID"),
        sources: vec![left, right],
        nodes: vec![QueryNodeV1 {
            inputs: vec![
                QueryInputV1::Source { source_id: left },
                QueryInputV1::Source { source_id: right },
            ],
            state_codec_version: Some(1),
            operation: QueryOperationV1::InnerJoin {
                left_id: named_field(0, "id"),
                left_key: named_field(0, "right_key"),
                right_id: named_field(1, "id"),
                right_payload: named_field(1, "payload"),
            },
        }],
        results: vec![QueryResultV1 {
            input_node: 1,
            fields: vec![
                QueryResultFieldV1 {
                    name: "id".into(),
                    value_slot: 0,
                    nullable: false,
                },
                QueryResultFieldV1 {
                    name: "payload".into(),
                    value_slot: 1,
                    nullable: true,
                },
            ],
            key_ordinals: vec![1],
        }],
    };
    compile_and_register(client, &spec).expect("register exact two-source graph");
}

fn named_field(input: u8, name: &str) -> QueryFieldV1 {
    QueryFieldV1 {
        input,
        selector: QuerySelectorV1::Name {
            name: name.into(),
            quoted: false,
        },
    }
}

pub(crate) fn assert_registered(client: &mut Client, fixture: &Fixture) {
    let row = client
        .query_one(
            "SELECT definition.source_count,
                    (SELECT count(*) FROM shiba_internal.graph_source_member
                     WHERE graph_id=definition.graph_id),
                    pg_catalog.convert_from(result.schema_payload,'UTF8')::jsonb
                        #> '{key_ordinals}' = '[1]'::jsonb,
                    result.result_status
             FROM shiba_internal.graph_definition AS definition
             JOIN shiba.graph_result AS result USING (graph_id)
             WHERE definition.graph_id=1 AND result.result_id=2",
            &[],
        )
        .expect("read registered graph authority");
    assert_eq!(row.get::<_, i16>(0), 2);
    assert_eq!(row.get::<_, i64>(1), 2);
    assert!(row.get::<_, bool>(2));
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
            "SELECT result_status FROM shiba.graph_result
             WHERE graph_id=1 AND result_id=2",
            &[],
        )
        .expect("read public building result");
    assert_eq!(row.get::<_, &str>(0), "building");
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
            "SELECT
                (convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,value}')::bigint,
                CASE WHEN convert_from(row_payload,'UTF8')::jsonb
                               #>> '{values,1,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb
                               #>> '{values,1,value}')::bigint END
             FROM shiba.graph_result_rows
             WHERE graph_id=1 AND result_id=2 ORDER BY 1",
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
