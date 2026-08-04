use postgres::{Client, NoTls};
use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryExpressionV1, QueryFieldV1, QueryInputV1, QueryNodeV1,
    QueryOperationV1, QueryResultFieldV1, QueryResultV1, QuerySelectorV1, QuerySpecV1,
};
use shiba_operator::TypedValue;
use shiba_protocol::{GraphId, SlotGeneration, SourceId};
use shiba_runtime::{
    PgoutputSource, ProcessOutcome, compile_and_register, decode_committed_changes, process,
};

mod support;

use support::{
    PgoutputCapture, canonical_result_rows, count_rows, keyed_int8_results, scalar_int8_result,
};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m16-wide-results.sh",
    env_prefix: "SHIBA_M16_WIDE_RESULTS",
    slot: "shiba_m16_wide_results_slot",
    publication: "shiba_m16_wide_results_pub",
};

fn spec() -> QuerySpecV1 {
    let source_id = SourceId::new(1).unwrap();
    let column = |name: &str| QueryExpressionV1::Column {
        field: QueryFieldV1 {
            input: 0,
            selector: QuerySelectorV1::Name {
                name: name.into(),
                quoted: false,
            },
        },
    };
    QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(1).unwrap(),
        sources: vec![source_id],
        nodes: vec![
            QueryNodeV1 {
                inputs: vec![QueryInputV1::Source { source_id }],
                state_codec_version: Some(1),
                operation: count_rows(),
            },
            QueryNodeV1 {
                inputs: vec![QueryInputV1::Source { source_id }],
                state_codec_version: None,
                operation: QueryOperationV1::Project {
                    expressions: vec![column("id"), column("payload")],
                },
            },
        ],
        results: vec![
            QueryResultV1 {
                input_node: 1,
                fields: vec![QueryResultFieldV1 {
                    name: "count".into(),
                    value_slot: 0,
                    nullable: false,
                }],
                key_ordinals: vec![],
            },
            QueryResultV1 {
                input_node: 2,
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
            },
        ],
    }
}

fn capture(
    client: &mut Client,
    source: PgoutputSource,
    sql: &str,
    name: &str,
) -> shiba_runtime::GraphTransaction {
    client.batch_execute(sql).expect("commit source DML");
    decode_committed_changes(
        &CAPTURE.capture(client, name),
        &support::singleton_graph(1, source),
    )
    .expect("decode committed source transaction")
}

fn durable(client: &mut Client) -> String {
    client
        .query_one(
            "SELECT jsonb_build_object(
                'source',(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY source_row_id),'[]')
                          FROM shiba_internal.source_row_state s),
                'state',(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY node_id,partition_key_payload),'[]')
                         FROM shiba_internal.graph_node_state s),
                'rows',(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY result_id,row_identity),'[]')
                        FROM shiba_internal.graph_result_row r),
                'continuation',(SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY commit_lsn),'[]')
                                FROM shiba_internal.graph_continuation c))::text",
            &[],
        )
        .expect("snapshot all Runtime-owned durable state")
        .get(0)
}

fn setup(client: &mut Client) -> PgoutputSource {
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION shiba_m16_wide_results_pub FOR TABLE source.events;
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);",
        )
        .expect("install source");
    compile_and_register(client, &spec()).expect("register wide result graph");
    let relation: i64 = client
        .query_one("SELECT 'source.events'::regclass::oid::bigint", &[])
        .unwrap()
        .get(0);
    let source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).unwrap(),
        SlotGeneration::new(1).unwrap(),
        u32::try_from(relation).unwrap(),
    );
    CAPTURE.create_slot();
    support::configure_graph_ingress(client, 1, CAPTURE.publication, CAPTURE.slot);
    source
}

fn assert_initial_wide_rows(client: &mut Client, source: PgoutputSource) {
    let insert = capture(
        client,
        source,
        "INSERT INTO source.events VALUES (1,10),(2,NULL)",
        "insert.pgoutput",
    );
    assert_eq!(process(client, &insert).unwrap(), ProcessOutcome::Applied);
    assert_eq!(scalar_int8_result(client, 1, 3), Some(2));
    assert_eq!(
        keyed_int8_results(client, 1, 4),
        vec![(Some(1), Some(10)), (Some(2), None)]
    );
    let (scalar_schema, scalar_rows) = canonical_result_rows(client, 1, 3);
    assert_eq!(scalar_schema.fields.len(), 1);
    assert_eq!(scalar_rows[0].values, [TypedValue::Int8(2)]);
    let (keyed_schema, _) = canonical_result_rows(client, 1, 4);
    assert_eq!(keyed_schema.fields.len(), 2);
    assert_eq!(keyed_schema.key_ordinals, [1]);
}

fn assert_sink_rollback_retry_and_replay(client: &mut Client, source: PgoutputSource) {
    let update = capture(
        client,
        source,
        "UPDATE source.events SET payload=20 WHERE id=1",
        "update.pgoutput",
    );
    client
        .batch_execute(
            "CREATE FUNCTION shiba_internal.m16_reject_sink() RETURNS trigger
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'reject wide sink'; END $$;
             CREATE TRIGGER m16_reject_sink BEFORE INSERT OR UPDATE OR DELETE
             ON shiba_internal.graph_result_row FOR EACH ROW
             EXECUTE FUNCTION shiba_internal.m16_reject_sink();",
        )
        .expect("install sink failure");
    let before_failure = durable(client);
    assert!(process(client, &update).is_err());
    assert_eq!(durable(client), before_failure);
    client
        .batch_execute(
            "DROP TRIGGER m16_reject_sink ON shiba_internal.graph_result_row;
             DROP FUNCTION shiba_internal.m16_reject_sink();",
        )
        .expect("remove sink failure");
    assert_eq!(process(client, &update).unwrap(), ProcessOutcome::Applied);
    let after_retry = durable(client);
    assert_eq!(
        keyed_int8_results(client, 1, 4),
        vec![(Some(1), Some(20)), (Some(2), None)]
    );
    assert_eq!(
        process(client, &update).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable(client), after_retry);
}

fn assert_schema_corruption_fails_closed(client: &mut Client, source: PgoutputSource) {
    let input = capture(
        client,
        source,
        "UPDATE source.events SET payload=30 WHERE id=1",
        "corrupt-schema.pgoutput",
    );
    let original: Vec<u8> = client
        .query_one(
            "SELECT schema_payload FROM shiba.graph_result WHERE graph_id=1 AND result_id=4",
            &[],
        )
        .unwrap()
        .get(0);
    client
        .execute(
            "UPDATE shiba.graph_result SET schema_payload=$1 WHERE graph_id=1 AND result_id=4",
            &[&b"{}".as_slice()],
        )
        .unwrap();
    let before = durable(client);
    assert!(process(client, &input).is_err());
    assert_eq!(durable(client), before);
    client
        .execute(
            "UPDATE shiba.graph_result SET schema_payload=$1 WHERE graph_id=1 AND result_id=4",
            &[&original],
        )
        .unwrap();
    assert_eq!(process(client, &input).unwrap(), ProcessOutcome::Applied);
}

#[test]
#[ignore = "requires scripts/test-m16-wide-results.sh"]
fn wide_schema_rows_sink_rollback_retry_and_replay_are_atomic() {
    let mut client = Client::connect(&CAPTURE.required("DATABASE_URL"), NoTls).expect("connect");
    let source = setup(&mut client);
    assert_initial_wide_rows(&mut client, source);
    assert_sink_rollback_retry_and_replay(&mut client, source);
    assert_schema_corruption_fails_closed(&mut client, source);

    client
        .execute(
            "UPDATE shiba.graph_result SET result_status='building' WHERE graph_id=1",
            &[],
        )
        .unwrap();
    let public_rows: i64 = client
        .query_one(
            "SELECT count(*) FROM shiba.graph_result_rows WHERE graph_id=1",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(public_rows, 0);
}
