use postgres::{Client, NoTls};
use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryExpressionV1, QueryFieldV1, QueryInputV1, QueryNodeV1,
    QueryOperationV1, QueryResultFieldV1, QueryResultV1, QuerySelectorV1, QuerySpecV1,
};
use shiba_operator::TypedValue;
use shiba_protocol::{GraphId, SlotGeneration, SourceId};
use shiba_runtime::{
    M2Error, PgoutputSource, ProcessOutcome, compile_and_register, decode_committed_changes,
    process,
};

#[path = "m13_operator_kernel/state.rs"]
mod state;
mod support;

use state::{durable, pairs};
use support::{PgoutputCapture, set_scalar_int8_result};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m13-operator-kernel.sh",
    env_prefix: "SHIBA_M13_OPERATOR_KERNEL",
    slot: "shiba_m13_operator_kernel_slot",
    publication: "shiba_m13_operator_kernel_pub",
};

fn spec() -> QuerySpecV1 {
    let source_id = SourceId::new(1).expect("source id");
    let source = || QueryInputV1::Source { source_id };
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
        graph_id: GraphId::new(1).expect("graph id"),
        sources: vec![source_id],
        nodes: vec![
            QueryNodeV1 {
                inputs: vec![source()],
                state_codec_version: Some(1),
                operation: QueryOperationV1::CountRows,
            },
            QueryNodeV1 {
                inputs: vec![source()],
                state_codec_version: Some(1),
                operation: QueryOperationV1::SumInt8 {
                    value: column("payload"),
                },
            },
            QueryNodeV1 {
                inputs: vec![source()],
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
                fields: vec![QueryResultFieldV1 {
                    name: "sum".into(),
                    value_slot: 0,
                    nullable: true,
                }],
                key_ordinals: vec![],
            },
            QueryResultV1 {
                input_node: 3,
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
) -> (Vec<u8>, shiba_runtime::GraphTransaction) {
    client.batch_execute(sql).expect("commit source DML");
    let wire = CAPTURE.capture(client, name);
    let input = decode_committed_changes(&wire, &support::singleton_graph(1, source))
        .expect("decode committed pgoutput");
    (wire, input)
}

fn assert_oracle(client: &mut Client, count: i64, sum: i64) {
    let oracle = pairs(client, "SELECT id, payload FROM source.events ORDER BY id");
    let state = durable(client);
    assert_eq!(state.scalar, (count, sum));
    assert_eq!(state.keyed, oracle);
    assert_eq!(state.source, oracle);
}

fn install_sink_failure(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m13_failure;
             CREATE FUNCTION m13_failure.reject_keyed_sink() RETURNS trigger
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'keyed sink failure'; END $$;
             CREATE TRIGGER reject_keyed_sink BEFORE INSERT OR UPDATE OR DELETE
             ON shiba_internal.graph_result_row FOR EACH ROW
             EXECUTE FUNCTION m13_failure.reject_keyed_sink();",
        )
        .expect("install keyed sink failure");
}

#[test]
#[ignore = "requires scripts/test-m13-operator-kernel.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered atomicity and replay proof"
)]
fn generic_kernel_persists_scalar_and_keyed_outputs_atomically() {
    let mut client = Client::connect(&CAPTURE.required("DATABASE_URL"), NoTls).expect("connect");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION shiba_m13_operator_kernel_pub FOR TABLE source.events;
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);",
        )
        .expect("install source authority");
    compile_and_register(&mut client, &spec()).expect("register generic graph");
    let relation: i64 = client
        .query_one("SELECT 'source.events'::regclass::oid::bigint", &[])
        .expect("relation oid")
        .get(0);
    let source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("source"),
        SlotGeneration::new(1).expect("generation"),
        u32::try_from(relation).expect("relation oid fits"),
    );
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    let (_, insert) = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (1, 10), (2, NULL)",
        "insert.pgoutput",
    );
    assert_eq!(
        process(&mut client, &insert).unwrap(),
        ProcessOutcome::Applied
    );
    assert_oracle(&mut client, 2, 10);
    let (_, update) = capture(
        &mut client,
        source,
        "UPDATE source.events SET payload = 7 WHERE id = 2",
        "update.pgoutput",
    );
    assert_eq!(
        process(&mut client, &update).unwrap(),
        ProcessOutcome::Applied
    );
    assert_oracle(&mut client, 2, 17);
    let (key_wire, key_change) = capture(
        &mut client,
        source,
        "UPDATE source.events SET id = 3, payload = NULL WHERE id = 1",
        "key-change.pgoutput",
    );
    assert!(
        key_wire
            .windows(6)
            .any(|frame| frame[0] == b'U' && frame[5] == b'K')
    );
    assert_eq!(
        process(&mut client, &key_change).unwrap(),
        ProcessOutcome::Applied
    );
    assert_oracle(&mut client, 2, 7);
    let (_, delete) = capture(
        &mut client,
        source,
        "DELETE FROM source.events WHERE id = 2",
        "delete.pgoutput",
    );
    assert_eq!(
        process(&mut client, &delete).unwrap(),
        ProcessOutcome::Applied
    );
    assert_oracle(&mut client, 1, 0);
    let after_delete = durable(&mut client);
    assert_eq!(
        process(&mut client, &delete).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable(&mut client), after_delete);

    let (_, sink_input) = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (4, 40)",
        "sink-failure.pgoutput",
    );
    install_sink_failure(&mut client);
    assert!(matches!(
        process(&mut client, &sink_input),
        Err(M2Error::Postgres(_))
    ));
    assert_eq!(durable(&mut client), after_delete);
    client
        .batch_execute("DROP SCHEMA m13_failure CASCADE")
        .unwrap();
    assert_eq!(
        process(&mut client, &sink_input).unwrap(),
        ProcessOutcome::Applied
    );
    assert_oracle(&mut client, 2, 40);

    let (_, corrupt_input) = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (5, 1)",
        "corrupt.pgoutput",
    );
    let before_corrupt = durable(&mut client);
    let plan: Vec<u8> = client
        .query_one(
            "SELECT graph_payload FROM shiba_internal.graph_definition WHERE graph_id=1",
            &[],
        )
        .unwrap()
        .get(0);
    client.execute("UPDATE shiba_internal.graph_definition SET graph_payload=graph_payload || decode('20','hex') WHERE graph_id=1", &[]).unwrap();
    assert!(matches!(
        process(&mut client, &corrupt_input),
        Err(M2Error::InvalidOperatorDefinition)
    ));
    client
        .execute(
            "UPDATE shiba_internal.graph_definition SET graph_payload=$1 WHERE graph_id=1",
            &[&plan],
        )
        .unwrap();
    assert_eq!(durable(&mut client), before_corrupt);
    let scalar_partition = TypedValue::Bool(true)
        .to_canonical_json()
        .expect("canonical scalar state partition");
    let scalar_item = b"null".as_slice();
    let state: Vec<u8> = client
        .query_one(
            "SELECT state_payload FROM shiba_internal.graph_node_state
             WHERE graph_id=1 AND node_id=1 AND namespace=0
               AND partition_key_payload=$1 AND item_key_payload=$2",
            &[&scalar_partition, &scalar_item],
        )
        .unwrap()
        .get(0);
    client
        .execute(
            "UPDATE shiba_internal.graph_node_state SET state_payload=decode('00','hex')
         WHERE graph_id=1 AND node_id=1 AND namespace=0
           AND partition_key_payload=$1 AND item_key_payload=$2",
            &[&scalar_partition, &scalar_item],
        )
        .unwrap();
    assert!(matches!(
        process(&mut client, &corrupt_input),
        Err(M2Error::Kernel(_))
    ));
    client
        .execute(
            "UPDATE shiba_internal.graph_node_state SET state_payload=$1
             WHERE graph_id=1 AND node_id=1 AND namespace=0
               AND partition_key_payload=$2 AND item_key_payload=$3",
            &[&state, &scalar_partition, &scalar_item],
        )
        .unwrap();
    assert_eq!(durable(&mut client), before_corrupt);

    client
        .execute(
            "UPDATE shiba_internal.graph_node_state
         SET state_payload=decode('7fffffffffffffff','hex')
         WHERE graph_id=1 AND node_id=2 AND namespace=0
           AND partition_key_payload=$1 AND item_key_payload=$2",
            &[&scalar_partition, &scalar_item],
        )
        .unwrap();
    set_scalar_int8_result(&mut client, 1, 5, Some(i64::MAX));
    let overflow = durable(&mut client);
    assert!(matches!(
        process(&mut client, &corrupt_input),
        Err(M2Error::Kernel(_))
    ));
    assert_eq!(durable(&mut client), overflow);
    client
        .execute(
            "UPDATE shiba_internal.graph_node_state
         SET state_payload=decode('0000000000000028','hex')
         WHERE graph_id=1 AND node_id=2 AND namespace=0
           AND partition_key_payload=$1 AND item_key_payload=$2",
            &[&scalar_partition, &scalar_item],
        )
        .unwrap();
    set_scalar_int8_result(&mut client, 1, 5, Some(40));
    assert_eq!(
        process(&mut client, &corrupt_input).unwrap(),
        ProcessOutcome::Applied
    );
    assert_oracle(&mut client, 3, 41);

    client.batch_execute("ALTER TABLE source.events DROP COLUMN payload; ALTER TABLE source.events ADD COLUMN payload bigint").unwrap();
    let (_, invalidated) = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (6, 6)",
        "invalidated.pgoutput",
    );
    let before_invalidated = durable(&mut client);
    assert!(matches!(
        process(&mut client, &invalidated),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(durable(&mut client), before_invalidated);
}
