use postgres::{Client, NoTls};
use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryExpressionV1, QueryFieldV1, QueryInputV1, QueryNodeV1,
    QueryOperationV1, QueryResultShapeV1, QueryResultV1, QuerySelectorV1, QuerySpecV1,
};
use shiba_protocol::{GraphId, SlotGeneration, SourceId};
use shiba_runtime::{
    M2Error, PgoutputSource, ProcessOutcome, compile_and_register, decode_committed_changes,
    process,
};

#[path = "m14_grouped/support.rs"]
mod grouped_support;
mod support;

use grouped_support::{
    assert_sql_oracle, durable_snapshot, node_state_payload, set_node_state_payload,
};
use support::PgoutputCapture;

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m14-grouped.sh",
    env_prefix: "SHIBA_M14_GROUPED",
    slot: "shiba_m14_grouped_slot",
    publication: "shiba_m14_grouped_pub",
};

fn spec() -> QuerySpecV1 {
    let source_id = SourceId::new(1).expect("source ID");
    let source = || QueryInputV1::Source { source_id };
    let key_by = |name: &str| QueryNodeV1 {
        inputs: vec![source()],
        state_codec_version: None,
        operation: QueryOperationV1::KeyBy {
            key: QueryExpressionV1::Column {
                field: name_field(name),
            },
        },
    };
    QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(1).expect("graph ID"),
        sources: vec![source_id],
        nodes: vec![
            key_by("payload"),
            QueryNodeV1 {
                inputs: vec![QueryInputV1::Node { node: 1 }],
                state_codec_version: Some(1),
                operation: QueryOperationV1::GroupedCount { key: slot_field(2) },
            },
            key_by("payload"),
            QueryNodeV1 {
                inputs: vec![QueryInputV1::Node { node: 3 }],
                state_codec_version: Some(1),
                operation: QueryOperationV1::GroupedSumInt8 {
                    key: slot_field(2),
                    value: slot_field(0),
                },
            },
            key_by("id"),
            QueryNodeV1 {
                inputs: vec![QueryInputV1::Node { node: 5 }],
                state_codec_version: Some(1),
                operation: QueryOperationV1::GroupedSumInt8 {
                    key: slot_field(2),
                    value: slot_field(1),
                },
            },
        ],
        results: (2..=6)
            .step_by(2)
            .map(|input_node| QueryResultV1 {
                input_node,
                shape: QueryResultShapeV1::Keyed {
                    key_slot: 0,
                    key_nullable: input_node != 6,
                    value_slot: 1,
                    value_nullable: input_node == 6,
                },
            })
            .collect(),
    }
}

fn name_field(name: &str) -> QueryFieldV1 {
    QueryFieldV1 {
        input: 0,
        selector: QuerySelectorV1::Name {
            name: name.into(),
            quoted: false,
        },
    }
}

fn slot_field(slot: u16) -> QueryFieldV1 {
    QueryFieldV1 {
        input: 0,
        selector: QuerySelectorV1::Slot { slot },
    }
}

fn capture(
    client: &mut Client,
    source: PgoutputSource,
    sql: &str,
    name: &str,
) -> shiba_runtime::GraphTransaction {
    client
        .batch_execute(sql)
        .expect("commit grouped source DML");
    decode_committed_changes(
        &CAPTURE.capture(client, name),
        &support::singleton_graph(1, source),
    )
    .expect("decode grouped source transaction")
}

fn apply_once(client: &mut Client, input: &shiba_runtime::GraphTransaction) {
    assert_eq!(
        process(client, input).expect("apply grouped transaction"),
        ProcessOutcome::Applied
    );
    assert_sql_oracle(client);
}

fn prove_permissions(client: &mut Client) {
    client
        .batch_execute("CREATE ROLE shiba_m14_reader NOLOGIN; SET ROLE shiba_m14_reader")
        .expect("assume result-reader role");
    assert!(
        client
            .query("SELECT * FROM shiba.graph_result_rows", &[])
            .is_ok()
    );
    assert!(
        client
            .query("SELECT * FROM shiba_internal.graph_node_state", &[])
            .is_err()
    );
    assert!(
        client
            .execute(
                "UPDATE shiba.graph_result SET value_bigint = 0 WHERE graph_id = 1 AND result_id = 7",
                &[],
            )
            .is_err()
    );
    client.batch_execute("RESET ROLE").expect("restore owner");
}

#[test]
fn grouped_runtime_sql_is_set_based() {
    let keyed = concat!(
        include_str!("../src/keyed_state.rs"),
        include_str!("../src/keyed_state/write.rs")
    );
    let sink = concat!(
        include_str!("../src/result_sink.rs"),
        include_str!("../src/result_sink/keyed.rs")
    );
    for required in ["FROM unnest(", "ON CONFLICT (graph_id, node_id, namespace"] {
        assert!(
            keyed.contains(required),
            "missing set-based keyed state SQL: {required}"
        );
    }
    for required in ["key_payload = ANY($3)", "FROM unnest($3::bytea[]"] {
        assert!(
            sink.contains(required),
            "missing set-based result SQL: {required}"
        );
    }
}

#[test]
#[ignore = "requires scripts/test-m14-grouped.sh"]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered grouped transaction proof"
)]
fn grouped_count_and_sum_are_atomic_and_sql_equal() {
    let mut client = Client::connect(&CAPTURE.required("DATABASE_URL"), NoTls).expect("connect");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION shiba_m14_grouped_pub FOR TABLE source.events;
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);",
        )
        .expect("install grouped source");
    compile_and_register(&mut client, &spec()).expect("register grouped graph");
    let relation: i64 = client
        .query_one("SELECT 'source.events'::regclass::oid::bigint", &[])
        .expect("source relation OID")
        .get(0);
    let source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("source ID"),
        SlotGeneration::new(1).expect("slot generation"),
        u32::try_from(relation).expect("relation OID fits"),
    );
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    let insert = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (1,10),(2,10),(3,NULL),(4,NULL)",
        "insert.pgoutput",
    );
    apply_once(&mut client, &insert);
    let null_key = client
        .query_one(
            "SELECT result_value_bigint, result_key_is_null, result_value_is_null
             FROM shiba.graph_result_rows
             WHERE graph_id = 1 AND result_id = 7 AND result_key_is_null",
            &[],
        )
        .expect("query NULL group count");
    assert_eq!(
        (
            null_key.get::<_, Option<i64>>(0),
            null_key.get::<_, bool>(1),
            null_key.get::<_, bool>(2),
        ),
        (Some(2), true, false)
    );
    let all_null_sum = client
        .query_one(
            "SELECT result_value_bigint, result_value_is_null
             FROM shiba.graph_result_rows
             WHERE graph_id = 1 AND result_id = 9 AND result_key_bigint = 3",
            &[],
        )
        .expect("query all-NULL SUM group");
    assert_eq!(
        (
            all_null_sum.get::<_, Option<i64>>(0),
            all_null_sum.get::<_, bool>(1),
        ),
        (None, true)
    );
    let after_insert = durable_snapshot(&mut client);
    assert_eq!(
        process(&mut client, &insert).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_snapshot(&mut client), after_insert);

    let group_change = capture(
        &mut client,
        source,
        "UPDATE source.events SET payload = 20 WHERE id IN (2,3)",
        "group-change.pgoutput",
    );
    apply_once(&mut client, &group_change);
    let source_key_change = capture(
        &mut client,
        source,
        "UPDATE source.events SET id = 40 WHERE id = 4",
        "source-key-change.pgoutput",
    );
    apply_once(&mut client, &source_key_change);
    let empty_group = capture(
        &mut client,
        source,
        "DELETE FROM source.events WHERE id = 1",
        "empty-group.pgoutput",
    );
    apply_once(&mut client, &empty_group);
    let deleted_group: i64 = client
        .query_one(
            "SELECT count(*) FROM shiba.graph_result_rows
             WHERE graph_id = 1 AND result_id IN (7, 8) AND result_key_bigint = 10",
            &[],
        )
        .expect("query deleted empty group")
        .get(0);
    assert_eq!(deleted_group, 0);

    let overflow_input = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (5,20)",
        "overflow.pgoutput",
    );
    let operator2 = node_state_payload(&mut client, 4, 20);
    let mut overflow = 2_i64.to_be_bytes().to_vec();
    overflow.extend_from_slice(&2_i64.to_be_bytes());
    overflow.extend_from_slice(&i64::MAX.to_be_bytes());
    set_node_state_payload(&mut client, 4, 20, &overflow);
    let before_overflow = durable_snapshot(&mut client);
    assert!(matches!(
        process(&mut client, &overflow_input),
        Err(M2Error::Kernel(_))
    ));
    assert_eq!(durable_snapshot(&mut client), before_overflow);
    set_node_state_payload(&mut client, 4, 20, &operator2);
    apply_once(&mut client, &overflow_input);

    let corrupt_input = capture(
        &mut client,
        source,
        "UPDATE source.events SET payload = 21 WHERE id = 5",
        "corrupt-state.pgoutput",
    );
    let operator3 = node_state_payload(&mut client, 6, 5);
    set_node_state_payload(&mut client, 6, 5, &[0]);
    let before_corrupt = durable_snapshot(&mut client);
    assert!(matches!(
        process(&mut client, &corrupt_input),
        Err(M2Error::Kernel(_))
    ));
    assert_eq!(durable_snapshot(&mut client), before_corrupt);
    set_node_state_payload(&mut client, 6, 5, &operator3);
    apply_once(&mut client, &corrupt_input);
    let after_retry = durable_snapshot(&mut client);
    assert_eq!(
        process(&mut client, &corrupt_input).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_snapshot(&mut client), after_retry);

    prove_permissions(&mut client);
}
