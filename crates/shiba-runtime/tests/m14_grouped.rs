use std::num::NonZeroU64;

use postgres::{Client, NoTls};
use shiba_compiler::{OPERATOR_SPEC_VERSION, OperatorOperationV1, OperatorSpecV1};
use shiba_operator::OperatorId;
use shiba_protocol::{SlotGeneration, SourceId};
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

fn spec(operator_id: u64, operation: OperatorOperationV1) -> OperatorSpecV1 {
    OperatorSpecV1 {
        version: OPERATOR_SPEC_VERSION,
        operator_id: OperatorId::new(NonZeroU64::new(operator_id).expect("operator ID")),
        source_id: SourceId::new(1).expect("source ID"),
        operation,
    }
}

fn capture(
    client: &mut Client,
    source: PgoutputSource,
    sql: &str,
    name: &str,
) -> shiba_runtime::SourceTransaction {
    client
        .batch_execute(sql)
        .expect("commit grouped source DML");
    decode_committed_changes(&CAPTURE.capture(client, name), source)
        .expect("decode grouped source transaction")
}

fn apply_once(client: &mut Client, input: &shiba_runtime::SourceTransaction) {
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
            .query("SELECT * FROM shiba.operator_result_rows", &[])
            .is_ok()
    );
    assert!(
        client
            .query("SELECT * FROM shiba_internal.operator_node_state", &[])
            .is_err()
    );
    assert!(
        client
            .execute(
                "UPDATE shiba.operator_result SET value_bigint = 0 WHERE operator_id = 1",
                &[],
            )
            .is_err()
    );
    client.batch_execute("RESET ROLE").expect("restore owner");
}

#[test]
fn grouped_runtime_sql_is_set_based() {
    let keyed = include_str!("../src/keyed_state.rs");
    let sink = include_str!("../src/result_sink.rs");
    for required in [
        "FROM unnest(",
        "ON CONFLICT (operator_id, node_id, namespace",
    ] {
        assert!(
            keyed.contains(required),
            "missing set-based keyed state SQL: {required}"
        );
    }
    for required in ["key_payload = ANY($2)", "FROM unnest($2::bytea[]"] {
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
    for operator in [
        spec(
            1,
            OperatorOperationV1::GroupedCount {
                key_column: "payload".into(),
            },
        ),
        spec(
            2,
            OperatorOperationV1::GroupedSumInt8 {
                key_column: "payload".into(),
                input_column: "id".into(),
            },
        ),
        spec(
            3,
            OperatorOperationV1::GroupedSumInt8 {
                key_column: "id".into(),
                input_column: "payload".into(),
            },
        ),
    ] {
        compile_and_register(&mut client, &operator).expect("register grouped plan");
    }
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
             FROM shiba.operator_result_rows
             WHERE operator_id = 1 AND result_key_is_null",
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
             FROM shiba.operator_result_rows
             WHERE operator_id = 3 AND result_key_bigint = 3",
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
            "SELECT count(*) FROM shiba.operator_result_rows
             WHERE operator_id IN (1, 2) AND result_key_bigint = 10",
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
    let operator2 = node_state_payload(&mut client, 2, 20);
    let mut overflow = 2_i64.to_be_bytes().to_vec();
    overflow.extend_from_slice(&2_i64.to_be_bytes());
    overflow.extend_from_slice(&i64::MAX.to_be_bytes());
    set_node_state_payload(&mut client, 2, 20, &overflow);
    let before_overflow = durable_snapshot(&mut client);
    assert!(matches!(
        process(&mut client, &overflow_input),
        Err(M2Error::Kernel(_))
    ));
    assert_eq!(durable_snapshot(&mut client), before_overflow);
    set_node_state_payload(&mut client, 2, 20, &operator2);
    apply_once(&mut client, &overflow_input);

    let corrupt_input = capture(
        &mut client,
        source,
        "UPDATE source.events SET payload = 21 WHERE id = 5",
        "corrupt-state.pgoutput",
    );
    let operator3 = node_state_payload(&mut client, 3, 5);
    set_node_state_payload(&mut client, 3, 5, &[0]);
    let before_corrupt = durable_snapshot(&mut client);
    assert!(matches!(
        process(&mut client, &corrupt_input),
        Err(M2Error::Kernel(_))
    ));
    assert_eq!(durable_snapshot(&mut client), before_corrupt);
    set_node_state_payload(&mut client, 3, 5, &operator3);
    apply_once(&mut client, &corrupt_input);
    let after_retry = durable_snapshot(&mut client);
    assert_eq!(
        process(&mut client, &corrupt_input).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_snapshot(&mut client), after_retry);

    prove_permissions(&mut client);
}
