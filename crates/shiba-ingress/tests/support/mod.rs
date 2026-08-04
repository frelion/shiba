use std::{
    thread,
    time::{Duration, Instant},
};

use postgres::Client;
use shiba_compiler::{
    QUERY_SPEC_VERSION, QueryAggregateCallV1, QueryExpressionV1, QueryFieldV1, QueryInputV1,
    QueryNodeV1, QueryOperationV1, QueryResultFieldV1, QueryResultV1, QuerySelectorV1, QuerySpecV1,
};
use shiba_operator::{AggregateFunctionV1, TypedValue};
use shiba_protocol::{GraphId, SourceId};

#[allow(dead_code)]
pub const TEST_GRAPH_ID: u64 = 1;

#[allow(dead_code)]
pub fn scalar_state_partition() -> Vec<u8> {
    TypedValue::Bool(true)
        .to_canonical_json()
        .expect("canonical scalar state partition")
}

#[allow(dead_code)]
pub const fn scalar_state_item() -> &'static [u8] {
    b"null"
}

#[allow(dead_code)]
pub fn count_sum_spec(source_id: u64) -> QuerySpecV1 {
    let source_id = SourceId::new(source_id).expect("source ID");
    QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(TEST_GRAPH_ID).expect("graph ID"),
        sources: vec![source_id],
        nodes: vec![
            query_node(source_id, count_rows(), true),
            query_node(source_id, sum_int8(column(0, "payload")), true),
        ],
        results: vec![
            scalar_result(1, "count", false),
            scalar_result(2, "sum", true),
        ],
    }
}

#[allow(dead_code)]
pub fn count_sum_project_spec(source_id: u64) -> QuerySpecV1 {
    let mut spec = count_sum_spec(source_id);
    let source_id = SourceId::new(source_id).expect("source ID");
    spec.nodes.push(QueryNodeV1 {
        inputs: vec![QueryInputV1::Source { source_id }],
        state_codec_version: None,
        operation: QueryOperationV1::Project {
            expressions: vec![column(0, "id"), column(0, "payload")],
        },
    });
    spec.results.push(QueryResultV1 {
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
    });
    spec
}

#[allow(dead_code)]
pub fn count_spec(source_id: u64) -> QuerySpecV1 {
    let source_id = SourceId::new(source_id).expect("source ID");
    QuerySpecV1 {
        version: QUERY_SPEC_VERSION,
        graph_id: GraphId::new(TEST_GRAPH_ID).expect("graph ID"),
        sources: vec![source_id],
        nodes: vec![query_node(source_id, count_rows(), true)],
        results: vec![scalar_result(1, "count", false)],
    }
}

pub fn count_rows() -> QueryOperationV1 {
    aggregate(AggregateFunctionV1::CountStar, None)
}

pub fn sum_int8(expression: QueryExpressionV1) -> QueryOperationV1 {
    aggregate(AggregateFunctionV1::SumInt8, Some(expression))
}

fn aggregate(
    function: AggregateFunctionV1,
    expression: Option<QueryExpressionV1>,
) -> QueryOperationV1 {
    QueryOperationV1::Aggregate {
        group_expressions: Vec::new(),
        calls: vec![QueryAggregateCallV1 {
            ordinal: 1,
            function,
            function_version: 1,
            expression,
        }],
        having: None,
    }
}

fn query_node(source_id: SourceId, operation: QueryOperationV1, stateful: bool) -> QueryNodeV1 {
    QueryNodeV1 {
        inputs: vec![QueryInputV1::Source { source_id }],
        state_codec_version: stateful.then_some(1),
        operation,
    }
}

fn column(input: u8, name: &str) -> QueryExpressionV1 {
    QueryExpressionV1::Column {
        field: QueryFieldV1 {
            input,
            selector: QuerySelectorV1::Name {
                name: name.into(),
                quoted: false,
            },
        },
    }
}

fn scalar_result(input_node: u16, name: &str, nullable: bool) -> QueryResultV1 {
    QueryResultV1 {
        input_node,
        fields: vec![QueryResultFieldV1 {
            name: name.into(),
            value_slot: 0,
            nullable,
        }],
        key_ordinals: vec![],
    }
}

pub fn slot_lsn(client: &mut Client, slot: &str) -> u64 {
    let value: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .expect("read slot position")
        .get(0);
    parse_lsn(&value)
}

pub fn wait_for_slot_lsn(client: &mut Client, slot: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let actual = slot_lsn(client, slot);
        if actual == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "slot position {actual:#x} did not reach exact durable LSN {expected:#x}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

pub fn wait_for_keepalive_reply(client: &mut Client, application: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let observed = client
            .query_opt(
                "SELECT write_lsn::text, flush_lsn::text, replay_lsn::text,
                        reply_time IS NOT NULL
                 FROM pg_stat_replication WHERE application_name = $1",
                &[&application],
            )
            .expect("query replication feedback")
            .and_then(|row| {
                let replied: bool = row.get(3);
                let write = row.get::<_, Option<String>>(0)?;
                let flush = row.get::<_, Option<String>>(1)?;
                let replay = row.get::<_, Option<String>>(2)?;
                replied.then(|| (parse_lsn(&write), parse_lsn(&flush), parse_lsn(&replay)))
            });
        if observed == Some((expected, expected, expected)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "requested keepalive did not report only durable LSN {expected:#x}; observed {observed:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn parse_lsn(value: &str) -> u64 {
    let (high, low) = value.split_once('/').expect("PostgreSQL LSN has slash");
    (u64::from_str_radix(high, 16).expect("valid high LSN") << 32)
        | u64::from_str_radix(low, 16).expect("valid low LSN")
}
