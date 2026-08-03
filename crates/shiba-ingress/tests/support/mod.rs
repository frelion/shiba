use std::{
    thread,
    time::{Duration, Instant},
};

use postgres::Client;
use shiba_compiler::{GRAPH_SPEC_VERSION, GraphOutputSpecV1, GraphSpecV1};
use shiba_operator::NodeId;
use shiba_protocol::{GraphId, SourceId};

#[allow(dead_code)]
pub const TEST_GRAPH_ID: u64 = 1;

#[allow(dead_code)]
pub fn node_id(value: u32) -> NodeId {
    NodeId::new(std::num::NonZeroU32::new(value).expect("node ID"))
}

#[allow(dead_code)]
pub fn count_sum_spec(source_id: u64) -> GraphSpecV1 {
    let source_id = SourceId::new(source_id).expect("source ID");
    GraphSpecV1 {
        version: GRAPH_SPEC_VERSION,
        graph_id: GraphId::new(TEST_GRAPH_ID).expect("graph ID"),
        sources: vec![source_id],
        outputs: vec![
            GraphOutputSpecV1::CountRows {
                source_id,
                aggregate_node_id: node_id(1),
                result_node_id: node_id(2),
            },
            GraphOutputSpecV1::SumInt8 {
                source_id,
                input_column: "payload".to_owned(),
                aggregate_node_id: node_id(3),
                result_node_id: node_id(4),
            },
        ],
    }
}

#[allow(dead_code)]
pub fn count_sum_project_spec(source_id: u64) -> GraphSpecV1 {
    let mut spec = count_sum_spec(source_id);
    let source_id = SourceId::new(source_id).expect("source ID");
    spec.outputs.push(GraphOutputSpecV1::MaterializedProject {
        source_id,
        key_column: "id".to_owned(),
        value_column: "payload".to_owned(),
        project_node_id: node_id(5),
        result_node_id: node_id(6),
    });
    spec
}

#[allow(dead_code)]
pub fn count_spec(source_id: u64) -> GraphSpecV1 {
    let source_id = SourceId::new(source_id).expect("source ID");
    GraphSpecV1 {
        version: GRAPH_SPEC_VERSION,
        graph_id: GraphId::new(TEST_GRAPH_ID).expect("graph ID"),
        sources: vec![source_id],
        outputs: vec![GraphOutputSpecV1::CountRows {
            source_id,
            aggregate_node_id: node_id(1),
            result_node_id: node_id(2),
        }],
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
