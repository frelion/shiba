#![allow(dead_code)]

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls};
use shiba_ingress::{
    BootstrapCatchupProgress, BootstrapCatchupSession, BootstrapOptions, BootstrapSession,
    BootstrapSpec, SnapshotProgress,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};
use shiba_sql_registration::compile_sql_and_register;

#[path = "m15_sql_aggregates/support.rs"]
mod support;

const GRAPH_ID: u64 = 8;
const SOURCE_ID: u64 = 8;
const SLOT: &str = "m16_indexed_state_1";
const SCHEMA: &str = "m16_indexed";
const ROWS: i64 = 100_000;
const BATCH_ROWS: usize = 10_000;

#[test]
#[ignore = "requires scripts/test-m16-indexed-state.sh"]
fn extrema_bootstrap_and_delete_use_bounded_ordered_candidates() {
    let database_url = support::required("SHIBA_M16_INDEXED_STATE_DATABASE_URL");
    let replication_url = support::required("SHIBA_M16_INDEXED_STATE_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect indexed-state database");
    let _m15_fixtures = support::install(&mut admin);
    install_source(&mut admin);
    let publication_oid = support::publication_oid(&mut admin, "m16_indexed_state_pub");
    register_graph(&mut admin);
    let (catchup, batches, bootstrap_elapsed) =
        bootstrap_to_live(&database_url, &replication_url, publication_oid);
    assert_eq!(scalar_extrema(&mut admin), (Some(1), Some(ROWS)));

    let explain_text = explain_index(&mut admin);

    let mut live = catchup.into_live().expect("enter indexed live receiver");
    let live_started = Instant::now();
    let token = apply_minimum_delete(&mut admin, &mut live);
    assert_eq!(scalar_extrema(&mut admin), (Some(2), Some(ROWS)));
    live.acknowledge(&token).expect("ACK indexed deletion");
    support::wait_for_slot_lsn(&mut admin, SLOT, token.end_lsn());
    live.detach().expect("detach indexed live receiver");

    let state_rows: i64 = admin
        .query_one(
            "SELECT count(*) FROM shiba_internal.graph_node_state WHERE graph_id=$1",
            &[&i64::try_from(GRAPH_ID).expect("graph ID fits")],
        )
        .expect("count indexed state rows")
        .get(0);
    assert!(
        state_rows >= ROWS,
        "extrema multiplicity state was truncated"
    );
    println!(
        "m16.8 indexed_state_metrics batches={batches} bootstrap_ms={} live_apply_ms={} state_rows={state_rows}\n{explain_text}",
        bootstrap_elapsed.as_millis(),
        live_started.elapsed().as_millis(),
    );
}

fn install_source(client: &mut Client) {
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {SCHEMA};
             CREATE TABLE {SCHEMA}.rows (id bigint PRIMARY KEY, payload bigint NULL);
             INSERT INTO {SCHEMA}.rows
             SELECT id, id FROM generate_series(1, {ROWS}) AS input(id);
             CREATE PUBLICATION m16_indexed_state_pub FOR TABLE {SCHEMA}.rows
                 WITH (publish='insert,update,delete');
             SELECT shiba_internal.register_source({SOURCE_ID}, '{SCHEMA}.rows'::regclass);"
        ))
        .expect("install high-cardinality extrema source");
}

fn register_graph(client: &mut Client) {
    compile_sql_and_register(
        client,
        GraphId::new(GRAPH_ID).expect("graph ID"),
        "SELECT min(payload) AS minimum, max(payload) AS maximum FROM m16_indexed.rows",
    )
    .expect("register indexed MIN/MAX graph");
}

fn bootstrap_to_live(
    database_url: &str,
    replication_url: &str,
    publication_oid: u32,
) -> (BootstrapCatchupSession, u64, Duration) {
    let options = BootstrapOptions::new(BATCH_ROWS, Duration::from_secs(10))
        .expect("bounded indexed bootstrap options");
    let started = Instant::now();
    let mut bootstrap = BootstrapSession::begin(
        database_url,
        replication_url,
        BootstrapSpec {
            graph_id: GraphId::new(GRAPH_ID).expect("graph ID"),
            bootstrap_id: BootstrapId::new(GRAPH_ID).expect("bootstrap ID"),
            publication_oid,
            slot_name: SLOT.to_owned(),
            slot_generation: SlotGeneration::new(1).expect("slot generation"),
        },
        options,
    )
    .expect("export indexed snapshot");
    let mut batches = 0_u64;
    while let SnapshotProgress::BatchApplied { rows, .. } =
        bootstrap.scan_next().expect("scan indexed snapshot batch")
    {
        batches += 1;
        assert!((1..=BATCH_ROWS).contains(&rows));
    }
    let mut catchup = bootstrap.into_catchup().expect("enter indexed catch-up");
    while !matches!(
        catchup.catch_up_next().expect("advance indexed catch-up"),
        BootstrapCatchupProgress::Active
    ) {}
    (catchup, batches, started.elapsed())
}

fn explain_index(client: &mut Client) -> String {
    let (node_id, namespace, partition) = client
        .query_one(
            "SELECT node_id, namespace, partition_key_payload
             FROM shiba_internal.graph_node_state
             WHERE graph_id=$1 AND item_order_key IS NOT NULL
             ORDER BY namespace LIMIT 1",
            &[&i64::try_from(GRAPH_ID).expect("graph ID fits")],
        )
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, i32>(1),
                row.get::<_, Vec<u8>>(2),
            )
        })
        .expect("read durable extrema partition");
    let plan = client
        .query(
            &format!(
                "EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF)
                 SELECT item_key_payload
                 FROM shiba_internal.graph_node_state
                 WHERE graph_id={GRAPH_ID} AND node_id={node_id}
                   AND namespace={namespace}
                   AND partition_key_payload=decode('{}','hex')
                   AND item_order_key IS NOT NULL
                 ORDER BY item_order_key ASC LIMIT 2",
                hex(&partition)
            ),
            &[],
        )
        .expect("explain indexed candidate read")
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("graph_node_state_ordered_item"),
        "candidate query did not use the ordered index:\n{plan}"
    );
    assert!(
        plan.contains("rows=2") || plan.contains("actual rows=2"),
        "candidate query was not bounded to two rows:\n{plan}"
    );
    plan
}

fn apply_minimum_delete(
    client: &mut Client,
    live: &mut shiba_ingress::GovernedGraphSession,
) -> shiba_ingress::DurableTransaction {
    client
        .batch_execute(&format!("DELETE FROM {SCHEMA}.rows WHERE id=1"))
        .expect("delete current minimum");
    live.receive_and_apply_one()
        .expect("apply indexed minimum deletion")
}

fn scalar_extrema(client: &mut Client) -> (Option<i64>, Option<i64>) {
    client
        .query_one(
            "SELECT
                CASE WHEN convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,value}')::bigint END,
                CASE WHEN convert_from(row_payload,'UTF8')::jsonb #>> '{values,1,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb #>> '{values,1,value}')::bigint END
             FROM shiba.graph_result_rows WHERE graph_id=$1",
            &[&i64::try_from(GRAPH_ID).expect("graph ID fits")],
        )
        .map(|row| (row.get(0), row.get(1)))
        .expect("query indexed extrema result")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut output, byte| {
        write!(&mut output, "{byte:02x}").expect("write hex");
        output
    })
}
