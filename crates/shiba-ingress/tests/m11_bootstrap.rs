use std::{
    thread,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use shiba_ingress::{
    BootstrapCatchupProgress, BootstrapOptions, BootstrapSession, BootstrapSpec, SnapshotProgress,
};
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};
use shiba_runtime::{ProcessOutcome, compile_and_register};

#[allow(dead_code)]
mod support;

use support::{count_sum_project_spec, slot_lsn, wait_for_slot_lsn};

const SLOT: &str = "shiba_m11_bootstrap_slot";
const PUBLICATION: &str = "shiba_m11_bootstrap_pub";

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("scripts/test-m11-bootstrap.sh must set {name}"))
}

fn public_results(client: &mut Client) -> Vec<(i64, String, Option<i64>)> {
    client
        .query(
            "SELECT result_id, result_status, value_bigint
             FROM shiba.graph_result WHERE graph_id = 1 ORDER BY result_id",
            &[],
        )
        .expect("query public results")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

fn private_state(client: &mut Client) -> Vec<(i64, i64)> {
    let scalar_partition = support::scalar_state_partition();
    client
        .query(
            "SELECT node_id, state_payload
             FROM shiba_internal.graph_node_state
             WHERE graph_id = 1 AND node_id IN (1, 2)
               AND partition_key_payload = $1 AND item_key_payload = $2
             ORDER BY node_id",
            &[&scalar_partition, &support::scalar_state_item()],
        )
        .expect("query private operator state")
        .into_iter()
        .map(|row| {
            let payload: Vec<u8> = row.get(1);
            (
                row.get(0),
                i64::from_be_bytes(payload.try_into().expect("int8 node state")),
            )
        })
        .collect()
}

fn projected_rows(client: &mut Client) -> Vec<(i64, Option<i64>)> {
    client
        .query(
            "SELECT result_key_bigint, result_value_bigint
             FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 6 ORDER BY 1",
            &[],
        )
        .expect("query active projected rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn source_rows(client: &mut Client) -> Vec<(i64, Option<i64>)> {
    client
        .query(
            "SELECT source_row_id, payload_int8
             FROM shiba_internal.source_row_state ORDER BY source_row_id",
            &[],
        )
        .expect("query current source rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn assert_building(client: &mut Client) {
    assert_eq!(
        public_results(client),
        vec![
            (4, "building".to_owned(), None),
            (5, "building".to_owned(), None),
            (6, "building".to_owned(), None),
        ]
    );
}

fn wait_for_slot_at_least(client: &mut Client, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let actual = slot_lsn(client, SLOT);
        if actual >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "slot position {actual:#x} did not cover bootstrap fence {expected:#x}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "requires scripts/test-m11-bootstrap.sh"]
#[allow(clippy::too_many_lines, reason = "one ordered snapshot-to-live proof")]
fn bootstrap_existing_rows_concurrent_wal_and_live_handoff() {
    let database_url = required("SHIBA_M11_BOOTSTRAP_DATABASE_URL");
    let replication_url = required("SHIBA_M11_BOOTSTRAP_REPLICATION_URL");
    let mut admin = Client::connect(&database_url, NoTls).expect("connect admin database");
    admin
        .batch_execute(&format!(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION {PUBLICATION} FOR TABLE source.events
                 WITH (publish = 'insert, update, delete');
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);
             INSERT INTO source.events VALUES (1, 10), (2, NULL), (3, 30);"
        ))
        .expect("install source with existing rows");
    compile_and_register(&mut admin, &count_sum_project_spec(1)).expect("register graph");
    assert_eq!(
        public_results(&mut admin),
        vec![
            (4, "active".to_owned(), Some(0)),
            (5, "active".to_owned(), Some(0)),
            (6, "active".to_owned(), None),
        ]
    );
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&SLOT],
            )
            .expect("verify absent bootstrap slot")
            .get::<_, i64>(0),
        0
    );

    let publication_oid: u32 = admin
        .query_one(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .expect("read publication OID")
        .get(0);
    let spec = BootstrapSpec {
        graph_id: GraphId::new(1).expect("graph ID"),
        bootstrap_id: BootstrapId::new(1).expect("bootstrap ID"),
        publication_oid,
        slot_name: SLOT.to_owned(),
        slot_generation: SlotGeneration::new(1).expect("slot generation"),
    };
    let options =
        BootstrapOptions::new(2, Duration::from_secs(5)).expect("bounded bootstrap options");
    let mut bootstrap = BootstrapSession::begin(&database_url, &replication_url, spec, options)
        .expect("begin exported-snapshot bootstrap");
    assert_building(&mut admin);
    assert!(projected_rows(&mut admin).is_empty());
    assert!(private_state(&mut admin).is_empty());
    assert_eq!(source_rows(&mut admin), Vec::<(i64, Option<i64>)>::new());

    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO source.events VALUES (4, 5);
             UPDATE source.events SET payload = 20 WHERE id = 1;
             DELETE FROM source.events WHERE id = 3;
             COMMIT;",
        )
        .expect("commit one concurrent source transaction");

    assert_eq!(
        bootstrap.scan_next().expect("apply first snapshot batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 1,
            rows: 2
        }
    );
    assert_building(&mut admin);
    assert!(projected_rows(&mut admin).is_empty());
    assert_eq!(private_state(&mut admin), vec![(1, 2), (2, 10)]);
    assert_eq!(source_rows(&mut admin), vec![(1, Some(10)), (2, None)]);
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation",
                &[]
            )
            .expect("snapshot must not create continuation")
            .get::<_, i64>(0),
        0
    );

    assert_eq!(
        bootstrap.scan_next().expect("apply second snapshot batch"),
        SnapshotProgress::BatchApplied {
            ordinal: 2,
            rows: 1
        }
    );
    assert_building(&mut admin);
    assert!(projected_rows(&mut admin).is_empty());
    assert_eq!(private_state(&mut admin), vec![(1, 3), (2, 40)]);
    assert_eq!(
        source_rows(&mut admin),
        vec![(1, Some(10)), (2, None), (3, Some(30))]
    );
    assert_eq!(
        bootstrap.scan_next().expect("complete snapshot scan"),
        SnapshotProgress::ScanComplete
    );
    assert_building(&mut admin);
    assert!(projected_rows(&mut admin).is_empty());

    let mut catchup = bootstrap.into_catchup().expect("enter M10 catch-up");
    assert_eq!(
        catchup.catch_up_next().expect("apply concurrent WAL"),
        BootstrapCatchupProgress::TransactionApplied
    );
    assert_building(&mut admin);
    assert_eq!(private_state(&mut admin), vec![(1, 3), (2, 25)]);
    assert_eq!(
        source_rows(&mut admin),
        vec![(1, Some(20)), (2, None), (4, Some(5))]
    );
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation",
                &[]
            )
            .expect("query real WAL continuation")
            .get::<_, i64>(0),
        1
    );

    assert_eq!(
        catchup.catch_up_next().expect("activate at terminal fence"),
        BootstrapCatchupProgress::Active
    );
    assert_eq!(
        public_results(&mut admin),
        vec![
            (4, "active".to_owned(), Some(3)),
            (5, "active".to_owned(), Some(25)),
            (6, "active".to_owned(), None),
        ]
    );
    assert_eq!(projected_rows(&mut admin), source_rows(&mut admin));
    let oracle = admin
        .query_one(
            "SELECT count(*), COALESCE(sum(payload), 0)::bigint FROM source.events",
            &[],
        )
        .expect("query SQL differential oracle");
    assert_eq!((oracle.get::<_, i64>(0), oracle.get::<_, i64>(1)), (3, 25));
    let fence_lsn: String = admin
        .query_one(
            "SELECT catchup_fence_lsn::text FROM shiba_internal.graph_bootstrap
             WHERE graph_id = 1",
            &[],
        )
        .expect("read immutable catch-up fence")
        .get(0);
    let (high, low) = fence_lsn.split_once('/').expect("fence LSN shape");
    let fence = (u64::from_str_radix(high, 16).expect("fence high") << 32)
        | u64::from_str_radix(low, 16).expect("fence low");
    wait_for_slot_at_least(&mut admin, fence);

    let mut live = catchup
        .into_live()
        .expect("convert to ordinary M10 session");
    admin
        .batch_execute("INSERT INTO source.events VALUES (5, 7)")
        .expect("commit later live source transaction");
    let applied = live
        .receive_and_apply_one()
        .expect("apply later transaction through M10 live ingress");
    assert_eq!(applied.outcome(), ProcessOutcome::Applied);
    live.acknowledge(&applied)
        .expect("ack later durable transaction");
    wait_for_slot_lsn(&mut admin, SLOT, applied.end_lsn());
    assert_eq!(
        public_results(&mut admin),
        vec![
            (4, "active".to_owned(), Some(4)),
            (5, "active".to_owned(), Some(32)),
            (6, "active".to_owned(), None),
        ]
    );
    assert_eq!(projected_rows(&mut admin), source_rows(&mut admin));
    assert_eq!(private_state(&mut admin), vec![(1, 4), (2, 32)]);
    assert_eq!(
        source_rows(&mut admin),
        vec![(1, Some(20)), (2, None), (4, Some(5)), (5, Some(7))]
    );
    assert_eq!(
        admin
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation",
                &[]
            )
            .expect("query WAL-only continuations")
            .get::<_, i64>(0),
        2
    );
    let oracle = admin
        .query_one(
            "SELECT count(*), COALESCE(sum(payload), 0)::bigint FROM source.events",
            &[],
        )
        .expect("query final SQL oracle");
    assert_eq!((oracle.get::<_, i64>(0), oracle.get::<_, i64>(1)), (4, 32));
    live.detach().expect("detach live session");
}
