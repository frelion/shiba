use std::time::{Duration, Instant};

use postgres::Client;
use shiba_ingress::SnapshotProgress;

#[path = "../m12_rebuild_admission/support.rs"]
#[allow(dead_code, unused_imports)]
mod admission;

pub(crate) use admission::{
    OLD_SLOT, RebuildFixture, TARGET_SLOT, establish_active_source, options as rebuild_options,
};

pub(crate) fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("scripts/test-m12-rebuild-snapshot-live.sh must set {name}"))
}

pub(crate) fn assert_building(client: &mut Client) {
    let rows = client
        .query(
            "SELECT result_status, value_bigint
             FROM shiba.graph_result WHERE graph_id = 1 ORDER BY result_id",
            &[],
        )
        .expect("query public result visibility");
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| {
        row.get::<_, &str>(0) == "building" && row.get::<_, Option<i64>>(1).is_none()
    }));
}

pub(crate) fn assert_active(client: &mut Client, count: i64, sum: i64) {
    let rows = client
        .query(
            "SELECT result_id, result_status, value_bigint
             FROM shiba.graph_result WHERE graph_id = 1 ORDER BY result_id",
            &[],
        )
        .expect("query active public results");
    assert_eq!(
        rows.into_iter()
            .map(|row| (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, Option<i64>>(2),
            ))
            .collect::<Vec<_>>(),
        vec![
            (4, "active".to_owned(), Some(count)),
            (5, "active".to_owned(), Some(sum)),
            (6, "active".to_owned(), None),
        ]
    );
}

pub(crate) fn assert_oracle(client: &mut Client, count: i64, sum: i64) {
    let oracle = client
        .query_one(
            "SELECT count(*), COALESCE(sum(payload), 0)::bigint FROM target.events",
            &[],
        )
        .expect("query target SQL differential oracle");
    assert_eq!(
        (oracle.get::<_, i64>(0), oracle.get::<_, i64>(1)),
        (count, sum)
    );
    let expected = client
        .query("SELECT id, payload FROM target.events ORDER BY id", &[])
        .expect("query target keyed SQL oracle")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
        .collect::<Vec<_>>();
    let actual = client
        .query(
            "SELECT result_key_bigint, result_value_bigint
             FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 6 ORDER BY 1",
            &[],
        )
        .expect("query rebuilt ProjectRows")
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

pub(crate) fn catalog_identity(client: &mut Client) -> Vec<Vec<String>> {
    [
        "SELECT row_to_json(x)::text FROM (
             SELECT source_id, binding_kind, address_classid, address_objid,
                    address_objsubid
             FROM shiba_internal.source_binding ORDER BY binding_kind, address_objsubid
         ) x",
        "SELECT row_to_json(x)::text FROM (
             SELECT * FROM shiba_internal.graph_ingress_config ORDER BY graph_id
         ) x",
        "SELECT row_to_json(x)::text FROM (
             SELECT graph_id, source_count, compiler_version, graph_format_version,
                    encode(graph_digest, 'hex') AS graph_digest, state_codec_version
             FROM shiba_internal.graph_definition ORDER BY graph_id
         ) x",
        "SELECT row_to_json(x)::text FROM (
             SELECT graph_id, bootstrap_id, slot_name, slot_generation
             FROM shiba_internal.graph_bootstrap ORDER BY graph_id
         ) x",
    ]
    .into_iter()
    .map(|query| {
        client
            .query(query, &[])
            .expect("snapshot sole target identity")
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    })
    .collect()
}

pub(crate) fn rebuild_state_snapshot(client: &mut Client) -> Vec<Vec<String>> {
    [
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_binding ORDER BY binding_kind, address_objsubid) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_ingress_config ORDER BY graph_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_bootstrap ORDER BY graph_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_row_state ORDER BY source_row_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_definition ORDER BY graph_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_node_state ORDER BY graph_id, node_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba.graph_result ORDER BY graph_id, result_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.graph_continuation ORDER BY slot_generation, commit_lsn) x",
        "SELECT row_to_json(x)::text FROM (
             SELECT slot_name, slot_type, plugin, database, temporary, active,
                    two_phase, failover, synced, restart_lsn::text,
                    confirmed_flush_lsn::text
             FROM pg_catalog.pg_replication_slots
             WHERE slot_name IN ('shiba_m12_contract_old', 'shiba_m12_admission_new')
             ORDER BY slot_name
         ) x",
    ]
    .into_iter()
    .map(|query| {
        client
            .query(query, &[])
            .expect("snapshot complete rebuild state")
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    })
    .collect()
}

pub(crate) fn source_rows(client: &mut Client) -> Vec<(i64, Option<i64>)> {
    client
        .query(
            "SELECT source_row_id, payload_int8
             FROM shiba_internal.source_row_state ORDER BY source_row_id",
            &[],
        )
        .expect("query current target rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

pub(crate) fn continuation_generations(client: &mut Client) -> Vec<i64> {
    client
        .query(
            "SELECT slot_generation FROM shiba_internal.graph_continuation
             WHERE graph_id = 1 ORDER BY commit_lsn",
            &[],
        )
        .expect("query WAL-only continuation")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

pub(crate) fn assert_slots(client: &mut Client, old_exists: bool, new_exists: bool) {
    for (name, expected) in [(OLD_SLOT, old_exists), (TARGET_SLOT, new_exists)] {
        let actual: bool = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1
                 )",
                &[&name],
            )
            .expect("query exact slot lifecycle")
            .get(0);
        assert_eq!(
            actual, expected,
            "unexpected physical state for slot {name}"
        );
    }
}

pub(crate) fn scan_all(
    bootstrap: &mut shiba_ingress::BootstrapSession,
    client: &mut Client,
) -> usize {
    let mut rows = 0;
    loop {
        match bootstrap.scan_next().expect("scan bounded target snapshot") {
            SnapshotProgress::BatchApplied { rows: batch, .. } => {
                assert!(batch <= rebuild_options().batch_rows());
                rows += batch;
                assert_building(client);
            }
            SnapshotProgress::ScanComplete => return rows,
        }
    }
}

pub(crate) fn wait_for_slot_lsn(client: &mut Client, slot: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let value: Option<String> = client
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots
                 WHERE slot_name = $1",
                &[&slot],
            )
            .expect("query slot feedback")
            .get(0);
        if value
            .as_deref()
            .map(parse_lsn)
            .is_some_and(|lsn| lsn >= expected)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "slot feedback did not reach {expected:#x}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn parse_lsn(value: &str) -> u64 {
    let (high, low) = value.split_once('/').expect("PostgreSQL LSN shape");
    (u64::from_str_radix(high, 16).expect("LSN high") << 32)
        | u64::from_str_radix(low, 16).expect("LSN low")
}
