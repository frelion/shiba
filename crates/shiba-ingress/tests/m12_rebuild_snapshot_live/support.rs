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
             FROM shiba.operator_result ORDER BY operator_id",
            &[],
        )
        .expect("query public result visibility");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| {
        row.get::<_, &str>(0) == "building" && row.get::<_, Option<i64>>(1).is_none()
    }));
}

pub(crate) fn assert_active(client: &mut Client, count: i64, sum: i64) {
    let rows = client
        .query(
            "SELECT operator_id, result_status, value_bigint
             FROM shiba.operator_result ORDER BY operator_id",
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
            (1, "active".to_owned(), Some(count)),
            (2, "active".to_owned(), Some(sum)),
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
}

pub(crate) fn catalog_identity(client: &mut Client) -> Vec<Vec<String>> {
    [
        "SELECT row_to_json(x)::text FROM (
             SELECT source_id, binding_kind, address_classid, address_objid,
                    address_objsubid
             FROM shiba_internal.source_binding ORDER BY binding_kind, address_objsubid
         ) x",
        "SELECT row_to_json(x)::text FROM (
             SELECT * FROM shiba_internal.source_ingress_config ORDER BY source_id
         ) x",
        "SELECT row_to_json(x)::text FROM (
             SELECT operator_id, source_id, compiler_version, operator_kind,
                    input_classid, input_objid, input_objsubid
             FROM shiba_internal.operator_definition ORDER BY operator_id
         ) x",
        "SELECT row_to_json(x)::text FROM (
             SELECT source_id, bootstrap_id, slot_name, slot_generation
             FROM shiba_internal.source_bootstrap ORDER BY source_id
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
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_ingress_config ORDER BY source_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_bootstrap ORDER BY source_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_row_state ORDER BY source_row_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.operator_definition ORDER BY operator_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.operator_state ORDER BY operator_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba.operator_result ORDER BY operator_id) x",
        "SELECT row_to_json(x)::text FROM (SELECT * FROM shiba_internal.source_continuation ORDER BY slot_generation, commit_lsn) x",
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
            "SELECT slot_generation FROM shiba_internal.source_continuation
             WHERE source_id = 1 ORDER BY commit_lsn",
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
